"""Stateless anomaly classifiers.

Every function here is a pure function of ONE canonical event. No I/O, no
shared mutable state, no dependence on any other event. That property is what
makes this file liftable into a stream processor as a map operator without
modification, and it is what makes the tests trivial to write.

Anomaly classes handled here, all evidenced by fixtures/event_sample.jsonl:

    null_tenant               evt-0011
    received_before_sent      evt-0005, evt-0006, evt-0016
    future_timestamp          evt-0016
    missing_received_at       evt-0009
    schema_drift              evt-0009
    pii_in_properties         evt-0007
    privacy_request           evt-0017
    client_asserted_aggregate evt-0019
    suspicious_referrer       evt-0012, evt-0013, evt-0014, evt-0015

Classes that CANNOT live here, because answering them requires memory ofwha
other events, live in state.py: duplicate delivery, visitor burst detection,
and anonymous-to-user identity stitching.

A note on what this file does NOT do: nothing here deletes, drops, or
"corrects" an event. Every classifier only labels. The decision about what to
do with a label is a policy decision that belongs to the pipeline operator,
not to a detection rule - and several of these signals are too weak to act on
alone. See the SUSPICIOUS_REFERRER docstring for the sharpest example.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable


# --------------------------------------------------------------------------
# Anomaly classes and severities
# --------------------------------------------------------------------------

class AnomalyClass:
    """Every anomaly class in the system, in one place.

    Kept together even though two modules produce them, so the report has a
    single vocabulary and nothing can drift apart.
    """

    # Produced here in rules.py - answerable from one event alone.
    NULL_TENANT = "null_tenant"
    RECEIVED_BEFORE_SENT = "received_before_sent"
    FUTURE_TIMESTAMP = "future_timestamp"
    MISSING_RECEIVED_AT = "missing_received_at"
    INVALID_TIMESTAMP = "invalid_timestamp"
    SCHEMA_DRIFT = "schema_drift"
    PII_IN_PROPERTIES = "pii_in_properties"
    PRIVACY_REQUEST = "privacy_request"
    CLIENT_ASSERTED_AGGREGATE = "client_asserted_aggregate"
    SUSPICIOUS_REFERRER = "suspicious_referrer"

    # Produced in state.py - require memory of other events.
    DUPLICATE_EVENT = "duplicate_event"
    VISITOR_BURST = "visitor_burst"
    IDENTITY_STITCH = "identity_stitch"
    DELETION_SCOPE = "deletion_scope"


class Severity:
    """How much the pipeline should care.

    INFO     record it, keep the event in the main path
    WARN     record it, keep the event but exclude it from time-windowed math
    CRITICAL record it, quarantine the event from analytics until a human looks
    """

    INFO = "info"
    WARN = "warn"
    CRITICAL = "critical"


@dataclass(frozen=True)
class Finding:
    event_id: str | None
    anomaly_class: str
    severity: str
    detail: str


# --------------------------------------------------------------------------
# Thresholds
#
# Every number below is [Assumed]: a starting policy chosen to make the
# pipeline concrete, not measured from production. In a real deployment these
# would be tuned against observed clock-skew distribution across the SDK fleet,
# which is data this brief does not provide. They are module constants rather
# than literals inside the functions precisely so they are easy to find,
# argue with, and change.
# --------------------------------------------------------------------------

# Below this, a client clock running ahead of the server is ordinary jitter
# (network delay, NTP drift) and not worth flagging.
CLOCK_TOLERANCE = timedelta(seconds=5)

# Above this, a clock offset is no longer drift. An hour-scale offset usually
# means a timezone or DST bug rather than an inaccurate clock.
CLOCK_SKEW_CRITICAL = timedelta(minutes=30)

# Beyond this far into the future, the timestamp is not merely wrong, it is
# dangerous: event-time windowing uses the maximum observed timestamp to decide
# which windows are complete, so one event from 2027 can advance that mark past
# every real window and cause the rest to be discarded as late.
FUTURE_HORIZON = timedelta(hours=24)


# --------------------------------------------------------------------------
# PII detection
# --------------------------------------------------------------------------

# Property names that indicate personal data regardless of their value.
PII_KEY_PATTERN = re.compile(
    r"(email|e_mail|phone|mobile|tel\b|ssn|social_security|passport|"
    r"credit_card|card_number|dob|date_of_birth|address|postcode|zip_code)",
    re.IGNORECASE,
)

# Value shapes that indicate personal data regardless of their key name,
# because a customer can name a custom property anything at all.
EMAIL_VALUE_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")
PHONE_VALUE_PATTERN = re.compile(r"^\+?[\d][\d\s().-]{6,}\d$")

# Property names that look like a number the CLIENT computed rather than an
# event the client observed.
CLIENT_AGGREGATE_KEY_PATTERN = re.compile(
    r"^(count_|num_|total_|sum_|n_)|(_count|_total|_sum)$", re.IGNORECASE
)

# Referrer hosts that suggest automated traffic. Deliberately narrow.
BOT_REFERRER_PATTERN = re.compile(r"(bot|crawler|spider|scraper|scanner)", re.IGNORECASE)


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def parse_timestamp(value: Any) -> datetime | None:
    """Parse an ISO-8601 timestamp. Returns None if it is absent or unparseable."""
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _event_id(event: dict[str, Any]) -> str | None:
    value = event.get("event_id")
    return value if isinstance(value, str) else None


def _properties(event: dict[str, Any]) -> dict[str, Any]:
    props = event.get("properties")
    return props if isinstance(props, dict) else {}


# --------------------------------------------------------------------------
# Classifiers - one per anomaly class, each pure
# --------------------------------------------------------------------------

def classify_null_tenant(event: dict[str, Any]) -> list[Finding]:
    """A multi-tenant event with no tenant cannot be routed, billed, or shown.

    CRITICAL rather than WARN because the failure mode on the other side is
    guessing. An event assigned to the wrong tenant of 500 is not a data
    quality issue, it is one customer seeing another customer's visitors.
    """
    tenant = event.get("tenant_id")
    if tenant is None or (isinstance(tenant, str) and not tenant.strip()):
        return [
            Finding(
                _event_id(event),
                AnomalyClass.NULL_TENANT,
                Severity.CRITICAL,
                "tenant_id is null: event is unroutable and must never be "
                "attributed to a tenant by inference",
            )
        ]
    return []


def classify_timestamps(event: dict[str, Any]) -> list[Finding]:
    """Compare the client clock (ts) against the server clock (received_at).

    `ts` is stamped by the visitor's browser. `received_at` is stamped by our
    own server. The server cannot receive an event before the browser created
    it, so ts > received_at is physically impossible and means the visitor's
    clock is wrong.

    It is one phenomenon at three magnitudes in this fixture, which is why
    this is a threshold ladder rather than three unrelated rules.
    """
    findings: list[Finding] = []
    eid = _event_id(event)

    raw_ts = event.get("ts")
    raw_received = event.get("received_at")

    ts = parse_timestamp(raw_ts)
    received = parse_timestamp(raw_received)

    if raw_ts is not None and ts is None:
        findings.append(
            Finding(eid, AnomalyClass.INVALID_TIMESTAMP, Severity.CRITICAL,
                    f"ts is present but unparseable: {raw_ts!r}")
        )

    if raw_received is None:
        # evt-0009: an old SDK that never sent a server timestamp.
        findings.append(
            Finding(
                eid,
                AnomalyClass.MISSING_RECEIVED_AT,
                Severity.WARN,
                "no received_at: ingest latency and clock skew cannot be "
                "measured for this event, and no value was synthesised",
            )
        )
        return findings

    if received is None:
        findings.append(
            Finding(eid, AnomalyClass.INVALID_TIMESTAMP, Severity.CRITICAL,
                    f"received_at is present but unparseable: {raw_received!r}")
        )
        return findings

    if ts is None:
        return findings

    drift = ts - received
    if drift > CLOCK_TOLERANCE:
        if drift > FUTURE_HORIZON:
            severity = Severity.CRITICAL
        elif drift > CLOCK_SKEW_CRITICAL:
            severity = Severity.CRITICAL
        else:
            severity = Severity.WARN
        findings.append(
            Finding(
                eid,
                AnomalyClass.RECEIVED_BEFORE_SENT,
                severity,
                f"client clock is {_humanise(drift)} ahead of the server: "
                f"ts={raw_ts} received_at={raw_received}",
            )
        )

    if drift > FUTURE_HORIZON:
        findings.append(
            Finding(
                eid,
                AnomalyClass.FUTURE_TIMESTAMP,
                Severity.CRITICAL,
                f"ts is {_humanise(drift)} beyond received_at: admitting this "
                f"into an event-time window would advance the completeness "
                f"mark past every real window",
            )
        )

    return findings


def _humanise(delta: timedelta) -> str:
    seconds = delta.total_seconds()
    if seconds < 90:
        return f"{seconds:.3f}s"
    if seconds < 5400:
        return f"{seconds / 60:.1f}min"
    if seconds < 172800:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f} days"


def classify_pii(event: dict[str, Any]) -> list[Finding]:
    """Personal data sitting in a free-form properties bag.

    Two detectors, because either alone has a blind spot. Key names catch
    `contact_email`; value shapes catch a customer who called the same field
    `field_7`. Both are heuristics and both can produce false positives - the
    output is a flag for review, never an automatic deletion.

    Why it matters beyond GDPR/CCPA: PII in a free-form bag is PII you did not
    plan for, which means it is not in your retention policy, not in your
    deletion tooling, and not in your SOC 2 data map.
    """
    findings: list[Finding] = []
    hits: list[str] = []

    for key, value in _properties(event).items():
        if PII_KEY_PATTERN.search(str(key)):
            hits.append(f"{key} (key name)")
            continue
        if isinstance(value, str):
            if EMAIL_VALUE_PATTERN.match(value.strip()):
                hits.append(f"{key} (email-shaped value)")
            elif PHONE_VALUE_PATTERN.match(value.strip()):
                hits.append(f"{key} (phone-shaped value)")

    if hits:
        findings.append(
            Finding(
                _event_id(event),
                AnomalyClass.PII_IN_PROPERTIES,
                Severity.CRITICAL,
                "personal data in event properties: " + ", ".join(sorted(hits)),
            )
        )
    return findings


def classify_privacy_request(event: dict[str, Any]) -> list[Finding]:
    """A deletion demand arriving as an event in the behavioural stream.

    Not an anomaly in the "bad data" sense. It is a control-plane instruction
    that happens to be travelling on the data plane, and it has to be routed
    somewhere with an audit trail and an SLA rather than counted as a visitor
    action. Treating it as behaviour is both a compliance failure and a
    (small) metrics error.
    """
    if event.get("type") != "privacy_request":
        return []

    props = _properties(event)
    return [
        Finding(
            _event_id(event),
            AnomalyClass.PRIVACY_REQUEST,
            Severity.CRITICAL,
            f"regulatory request in the event stream: "
            f"request={props.get('request')!r} regulation={props.get('regulation')!r} "
            f"user_id={event.get('user_id')!r} anonymous_id={event.get('anonymous_id')!r} "
            f"- must trigger the deletion workflow, and deletion must cover the "
            f"anonymous history that predates identification",
        )
    ]


def classify_client_asserted_aggregate(event: dict[str, Any]) -> list[Finding]:
    """A number the client computed, rather than an event the client observed.

    This is the one worth reading twice. The brief's own customer requirement
    asks to "segment users by behaviour patterns (viewed pricing 3x)". The
    fixture supplies exactly that: a client-side counter claiming a value.

    A counter computed in the browser is untrusted input. It cannot be audited,
    it cannot be recomputed, it is trivially forged, and it silently disagrees
    with the server's own record when either side loses events. Building a
    product segment on it means the segment cannot be explained to the customer
    who asks why it fired.

    The rule is to ingest the claim with provenance and never let it be the
    source of truth. See state.py for the reconciliation that turns the
    disagreement into a useful signal instead of noise.
    """
    findings: list[Finding] = []
    hits: list[str] = []

    for key, value in _properties(event).items():
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)) and CLIENT_AGGREGATE_KEY_PATTERN.search(str(key)):
            hits.append(f"{key}={value}")

    if hits:
        findings.append(
            Finding(
                _event_id(event),
                AnomalyClass.CLIENT_ASSERTED_AGGREGATE,
                Severity.WARN,
                "client-computed aggregate present: " + ", ".join(sorted(hits))
                + " - store with provenance, never use as the source of truth "
                  "for segmentation; recompute server-side from raw events",
            )
        )
    return findings


def classify_suspicious_referrer(event: dict[str, Any]) -> list[Finding]:
    """Automated traffic, flagged on a deliberately weak signal.

    A referrer hostname containing "bot" or "scanner" is suggestive and
    nothing more. Anyone can set a referrer, plenty of legitimate hosts
    contain those substrings, and a real customer's marketing site could be
    called anything.

    Severity is WARN, and the finding says "quarantine", not "delete". If this
    heuristic is wrong and the events are dropped, real customer data is gone
    with no way to recover it. If it is wrong and the events are merely
    labelled, the cost is one column in a table.

    The stronger signal for the same conclusion - four page views from one
    visitor inside 51 milliseconds - is in state.py, because it needs memory.
    Two independent weak signals agreeing is worth far more than either alone.
    """
    referrer = _properties(event).get("referrer")
    if not isinstance(referrer, str) or not referrer:
        return []
    match = BOT_REFERRER_PATTERN.search(referrer)
    if not match:
        return []
    return [
        Finding(
            _event_id(event),
            AnomalyClass.SUSPICIOUS_REFERRER,
            Severity.WARN,
            f"referrer suggests automated traffic ({match.group(0)!r} in "
            f"{referrer!r}) - quarantine from customer-facing metrics, do not "
            f"delete on this signal alone",
        )
    ]


def findings_from_normalization(
    event: dict[str, Any], notes: Iterable[str]
) -> list[Finding]:
    """Turn normalize.py's audit notes into a schema-drift finding.

    Kept here rather than in normalize.py so that every Finding in the system
    is produced by one module and the output stays uniform. Still pure.
    """
    notes = [n for n in notes if n]
    if not notes:
        return []
    return [
        Finding(
            _event_id(event),
            AnomalyClass.SCHEMA_DRIFT,
            Severity.WARN,
            "event arrived in a legacy SDK shape and was normalised at the "
            "edge: " + "; ".join(notes),
        )
    ]


# --------------------------------------------------------------------------
# The pure entry point
# --------------------------------------------------------------------------

STATELESS_CLASSIFIERS = (
    classify_null_tenant,
    classify_timestamps,
    classify_pii,
    classify_privacy_request,
    classify_client_asserted_aggregate,
    classify_suspicious_referrer,
)


def classify_event(event: dict[str, Any]) -> list[Finding]:
    """Run every stateless classifier over one canonical event.

    Pure. Same input always produces the same output. This is the function
    that would be lifted unchanged into a stream processor as a map operator.
    """
    findings: list[Finding] = []
    for classifier in STATELESS_CLASSIFIERS:
        findings.extend(classifier(event))
    return findings