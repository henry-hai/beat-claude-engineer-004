"""Keyed state for checks that cannot be pure per-event.

This module is the honest part of the design. Four findings in this fixture
are impossible to produce while looking at a single event in isolation:

    duplicate_event    requires remembering event_ids already seen
    visitor_burst      requires the recent history of one visitor
    identity_stitch    requires linking an anonymous_id to a user_id that
                       only appears in a LATER event
    deletion_scope     requires knowing everything that visitor ever did

Each needs state keyed by something, and every keyed state must be BOUNDED,
because unbounded state is a memory leak with a schedule. At 50M events/day
[Observed: brief] an unbounded dedupe set would grow forever and the consumer
would eventually die - which is itself a way to lose events.

The size of each bound is a real design decision with a real cost, and it is
the reason the production system needs a stateful stream processor with
checkpointing rather than a stateless consumer you can restart freely.

WHY THIS SPLIT MATTERS: a stateless operation can run on any machine, in any
order, and be duplicated freely - if a worker dies you just rerun it. A
stateful operation cannot, because the answer depends on what that specific
worker has already seen. Kill it and the memory dies with it. Everything in
rules.py is the first kind. Everything here is the second.
"""

from __future__ import annotations

from collections import OrderedDict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Deque

from pipeline.rules import (
    AnomalyClass,
    Finding,
    Severity,
    parse_timestamp,
)


# --------------------------------------------------------------------------
# Bounds and thresholds
#
# All [Assumed]: starting policy to make the design concrete, not measured
# from production. Each one is a memory-versus-detection trade with a real
# cost, spelled out beside it.
# --------------------------------------------------------------------------

# How many recent event_ids to remember for duplicate detection. Too small and
# a duplicate that arrives after a long retry delay is missed. Too large and
# the consumer's memory grows without bound. In production this would be a
# TIME-bounded window (remember every id seen in the last N minutes) sized
# against the SDK's maximum retry backoff, because that is what actually
# determines how late a duplicate can arrive. A count bound is used here
# because it is honest about what a 25-line fixture can justify.
DEDUPE_CAPACITY = 100_000

# A visitor producing this many events inside this window is not browsing.
# evt-0012..evt-0015 are four page views spanning 51ms [Observed: fixture].
BURST_THRESHOLD = 4
BURST_WINDOW = timedelta(seconds=1)

# How many distinct visitors to track at once, for burst detection and for
# identity history. Same memory-versus-coverage trade as DEDUPE_CAPACITY.
VISITOR_CAPACITY = 100_000

# How many event_ids to retain per visitor for deletion-scope reporting.
# Deliberately small: the point is to prove the anonymous history EXISTS and
# must be covered, not to hold a customer's full event log in RAM.
VISITOR_HISTORY_DEPTH = 50


# --------------------------------------------------------------------------
# Duplicate detection
# --------------------------------------------------------------------------

@dataclass
class DedupeWindow:
    """Remembers recently seen event ids, bounded, oldest evicted first.

    The key is (tenant_id, event_id), not event_id alone. Two tenants can
    independently generate the same id - the SDK does not coordinate across
    customers - and collapsing them would silently delete one customer's event
    because a different customer happened to produce a matching id.
    """

    capacity: int = DEDUPE_CAPACITY
    seen: OrderedDict[tuple[Any, Any], str] = field(default_factory=OrderedDict)

    def observe(self, event: dict[str, Any]) -> list[Finding]:
        event_id = event.get("event_id")
        if event_id is None:
            return []

        key = (event.get("tenant_id"), event_id)

        if key in self.seen:
            first_received = self.seen[key]
            self.seen.move_to_end(key)
            return [
                Finding(
                    event_id if isinstance(event_id, str) else None,
                    AnomalyClass.DUPLICATE_EVENT,
                    Severity.WARN,
                    f"event_id already seen for this tenant: first received_at="
                    f"{first_received}, this copy received_at="
                    f"{event.get('received_at')} - at-least-once delivery, "
                    f"deduplicate on (tenant_id, event_id) before any counting",
                )
            ]

        self.seen[key] = str(event.get("received_at"))
        if len(self.seen) > self.capacity:
            self.seen.popitem(last=False)  # evict oldest
        return []


# --------------------------------------------------------------------------
# Burst detection
# --------------------------------------------------------------------------

@dataclass
class VisitorWindow:
    """Per-visitor recent activity, for detecting non-human event rates.

    Fires once when the threshold is crossed rather than on every subsequent
    event, so one burst produces one finding naming all participants instead
    of a wall of near-identical rows.
    """

    threshold: int = BURST_THRESHOLD
    window: timedelta = BURST_WINDOW
    capacity: int = VISITOR_CAPACITY
    recent: OrderedDict[str, Deque[tuple[datetime, str]]] = field(default_factory=OrderedDict)
    already_reported: set[str] = field(default_factory=set)

    def observe(self, event: dict[str, Any]) -> list[Finding]:
        anonymous_id = event.get("anonymous_id")
        if not isinstance(anonymous_id, str):
            return []

        ts = parse_timestamp(event.get("ts"))
        if ts is None:
            return []

        event_id = event.get("event_id")
        bucket = self.recent.setdefault(anonymous_id, deque())
        self.recent.move_to_end(anonymous_id)
        bucket.append((ts, str(event_id)))

        # Drop anything that has fallen out of the window.
        while bucket and (ts - bucket[0][0]) > self.window:
            bucket.popleft()

        if len(self.recent) > self.capacity:
            evicted, _ = self.recent.popitem(last=False)
            self.already_reported.discard(evicted)

        if len(bucket) >= self.threshold and anonymous_id not in self.already_reported:
            self.already_reported.add(anonymous_id)
            participants = [eid for _, eid in bucket]
            span = (bucket[-1][0] - bucket[0][0]).total_seconds()
            return [
                Finding(
                    str(event_id) if event_id is not None else None,
                    AnomalyClass.VISITOR_BURST,
                    Severity.WARN,
                    f"visitor {anonymous_id} produced {len(bucket)} events in "
                    f"{span * 1000:.0f}ms ({', '.join(participants)}) - a rate no "
                    f"human produces; quarantine from customer-facing metrics, "
                    f"do not delete on a heuristic",
                )
            ]
        return []


# --------------------------------------------------------------------------
# Identity stitching
# --------------------------------------------------------------------------

@dataclass
class IdentityGraph:
    """Links anonymous_id to user_id, and remembers what happened before.

    The hard direction is BACKWARDS. A visitor browses anonymously, then signs
    in. The events they generated before signing in are theirs, but at the time
    they were written nobody knew whose they were. When the link is finally
    established, the earlier history has to be re-attributed retroactively.

    This is not a nicety. `evt-0017` is a GDPR deletion request for u-1077.
    Deleting only rows keyed by user_id would leave that person's anonymous
    history behind and the deletion would be incomplete - which is a regulatory
    failure, not a bug report.
    """

    capacity: int = VISITOR_CAPACITY
    history_depth: int = VISITOR_HISTORY_DEPTH
    anon_to_user: OrderedDict[str, str] = field(default_factory=OrderedDict)
    anon_history: OrderedDict[str, list[str]] = field(default_factory=OrderedDict)

    def observe(self, event: dict[str, Any]) -> list[Finding]:
        anonymous_id = event.get("anonymous_id")
        if not isinstance(anonymous_id, str):
            return []

        user_id = event.get("user_id")
        event_id = event.get("event_id")
        findings: list[Finding] = []

        history = self.anon_history.setdefault(anonymous_id, [])
        self.anon_history.move_to_end(anonymous_id)

        if isinstance(user_id, str) and user_id:
            known = self.anon_to_user.get(anonymous_id)
            if known is None:
                prior = list(history)
                self.anon_to_user[anonymous_id] = user_id
                self.anon_to_user.move_to_end(anonymous_id)
                if prior:
                    findings.append(
                        Finding(
                            str(event_id) if event_id is not None else None,
                            AnomalyClass.IDENTITY_STITCH,
                            Severity.INFO,
                            f"{anonymous_id} resolved to {user_id}; "
                            f"{len(prior)} earlier anonymous event(s) "
                            f"({', '.join(prior)}) must be re-attributed "
                            f"retroactively and are now in scope for any "
                            f"deletion request against {user_id}",
                        )
                    )
            elif known != user_id:
                findings.append(
                    Finding(
                        str(event_id) if event_id is not None else None,
                        AnomalyClass.IDENTITY_STITCH,
                        Severity.WARN,
                        f"{anonymous_id} previously resolved to {known}, now "
                        f"claims {user_id} - shared device or identity collision; "
                        f"do not silently overwrite the link",
                    )
                )

        if event_id is not None and len(history) < self.history_depth:
            history.append(str(event_id))

        if len(self.anon_history) > self.capacity:
            self.anon_history.popitem(last=False)
        if len(self.anon_to_user) > self.capacity:
            self.anon_to_user.popitem(last=False)

        return findings

    def deletion_scope(self, event: dict[str, Any]) -> list[Finding]:
        """What a deletion request actually has to cover.

        Called only for privacy_request events. rules.py flags that the request
        EXISTS; this answers how far it reaches, which needs memory.
        """
        if event.get("type") != "privacy_request":
            return []

        anonymous_id = event.get("anonymous_id")
        user_id = event.get("user_id")
        event_id = event.get("event_id")

        anon_ids = [
            anon for anon, uid in self.anon_to_user.items() if uid == user_id
        ]
        if isinstance(anonymous_id, str) and anonymous_id not in anon_ids:
            anon_ids.append(anonymous_id)

        covered: list[str] = []
        for anon in anon_ids:
            covered.extend(self.anon_history.get(anon, []))

        return [
            Finding(
                str(event_id) if event_id is not None else None,
                AnomalyClass.DELETION_SCOPE,
                Severity.CRITICAL,
                f"deletion for user_id={user_id!r} reaches beyond that key: "
                f"anonymous_id(s) {anon_ids or ['none known']} and "
                f"{len(covered)} event(s) observed in this sample "
                f"({', '.join(covered) if covered else 'none'}). Deleting only "
                f"rows keyed by user_id would leave the pre-identification "
                f"history behind. Scope is bounded by what state remembers, so "
                f"deletion must run against durable storage, not this window.",
            )
        ]


# --------------------------------------------------------------------------
# Composition
# --------------------------------------------------------------------------

@dataclass
class StreamState:
    """The three keyed stores, driven by one call per event.

    In a stream processor each of these is a separate keyed operator with its
    own checkpointed state backend. Here they are one object because a single
    Python process replaying 25 lines does not need a cluster - but the shape
    is the same, and so is the memory question.
    """

    dedupe: DedupeWindow = field(default_factory=DedupeWindow)
    visitors: VisitorWindow = field(default_factory=VisitorWindow)
    identities: IdentityGraph = field(default_factory=IdentityGraph)

    def observe(self, event: dict[str, Any]) -> list[Finding]:
        findings: list[Finding] = []
        findings.extend(self.dedupe.observe(event))
        findings.extend(self.visitors.observe(event))
        findings.extend(self.identities.observe(event))
        findings.extend(self.identities.deletion_scope(event))
        return findings