"""Raw line -> canonical event, or dead letter.

Stateless. Pure. One line in, one result out. No I/O, no globals.

Two jobs:

1. Parse. A malformed line must not kill the consumer. It is routed to a
   dead-letter sink with the reason and the ORIGINAL bytes preserved, so it
   can be replayed after a fix rather than lost. The current system loses
   ~3% of events at peak; a consumer that dies on one bad record is one way
   that happens.

2. Normalize schema drift. The brief's hard constraint is that customers
   cannot be required to update the SDK, so old field names are live in
   production forever. Normalization happens once, at the edge, so every
   downstream stage sees exactly one shape.

Design decision worth defending: every transformation is RECORDED, not
applied silently. `NormalizedEvent.notes` answers "what did the pipeline
change about this event?" for any event, at any time. Silent normalization
is how a data platform loses the trust of the customers reading its numbers.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

# The one shape every downstream stage is allowed to assume.
CANONICAL_FIELDS: tuple[str, ...] = (
    "event_id",
    "tenant_id",
    "anonymous_id",
    "user_id",
    "type",
    "ts",           # client-stamped: when the event happened
    "received_at",  # server-stamped: when we got it
    "properties",
)

# Legacy SDK -> canonical mappings.
#
# These are DATA, not code, on purpose. Registering another SDK generation is
# a one-line config change reviewed by whoever owns the schema, not a code
# change that has to survive a deploy. Every entry below is evidenced by an
# event in fixtures/event_sample.jsonl - none are speculative. This dict is
# the extension point when the next legacy shape is discovered in production.
LEGACY_TOP_LEVEL_FIELDS: dict[str, str] = {
    "timestamp": "ts",  # evt-0009
}

LEGACY_PROPERTY_FIELDS: dict[str, str] = {
    "page_path": "path",      # evt-0009
    "ref": "referrer",        # evt-0009
}

LEGACY_TYPES: dict[str, str] = {
    "pageview": "page_view",  # evt-0009
}


@dataclass(frozen=True)
class DeadLetter:
    """A line the pipeline refused to parse.

    `raw` is kept in full and unmodified. Truncating it would make the record
    unreplayable, which defeats the purpose of having a dead-letter sink at
    all: the point is that a fix plus a replay recovers the data.
    """

    line_no: int
    raw: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {"line_no": self.line_no, "reason": self.reason, "raw": self.raw}


@dataclass(frozen=True)
class ParseResult:
    """Outcome of parsing one line.

    Three states, deliberately distinguished:
      event set, dead_letter None  -> parsed
      event None, dead_letter set  -> rejected, routed to the dead-letter sink
      both None                    -> blank line, nothing to do
    """

    event: dict[str, Any] | None = None
    dead_letter: DeadLetter | None = None

    @property
    def ok(self) -> bool:
        return self.event is not None


@dataclass(frozen=True)
class NormalizedEvent:
    """A canonical event plus an audit trail of what was changed to get there."""

    event: dict[str, Any]
    notes: list[str] = field(default_factory=list)

    @property
    def was_normalized(self) -> bool:
        return bool(self.notes)


def parse_line(raw: str, line_no: int) -> ParseResult:
    """Parse one JSONL line. Never raises.

    Anything that cannot become a JSON object becomes a dead letter. The
    consumer keeps running either way - that is the entire point.
    """
    if not raw.strip():
        return ParseResult()

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        return ParseResult(
            dead_letter=DeadLetter(
                line_no=line_no,
                raw=raw.rstrip("\n"),
                reason=f"invalid JSON: {exc.msg} at position {exc.pos}",
            )
        )

    if not isinstance(parsed, dict):
        return ParseResult(
            dead_letter=DeadLetter(
                line_no=line_no,
                raw=raw.rstrip("\n"),
                reason=f"top-level value is {type(parsed).__name__}, expected object",
            )
        )

    return ParseResult(event=parsed)


def normalize_event(event: dict[str, Any]) -> NormalizedEvent:
    """Map a parsed event onto the canonical shape.

    Pure: the input dict is not mutated. Returns a new dict plus a note for
    every change made.

    What this deliberately does NOT do: invent missing values. If an old SDK
    never sent `received_at`, the field stays None and a note records its
    absence. Synthesising a plausible server timestamp would make the event
    look complete to every downstream consumer while quietly fabricating the
    one number you would use to measure ingest latency and detect clock skew.
    A missing value you can see beats a fake value you cannot.
    """
    notes: list[str] = []
    out: dict[str, Any] = dict(event)

    # 1. Legacy top-level field names.
    for legacy, canonical in LEGACY_TOP_LEVEL_FIELDS.items():
        if legacy in out:
            value = out.pop(legacy)
            if canonical in out and out[canonical] is not None:
                notes.append(
                    f"legacy field '{legacy}' dropped: canonical '{canonical}' already present"
                )
            else:
                out[canonical] = value
                notes.append(f"renamed top-level field '{legacy}' -> '{canonical}'")

    # 2. Legacy event type names.
    event_type = out.get("type")
    if isinstance(event_type, str) and event_type in LEGACY_TYPES:
        out["type"] = LEGACY_TYPES[event_type]
        notes.append(f"renamed event type '{event_type}' -> '{out['type']}'")

    # 3. Legacy property names, inside the properties bag.
    properties = out.get("properties")
    if isinstance(properties, dict):
        new_properties = dict(properties)
        for legacy, canonical in LEGACY_PROPERTY_FIELDS.items():
            if legacy in new_properties:
                value = new_properties.pop(legacy)
                if canonical in new_properties and new_properties[canonical] is not None:
                    notes.append(
                        f"legacy property '{legacy}' dropped: '{canonical}' already present"
                    )
                else:
                    new_properties[canonical] = value
                    notes.append(f"renamed property '{legacy}' -> '{canonical}'")
        out["properties"] = new_properties
    elif properties is None:
        out["properties"] = {}
        notes.append("missing 'properties' defaulted to empty object")

    # 4. Fill absent canonical fields with None so downstream code can rely on
    #    the keys existing. Absence is recorded, never invented.
    for canonical_field in CANONICAL_FIELDS:
        if canonical_field not in out:
            out[canonical_field] = None
            notes.append(f"canonical field '{canonical_field}' absent, left null")

    return NormalizedEvent(event=out, notes=notes)


def parse_and_normalize(raw: str, line_no: int) -> tuple[NormalizedEvent | None, DeadLetter | None]:
    """Convenience composition of the two stages above.

    In a stream processor these are two operators; here they are one call so
    the driver in run.py stays readable.
    """
    result = parse_line(raw, line_no)
    if result.dead_letter is not None:
        return None, result.dead_letter
    if result.event is None:
        return None, None
    return normalize_event(result.event), None