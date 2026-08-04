"""CLI driver. Wires normalize -> rules -> state and writes the sinks.

Reproduce every [Observed] number in the written answer with one command:

    python -m pipeline.run fixtures/event_sample.jsonl

Sinks written to results/:
    findings.txt        human-readable, grouped by anomaly class, event_ids cited
    findings.json       machine-readable, for citing exact counts
    dead_letter.jsonl   lines the pipeline refused to parse

The report includes the SHA-256 of the input file, computed at run time. The
checksum quoted in the written answer is therefore produced by the tool rather
than typed by hand, so a reviewer re-running this command can confirm the
number and the analysis came from the same bytes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from pipeline.normalize import parse_and_normalize
from pipeline.rules import AnomalyClass, Finding, Severity, classify_event, findings_from_normalization
from pipeline.state import StreamState

SEVERITY_ORDER = {Severity.CRITICAL: 0, Severity.WARN: 1, Severity.INFO: 2}

STATEFUL_CLASSES = {
    AnomalyClass.DUPLICATE_EVENT,
    AnomalyClass.VISITOR_BURST,
    AnomalyClass.IDENTITY_STITCH,
    AnomalyClass.DELETION_SCOPE,
}


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def analyse(path: Path) -> dict:
    """Run the full pipeline over a JSONL file. Returns a report dict."""
    state = StreamState()
    findings: list[Finding] = []
    dead_letters = []
    lines_read = 0
    events_parsed = 0
    events_normalized = 0

    with path.open(encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, 1):
            if raw.strip():
                lines_read += 1
            normalized, dead = parse_and_normalize(raw, line_no)
            if dead is not None:
                dead_letters.append(dead)
                continue
            if normalized is None:
                continue

            events_parsed += 1
            if normalized.was_normalized:
                events_normalized += 1

            findings.extend(findings_from_normalization(normalized.event, normalized.notes))
            findings.extend(classify_event(normalized.event))
            findings.extend(state.observe(normalized.event))

    grouped: dict[str, list[Finding]] = defaultdict(list)
    for finding in findings:
        grouped[finding.anomaly_class].append(finding)

    return {
        "source": {
            "path": str(path),
            "sha256": sha256_of(path),
            "bytes": path.stat().st_size,
        },
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "counts": {
            "lines_read": lines_read,
            "events_parsed": events_parsed,
            "dead_lettered": len(dead_letters),
            "events_normalized": events_normalized,
            "findings": len(findings),
            "anomaly_classes": len(grouped),
            "stateless_classes": len(set(grouped) - STATEFUL_CLASSES),
            "stateful_classes": len(set(grouped) & STATEFUL_CLASSES),
            "by_severity": dict(Counter(f.severity for f in findings)),
        },
        "grouped": grouped,
        "dead_letters": dead_letters,
    }


def render_text(report: dict) -> str:
    src = report["source"]
    counts = report["counts"]
    grouped: dict[str, list[Finding]] = report["grouped"]

    out: list[str] = []
    add = out.append

    add("=" * 78)
    add("INGEST VALIDATION REPORT")
    add("=" * 78)
    add(f"source        : {src['path']}")
    add(f"sha256        : {src['sha256']}")
    add(f"bytes         : {src['bytes']}")
    add(f"generated_at  : {report['generated_at']}")
    add("")
    add(f"lines read        : {counts['lines_read']}")
    add(f"events parsed     : {counts['events_parsed']}")
    add(f"dead-lettered     : {counts['dead_lettered']}")
    add(f"events normalized : {counts['events_normalized']}")
    add("")
    add(f"findings          : {counts['findings']}")
    add(f"anomaly classes   : {counts['anomaly_classes']} "
        f"({counts['stateless_classes']} stateless, {counts['stateful_classes']} stateful)")
    add(f"by severity       : {counts['by_severity']}")
    add("")

    def sort_key(item):
        cls, group = item
        worst = min(SEVERITY_ORDER.get(f.severity, 9) for f in group)
        return (worst, cls)

    for cls, group in sorted(grouped.items(), key=sort_key):
        stateful = " [needs keyed state]" if cls in STATEFUL_CLASSES else ""
        event_ids = sorted({f.event_id for f in group if f.event_id})
        add("-" * 78)
        add(f"{cls.upper()}{stateful}")
        add(f"  events ({len(group)}): {', '.join(event_ids)}")
        add("")
        for finding in group:
            add(f"  [{finding.severity:8}] {finding.event_id}")
            for line in _wrap(finding.detail, 70):
                add(f"             {line}")
            add("")

    if report["dead_letters"]:
        add("-" * 78)
        add("DEAD LETTER QUEUE")
        add("  Lines the pipeline refused to parse. The consumer did not stop;")
        add("  original bytes are preserved so these can be replayed after a fix.")
        add("")
        for dead in report["dead_letters"]:
            add(f"  line {dead.line_no}: {dead.reason}")
            add(f"    {dead.raw[:110]}")
            add("")

    add("=" * 78)
    return "\n".join(out)


def _wrap(text: str, width: int) -> list[str]:
    words = text.split()
    lines: list[str] = []
    current = ""
    for word in words:
        if current and len(current) + 1 + len(word) > width:
            lines.append(current)
            current = word
        else:
            current = f"{current} {word}".strip()
    if current:
        lines.append(current)
    return lines


def render_json(report: dict) -> str:
    payload = {
        "source": report["source"],
        "generated_at": report["generated_at"],
        "counts": report["counts"],
        "findings_by_class": {
            cls: [asdict(f) for f in group]
            for cls, group in sorted(report["grouped"].items())
        },
        "dead_letters": [d.to_dict() for d in report["dead_letters"]],
    }
    return json.dumps(payload, indent=2, sort_keys=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Ingest-time validation over a JSONL event sample.",
    )
    parser.add_argument("path", type=Path, help="JSONL file of events")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("results"),
        help="directory for findings.txt / findings.json / dead_letter.jsonl",
    )
    args = parser.parse_args(argv)

    if not args.path.exists():
        parser.error(f"no such file: {args.path}")

    report = analyse(args.path)
    args.out.mkdir(parents=True, exist_ok=True)

    text = render_text(report)
    (args.out / "findings.txt").write_text(text, encoding="utf-8", newline="\n")
    (args.out / "findings.json").write_text(render_json(report), encoding="utf-8", newline="\n")
    (args.out / "dead_letter.jsonl").write_text(
        "".join(json.dumps(d.to_dict(), sort_keys=False) + "\n" for d in report["dead_letters"]),
        encoding="utf-8",
        newline="\n",
    )

    print(text)
    print(f"wrote {args.out / 'findings.txt'}")
    print(f"wrote {args.out / 'findings.json'}")
    print(f"wrote {args.out / 'dead_letter.jsonl'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())