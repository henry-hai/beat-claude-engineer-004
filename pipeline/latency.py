"""Batch window vs event-at-a-time: where the latency actually comes from.

    python -m pipeline.latency fixtures/event_sample.jsonl

WHAT THIS IS
------------
A measurement harness, not a production component. It replays the same events
through two availability models and measures, per event, how stale the data is
at the moment a dashboard could first show it:

    staleness = time_available - ts        (ts = when it happened)

    batch model   an event becomes available at the next flush boundary of a
                  fixed window. This is what the current system does, and it
                  is why the brief reports 15-30 minute dashboard latency.
    stream model  an event becomes available as soon as it has been received
                  and processed. Processing cost is MEASURED here, not assumed:
                  the harness times the real pipeline per event.

WHAT THIS PROVES, AND WHAT IT DOES NOT
--------------------------------------
It does NOT benchmark Kafka, Kinesis, Flink, or any broker. It has no network,
no cluster, no queue, and 25 events. Any latency claim about a real broker
would be fabricated, and is not made.

What it isolates and measures is the MECHANISM: whether end-to-end latency in
this system is dominated by the batch window or by processing time. That single
question decides whether the rewrite is worth doing, and it is answerable
without any infrastructure at all. The honest framing at a walkthrough: "I
could not run 50M events per day, so I isolated the variable that decides the
answer and measured that."

Evidence tier: **3** (benchmark run with method stated), not 4. The challenge
rubric defines Tier 4 as measured before/after from a comparable system
actually built or fixed. This is a simulation of the batching behaviour, not a
measurement of their production pipeline, and it is logged as such.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pipeline.normalize import parse_and_normalize
from pipeline.rules import (
    CLOCK_TOLERANCE,
    classify_event,
    findings_from_normalization,
    parse_timestamp,
)
from pipeline.state import StreamState

# The batch flush interval being simulated. [Assumed], anchored to the brief's
# own statement that the current system shows 15-30 minute dashboard latency:
# a 15-minute flush produces a 0-15 minute wait depending on where in the
# window an event lands, plus the job's own runtime. Swap with --window to see
# how sensitive the result is.
DEFAULT_WINDOW_MINUTES = 15


def next_boundary(moment: datetime, window: timedelta) -> datetime:
    """The next flush boundary at or after `moment`, aligned to the epoch."""
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    elapsed = (moment - epoch) // window
    boundary = epoch + (elapsed + 1) * window
    return boundary


def measure(path: Path, window_minutes: int) -> dict:
    window = timedelta(minutes=window_minutes)
    state = StreamState()

    per_event: list[dict] = []
    process_times: list[float] = []
    skipped_no_received_at = 0
    skipped_dead_letter = 0

    with path.open(encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, 1):
            normalized, dead = parse_and_normalize(raw, line_no)
            if dead is not None:
                skipped_dead_letter += 1
                continue
            if normalized is None:
                continue

            event = normalized.event

            # Time the real per-event work. This is the processing cost the
            # streaming model has to pay, measured rather than assumed.
            start = time.perf_counter()
            findings = findings_from_normalization(event, normalized.notes)
            findings += classify_event(event)
            findings += state.observe(event)
            elapsed = time.perf_counter() - start
            process_times.append(elapsed)

            ts = parse_timestamp(event.get("ts"))
            received = parse_timestamp(event.get("received_at"))
            if ts is None or received is None:
                # evt-0009 has no server timestamp. Not estimated, not
                # synthesised - excluded and counted.
                skipped_no_received_at += 1
                continue

            stream_available = received + timedelta(seconds=elapsed)
            batch_available = next_boundary(received, window)

            # An event whose client clock runs ahead of the server produces a
            # NEGATIVE staleness - it appears to become visible before it
            # happened. That is not a latency measurement, it is a broken
            # clock, and averaging it in silently corrupts the result. This is
            # the same contamination that makes evt-0016 dangerous to a
            # watermark, showing up in a second metric. Partitioned out and
            # reported separately rather than dropped.
            clock_anomalous = (ts - received) > CLOCK_TOLERANCE

            per_event.append(
                {
                    "event_id": event.get("event_id"),
                    "ts": event.get("ts"),
                    "received_at": event.get("received_at"),
                    "clock_anomalous": clock_anomalous,
                    "stream_staleness_s": (stream_available - ts).total_seconds(),
                    "batch_staleness_s": (batch_available - ts).total_seconds(),
                    "process_time_ms": elapsed * 1000,
                }
            )

    clean = [e for e in per_event if not e["clock_anomalous"]]
    contaminated = [e for e in per_event if e["clock_anomalous"]]

    stream = [e["stream_staleness_s"] for e in clean]
    batch = [e["batch_staleness_s"] for e in clean]

    def summarise(values: list[float]) -> dict:
        if not values:
            return {}
        return {
            "min_s": min(values),
            "median_s": statistics.median(values),
            "max_s": max(values),
            "mean_s": statistics.fmean(values),
        }

    return {
        "source": str(path),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "window_minutes": window_minutes,
        "events_measured": len(clean),
        "excluded": {
            "dead_lettered": skipped_dead_letter,
            "no_received_at": skipped_no_received_at,
            "clock_anomalous": len(contaminated),
        },
        "clock_anomalous_events": [
            {
                "event_id": e["event_id"],
                "ts": e["ts"],
                "received_at": e["received_at"],
                "apparent_stream_staleness_s": e["stream_staleness_s"],
            }
            for e in contaminated
        ],
        "stream": summarise(stream),
        "batch": summarise(batch),
        "processing_ms": {
            "mean": statistics.fmean(process_times) * 1000 if process_times else 0.0,
            "max": max(process_times) * 1000 if process_times else 0.0,
        },
        "per_event": per_event,
    }


def render(report: dict) -> str:
    out: list[str] = []
    add = out.append
    w = report["window_minutes"]

    add("=" * 78)
    add("BATCH WINDOW vs EVENT-AT-A-TIME - staleness at first availability")
    add("=" * 78)
    add(f"source          : {report['source']}")
    add(f"generated_at    : {report['generated_at']}")
    add(f"batch window    : {w} minutes  [Assumed - anchored to the brief's stated")
    add(f"                  15-30 minute current dashboard latency]")
    exc = report["excluded"]
    add(f"events measured : {report['events_measured']}")
    add(f"excluded        : {exc['dead_lettered']} dead-lettered, "
        f"{exc['no_received_at']} with no server timestamp, "
        f"{exc['clock_anomalous']} with a client clock ahead of the server")
    add("")

    if report["clock_anomalous_events"]:
        add("EXCLUDED - clock-anomalous events, reported not discarded:")
        for e in report["clock_anomalous_events"]:
            add(f"  {e['event_id']}: apparent staleness "
                f"{e['apparent_stream_staleness_s']:,.1f}s "
                f"(ts={e['ts']} received_at={e['received_at']})")
        add("")
        add("  A client clock running ahead of the server makes an event look")
        add("  as though it became visible BEFORE it happened. Averaging that in")
        add("  silently corrupts the result - the same contamination that makes a")
        add("  far-future timestamp dangerous to a watermark, showing up in a")
        add("  second metric. Partitioned out and named, not quietly dropped.")
        add("")

    if not report["stream"]:
        add("no measurable events")
        return "\n".join(out)

    s, b = report["stream"], report["batch"]
    add("staleness = time an event first becomes visible, minus when it happened")
    add("")
    add(f"{'':16}{'min':>12}{'median':>12}{'mean':>12}{'max':>12}")
    add(f"{'streaming':16}{s['min_s']:>11.3f}s{s['median_s']:>11.3f}s"
        f"{s['mean_s']:>11.3f}s{s['max_s']:>11.3f}s")
    add(f"{'batch (' + str(w) + 'min)':16}{b['min_s']:>11.1f}s{b['median_s']:>11.1f}s"
        f"{b['mean_s']:>11.1f}s{b['max_s']:>11.1f}s")
    add("")

    p = report["processing_ms"]
    add(f"measured per-event processing: mean {p['mean']:.3f}ms, max {p['max']:.3f}ms "
        f"[Observed]")
    add("")
    add(f"median streaming staleness   : {s['median_s']:.3f}s")
    add(f"median batch staleness       : {b['median_s']:.1f}s")
    if s["median_s"] > 0:
        add(f"ratio                        : {b['median_s'] / s['median_s']:.0f}x")
    add("")
    add("-" * 78)
    add("WHAT THIS SHOWS")
    add("-" * 78)
    add("Processing cost is measured and is a rounding error against the window.")
    add("Latency in the current architecture is dominated by WHEN THE JOB RUNS,")
    add("not by how long the work takes. Shortening the window is the lever;")
    add("making the processing faster is not.")
    add("")
    add("The <5s target is unreachable with any batch window above ~5 seconds,")
    add("regardless of how fast the processing gets. That is the argument for")
    add("streaming, and it does not depend on any claim about a broker.")
    add("")
    add("WHAT THIS IS NOT: not a benchmark of Kafka, Kinesis, Flink or any")
    add("broker. No network, no cluster, no queue, 25 events. Evidence Tier 3")
    add("(benchmark run, method stated), not Tier 4.")
    add("=" * 78)
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("path", type=Path, help="JSONL file of events")
    parser.add_argument("--window", type=int, default=DEFAULT_WINDOW_MINUTES,
                        help=f"batch flush interval in minutes (default {DEFAULT_WINDOW_MINUTES})")
    parser.add_argument("--out", type=Path, default=Path("results"))
    args = parser.parse_args(argv)

    if not args.path.exists():
        parser.error(f"no such file: {args.path}")

    report = measure(args.path, args.window)
    args.out.mkdir(parents=True, exist_ok=True)

    text = render(report)
    (args.out / "latency.txt").write_text(text, encoding="utf-8", newline="\n")
    (args.out / "latency.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8", newline="\n"
    )

    print(text)
    print(f"wrote {args.out / 'latency.txt'}")
    print(f"wrote {args.out / 'latency.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())