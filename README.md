# Ingest-time validation for a real-time analytics pipeline

Operating artifact for Beat Claude challenge **engineer-004** (brief version
2026-07). The written answer is in [`submission/answer.md`](submission/answer.md).

Pure Python 3, standard library only. No dependencies, no Docker, no network.

---

## Why this component

The brief asks for an architecture. This repo is a runnable prototype of the
part of it I think is actually risky.

**The message broker is not that part.** MSK, Kinesis, and self-managed Kafka
all move bytes, all have public pricing, all have a vendor SLA. Choosing one is
a purchasing decision with three acceptable answers.

**Ingest-time normalization and identity stitching are.** They are bespoke,
unbuyable, and each one breaks a stated hard constraint if it is wrong:

- `evt-0009` arrives from an SDK generation using different field names, and
  the brief forbids requiring customers to upgrade. Reject it and a paying
  customer silently loses data.
- `evt-0007` carries a raw email address and phone number while `user_id` is
  still null. Fifteen events later `evt-0022` identifies that visitor, and
  `evt-0017` demands GDPR deletion for them. Deleting rows keyed on `user_id`
  alone leaves the pre-identification history behind.
- `evt-0011` has no `tenant_id`. In a 500-tenant system, attributing it by
  inference is not a data-quality issue, it is one customer seeing another
  customer's visitors.

So that is what I built and measured.

---

## Run it

```bash
python -m pipeline.run fixtures/event_sample.jsonl
```

Writes three inspectable files to `results/`:

| File | What it is |
|---|---|
| `findings.txt` | human-readable, grouped by anomaly class, every `event_id` cited |
| `findings.json` | machine-readable, for citing exact counts |
| `dead_letter.jsonl` | lines the pipeline refused to parse, bytes preserved |

The report computes the input file's SHA-256 at run time, so the checksum
quoted in the written answer is produced by the tool rather than typed by hand.

---

## What it found

All figures **[Observed]** — produced by the command above, reproducible by
anyone who clones this repo.

| | |
|---|---|
| Lines read | 25 |
| Events parsed | 24 |
| Dead-lettered | 1 (`evt-0020`, unclosed brace) |
| Normalized from a legacy SDK shape | 1 (`evt-0009`) |
| Findings | 21 |
| Distinct anomaly classes | **13** |
| — answerable from one event | 9 |
| — requiring memory of other events | 4 |
| By severity | 7 critical, 10 warn, 4 info |

**That 9/4 split is itself an architectural finding.** Duplicate delivery,
visitor burst detection, retroactive identity stitching, and deletion scope
cannot be answered by a stateless consumer no matter how it is written. They
are the argument for keyed state in the production system, and they are why
`pipeline/state.py` exists as a separate module rather than being quietly
folded into the classifiers.

---

## The latency measurement

```bash
python -m pipeline.latency fixtures/event_sample.jsonl
```

Replays the same events through a simulated batch window and an
event-at-a-time path, measuring staleness at first availability.

| | median staleness |
|---|---|
| streaming | **0.164 s** [Observed] |
| batch, 15-minute window | **481.9 s** [Observed] |

Measured per-event processing cost: **0.017 ms mean, 0.042 ms max**
[Observed]. The window is **[Assumed]** at 15 minutes, anchored to the brief's
own statement of 15–30 minute current dashboard latency.

The point is the ratio between those two things. Processing costs microseconds;
the window costs minutes. **Latency in the current architecture is dominated by
when the job runs, not by how long the work takes** — so the sub-5-second target
is unreachable behind any batch window, regardless of how fast the processing
becomes. That is the argument for streaming, and it does not rest on any claim
about a broker.

Three clock-anomalous events (`evt-0005`, `evt-0006`, `evt-0016`) are
partitioned out and named in the report rather than silently dropped. A client
clock running ahead of the server makes an event appear to become visible
before it happened, which corrupts the mean without corrupting the median —
the same contamination that makes a far-future timestamp dangerous to a
watermark, surfacing in a second metric.

---

## Tests

```bash
python -m unittest discover -s tests -t .
```

50 tests, standard library only. Three layers, because "how do you validate
data accuracy?" is a question the brief actually asks:

1. **Unit tests** over the pure functions, one per anomaly class. They are
   cheap to write *only* because `normalize.py` and `rules.py` are pure
   functions of a single event. Architecture that is easy to test is not a
   coincidence.
2. **Property tests** on purity itself: classifiers must not mutate their
   input and must be order-independent. If that ever stops holding, the claim
   that this code lifts into a stream processor unchanged is dead, and the
   suite says so.
3. **Fixture regression** pinning the exact counts above, plus the fixture's
   own SHA-256. Any rule change that moves a number fails loudly and names
   what moved. This is the layer that would catch a bad deploy.

## What this deliberately is not

- **Not a broker benchmark.** No network, no cluster, no queue, 25 events. Any
  latency claim about Kafka, Kinesis, or Flink from this harness would be
  fabricated, and none is made. Evidence tier **3** (benchmark run with method
  stated), not tier 4.
- **Not a production pipeline.** It is the ingest stage, built to be
  interrogated.
- **Not a basis for any rate.** A 25-event sample cannot establish an error
  rate, a loss rate, or a distribution. See the written answer for what I
  refused to conclude from it and why.

Every threshold in the code is **[Assumed]** — a starting policy that makes the
design concrete, not a value measured from production. They are named module
constants specifically so they are easy to find and argue with.

---

## How it is built

Four modules, and the split maps onto how stream processors actually execute
work rather than onto tidiness:

| Module | Role | Stream-processor equivalent |
|---|---|---|
| `pipeline/normalize.py` | one raw line in, one canonical event out | stateless `map` |
| `pipeline/rules.py` | classify one event, no memory | stateless `map` / `filter` |
| `pipeline/state.py` | needs memory across events | keyed aggregate over a window |
| `pipeline/run.py` | wires them, writes the sinks | job graph + sinks |

Everything in `normalize.py` and `rules.py` is a pure function of a single
event, which is exactly the contract a stream processor needs — so those lift
into production unchanged. Everything that could not satisfy that contract was
pushed into `state.py` rather than smuggled in, and every store there is
bounded, because unbounded state at 50M events/day is a memory leak with a
schedule.

**Nothing in this repo deletes or corrects an event.** Classifiers label; what
to do with a label is a policy decision belonging to the operator. Several of
these signals are deliberately too weak to act on alone — a referrer hostname
containing "bot" is suggestive and nothing more, and if that heuristic is wrong
and the events were deleted, real customer data is gone with no recovery.

---

## The fixture

`fixtures/event_sample.jsonl`, vendored from the challenge repository so this
runs standalone.

```
SHA-256  1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235
bytes    5749
```

Verify it yourself:

```bash
shasum -a 256 fixtures/event_sample.jsonl
```

`.gitattributes` pins `*.jsonl` with `-text` so Git never translates line
endings for this file. Without that pin, cloning on Windows rewrites all 25
line endings to CRLF, changing the file to 5774 bytes and producing a different
hash — same text to a human, different bytes to SHA-256. The pin means the
checksum above reproduces on Windows, macOS, and Linux alike.

---

## Repository map

```
pipeline/       the artifact
fixtures/       the challenge fixture, line-ending pinned
results/        committed output, so the findings are readable without running anything
submission/     the written answer
tests/          tests over the pure classifiers
```