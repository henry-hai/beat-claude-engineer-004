# Real-Time Analytics Pipeline — Engineer 004

**Brief version: 2026-07** · Henry Hai Nguyen
Fixture worked from: `fixtures/event_sample.jsonl`, SHA-256
`1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235`
Artifact: <REPO URL — task 16, verified logged-out before this PDF is final>

---

# Written answer

<!-- BUDGET: ~2,400 words total across this section. 4 pages max.
     Diagrams do not count toward the limit. Every paragraph containing a
     number must carry [Observed] / [Estimated] / [Benchmarked] / [Assumed]
     in that same paragraph - the pre-screen linter warns on bare figures
     like "50M" and "3 months". -->

## Thesis: the risk is at ingest, not in the broker

<!-- ~120 words. This is the framing move and it must come first. The
     baseline answer will spend most of its words choosing a broker. Say
     that transport is a purchasing decision with three acceptable answers
     (MSK / Kinesis / self-managed), each with public pricing and a vendor
     SLA, and that the unbuyable, bespoke, constraint-breaking risk is
     ingest-time normalisation plus identity stitching. Cite the fixture as
     the evidence: evt-0009 (old SDK that cannot be forced to update),
     evt-0007 -> evt-0022 -> evt-0017 (PII crossing the anonymous-to-user
     boundary ahead of a GDPR deletion), evt-0011 (null tenant in a
     500-tenant system). Then: "that is the component I built." -->

## 1. Architecture and technology choices

<!-- ~500 words + diagram. Required by the brief:
     - system diagram, SDK -> dashboard, with data flow
     - technology per component AND why over the alternatives (the rubric is
       explicit: "a component diagram without rejected options reads as a
       tutorial, not a decision")
     - event schema structure and identity stitching approach

     Include a rejected-alternatives table. Keep it to one line per choice.
     Identity stitching must reference the retroactive re-attribution the
     detector found: anon-3d0 -> u-7304 with evt-0007 already written. -->

```
<!-- ASCII or Mermaid diagram here. Does not count toward page limit. -->
```

| Component | Choice | Rejected | Why |
|---|---|---|---|
| | | | |

## 2. Scale, reliability and migration

<!-- ~500 words. Required by the brief:
     - 50M events/day and 10x spikes with zero loss
     - migration without breaking 500+ customers; rollback plan
     - how data accuracy is validated

     Sizing math must be visible and every input labelled. The rubric calls
     migration "most of the actual risk" and names the skipped migration as a
     losing failure mode. Needs: phased cutover, parallel-run comparison, a
     named rollback TRIGGER (not just "we can roll back"), and a testable
     definition of "data accuracy verified".

     Reconciliation angle: the client-side counter in evt-0019 that we refuse
     to trust for segmentation is exactly the parallel-run comparison signal.
     Also: producer-side sequence numbers, because event_id continuity cannot
     detect loss under sampling (see refusal 3). -->

## 3. Trade-offs and risks

<!-- ~250 words. What is being optimised for vs sacrificed, and what would be
     done differently with more time or budget. Scope discipline: say what
     the 3-month MVP EXCLUDES. Two dedicated engineers is the real budget. -->

## 4. What is actually in the sample data

<!-- ~400 words. REQUIRED by the brief, anomaly class by class, citing
     specific event_ids. 13 classes detected [Observed], 9 stateless and 4
     requiring keyed state. Do not list all 13 in prose - group them and let
     the artifact carry the detail. Each class needs one line on how the
     pipeline HANDLES it, not just that it was spotted.

     The stateless/stateful split is itself an architectural finding: the 4
     classes that need memory are the argument for a stateful stream
     processor over a stateless consumer. -->

| Anomaly class | Events | Pipeline response |
|---|---|---|
| | | |

## 5. Three things I would not act on at face value

<!-- ~250 words. REQUIRED by the brief ("at least three"). These are the
     refusals, and they are the move a generated baseline reliably will not
     make. Each needs the reason, not just the refusal.

     1. evt-0019 count_today=3 vs one server-side /pricing view for anon-9f2.
        The brief ASKS for a "viewed pricing 3x" segment. The trap is that
        the requested feature is built on the untrustworthy number.
     2. The bot cluster evt-0012..0015. Flag and quarantine, never delete on
        a referrer string - asymmetric risk.
     3. The brief's own ~3% loss premise. event_id continuity is not a loss
        detector under sampling, so this fixture can neither confirm nor
        refute it. State what evidence WOULD settle it (producer-side
        sequence numbers / client send log) - that converts a refusal into a
        design requirement rather than a dodge.
     4. (optional) The clock-skew events - do not "correct" ts by trusting
        received_at when downstream consumers may already depend on it. -->

---

# Operating artifact

<!-- What it is, what it does, how to run it, what it proves. Must contain a
     reproduction command so the [Observed] numbers are checkable in the same
     section - the linter treats high-tier claims with nothing checkable as
     Tier 0. -->

```
python -m pipeline.run fixtures/event_sample.jsonl
```

# Artifact access

<!-- REQUIRED by scripts/validate_submission.py (hard FAIL if the literal
     phrase "Artifact access" is missing) but NOT listed in the brief's own
     packet of 7. Public repo URL, what is in it, how to run it, verified
     logged-out. Keep the PDF text-light and link heavy output here. -->

# Evidence log

<!-- 8-12 headline claims, each mapped to a tier 0-5 with a checkable
     reference in this same section. Includes the explicit Tier 4 REFUSAL:
     no before/after from a comparable system I built or fixed is available
     to me, here is the closest honest substitute (Tier 3 benchmark run) and
     why it falls short. -->

| Claim | Tier | Where to check it |
|---|---|---|
| | | |

# Number source labels

<!-- Every number in this document, labelled. State the convention here and
     apply it inline throughout. -->

# AI usage disclosure

<!-- Tools used, what AI helped with, what I decided personally, what I
     checked or changed myself, known weak spots in the output. Honest and
     specific - vagueness here reads worse than heavy use. -->

# What breaks it

<!-- Most likely failure modes, bad inputs, missing data, constraints that
     would make this answer wrong. Include: the 25-event sample is too small
     to establish any rate; thresholds are [Assumed] not tuned; the bot
     heuristic has false positives; keyed state is bounded so deletion scope
     from this window is a lower bound, not the truth. -->

# What stays human

<!-- REQUIRED by the brief but NOT checked by the linter - do not drop it.
     Which decisions must not be automated and why. Candidates: deleting
     suspected bot traffic; approving a GDPR deletion's scope; the rollback
     trigger during migration; changing a tenant attribution; tuning the
     clock-skew thresholds. The through-line is that every one of these is
     irreversible or legally binding, and the detector deliberately labels
     rather than acts. -->
