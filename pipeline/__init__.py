"""Ingest-time validation layer for a real-time analytics pipeline.

This package is the runnable prototype of what I argue is the riskiest
component in the system: not the message broker (a purchasing decision with
three acceptable answers and a vendor SLA) but ingest-time normalization,
anomaly classification, and identity stitching, which are bespoke and break
a stated hard constraint if they are wrong.

The module split is deliberate and maps to stream-processor operator types:

    normalize.py   a stateless transform      (map)
    rules.py       stateless classification   (map / filter)
    state.py       keyed state over a window  (keyed aggregate)
    run.py         the driver                 (job graph + sinks)

Everything in normalize.py and rules.py is a pure function of a single event,
so the same code can be lifted into a stream processor unchanged. state.py
exists precisely because some checks cannot be pure per-event, and that
boundary is the argument for whether the production system needs keyed state.
"""