"""AWS cost model for the proposed pipeline, sized against the brief's ceiling.

    python -m pipeline.cost
    python -m pipeline.cost --spike 20 --kpus 12

Every input carries a source label:

    [Observed]     measured from the brief or from the fixture
    [Benchmarked]  a published AWS list price, with the page it came from
    [Assumed]      a stated assumption, chosen to make the plan concrete

Nothing here is a guess dressed as a fact. Two components the design would
need - a managed OLAP serving tier and Aurora - are deliberately NOT priced,
because AWS's published pages did not render their rate tables when checked
and inventing a number would be worse than leaving a hole. See UNPRICED below.

The output of this model is not really the total. It is the RATIO between the
total and the ceiling, which turns out to decide the architecture.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path

PRICES_RETRIEVED = "2026-08-04"
PRICE_REGION = "us-east-1 (US East, N. Virginia)"


@dataclass(frozen=True)
class Input:
    value: float
    unit: str
    label: str
    source: str


# --------------------------------------------------------------------------
# Workload - what the system has to carry
# --------------------------------------------------------------------------

EVENTS_PER_DAY = Input(
    50_000_000, "events/day", "[Observed]", "brief: '~50M events/day'"
)
MEAN_EVENT_BYTES = Input(
    229, "bytes/event", "[Observed]",
    "measured over fixtures/event_sample.jsonl: 25 events, mean 229B, "
    "median 228B, range 200-285B",
)
SPIKE_MULTIPLIER = Input(
    10, "x", "[Observed]", "brief: 'handle 50M+ events/day and 10x traffic spikes'"
)
HEADROOM = Input(
    1.3, "x", "[Assumed]",
    "provision 30% above the modelled peak so a spike does not land exactly at "
    "capacity; the alternative is discovering the ceiling during Black Friday",
)

# --------------------------------------------------------------------------
# AWS list prices - us-east-1, retrieved 2026-08-04
# All [Benchmarked] from the AWS pricing page named in each source string.
# --------------------------------------------------------------------------

KINESIS_SHARD_HOUR = Input(
    0.015, "USD/shard-hour", "[Benchmarked]", "aws.amazon.com/kinesis/data-streams/pricing/"
)
KINESIS_PUT_UNITS_PER_MILLION = Input(
    0.014, "USD/million PUT units", "[Benchmarked]", "aws.amazon.com/kinesis/data-streams/pricing/"
)
FLINK_KPU_HOUR = Input(
    0.11, "USD/KPU-hour", "[Benchmarked]", "aws.amazon.com/managed-service-apache-flink/pricing/"
)
FLINK_STORAGE_GB_MONTH = Input(
    0.10, "USD/GB-month", "[Benchmarked]",
    "aws.amazon.com/managed-service-apache-flink/pricing/ (50GB allocated per KPU)",
)
S3_STANDARD_GB_MONTH = Input(
    0.0265, "USD/GB-month", "[Benchmarked]", "aws.amazon.com/s3/pricing/ (first 50TB tier)"
)
S3_PUT_PER_1000 = Input(
    0.005, "USD/1000 requests", "[Benchmarked]", "aws.amazon.com/s3/pricing/"
)
DDB_WRITE_PER_MILLION = Input(
    0.625, "USD/million WRU", "[Benchmarked]", "aws.amazon.com/dynamodb/pricing/on-demand/"
)
DDB_READ_PER_MILLION = Input(
    0.125, "USD/million RRU", "[Benchmarked]", "aws.amazon.com/dynamodb/pricing/on-demand/"
)
DDB_STORAGE_GB_MONTH = Input(
    0.25, "USD/GB-month", "[Benchmarked]", "aws.amazon.com/dynamodb/pricing/on-demand/"
)
ELASTICACHE_R7G_LARGE_HOUR = Input(
    0.1752, "USD/node-hour", "[Benchmarked]",
    "aws.amazon.com/elasticache/pricing/ cache.r7g.large - taken from a worked "
    "example on the page, not from a rate table; confirm before committing budget",
)

BUDGET_CEILING = Input(
    50_000, "USD/month", "[Observed]", "brief: 'Budget: $50K/month infrastructure ceiling'"
)

HOURS_PER_MONTH = 730  # [Assumed] standard AWS billing convention

# --------------------------------------------------------------------------
# Sizing assumptions
# --------------------------------------------------------------------------

KINESIS_RECORDS_PER_SHARD = Input(
    1000, "records/s/shard", "[Benchmarked]",
    "aws.amazon.com/kinesis/data-streams/pricing/: 'One shard provides an ingest "
    "capacity of 1 MB/second or 1,000 records/second'",
)
KINESIS_MB_PER_SHARD = Input(
    1.0, "MB/s/shard", "[Benchmarked]", "same page as above"
)
PARQUET_COMPRESSION = Input(
    5.0, "x", "[Assumed]",
    "columnar Parquet with Snappy over highly repetitive JSON event data; "
    "unverified for this payload shape and worth measuring in week one",
)
S3_FLUSH_SECONDS = Input(
    60, "s", "[Assumed]",
    "how often the stream job writes a file to S3; trades object count against "
    "freshness of the lake, and does not affect dashboard latency",
)
DDB_WRITES_PER_EVENT = Input(
    1.0, "writes/event", "[Assumed]",
    "naive model: one visitor-profile update per event. See WRITE AMPLIFICATION "
    "in the output - this is the single most expensive assumption in the model",
)
DDB_READ_RATIO = Input(
    0.10, "reads/event", "[Assumed]",
    "fraction of events that trigger a personalisation lookup",
)
DDB_STORAGE_GB = Input(
    500, "GB", "[Assumed]", "visitor profile store across 500 tenants, steady state"
)
FLINK_KPUS = Input(
    4, "KPU", "[Assumed]",
    "1 KPU = 1 vCPU + 4GB. Throughput per KPU is entirely job-dependent and this "
    "is the least defensible number in the model. Sensitivity is reported below",
)
ELASTICACHE_NODES = Input(
    2, "nodes", "[Assumed]", "primary plus one replica for failover"
)
LAKE_MONTHS = Input(
    12, "months", "[Assumed]", "S3 cost shown at end of year one, when it is largest"
)

# --------------------------------------------------------------------------
# Serving-tier tripwire
#
# The MVP serves dashboards from pre-aggregated rollups in PostgreSQL, which
# the team already operates. That is a REVERSIBLE decision, not a permanent
# one, and the condition under which it stops being correct is computed here
# rather than asserted in prose.
#
# The unknown is path cardinality per tenant. The brief does not contain it,
# and it is the single input that decides Postgres versus a columnar store.
# So the decision is deliberately deferred to a measured threshold.
# --------------------------------------------------------------------------

TENANTS = Input(500, "tenants", "[Observed]", "brief: 'multi-tenant architecture (500+ customers)'")
EVENT_TYPES = Input(
    4, "types", "[Observed]",
    "brief: 'page views, clicks, form submissions, custom events'",
)
PATHS_PER_TENANT = Input(
    50, "distinct paths", "[Assumed]",
    "THE UNKNOWN. A marketing site's trackable page count. The brief does not "
    "state it and it cannot be inferred from a 25-event fixture. This is the "
    "input the tripwire exists to measure in production",
)
HOT_BUCKET_SECONDS = Input(
    60, "s", "[Assumed]", "1-minute rollup granularity for the real-time dashboard"
)
HOT_RETENTION_HOURS = Input(
    48, "h", "[Assumed]",
    "how long 1-minute granularity is kept before rolling up to hourly",
)
POSTGRES_HOT_ROW_CEILING = Input(
    250_000_000, "rows", "[Assumed]",
    "the point at which a partitioned Postgres rollup table stops being "
    "comfortable for sub-second dashboard queries on modest hardware. NOT a "
    "measured limit - it is the threshold that triggers a real load test, not "
    "an automatic migration",
)
DASHBOARD_P95_SLO = Input(
    1.0, "s", "[Assumed]",
    "dashboard query p95. The product promises sub-5-second freshness; if the "
    "QUERY alone spends a second, the end-to-end budget is already half gone",
)


def tripwire() -> dict:
    """Compute when Postgres stops being the right serving tier.

    Rollup tables are SPARSE, and getting this wrong changes the architecture.
    A first pass at this model multiplied tenants x paths x event types x time
    buckets - the full cartesian product - and concluded Postgres was already
    over capacity before launch. That is wrong: it assumes every path receives
    every event type in every single minute.

    The real bound is traffic. A tenant averaging N events per bucket can
    populate at most N distinct rows in that bucket, however many paths it has.
    So rows per bucket is min(cartesian, events), and at this volume the
    EVENT COUNT binds, not the path count.

    Same class of mistake as sizing Kinesis on bandwidth instead of record
    count, in the opposite direction. Both come from modelling the shape of the
    data instead of its volume.
    """
    buckets_hot = HOT_RETENTION_HOURS.value * 3600 / HOT_BUCKET_SECONDS.value
    buckets_per_day = 86_400 / HOT_BUCKET_SECONDS.value

    cartesian_per_bucket = PATHS_PER_TENANT.value * EVENT_TYPES.value
    events_per_tenant_per_bucket = (
        EVENTS_PER_DAY.value / TENANTS.value / buckets_per_day
    )
    # Worst case: every event in a bucket lands on a distinct (path, type) pair.
    rows_per_bucket = min(cartesian_per_bucket, events_per_tenant_per_bucket)
    binding = "path cardinality" if cartesian_per_bucket < events_per_tenant_per_bucket else "event volume"

    rows_per_tenant = rows_per_bucket * buckets_hot
    hot_rows = rows_per_tenant * TENANTS.value
    ceiling = POSTGRES_HOT_ROW_CEILING.value

    # Cardinality at which the cross-product starts binding instead of volume,
    # i.e. the point where adding tracked paths begins to cost rows.
    paths_where_cardinality_binds = events_per_tenant_per_bucket / EVENT_TYPES.value

    # Traffic-per-tenant multiple that would reach the ceiling, all else equal.
    growth_to_ceiling = ceiling / hot_rows if hot_rows else None

    return {
        "hot_buckets_retained": buckets_hot,
        "cartesian_rows_per_bucket": cartesian_per_bucket,
        "events_per_tenant_per_bucket": events_per_tenant_per_bucket,
        "rows_per_bucket": rows_per_bucket,
        "binding_constraint": binding,
        "rows_per_tenant": rows_per_tenant,
        "hot_rows_modelled": hot_rows,
        "postgres_row_ceiling": ceiling,
        "utilisation": hot_rows / ceiling,
        "paths_where_cardinality_binds": paths_where_cardinality_binds,
        "headroom_multiple": growth_to_ceiling,
    }


def size(spike: float, kpus: int) -> dict:
    events_day = EVENTS_PER_DAY.value
    events_sec = events_day / 86_400
    peak_events_sec = events_sec * spike
    bytes_sec = events_sec * MEAN_EVENT_BYTES.value
    peak_mb_sec = (peak_events_sec * MEAN_EVENT_BYTES.value) / 1e6

    shards_by_records = peak_events_sec / KINESIS_RECORDS_PER_SHARD.value
    shards_by_bytes = peak_mb_sec / KINESIS_MB_PER_SHARD.value
    binding = "records/s" if shards_by_records >= shards_by_bytes else "MB/s"
    shards = int(-(-max(shards_by_records, shards_by_bytes) * HEADROOM.value // 1))

    gb_day = events_day * MEAN_EVENT_BYTES.value / 1e9
    gb_month_raw = gb_day * 30
    gb_month_compressed = gb_month_raw / PARQUET_COMPRESSION.value
    events_month = events_day * 30

    return {
        "events_per_second_avg": events_sec,
        "events_per_second_peak": peak_events_sec,
        "bytes_per_second_avg": bytes_sec,
        "peak_mb_per_second": peak_mb_sec,
        "shards_by_records": shards_by_records,
        "shards_by_bytes": shards_by_bytes,
        "binding_constraint": binding,
        "shards_provisioned": shards,
        "gb_per_day_raw": gb_day,
        "gb_per_month_raw": gb_month_raw,
        "gb_per_month_compressed": gb_month_compressed,
        "events_per_month": events_month,
        "kpus": kpus,
    }


def cost(s: dict) -> dict:
    events_month = s["events_per_month"]

    kinesis_shards = s["shards_provisioned"] * KINESIS_SHARD_HOUR.value * HOURS_PER_MONTH
    # Each event is far below the 25KB PUT payload unit, so one event = one unit.
    kinesis_puts = (events_month / 1e6) * KINESIS_PUT_UNITS_PER_MILLION.value

    flink_compute = s["kpus"] * FLINK_KPU_HOUR.value * HOURS_PER_MONTH
    flink_storage = s["kpus"] * 50 * FLINK_STORAGE_GB_MONTH.value

    lake_gb = s["gb_per_month_compressed"] * LAKE_MONTHS.value
    s3_storage = lake_gb * S3_STANDARD_GB_MONTH.value
    s3_puts = ((86_400 / S3_FLUSH_SECONDS.value) * 30 / 1000) * S3_PUT_PER_1000.value

    ddb_writes = (events_month * DDB_WRITES_PER_EVENT.value / 1e6) * DDB_WRITE_PER_MILLION.value
    ddb_reads = (events_month * DDB_READ_RATIO.value / 1e6) * DDB_READ_PER_MILLION.value
    ddb_storage = DDB_STORAGE_GB.value * DDB_STORAGE_GB_MONTH.value

    cache = ELASTICACHE_NODES.value * ELASTICACHE_R7G_LARGE_HOUR.value * HOURS_PER_MONTH

    lines = {
        "Kinesis Data Streams - shards": kinesis_shards,
        "Kinesis Data Streams - PUT units": kinesis_puts,
        "Managed Flink - compute": flink_compute,
        "Managed Flink - state storage": flink_storage,
        "S3 event lake - storage (end of yr 1)": s3_storage,
        "S3 event lake - requests": s3_puts,
        "DynamoDB - profile writes": ddb_writes,
        "DynamoDB - personalisation reads": ddb_reads,
        "DynamoDB - storage": ddb_storage,
        "ElastiCache Redis - nodes": cache,
    }
    return lines


def render(s: dict, lines: dict, spike: float) -> str:
    out: list[str] = []
    add = out.append
    total = sum(lines.values())
    ceiling = BUDGET_CEILING.value

    add("=" * 78)
    add("AWS COST MODEL - real-time analytics pipeline")
    add("=" * 78)
    add(f"region         : {PRICE_REGION}")
    add(f"prices retrieved: {PRICES_RETRIEVED} from the AWS pricing pages cited below")
    add(f"generated_at   : {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    add("")
    add("-" * 78)
    add("SIZING")
    add("-" * 78)
    add(f"  events/day                     {EVENTS_PER_DAY.value:>15,.0f}   [Observed] brief")
    add(f"  mean event size                {MEAN_EVENT_BYTES.value:>15,.0f} B [Observed] measured on the fixture")
    add(f"  spike multiplier               {spike:>15,.0f} x [Observed] brief")
    add("")
    add(f"  average events/sec             {s['events_per_second_avg']:>15,.0f}")
    add(f"  peak events/sec ({spike:.0f}x)          {s['events_per_second_peak']:>15,.0f}")
    add(f"  peak throughput                {s['peak_mb_per_second']:>15,.2f} MB/s")
    add(f"  raw volume                     {s['gb_per_day_raw']:>15,.1f} GB/day")
    add("")
    add(f"  shards needed by records/s     {s['shards_by_records']:>15,.1f}")
    add(f"  shards needed by MB/s          {s['shards_by_bytes']:>15,.1f}")
    add(f"  BINDING CONSTRAINT             {s['binding_constraint']:>15}")
    add(f"  shards provisioned (+{(HEADROOM.value - 1) * 100:.0f}% head) {s['shards_provisioned']:>15,d}")
    add("")
    add("  Note: record COUNT binds, not bandwidth. At 229 bytes an event, a shard")
    add("  saturates its 1,000 records/s limit while using ~23% of its 1 MB/s. Sizing")
    add("  this on bandwidth would under-provision by roughly 4x.")
    add("")
    add("-" * 78)
    add("MONTHLY COST")
    add("-" * 78)
    for name, value in lines.items():
        add(f"  {name:<42}{value:>12,.2f}")
    add(f"  {'':<42}{'-' * 12:>12}")
    add(f"  {'TOTAL':<42}{total:>12,.2f}")
    add("")
    add(f"  budget ceiling                            {ceiling:>12,.2f}   [Observed] brief")
    add(f"  headroom remaining                        {ceiling - total:>12,.2f}")
    add(f"  ceiling consumed                          {total / ceiling * 100:>11,.1f}%")
    add("")
    add("-" * 78)
    add("WHAT THIS ACTUALLY TELLS YOU")
    add("-" * 78)
    add(f"  The modelled spend is {total / ceiling * 100:.1f}% of the stated ceiling. Even if every")
    add("  [Assumed] input here is wrong by 5x, the total stays under budget.")
    add("")
    add("  So the $50K/month ceiling is NOT the binding constraint on this design.")
    add("  Two dedicated engineers and a three-month MVP window are.")
    add("")
    add("  That inverts a normal build-vs-buy decision. Self-managed Kafka on EC2 is")
    add("  cheaper per byte than Kinesis, but it spends the resource that is actually")
    add("  scarce: engineer-hours. With 96% of the budget unused, paying AWS list")
    add("  price to avoid operating a broker is straightforwardly correct here.")
    add("")
    add("-" * 78)
    add("WRITE AMPLIFICATION - the number worth arguing about")
    add("-" * 78)
    ddb_total = (lines["DynamoDB - profile writes"] + lines["DynamoDB - personalisation reads"]
                 + lines["DynamoDB - storage"])
    add(f"  DynamoDB is {ddb_total / total * 100:.0f}% of the total, and profile writes alone are")
    add(f"  ${lines['DynamoDB - profile writes']:,.0f}/month, because the model assumes one write per event.")
    add("")
    add("  That assumption is a design choice, not a fact. If the stream job keeps")
    add("  per-visitor state and flushes on session end rather than per event, the")
    add("  write count drops by roughly the average events-per-session. The cost")
    add("  model is what surfaces that: it is not visible from the architecture")
    add("  diagram, only from the arithmetic.")
    add("")
    add("-" * 78)
    add("SENSITIVITY")
    add("-" * 78)
    for factor in (2, 5, 10):
        scaled = total * factor
        verdict = "under ceiling" if scaled < ceiling else "OVER CEILING"
        add(f"  every assumption wrong by {factor:>2}x  ->  ${scaled:>10,.0f}/month   {verdict}")
    add("")
    add(f"  The conclusion survives to {int(ceiling / total)}x. That is what makes it safe to say")
    add("  the budget is not the constraint, rather than merely hoping so.")
    add("")
    add("-" * 78)
    add("DELIBERATELY NOT PRICED")
    add("-" * 78)
    add("  A managed OLAP serving tier (OpenSearch Service or equivalent) and Aurora")
    add("  PostgreSQL are both part of the design but are NOT in the total above.")
    add("  AWS's published pricing pages did not render their rate tables when")
    add("  checked on " + PRICES_RETRIEVED + ", and inventing a plausible figure would be worse")
    add("  than leaving a visible hole. These are a look-it-up before committing")
    add("  budget, not an estimate.")
    add("")
    add("  Given the headroom above, neither is likely to change the conclusion -")
    add("  but that is a prediction, not a calculation, and it is labelled as such.")
    add("")
    t = tripwire()
    add("-" * 78)
    add("SERVING-TIER TRIPWIRE - when Postgres stops being the right answer")
    add("-" * 78)
    add("  The MVP serves dashboards from pre-aggregated rollups in PostgreSQL,")
    add("  which this team already runs. That is a reversible decision. Here is the")
    add("  condition that reverses it, computed rather than asserted.")
    add("")
    add(f"  1-minute buckets retained hot        {t['hot_buckets_retained']:>15,.0f}   ({HOT_RETENTION_HOURS.value:.0f}h)")
    add(f"  cartesian rows/bucket/tenant        {t['cartesian_rows_per_bucket']:>15,.0f}   (paths x event types)")
    add(f"  events/bucket/tenant                {t['events_per_tenant_per_bucket']:>15,.0f}   (traffic bound)")
    add(f"  rows/bucket/tenant = min of those   {t['rows_per_bucket']:>15,.0f}")
    add(f"  BINDING CONSTRAINT                  {t['binding_constraint']:>15}")
    add("")
    add(f"  rollup rows per tenant              {t['rows_per_tenant']:>15,.0f}")
    add(f"  hot rollup rows, 500 tenants        {t['hot_rows_modelled']:>15,.0f}")
    add(f"  Postgres comfort ceiling            {t['postgres_row_ceiling']:>15,.0f}   [Assumed]")
    add(f"  utilisation                         {t['utilisation'] * 100:>14,.1f}%")
    add(f"  headroom                            {t['headroom_multiple']:>14,.1f}x")
    add("")
    add("  Rollups are SPARSE. A tenant averaging 69 events a minute cannot fill")
    add("  200 (path x type) buckets, so row count is bounded by TRAFFIC, not by")
    add("  how many pages the customer tracks. Modelling the cross-product instead")
    add("  overstates this by ~3x and would have picked the wrong architecture.")
    add("")
    add("  THE TRIPWIRE, three conditions. Any one fires a migration review:")
    add("")
    add(f"    1. CAPACITY   hot rollup rows exceed {t['postgres_row_ceiling']:,.0f}, which at")
    add(f"                  current shape means ~{t['headroom_multiple']:,.1f}x growth in events per tenant.")
    add(f"                  Path cardinality only starts to matter above ~{t['paths_where_cardinality_binds']:,.0f} paths")
    add(f"                  per tenant (modelled at {PATHS_PER_TENANT.value:,.0f}); below that, traffic binds")
    add(f"    2. LATENCY    dashboard query p95 exceeds {DASHBOARD_P95_SLO.value:.1f}s over a 7-day window")
    add("    3. FRESHNESS  rollup write lag exceeds 30s at p95")
    add("")
    add("  Condition 1 is a leading indicator - it fires before customers feel")
    add("  anything. Conditions 2 and 3 are trailing: by then they already have.")
    add("  Alerting on 1 is the point of computing it.")
    add("")
    add("  WHY DEFER RATHER THAN COMMIT: growth in events per tenant is what")
    add("  decides this, and the brief gives a fleet-wide total rather than a")
    add("  per-tenant distribution. 500 tenants averaging 100k events/day is a")
    add("  very different system from 20 tenants at 2M and 480 at 8k, and the")
    add("  brief does not say which it is. Committing to a columnar store on day")
    add("  one is not more rigorous than deferring - it is guessing with more")
    add("  confidence. The MVP ships on infrastructure the team already operates,")
    add("  and the threshold that would prove that wrong is instrumented in week")
    add("  one. Per-tenant distribution is the first thing I would measure.")
    add("")
    add("  This is an option held, not a migration scheduled. It may never fire.")
    add("=" * 78)
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="AWS cost model sized against the brief's ceiling")
    parser.add_argument("--spike", type=float, default=SPIKE_MULTIPLIER.value,
                        help="peak traffic multiplier over average")
    parser.add_argument("--kpus", type=int, default=int(FLINK_KPUS.value),
                        help="Flink KPUs provisioned")
    parser.add_argument("--out", type=Path, default=Path("results"))
    args = parser.parse_args(argv)

    s = size(args.spike, args.kpus)
    lines = cost(s)
    text = render(s, lines, args.spike)

    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "cost_model.txt").write_text(text, encoding="utf-8", newline="\n")
    (args.out / "cost_model.json").write_text(
        json.dumps(
            {
                "region": PRICE_REGION,
                "prices_retrieved": PRICES_RETRIEVED,
                "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "sizing": s,
                "monthly_cost": lines,
                "total": sum(lines.values()),
                "ceiling": BUDGET_CEILING.value,
                "inputs": {
                    name: asdict(value)
                    for name, value in sorted(globals().items())
                    if isinstance(value, Input)
                },
            },
            indent=2,
        ),
        encoding="utf-8",
        newline="\n",
    )

    print(text)
    print(f"wrote {args.out / 'cost_model.txt'}")
    print(f"wrote {args.out / 'cost_model.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
