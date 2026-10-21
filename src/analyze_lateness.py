"""
CIS 531/731 — MP 5
Execution Step 3: Event-Time Calibration.

Measures the lateness distribution (ingest_ts - ts_end) over the landing zone
and derives the watermark delay from it.

Two decisions are baked in here and justified in report.md:

  1. Lateness is measured on ADMITTED records only. A record with a reversed
     interval has a meaningless ts_end, so its "lateness" is an artifact of the
     corruption, not of the arrival process. Measuring the distribution on
     unvalidated input would set the watermark from noise.

  2. The watermark is set to p99 rounded up to the nearest 30s, not to the
     observed maximum. The maximum of a heavy-tailed sample is a property of
     the sample size, not of the population; p99 is stable across replays.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from contracts import CORPUS_CHUNK_SCHEMA, admission_filter, violation_reasons

LANDING = "landing"
OUT = Path("out/lateness.json")


def main() -> None:
    spark = (
        SparkSession.builder.appName("mp5-event-time-calibration")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Batch read with the SAME explicit schema the stream uses. No inference:
    # inferred schemas are not stable across micro-batches and would let a
    # malformed file silently change column types mid-run.
    raw = spark.read.schema(CORPUS_CHUNK_SCHEMA).json(LANDING)

    total = raw.count()

    gate = admission_filter()
    admitted = raw.where(gate)
    rejected = raw.where(~gate | gate.isNull())

    n_admitted = admitted.count()
    n_rejected = total - n_admitted

    # Which contracts actually fired, and how often.
    by_reason = (
        raw.where(~gate | gate.isNull())
        .select(F.explode(violation_reasons()).alias("contract"))
        .groupBy("contract")
        .count()
        .orderBy(F.desc("count"))
        .collect()
    )

    lat = admitted.withColumn(
        "lateness_sec",
        F.col("ingest_ts").cast("double") - F.col("ts_end").cast("double"),
    )

    # relativeError=0.0 -> exact percentiles. The fixture is 1k records; on a
    # real corpus this would be relaxed to 0.001.
    p = lat.stat.approxQuantile("lateness_sec", [0.5, 0.95, 0.99], 0.0)
    p50, p95, p99 = p

    stats = lat.select(
        F.min("lateness_sec").alias("min"),
        F.max("lateness_sec").alias("max"),
        F.mean("lateness_sec").alias("mean"),
        F.stddev("lateness_sec").alias("stddev"),
    ).collect()[0]

    # Watermark: p99 rounded UP to the next 30s boundary.
    wm = int((p99 // 30 + 1) * 30)

    # What does that watermark actually cost us? Count the admitted records
    # whose lateness exceeds it — those are the records a real stream would
    # drop. This is the completeness side of the trade-off, measured.
    dropped = lat.where(F.col("lateness_sec") > F.lit(wm)).count()

    # Event-time span, needed for the state-growth bound in the report.
    span = admitted.select(
        F.min("ts_start").alias("t0"), F.max("ts_end").alias("t1")
    ).collect()[0]
    span_sec = (span["t1"] - span["t0"]).total_seconds()

    distinct_sources = admitted.select("source_id").distinct().count()

    result = {
        "records_total": total,
        "records_admitted": n_admitted,
        "records_quarantined": n_rejected,
        "quarantine_rate": round(n_rejected / total, 4),
        "violations_by_contract": {r["contract"]: r["count"] for r in by_reason},
        "lateness_sec": {
            "p50": round(p50, 3),
            "p95": round(p95, 3),
            "p99": round(p99, 3),
            "min": round(float(stats["min"]), 3),
            "max": round(float(stats["max"]), 3),
            "mean": round(float(stats["mean"]), 3),
            "stddev": round(float(stats["stddev"]), 3),
        },
        "watermark_delay_sec": wm,
        "watermark_rule": "ceil(p99 / 30s) * 30s",
        "records_beyond_watermark": dropped,
        "completeness_at_watermark": round(1 - dropped / n_admitted, 5),
        "event_time_span_sec": span_sec,
        "distinct_source_ids": distinct_sources,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))

    spark.stop()


if __name__ == "__main__":
    main()
