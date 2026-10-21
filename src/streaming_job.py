"""
CIS 531/731 — MP 5
Fault-tolerant Structured Streaming job over the replayed landing zone.

Three concurrent queries, all keyed on event time (ts_end) with a watermark
derived from the measured lateness distribution (see analyze_lateness.py):

  QA  record grain,  60s tumbling      -> record count, mean ingest latency,
                                          approx_count_distinct(source_id)
  QB  token grain,   60s tumbling      -> approx_count_distinct(token),
                                          total token count
  QC  token grain,   5min / 1min slide -> per-(window, token) counts; the
                                          top-10 is taken at READ time

Why QC does not compute the top-10 in-stream
--------------------------------------------
A streaming top-K per window would have to hold every candidate term for every
open window in state until that window closes, then sort. Spark has no
incremental top-K aggregator, so the only in-stream formulation is a full
per-(window, token) count followed by a windowed rank — and a rank over a
streaming aggregate is not supported in append mode at all. Counting
per-(window, token) keeps state bounded by (open windows x vocabulary) and
defers the ordering to a cheap batch read over the finished Parquet. The
numbers are identical; only the place the sort happens moves.

Why record grain and token grain are separate queries
-----------------------------------------------------
Exact COUNT(DISTINCT ...) is not supported in a streaming aggregation, and
exploding text to tokens destroys the record grain (count(*) would become a
token count). Running the two grains as two queries over the same source is
the honest formulation; collapsing them into one would require an approximate
distinct count of record_id in place of an exact record count.

Fault injection
---------------
--kill-after-batches N hard-kills the driver with os._exit(1) once QA has
committed N batches. os._exit bypasses atexit handlers, shutdown hooks and
graceful query termination, so the restart exercises checkpoint recovery
rather than a clean stop.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from contracts import CORPUS_CHUNK_SCHEMA, admission_filter, violation_reasons

LANDING = Path("landing")
REPLAY = Path("replay")
OUT = Path("out")
CKPT = Path("checkpoints")
METRICS = OUT / "metrics.jsonl"


def start_feeder(interval: float, stop: threading.Event) -> threading.Thread:
    """Stage landing-zone files into the replay directory one at a time.

    WHY THIS EXISTS. Pointing readStream at a directory that already holds
    every file does not produce a controlled-rate replay, and — more
    importantly — it produces no defined arrival order. FileStreamSource takes
    unread files in whatever order the filesystem listing returns, so which
    records arrive behind the watermark changes with the listing and cannot be
    predicted from the filenames. An earlier version of this job did exactly
    that; the reconciliation could not account for 163 records and the engine's
    own drop counter disagreed with the per-window deficit.

    Staging one file at a time fixes both problems at once. A file that does
    not yet exist cannot be read, so arrival order is the staging order by
    construction; and with maxFilesPerTrigger=1 each micro-batch holds exactly
    one file, which makes the watermark in force for every record predictable
    and the reconciliation exact.

    Writing to a temp name and renaming matters: a file source can otherwise
    pick up a half-written file and parse a truncated final line.
    """
    REPLAY.mkdir(parents=True, exist_ok=True)

    def run() -> None:
        for src in sorted(LANDING.glob("part-*.jsonl")):
            if stop.is_set():
                return
            dst = REPLAY / src.name
            if dst.exists():      # already staged before the kill
                continue
            tmp = REPLAY / (src.name + ".staging")
            shutil.copyfile(src, tmp)
            tmp.rename(dst)
            time.sleep(interval)

    t = threading.Thread(target=run, name="replay-feeder", daemon=True)
    t.start()
    return t


def build_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("mp5-structured-streaming")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        # 2 cores, 3 concurrent queries: a high shuffle-partition count would
        # spend the whole micro-batch budget on task scheduling.
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def tokens(df: DataFrame) -> DataFrame:
    """Explode text to lowercase tokens. Grain changes: one row per token."""
    return (
        df.withColumn(
            "token_arr", F.split(F.lower(F.col("text")), r"[^a-z0-9']+")
        )
        .withColumn("token", F.explode("token_arr"))
        .where(F.length(F.col("token")) >= 2)
        .drop("token_arr")
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--watermark-sec", type=int, required=True,
                    help="Derived from measured p99; see out/lateness.json")
    ap.add_argument("--max-files-per-trigger", type=int, default=1,
                    help="One file per micro-batch keeps the watermark in "
                         "force for each record predictable.")
    ap.add_argument("--feed-interval", type=float, default=1.1,
                    help="Seconds between staged files. Must exceed the "
                         "trigger interval, or a batch picks up two files.")
    ap.add_argument("--kill-after-batches", type=int, default=0,
                    help="0 = run to completion; N = hard-kill after N "
                         "committed QA batches.")
    ap.add_argument("--phase", default="run",
                    help="Label written into every metrics.jsonl row.")
    ap.add_argument("--idle-timeout", type=float, default=25.0,
                    help="Seconds with no new batch before declaring the "
                         "replay drained.")
    a = ap.parse_args()

    spark = build_session()
    OUT.mkdir(parents=True, exist_ok=True)

    stop_feeder = threading.Event()
    feeder = start_feeder(a.feed_interval, stop_feeder)

    wm = f"{a.watermark_sec} seconds"

    raw = (
        spark.readStream
        # Explicit schema. Schema inference on a file source re-reads files to
        # guess types and is not stable across micro-batches.
        .schema(CORPUS_CHUNK_SCHEMA)
        .option("maxFilesPerTrigger", a.max_files_per_trigger)
        # Only completed files; the feeder renames into place, but this also
        # excludes any .staging residue from a killed run.
        .option("pathGlobFilter", "part-*.jsonl")
        .json(str(REPLAY))
    )

    gate = admission_filter()

    enriched = raw.withColumn(
        "ingest_latency_sec",
        F.col("ingest_ts").cast("double") - F.col("ts_end").cast("double"),
    )

    admitted = enriched.where(gate)
    quarantined = enriched.where(~gate).withColumn(
        "violations", violation_reasons()
    )

    # ---------------------------------------------------------------- QA
    qa_df = (
        admitted.withWatermark("ts_end", wm)
        .groupBy(F.window("ts_end", "60 seconds").alias("w"))
        .agg(
            F.count(F.lit(1)).alias("record_count"),
            F.mean("ingest_latency_sec").alias("mean_ingest_latency_sec"),
            F.approx_count_distinct("source_id").alias("approx_distinct_sources"),
            F.min("ts_end").alias("min_ts_end"),
            F.max("ts_end").alias("max_ts_end"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "record_count",
            F.round("mean_ingest_latency_sec", 4).alias("mean_ingest_latency_sec"),
            "approx_distinct_sources",
            "min_ts_end",
            "max_ts_end",
        )
    )

    # ---------------------------------------------------------------- QB
    tok = tokens(admitted)
    qb_df = (
        tok.withWatermark("ts_end", wm)
        .groupBy(F.window("ts_end", "60 seconds").alias("w"))
        .agg(
            F.count(F.lit(1)).alias("token_count"),
            F.approx_count_distinct("token").alias("approx_distinct_tokens"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "token_count",
            "approx_distinct_tokens",
        )
    )

    # ---------------------------------------------------------------- QC
    qc_df = (
        tok.withWatermark("ts_end", wm)
        .groupBy(
            F.window("ts_end", "5 minutes", "1 minute").alias("w"),
            F.col("token"),
        )
        .agg(F.count(F.lit(1)).alias("term_count"))
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "token",
            "term_count",
        )
    )

    # ------------------------------------------------------- quarantine sink
    # Record grain, no aggregation, so append mode needs no watermark. A
    # violating record is written out with the names of the contracts it
    # failed; nothing is dropped silently.
    qz_df = quarantined.select(
        "record_id", "track", "source_id", "ts_start", "ts_end",
        "license_class", "ingest_ts", "violations",
    )

    def sink(df: DataFrame, name: str, mode: str = "append"):
        return (
            df.writeStream.queryName(name)
            .outputMode(mode)
            .format("parquet")
            .option("path", str(OUT / name))
            .option("checkpointLocation", str(CKPT / name))
            .trigger(processingTime="1 second")
            .start()
        )

    qa = sink(qa_df, "qa_tumbling_60s")
    qb = sink(qb_df, "qb_tokens_60s")
    qc = sink(qc_df, "qc_sliding_5m_1m")
    qz = sink(qz_df, "quarantine")
    queries = [qa, qb, qc, qz]

    # ------------------------------------------------------------- metrics
    # Progress is appended per batch rather than collected at the end: a hard
    # kill destroys the in-memory recentProgress ring buffer, so anything not
    # already on disk is lost. This is also why the metrics log is the
    # evidence for the liveness targets (INV-5) rather than the contracts.
    seen: dict[str, set[int]] = {q.name: set() for q in queries}
    fh = METRICS.open("a", encoding="utf-8")

    def drain_progress() -> None:
        for q in queries:
            for p in q.recentProgress:
                bid = p.get("batchId")
                if bid is None or bid in seen[q.name]:
                    continue
                seen[q.name].add(bid)
                so = p.get("stateOperators") or [{}]
                row = {
                    "phase": a.phase,
                    "query": q.name,
                    "batchId": bid,
                    "timestamp": p.get("timestamp"),
                    "numInputRows": p.get("numInputRows"),
                    "inputRowsPerSecond": p.get("inputRowsPerSecond"),
                    "processedRowsPerSecond": p.get("processedRowsPerSecond"),
                    # StreamingQueryProgress has no top-level "batchDuration";
                    # the figure the spec asks for is durationMs.triggerExecution,
                    # the wall-clock cost of the whole trigger. Reading
                    # p["batchDuration"] silently yields None, which is how an
                    # all-null metrics column gets into a submitted report.
                    "batchDuration": (p.get("durationMs") or {})
                        .get("triggerExecution"),
                    "durationMs": p.get("durationMs"),
                    "stateOperators.numRowsTotal":
                        so[0].get("numRowsTotal") if so else None,
                    "stateOperators.numRowsUpdated":
                        so[0].get("numRowsUpdated") if so else None,
                    "stateOperators.memoryUsedBytes":
                        so[0].get("memoryUsedBytes") if so else None,
                    # Spark's own count of rows discarded for arriving behind
                    # the watermark. This is the authoritative completeness
                    # figure: it is the only place the engine reports what the
                    # watermark actually cost, and the reconciliation balances
                    # against it rather than against a prediction.
                    "stateOperators.numRowsDroppedByWatermark":
                        so[0].get("numRowsDroppedByWatermark") if so else None,
                    "watermark": (p.get("eventTime") or {}).get("watermark"),
                }
                fh.write(json.dumps(row) + "\n")
        fh.flush()
        os.fsync(fh.fileno())

    last_change = time.time()
    last_total = -1
    print(f"[{a.phase}] watermark={wm} "
          f"maxFilesPerTrigger={a.max_files_per_trigger} "
          f"kill_after={a.kill_after_batches or 'none'}", flush=True)

    while True:
        time.sleep(0.5)
        drain_progress()
        total = sum(len(s) for s in seen.values())

        if a.kill_after_batches and len(seen[qa.name]) >= a.kill_after_batches:
            qa_in = sum(1 for _ in seen[qa.name])
            print(f"[{a.phase}] HARD KILL after {qa_in} QA batches "
                  f"(os._exit(1), no graceful stop)", flush=True)
            fh.flush()
            os.fsync(fh.fileno())
            fh.close()
            os._exit(1)

        if total != last_total:
            last_total = total
            last_change = time.time()
        elif not feeder.is_alive() and time.time() - last_change > a.idle_timeout:
            staged = len(list(REPLAY.glob("part-*.jsonl")))
            print(f"[{a.phase}] replay drained ({total} batches across "
                  f"{len(queries)} queries, {staged} files staged)",
                  flush=True)
            break

        if not all(q.isActive for q in queries):
            dead = [q.name for q in queries if not q.isActive]
            print(f"[{a.phase}] query terminated unexpectedly: {dead}",
                  flush=True)
            for q in queries:
                if q.exception():
                    print(q.exception(), flush=True)
            break

    stop_feeder.set()
    for q in queries:
        if q.isActive:
            q.stop()
    drain_progress()
    fh.close()
    print(f"[{a.phase}] stopped cleanly", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
