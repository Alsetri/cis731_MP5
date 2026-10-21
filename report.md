# MP 5 — Data Analytics Pipelines (Structured Streaming)

**Abdulla Mansoor · CIS 731 · abdulla03@ksu.edu · Track 2 (VDB & Trope Ontologies) · Due Tue 20 Oct 2026**

Reproduce with `./run_all.sh`: it wipes all state, regenerates the seeded fixture, derives the watermark, runs the stream, hard-kills it, restarts it, reconciles, and exits non-zero if any invariant fails. Every figure below is read from `out/lateness.json`, `out/reconciliation.json` and `out/metrics_summary.json`; none is typed by hand.

**Fixture disclosure.** The MP 3 corpus snapshot is still being assembled by Sub-Team A, so the landing zone is a seeded synthetic `CorpusChunk` fixture (1,000 records, `seed=20261020`), not scraped lyrics. It conforms to the PS 4 contract exactly and carries a known injected violation population so the quarantine path is exercised rather than asserted. Only the landing-zone path changes when the real corpus lands.

## Schema and Contracts

`CORPUS_CHUNK_SCHEMA` (`src/contracts.py`) is passed explicitly to `readStream`; inference re-reads files to guess types and is not stable across micro-batches.

| field | type | non-null because |
|---|---|---|
| `record_id` | String | A chunk without identity cannot be deduplicated on restart, breaking exactly-once. |
| `track` | String | PS 6 and MP 7 consume this stream and branch on it. |
| `source_id` | String | The whitelist gate is evaluated against it; a null is unauditable by construction. |
| `ts_start`, `ts_end` | Timestamp | `ts_end` is the watermark column; a null watermark column silently disables watermarking for that row. |
| `text` | String | An empty chunk yields no tokens but still occupies window state. |
| `ingest_ts` | Timestamp | Lateness (`ingest_ts − ts_end`) is what the watermark is derived from. |
| `license_class` | String | Unlicensed text must not reach the vector store, so "unknown" is an explicit sentinel, never null. |

**`nullable=False` enforces nothing** — it is a statement of intent. The file source will materialise a null into a non-nullable field and fail later at an arbitrary point downstream. Enforcement is seven `deal` contracts run as an admission filter immediately after the source: `TemporalOrdering` (`ts_start < ts_end`), `BoundedChunking` (`0 < duration ≤ 30 s`), `Provenance` (`record_id`/`source_id` present, non-blank), `SourceWhitelist` (approved prefix, INV-10), `LicenseGate`, `NonEmptyText`, and `IngestCausality` (`ingest_ts ≥ ts_end − 2 s`, guarding the lateness distribution against corrupt producer clocks). Violations are routed to a quarantine sink **with the names of the contracts they failed**, never dropped: a dropped record is indistinguishable from one never produced, which makes reconciliation impossible.

Two things only running it reveals. (1) `admission_filter()` is wrapped in `coalesce(..., False)`: Spark's `AND` is three-valued, so a null anywhere yields null, and a null-gated row is admitted by neither `where(gate)` nor `where(not gate)` — it would fall out of **both** branches and vanish from every count. (2) `violation_reasons()` cannot use `array_remove(array(...), None)` — `array_remove` returns null when the element to remove is null, so the column comes back null and every reason is silently lost; `filter(..., isNotNull)` is correct. The first run reported 57 quarantined records and zero reasons, caught only by reconciling per-contract counts against the manifest.

**943 admitted, 57 quarantined (5.7%).** The 57 match the generator's ground truth exactly, and the quarantined *set* is identical to that of a second, independent plain-Python implementation of the contracts in `reconcile.py` — deliberately not a call into `contracts.py`, since sharing one predicate proves only that it is deterministic, not that it is right. The 57 records carry 84 contract hits, so 27 violate more than one.

## Watermark and State Analysis

Lateness over admitted records only — a reversed interval has a meaningless `ts_end`, so its "lateness" is an artifact of the corruption:

| p50 | p95 | p99 | max | mean | σ |
|---|---|---|---|---|---|
| 4.147 s | 26.098 s | 160.591 s | 592.900 s | 10.758 s | 34.591 s |

**Watermark = 180 s**, from `ceil(p99 / 30 s) × 30 s`. Not the observed maximum: the maximum of a heavy-tailed sample is a property of sample size, not population, so a 593 s watermark would be tuned to one record and triple state retention.

**Trade-off, measured.** At 180 s the engine discarded **4 of 943 admitted records (99.58% completeness)**, and every window waits 180 s past its close before emission. Raising the delay to 600 s recovers those 4 and costs every window 7 further minutes; dropping to p95 (30 s) publishes 2.5 minutes sooner and loses roughly an order of magnitude more.

**The drop rule is on the window, not the record.** Spark discards a row because the row's *window* has been evicted, not because the row is behind the watermark: the late-data filter compares window end against the watermark, so a record 30 s late survives if its window ends 34 s ahead. Effective tolerance is therefore the watermark **plus up to one window length**, and the unit of loss is a whole window. Comparing `ts_end` directly — the intuitive reading — over-predicts the drop set.

**Output mode: append**, required rather than chosen. Append emits each window key exactly once, when the watermark passes it, which is what makes a file sink idempotent. `update`/`complete` would re-emit evolving keys into an append-only Parquet sink and produce duplicate keys every batch — precisely the failure the reconciliation tests for.

**State-growth bound.** Open window keys ≤ `ceil((watermark + window) / window)`, plus the batch in flight and the one-batch watermark lag:

| query | bound | observed max |
|---|---|---|
| QA, QB (60 s tumbling) | `ceil((180+60)/60) + 2 = 6` keys | **6** |
| QC (5 min / 1 min sliding) | `(ceil((180+300)/60) + 2) × 36 tokens = 360` rows | **352** |

State is bounded by the watermark and live vocabulary, not by corpus size — the property that matters for a term project corpus of 10–14k tracks.

## Query Design

| | grain | window | aggregates |
|---|---|---|---|
| **QA** | record | 60 s tumbling | `count(*)`, `mean(ingest_latency_sec)`, `approx_count_distinct(source_id)`, min/max `ts_end` |
| **QB** | token (exploded) | 60 s tumbling | `count(*)`, `approx_count_distinct(token)` |
| **QC** | token | 5 min, sliding 1 min | per-`(window, token)` count → top-10 at read time |
| **Quarantine** | record | none | violating records + reason array |

Two grains need two queries: exact `COUNT(DISTINCT …)` is unsupported in a streaming aggregation, and exploding to tokens destroys the record grain — `count(*)` would become a token count. Merging QA and QB would mean reporting an *approximate* distinct count of `record_id` instead of an exact record count.

**Top-10 is computed at read time from Parquet, not in-stream.** A streaming top-K must hold every candidate term for every open window until it closes and then sort; Spark has no incremental top-K, and ranking a streaming aggregate is unsupported in append mode. Counting per-`(window, token)` bounds state by (open windows × vocabulary) and defers only the ordering — identical numbers, different place for the sort. Measured: `trap` 13,656 · `drill` 7,306 · `boom` 5,381 · `bap` 4,017 · `funk` 3,376 · `hyphy` 2,852 · `crunk` 2,707 · `memphis` 2,236 · `atlanta` 2,102 · `compton` 1,898.

**Replay design.** The job stages landing-zone files into `replay/` one at a time (temp name then rename, or the source parses a truncated final line) with `maxFilesPerTrigger=1`. Pointing `readStream` at a directory already holding every file is not a controlled-rate replay and has **no defined arrival order** — `FileStreamSource` takes unread files in filesystem-listing order. An earlier version did that, with concrete consequences: written in event-time order the watermark never fell behind any record and was decorative; repartitioned by `ingest_ts`, the reconciliation could not account for 163 records because the drop set was unpredictable. Staging fixes both — a file that does not exist cannot be read. The fixture is partitioned by `ingest_ts`, so **372 of 1,000 records arrive out of event-time order**, making the completeness figure real rather than a tautological zero. Sinks are append-mode Parquet with one `checkpointLocation` per query; each query keeps its own source offsets and resumes independently.

## Fault-Tolerance Reconciliation

**Procedure.** `--kill-after-batches 10` calls `os._exit(1)` once QA has committed 10 batches, bypassing `atexit`, JVM shutdown hooks and graceful termination, so the restart exercises checkpoint recovery rather than a clean stop. The job is then restarted with identical arguments and no cleanup.

| sink | rows | distinct keys | key |
|---|---|---|---|
| `qa_tumbling_60s` | 46 | 46 | `window_start` |
| `qb_tokens_60s` | 46 | 46 | `window_start` |
| `qc_sliding_5m_1m` | 1,655 | 1,655 | `(window_start, token)` |
| `quarantine` | 57 | 57 | `record_id` |

**Closed balance over all 1,000 records:** emitted **875** + dropped behind the watermark **4** + held in 4 windows still open at end-of-stream **64** + quarantined **57** = **1,000**. Also checked: no emitted window is absent from the batch ground truth; none holds *more* records than the batch truth (which would mean double processing); every un-emitted window is one the watermark had not passed; and the engine's `numRowsDroppedByWatermark` (4) equals the independently computed per-window deficit (4).

**Restart continuity.** QA committed batches 0–62 contiguously — last pre-kill batch 9, first post-restart batch 10 — with **zero batches reprocessed across any of the four queries**. An independent reimplementation of Spark's watermark semantics reproduces the engine's reported watermark for **all 40 non-empty QA batches**, the same 46 closed windows and the same final watermark (14:46:15Z), and predicts 4 of the 5 drops. The fifth (`c000306`: window ending 14:16:00 against a reported watermark of 14:16:05) was admitted. The late-data filter's boundary at a sub-batch margin is not determinable from the progress stream, so this is reported as a 1-record residual rather than resolved by fitting the rule to one observation; the accounting above does not depend on it.

**Which guarantee comes from where.** *Checkpointing* gives at-least-once: offsets and state commit before a batch is acknowledged, so a restart resumes at the next uncommitted batch and loses nothing — but it does not stop a partially written batch being re-executed. The *idempotent sink* supplies the rest: the Parquet sink's `_spark_metadata` log names the files belonging to each committed batch and readers honour it, so files from a batch that died before committing are invisible. Exactly-once is the conjunction; neither alone suffices. Concretely, the hard kill cost `quarantine` the progress row for committed batch 9, which never reached `metrics.jsonl` — output intact, telemetry lost. The checkpoint commit log, not the metrics file, is authoritative for what committed.

## Metrics and Reproducibility

`out/metrics.jsonl` holds one row per query per batch, appended and `fsync`ed as the run proceeds: a hard kill destroys the in-memory `recentProgress` buffer, so anything not already on disk is gone. `batchDuration` is `durationMs.triggerExecution` — `StreamingQueryProgress` has no top-level `batchDuration`, and reading one returns `None`, which is how an all-null metrics column reaches a submitted report (the first version of this log had exactly that).

| query | batches (with input) | inputRows/s | processedRows/s | batchDuration med / p95 | state rows med / max | dropped |
|---|---|---|---|---|---|---|
| `qa_tumbling_60s` | 64 (40) | 29.80 | 15.04 | 1,667 / 3,013 ms | 5 / 6 | 4 |
| `qb_tokens_60s` | 62 (40) | 27.13 | 15.10 | 1,633 / 6,422 ms | 5 / 6 | 4 |
| `qc_sliding_5m_1m` | 64 (40) | 27.23 | 14.44 | 1,773 / 5,420 ms | 315 / 352 | 102 |
| `quarantine` | 40 (39) | 20.92 | 21.28 | 1,146 / 4,405 ms | — | 0 |

QC's drop count is per window-row, not per record: a sliding window places each record in five windows, and one near the boundary has some evicted and some retained. Throughput is low in absolute terms because the replay is deliberately rate-limited to one file every 3 s — these figures measure the pipeline, not the machine. Per INV-5 they are liveness properties read off this log, not contract assertions; no threshold is checked anywhere in the code.

**Pinned environment (CPU baseline, INV-14/INV-15).** PySpark 3.5.1 · Python 3.13.16 · OpenJDK 21.0.12.1 · Linux x86_64 · accelerator: none. `pip freeze` SHA-256 `40df0f6da681ac9b…`, source SHA-256 `8e701cd7e6256f6c…` (full digests in `out/metrics_summary.json`, dependencies in `out/pip_freeze.txt`). The fixture is seeded, so a clean `./run_all.sh` reproduces every figure here — confirmed by running it end-to-end from an empty directory — and the reconciliation exits non-zero on any failed invariant, so stale numbers fail the build rather than reaching the report.

**CI status.** `./run_all.sh` is the CI entry point and is green on the CPU baseline: **28 reconciliation assertions pass, 0 fail, exit code 0**. No GPU path exists. PySpark 3.5.1 on Java 21 is outside the officially supported matrix (3.5.x targets Java 8/11/17); it runs correctly here and produced every number above, but Java 17 is what I would pin for the DevContainer. **No AssemblyAI quota log:** VDB-track, so no live ASR capture and no API quota consumed (INV-7; INV-18 not engaged).

## GenAI Audit

Yes — the Generative AI Kit was used; `GenAI.pdf` is included. It assisted with contract derivation from this specification, the `readStream`/`withWatermark`/`groupBy(window(...))` scaffold, and drafting this report, per the Section 2 procedure. I verified every nullability choice myself, as Stage 1 requires. The substantive corrections during the build are documented above rather than smoothed over: the `array_remove`-returns-null bug, the three-valued-logic hole in the admission filter, the absent `batchDuration` field, `FileStreamSource` guaranteeing no arrival order, and the window-level semantics of the late-data filter. The 1-record prediction residual is disclosed, not tuned away.
