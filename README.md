# CIS 731 — MP 5: Structured Streaming

Abdulla Mansoor · abdulla03@ksu.edu · Track 2 (VDB & Trope Ontologies)

Event-time windowed analytics over a replayed corpus-chunk landing zone, with
contract-validated ingest, a watermark derived from measured lateness, and
checkpointed recovery from a hard kill.

## Reproduce

```bash
./run_all.sh
```

Wipes all state, regenerates the seeded fixture, derives the watermark, runs
the stream, hard-kills it after 10 committed batches, restarts it from the
checkpoint, reconciles against an independent ground truth, and writes the
metrics summary. **Exits non-zero if any invariant fails**, so a stale number
cannot reach the report.

```bash
./run.sh src/build_notebook.py   # regenerate + execute the notebook
python3 src/build_genai.py       # rebuild GenAI.pdf
```

`run.sh` is a thin wrapper that pins the interpreter (`/home/claude/sparkenv`)
and `JAVA_HOME`. Change the venv path there for a different environment.

## Layout

| path | what it is |
|---|---|
| `report.md` | **The submission.** Six required sections; every number measured. |
| `GenAI.pdf` | Generative AI audit log for this milestone. |
| `src/contracts.py` | `CORPUS_CHUNK_SCHEMA` + the seven `deal` admission gates. |
| `src/make_landing_zone.py` | Seeded replay-fixture generator (synthetic; see report). |
| `src/analyze_lateness.py` | Event-time calibration → `out/lateness.json`. |
| `src/streaming_job.py` | The streaming job, staged feeder, and fault injection. |
| `src/reconcile.py` | 28 invariant assertions + an independent watermark model. |
| `src/metrics_summary.py` | `StreamingQueryProgress` summary + environment digest. |
| `notebooks/mp5_reconciliation.ipynb` | Executed reconciliation evidence. |
| `out/metrics.jsonl` | One row per query per batch, appended and fsynced live. |
| `out/reconciliation.json` | Full reconciliation result. |
| `landing/`, `replay/` | Fixture, and the staged replay directory. |
| `checkpoints/` | Per-query `checkpointLocation` (offsets, commits, state). |

## Headline result

```
emitted 875 + dropped-by-watermark 4 + held in 4 open windows 64
        + quarantined 57  =  1000 records
46 window keys, 46 distinct — no duplicates, none missing
0 batches reprocessed across the restart
RECONCILIATION: PASS (28 assertions, 0 failures)
```

Watermark 180 s, derived from measured p99 = 160.591 s. PySpark 3.5.1 on the
CPU baseline (INV-14/INV-15); no GPU path.
