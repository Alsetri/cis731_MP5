"""
Build and EXECUTE the MP 5 reconciliation notebook.

The notebook is generated from this script rather than hand-edited so that
its cell order is fixed and its outputs cannot be stale: every build runs the
cells top to bottom in a fresh kernel. (MP 3 shipped with a query cell that
had executed before the cell it depended on; generating and executing the
notebook makes that failure mode impossible.)
"""

from __future__ import annotations

from pathlib import Path

import nbformat as nbf
from nbclient import NotebookClient

NB = Path("notebooks/mp5_reconciliation.ipynb")

MD = [
    ("""# MP 5 — Fault-Tolerance Reconciliation

**Abdulla Mansoor (CIS 731) · abdulla03@ksu.edu**

This notebook is the evidence for the *Fault-Tolerance Reconciliation* section
of `report.md`. It re-reads the artifacts produced by `./run_all.sh` and checks
them; it does not recompute them, so nothing here can quietly disagree with the
submitted numbers.

The run it inspects was hard-killed with `os._exit(1)` after 10 committed
micro-batches and then restarted from `checkpointLocation`.""", None),

    ("""## 1. What the pipeline was asked to do

Watermark delay is not a guess. `analyze_lateness.py` measures the lateness
distribution `ingest_ts - ts_end` over the admitted records and rounds p99 up
to the next 30-second boundary.""", """
import json, pandas as pd
from pathlib import Path
pd.set_option("display.width", 120)

lat = json.loads(Path("../out/lateness.json").read_text())
man = json.loads(Path("../landing_manifest.json").read_text())
print("fixture:", man["records_total"], "records, seed", man["seed"],
      "| file partition order:", man["file_partition_order"])
print()
print("lateness (seconds):")
for k, v in lat["lateness_sec"].items():
    print(f"  {k:>7} {v:>9.3f}")
print()
print("watermark delay:", lat["watermark_delay_sec"], "s  rule:", lat["watermark_rule"])
print("records beyond that delay:", lat["records_beyond_watermark"],
      f"({lat['completeness_at_watermark']:.3%} completeness)")
"""),

    ("""## 2. The contracts actually rejected something

A quarantine path that is never exercised is an assertion, not a gate. The
fixture injects a known population of violations; the counts below are the
contracts firing on them. Records violating more than one contract appear
under each, which is why the per-contract counts sum to more than the
record count.""", """
print("admitted  ", lat["records_admitted"])
print("quarantined", lat["records_quarantined"],
      f"({lat['quarantine_rate']:.1%})")
print()
print("injected by the generator (ground truth):")
for k, v in sorted(man["violations_by_kind"].items(), key=lambda x: -x[1]):
    print(f"  {k:<20} {v:>3}")
print()
print("contracts that fired:")
for k, v in lat["violations_by_contract"].items():
    print(f"  {k:<20} {v:>3}")
print()
print("sum of contract hits:", sum(lat["violations_by_contract"].values()),
      "across", lat["records_quarantined"], "records")
"""),

    ("""## 3. Restart continuity

The checkpoint commit log — not `metrics.jsonl` — is authoritative. A commit
is written by the engine before a batch is acknowledged; the metrics file is
written by the driver and can lose its last rows to a hard kill. Both are
shown so the difference is visible.""", """
rec = json.loads(Path("../out/reconciliation.json").read_text())
rows = []
for q, c in rec["restart_continuity"].items():
    rows.append({
        "query": q,
        "committed": len(c["committed_batches"]),
        "last_batch_before_kill": max(c["metrics_pre_kill"]) if c["metrics_pre_kill"] else None,
        "first_batch_after_restart": min(c["metrics_post_restart"]) if c["metrics_post_restart"] else None,
        "reprocessed": len(c["reprocessed"]),
        "progress_rows_lost_to_kill": c["progress_rows_lost_to_kill"],
    })
display(pd.DataFrame(rows))
print("\\nreprocessed batches across all queries:",
      sum(len(c["reprocessed"]) for c in rec["restart_continuity"].values()))
"""),

    ("""## 4. No duplicate window keys

This is the property the restart has to preserve. Append mode plus a
watermark means a window is written once, when it closes; if the restart had
replayed committed batches, a window key would appear twice.""", """
d = rec["duplicate_check"]
display(pd.DataFrame([
    {"sink": "qa_tumbling_60s", "rows": d["qa_rows"], "distinct_keys": d["qa_distinct_keys"], "key": "window_start"},
    {"sink": "qb_tokens_60s",   "rows": d["qb_rows"], "distinct_keys": d["qb_distinct_keys"], "key": "window_start"},
    {"sink": "qc_sliding_5m_1m","rows": d["qc_rows"], "distinct_keys": d["qc_distinct_keys"], "key": "(window_start, token)"},
    {"sink": "quarantine",      "rows": d["quarantine_rows"], "distinct_keys": d["quarantine_distinct_keys"], "key": "record_id"},
]))
"""),

    ("""## 5. Every record is accounted for

The reconciliation is a closed balance over all 1000 records, not a spot
check. A record is emitted in a closed window, discarded by the watermark,
still held in a window that had not closed when the replay drained, or
quarantined by a contract. Nothing else is possible, and the four buckets
have to sum to the input.""", """
a = rec["accounting"]
print(f"  emitted in closed windows      {a['emitted_in_closed_windows']:>5}")
print(f"  dropped behind the watermark    {a['dropped_behind_watermark_per_engine']:>5}   (engine counter)")
print(f"  held in windows open at EOS     {a['held_in_windows_open_at_eos']:>5}   ({a['windows_still_open']} windows)")
print(f"  quarantined by a contract       {a['quarantined']:>5}")
print(f"  {'-' * 38}")
print(f"  total                          {a['sum']:>5}   input {a['records_total']}")
assert a["sum"] == a["records_total"], "accounting does not balance"
print()
print(f"windows in batch truth {a['windows_in_batch_truth']},",
      f"emitted {a['windows_emitted']}, still open {a['windows_still_open']}")
print("final watermark:", a["final_watermark"])
print("completeness of the emitted windows:",
      f"{a['completeness_of_emitted_windows']:.3%}")
"""),

    ("""### 5b. Where the prediction and the engine disagree

The watermark model in `reconcile.py` reproduces the engine's reported
watermark for all 40 non-empty batches, and predicts four of the five drops
the engine performed. The fifth record sits on the late-data filter boundary.
It is reported rather than tuned away.""", """
p = rec["independent_prediction"]
print("predicted closed windows", p["predicted_closed_windows"],
      "| engine", a["windows_emitted"])
print("predicted drops", p["predicted_drop_count"],
      "| engine", p["engine_drop_count"],
      "| residual", p["residual_records"], "record")
print("watermark model agreement:", p["watermark_model_batches_matched"])
print()
display(pd.DataFrame(p["predicted_dropped"]))
"""),

    ("""## 6. Query output

QA is the 60-second tumbling record-grain aggregate; QC is the 5-minute /
1-minute sliding token aggregate. The top-10 is computed here, at read time,
rather than in the stream — an in-stream top-K would have to hold every
candidate term for every open window and then sort, and a rank over a
streaming aggregate is not supported in append mode.""", """
import glob
qa = pd.concat([pd.read_parquet(f) for f in glob.glob("../out/qa_tumbling_60s/*.parquet")])
qa = qa.sort_values("window_start").reset_index(drop=True)
print(f"QA: {len(qa)} windows, {qa.record_count.sum()} records")
display(qa.head(8))
"""),

    (None, """
qc = pd.concat([pd.read_parquet(f) for f in glob.glob("../out/qc_sliding_5m_1m/*.parquet")])
top = (qc.groupby("token", as_index=False)["term_count"].sum()
         .sort_values(["term_count", "token"], ascending=[False, True]).head(10))
top.insert(0, "rank", range(1, len(top) + 1))
print("Top-10 terms, summed over the sliding windows:")
display(top.reset_index(drop=True))
print("distinct tokens:", qc.token.nunique(),
      "| distinct sliding windows:", qc.window_start.nunique(),
      "| (window, token) rows:", len(qc))
"""),

    ("""## 7. State growth stayed inside the predicted bound

For a tumbling window the number of open window keys is bounded by
`ceil((watermark + window) / window)` plus the batch in flight and the
one-batch watermark lag. For the sliding window the same count is multiplied
by the live vocabulary. These are bounds, not measurements — the point is
that the observed maximum does not exceed them.""", """
ms = json.loads(Path("../out/metrics_summary.json").read_text())
import math
WM, VOCAB = lat["watermark_delay_sec"], qc.token.nunique()
qa_bound = math.ceil((WM + 60) / 60) + 2
qc_bound = (math.ceil((WM + 300) / 60) + 2) * VOCAB
rows = []
for q, v in ms["queries"].items():
    rows.append({
        "query": q,
        "state_rows_median": v["stateOperators.numRowsTotal"]["median"],
        "state_rows_max": v["stateOperators.numRowsTotal"]["max"],
        "predicted_bound": qa_bound if "60s" in q else (qc_bound if "sliding" in q else 0),
        "rows_dropped_by_watermark": v["rows_dropped_by_watermark"],
    })
display(pd.DataFrame(rows))
print(f"\\nQA/QB bound = ceil(({WM}+60)/60) + 2 = {qa_bound} window keys")
print(f"QC bound    = (ceil(({WM}+300)/60) + 2) x {VOCAB} tokens = {qc_bound} rows")
"""),

    ("""## 8. Throughput, latency and the pinned environment

Throughput and latency are liveness properties (INV-5), read off this log
rather than asserted by a contract. `batchDuration` is
`durationMs.triggerExecution`; there is no top-level `batchDuration` field on
`StreamingQueryProgress`, and reading one returns `None`.

The figures are low in absolute terms because the replay is deliberately rate
limited to one file every three seconds — this measures the pipeline, not the
machine.""", """
rows = []
for q, v in ms["queries"].items():
    rows.append({
        "query": q,
        "batches": v["batches_logged"],
        "with_input": v["batches_with_input"],
        "inputRows/s (mean)": v["inputRowsPerSecond"]["mean"],
        "processedRows/s (mean)": v["processedRowsPerSecond"]["mean"],
        "batchDuration ms (median)": v["batchDuration_ms"]["median"],
        "batchDuration ms (p95)": v["batchDuration_ms"]["p95"],
    })
display(pd.DataFrame(rows))
e = ms["environment"]
print()
for k in ("python", "pyspark", "java", "platform", "accelerator"):
    print(f"  {k:<12} {e[k]}")
print(f"  {'source':<12} sha256 {e['source_sha256']}")
print(f"  {'deps':<12} sha256 {e['pip_freeze_sha256']} (pip freeze)")
"""),

    ("""## 9. Verdict""", """
print("RECONCILIATION:", rec["result"])
if rec["failures"]:
    for f in rec["failures"]:
        print("  -", f)
else:
    print("  all invariants hold; no duplicate or missing window keys,")
    print("  no batch reprocessed across the restart, and the record")
    print("  accounting balances exactly against the input.")
"""),
]


def main() -> None:
    nb = nbf.v4.new_notebook()
    cells = []
    for md, code in MD:
        if md:
            cells.append(nbf.v4.new_markdown_cell(md))
        if code:
            cells.append(nbf.v4.new_code_cell(code.strip()))
    nb["cells"] = cells
    nb["metadata"] = {
        "kernelspec": {"display_name": "Python 3", "language": "python",
                       "name": "python3"},
        "language_info": {"name": "python"},
    }
    NB.parent.mkdir(parents=True, exist_ok=True)
    nbf.write(nb, NB)

    client = NotebookClient(
        nb, timeout=600, kernel_name="python3",
        resources={"metadata": {"path": str(NB.parent)}},
        allow_errors=False,
    )
    client.execute()
    nbf.write(nb, NB)

    n_out = sum(1 for c in nb["cells"]
                if c["cell_type"] == "code" and c.get("outputs"))
    print(f"executed {NB}: "
          f"{sum(1 for c in nb['cells'] if c['cell_type'] == 'code')} code cells, "
          f"{n_out} with output")
    order = [c["execution_count"] for c in nb["cells"]
             if c["cell_type"] == "code"]
    print("execution order:", order)
    assert order == sorted(o for o in order if o is not None), \
        "cells did not execute in order"


if __name__ == "__main__":
    main()
