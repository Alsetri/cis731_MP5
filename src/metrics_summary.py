"""
CIS 531/731 — MP 5
Summarise metrics.jsonl into the StreamingQueryProgress table the report
requires, and record the pinned-environment digest.

Throughput and latency are liveness properties (INV-5): they are assessed
from this log, not asserted by a contract. Nothing here is a threshold check.
"""

from __future__ import annotations

import hashlib
import json
import platform
import subprocess
import sys
from pathlib import Path
from statistics import mean, median

OUT = Path("out")


def pct(xs: list[float], q: float) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    i = min(len(s) - 1, int(q * len(s)))
    return s[i]


def main() -> None:
    rows = [json.loads(l) for l in (OUT / "metrics.jsonl").open()]
    summary: dict = {"queries": {}}

    for q in sorted({r["query"] for r in rows}):
        rs = [r for r in rows if r["query"] == q]
        live = [r for r in rs if (r["numInputRows"] or 0) > 0]
        irs = [r["inputRowsPerSecond"] for r in live
               if r["inputRowsPerSecond"] is not None]
        prs = [r["processedRowsPerSecond"] for r in live
               if r["processedRowsPerSecond"] is not None]
        bd = [r["batchDuration"] for r in live if r["batchDuration"] is not None]
        st = [r["stateOperators.numRowsTotal"] for r in rs
              if r["stateOperators.numRowsTotal"] is not None]
        summary["queries"][q] = {
            "batches_logged": len(rs),
            "batches_with_input": len(live),
            "input_rows_total": sum(r["numInputRows"] or 0 for r in rs),
            "inputRowsPerSecond": {
                "mean": round(mean(irs), 2) if irs else None,
                "median": round(median(irs), 2) if irs else None,
                "max": round(max(irs), 2) if irs else None,
            },
            "processedRowsPerSecond": {
                "mean": round(mean(prs), 2) if prs else None,
                "median": round(median(prs), 2) if prs else None,
                "max": round(max(prs), 2) if prs else None,
            },
            "batchDuration_ms": {
                "median": round(median(bd)) if bd else None,
                "p95": round(pct(bd, 0.95)) if bd else None,
                "max": max(bd) if bd else None,
            },
            "stateOperators.numRowsTotal": {
                "median": round(median(st)) if st else None,
                "max": max(st) if st else None,
            },
            "rows_dropped_by_watermark": sum(
                r.get("stateOperators.numRowsDroppedByWatermark") or 0
                for r in rs
            ),
        }

    # Pinned-environment digest. Hashing the resolved dependency set, the
    # interpreter and the JVM makes "reproduces on the CPU baseline" a
    # checkable claim rather than a sentence in a report.
    def sh(cmd: list[str]) -> str:
        try:
            return subprocess.run(cmd, capture_output=True, text=True,
                                  timeout=60).stdout.strip()
        except Exception as e:  # noqa: BLE001
            return f"<unavailable: {e}>"

    freeze = sh([sys.executable, "-m", "pip", "freeze"])
    src_files = sorted(Path("src").glob("*.py"))
    src_digest = hashlib.sha256(
        b"".join(p.read_bytes() for p in src_files)
    ).hexdigest()

    env = {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "machine": platform.machine(),
        "pyspark": next((l.split("==")[1] for l in freeze.splitlines()
                         if l.lower().startswith("pyspark==")), "unknown"),
        "java": sh(["java", "-version"]).splitlines()[0]
        if sh(["java", "-version"]) else sh(["bash", "-c",
                                             "java -version 2>&1 | head -1"]),
        "accelerator": "none (CPU baseline, INV-14/INV-15)",
        "pip_freeze_sha256": hashlib.sha256(freeze.encode()).hexdigest(),
        "source_sha256": src_digest,
        "source_files": [str(p) for p in src_files],
    }
    summary["environment"] = env
    (OUT / "pip_freeze.txt").write_text(freeze + "\n", encoding="utf-8")

    (OUT / "metrics_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
