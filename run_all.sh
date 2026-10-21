#!/usr/bin/env bash
# CIS 531/731 MP 5 — full reproducible pipeline, clean slate to report inputs.
#
#   ./run_all.sh
#
# Deterministic: the fixture is seeded, the replay is staged one file at a
# time, and the reconciliation exits non-zero if any invariant fails, so this
# script fails loudly rather than producing a report full of stale numbers.
set -euo pipefail
cd "$(dirname "$0")"

echo "== 0. clean slate =="
rm -rf out checkpoints replay landing landing_manifest.json
mkdir -p out checkpoints

echo "== 1. generate the replay fixture (seeded) =="
./run.sh src/make_landing_zone.py --outdir landing --order arrival >/dev/null
python3 -c "import json;m=json.load(open('landing_manifest.json'));print(f\"   {m['records_total']} records, {m['violations_injected']} injected violations, seed {m['seed']}\")"

echo "== 2. event-time calibration (derive the watermark) =="
./run.sh src/analyze_lateness.py >out/analyze_lateness.log 2>&1
WM=$(python3 -c "import json;print(json.load(open('out/lateness.json'))['watermark_delay_sec'])")
python3 -c "
import json;d=json.load(open('out/lateness.json'));l=d['lateness_sec']
print(f\"   lateness p50={l['p50']}s p95={l['p95']}s p99={l['p99']}s -> watermark {d['watermark_delay_sec']}s\")"

echo "== 3. streaming run, hard-killed after 10 committed batches =="
./run.sh src/streaming_job.py --watermark-sec "$WM" --feed-interval 3.0 \
    --kill-after-batches 10 --phase pre_kill >out/pre_kill.log 2>&1 || true
grep -E "^\[pre_kill\]" out/pre_kill.log | sed 's/^/   /'

echo "== 4. restart from checkpoint, run to completion =="
./run.sh src/streaming_job.py --watermark-sec "$WM" --feed-interval 3.0 \
    --phase post_restart >out/post_restart.log 2>&1
grep -E "^\[post_restart\]" out/post_restart.log | sed 's/^/   /'

echo "== 5. reconcile against independent ground truth =="
./run.sh src/reconcile.py >out/reconcile_stdout.txt 2>&1 || RC=$?
grep -E "^\s+\[(PASS|FAIL)\]|^={3}|^RECONCILIATION|^\s+note:" out/reconcile_stdout.txt
if grep -q "RECONCILIATION: FAIL" out/reconcile_stdout.txt; then
    echo "!! reconciliation failed — report numbers are NOT valid"; exit 1
fi

echo "== 6. metrics summary and environment digest =="
./run.sh src/metrics_summary.py >out/metrics_summary.log 2>&1
python3 -c "
import json;s=json.load(open('out/metrics_summary.json'));e=s['environment']
print(f\"   pyspark {e['pyspark']} / python {e['python']} / {e['java']}\")
print(f\"   source sha256 {e['source_sha256'][:16]}...  pip-freeze sha256 {e['pip_freeze_sha256'][:16]}...\")"

echo
echo "DONE. Artifacts in out/: metrics.jsonl, reconciliation.json,"
echo "      metrics_summary.json, lateness.json, pip_freeze.txt"
