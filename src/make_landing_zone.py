"""
CIS 531/731 — MP 5
Replay-fixture generator for the landing zone.

HONESTY NOTE
------------
This generates a SYNTHETIC, SEEDED corpus-chunk fixture. It is not scraped
lyric data. The MP 3 corpus snapshot for this team is still being assembled by
Sub-Team A (Obtain stage), so this fixture stands in for it: it conforms to the
PS 4 `CorpusChunk` contract exactly, carries a realistic lateness distribution,
and includes a known population of contract violations so the quarantine path
is exercised rather than merely asserted.

Every number in report.md is measured from this fixture. When the real corpus
lands, the same pipeline runs against it unchanged — only the landing-zone
path changes.

Determinism: seeded with --seed (default 20261020). Re-running with the same
seed reproduces the fixture byte-for-byte, which is what makes the reported
percentiles reproducible.
"""

from __future__ import annotations

import argparse
import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

# A small controlled vocabulary. Zipf-ish sampling gives a realistic
# head/tail split so top-10 terms is a meaningful query rather than noise.
VOCAB = [
    "trap", "drill", "boom", "bap", "funk", "hyphy", "crunk", "memphis",
    "atlanta", "compton", "brooklyn", "houston", "detroit", "chicago",
    "slang", "drip", "cap", "flow", "bars", "beat", "verse", "hook",
    "sample", "bassline", "hi", "hat", "snare", "synth", "chopped",
    "screwed", "block", "coast", "region", "era", "tape", "crew",
]

SOURCES = [
    ("genius:", "licensed_api"),
    ("rym:", "fair_use_excerpt"),
    ("spotify:", "licensed_api"),
    ("librivox:", "public_domain"),
]

# Deliberate violations, injected at a known rate so quarantine counts can be
# checked against ground truth.
VIOLATION_KINDS = [
    "reversed_interval",     # ts_start > ts_end         -> TemporalOrdering
    "overlong_chunk",        # duration 45s              -> BoundedChunking
    "unlisted_source",       # source_id "pastebin:..."  -> SourceWhitelist
    "unknown_license",       # license_class "unknown"   -> LicenseGate
    "blank_text",            # text ""                   -> NonEmptyText
    "blank_source",          # source_id ""              -> Provenance + whitelist
]


def zipf_sample(rng: random.Random, n: int) -> int:
    """Index into VOCAB with a Zipf-like bias toward the head."""
    weights = [1.0 / (i + 1) ** 0.85 for i in range(n)]
    total = sum(weights)
    r = rng.random() * total
    acc = 0.0
    for i, w in enumerate(weights):
        acc += w
        if r <= acc:
            return i
    return n - 1


def lateness_seconds(rng: random.Random) -> float:
    """Right-skewed arrival delay.

    Body is lognormal (most records arrive within a few seconds); a 4% tail is
    drawn an order of magnitude later, which is what makes the p99 meaningfully
    separate from the p50 and forces a real watermark trade-off.
    """
    if rng.random() < 0.04:
        return min(600.0, rng.lognormvariate(math.log(90), 0.7))
    return max(0.0, rng.lognormvariate(math.log(4.0), 0.9))


def make_text(rng: random.Random) -> str:
    n = rng.randint(8, 26)
    return " ".join(VOCAB[zipf_sample(rng, len(VOCAB))] for _ in range(n))


def build(outdir: Path, n_files: int, per_file: int, seed: int,
          violation_rate: float, order: str = "arrival") -> dict:
    rng = random.Random(seed)
    outdir.mkdir(parents=True, exist_ok=True)
    for stale in outdir.glob("*.jsonl"):
        stale.unlink()

    # Event times march forward from a fixed epoch so windows are stable
    # across runs.
    t0 = datetime(2026, 10, 19, 14, 0, 0, tzinfo=timezone.utc)
    cursor = t0
    total = 0
    violations = 0
    counts_by_kind: dict[str, int] = {}

    all_rows: list[dict] = []
    for _ in range(n_files * per_file):
        if True:
            dur = rng.randint(5, 28)
            ts_start = cursor
            ts_end = ts_start + timedelta(seconds=dur)
            # Advance event time by a jittered step; some records overlap,
            # which is realistic for parallel chunking.
            cursor = cursor + timedelta(seconds=rng.randint(1, 5))

            late = lateness_seconds(rng)
            ingest_ts = ts_end + timedelta(seconds=late)

            prefix, lic = SOURCES[rng.randrange(len(SOURCES))]
            rec = {
                "record_id": f"c{total:06d}",
                "track": "VDB",
                "source_id": f"{prefix}doc{rng.randrange(1, 240):04d}",
                "ts_start": ts_start.isoformat(),
                "ts_end": ts_end.isoformat(),
                "text": make_text(rng),
                "ingest_ts": ingest_ts.isoformat(),
                "license_class": lic,
            }

            if rng.random() < violation_rate:
                kind = VIOLATION_KINDS[rng.randrange(len(VIOLATION_KINDS))]
                if kind == "reversed_interval":
                    rec["ts_start"], rec["ts_end"] = rec["ts_end"], rec["ts_start"]
                elif kind == "overlong_chunk":
                    rec["ts_end"] = (ts_start + timedelta(seconds=45)).isoformat()
                elif kind == "unlisted_source":
                    rec["source_id"] = f"pastebin:doc{rng.randrange(1, 99):03d}"
                elif kind == "unknown_license":
                    rec["license_class"] = "unknown"
                elif kind == "blank_text":
                    rec["text"] = "   "
                elif kind == "blank_source":
                    rec["source_id"] = ""
                violations += 1
                counts_by_kind[kind] = counts_by_kind.get(kind, 0) + 1

            all_rows.append(rec)
            total += 1

    # ARRIVAL ORDER MATTERS.
    #
    # A file source hands micro-batches to the stream in filename order, and
    # Spark advances the watermark from the event times it has already seen.
    # If records are written to files in event-time order, every record
    # arrives in roughly event-time order too, the watermark never falls
    # behind any record, and NOTHING is ever late. The watermark would then be
    # decorative: the job would produce identical output with a 0s delay, and
    # the completeness-vs-latency trade-off could not be measured at all.
    #
    # Partitioning by ingest_ts instead reproduces the real arrival process:
    # a record the producer wrote 500s after the event it describes lands in a
    # file far downstream of its event-time neighbours and genuinely arrives
    # behind the watermark. This is what makes the measured drop count in
    # report.md a real number rather than a tautological zero.
    if order == "arrival":
        all_rows.sort(key=lambda r: r["ingest_ts"])
    elif order == "event":
        pass  # legacy behaviour, kept so the report can contrast the two
    else:
        raise SystemExit(f"unknown --order {order!r}")

    for f in range(n_files):
        chunk = all_rows[f * per_file:(f + 1) * per_file]
        path = outdir / f"part-{f:04d}.jsonl"
        with path.open("w", encoding="utf-8") as fh:
            for r in chunk:
                fh.write(json.dumps(r) + "\n")

    manifest = {
        "generator": "make_landing_zone.py",
        "seed": seed,
        "files": n_files,
        "records_per_file": per_file,
        "records_total": total,
        "violations_injected": violations,
        "violations_by_kind": counts_by_kind,
        "violation_rate_requested": violation_rate,
        "event_time_start": t0.isoformat(),
        "event_time_end": cursor.isoformat(),
        "file_partition_order": order,
        "synthetic": True,
        "note": "Seeded replay fixture standing in for the MP 3 corpus "
                "snapshot; conforms to the PS 4 CorpusChunk contract.",
    }
    (outdir.parent / "landing_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    return manifest


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="landing")
    ap.add_argument("--files", type=int, default=40)
    ap.add_argument("--per-file", type=int, default=25)
    ap.add_argument("--seed", type=int, default=20261020)
    ap.add_argument("--violation-rate", type=float, default=0.05)
    ap.add_argument("--order", choices=("arrival", "event"), default="arrival",
                    help="arrival: partition files by ingest_ts so late "
                         "records genuinely arrive late (default). "
                         "event: partition in event-time order, which makes "
                         "the watermark a no-op.")
    a = ap.parse_args()
    m = build(Path(a.outdir), a.files, a.per_file, a.seed, a.violation_rate,
              a.order)
    print(json.dumps(m, indent=2))


if __name__ == "__main__":
    main()
