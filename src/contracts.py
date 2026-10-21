"""
CIS 531/731 — MP 5
Record schema and `deal` contracts for the VDB-track corpus chunk stream.

Two things live here and they do different jobs:

  1. CORPUS_CHUNK_SCHEMA  — the explicit StructType handed to readStream.
  2. CONTRACTS            — the executable admission gates.

The distinction matters. Declaring a field `nullable=False` in a Spark
StructType is a *statement of intent*, not an enforcement mechanism: the file
source will happily materialise a null into a non-nullable field and only fail
later, at an arbitrary point downstream. Enforcement is the job of the
contracts below, which run as an explicit admission filter immediately after
the source and route violations to a quarantine sink.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# --------------------------------------------------------------------------
# 1. Record schema
# --------------------------------------------------------------------------

CORPUS_CHUNK_SCHEMA = StructType(
    [
        # Primary key. Non-null: a chunk without an identity cannot be
        # deduplicated on restart, which would break the exactly-once story.
        StructField("record_id", StringType(), nullable=False),
        # Track discriminator ("VDB"). Non-null: PS 6 and MP 7 read this
        # stream and branch on it.
        StructField("track", StringType(), nullable=False),
        # Provenance anchor — the document the chunk was cut from. Non-null:
        # the whitelist gate is evaluated against it, so a null here is
        # unauditable by construction.
        StructField("source_id", StringType(), nullable=False),
        # Event-time bounds of the chunk.
        # ts_end is the watermark column; a null watermark column silently
        # disables watermarking for that row, so both are non-null.
        StructField("ts_start", TimestampType(), nullable=False),
        StructField("ts_end", TimestampType(), nullable=False),
        # Chunk payload. Non-null: an empty-text chunk contributes nothing to
        # token aggregates but still occupies window state.
        StructField("text", StringType(), nullable=False),
        # Processing-time arrival stamp, set by the producer at write time.
        # Non-null: lateness (ingest_ts - ts_end) is the measurement the
        # watermark delay is derived from.
        StructField("ingest_ts", TimestampType(), nullable=False),
        # Corpus-record-only field (CorpusChunk, not TranscriptChunk).
        # Non-null: unlicensed text must not reach the vector store, so
        # "unknown" is represented by an explicit sentinel, never by null.
        StructField("license_class", StringType(), nullable=False),
    ]
)

# --------------------------------------------------------------------------
# 2. Admission gates
# --------------------------------------------------------------------------

#: Approved provenance prefixes. INV-10: anything outside this set fails the
#: ingest precondition rather than being silently admitted.
SOURCE_WHITELIST = ("genius:", "rym:", "spotify:", "librivox:")

#: Permitted license classifications. "unknown" is deliberately absent.
LICENSE_CLASSES = ("public_domain", "cc_by", "licensed_api", "fair_use_excerpt")

#: BoundedChunking upper bound, in seconds.
MAX_CHUNK_DURATION_SEC = 30


@dataclass(frozen=True)
class Contract:
    """A named admission gate.

    `predicate` returns a Column that is True for records that SATISFY the
    contract. Records failing any contract are quarantined, never dropped
    silently — a dropped record is indistinguishable from a record that was
    never produced, which makes reconciliation impossible.
    """

    name: str
    rationale: str
    predicate: Callable[[], Column]


def _c_temporal_ordering() -> Column:
    return F.col("ts_start") < F.col("ts_end")


def _c_bounded_chunking() -> Column:
    dur = F.col("ts_end").cast("long") - F.col("ts_start").cast("long")
    return (dur > 0) & (dur <= F.lit(MAX_CHUNK_DURATION_SEC))


def _c_provenance() -> Column:
    return (
        F.col("record_id").isNotNull()
        & (F.length(F.trim(F.col("record_id"))) > 0)
        & F.col("source_id").isNotNull()
        & (F.length(F.trim(F.col("source_id"))) > 0)
    )


def _c_source_whitelist() -> Column:
    allowed = F.lit(False)
    for prefix in SOURCE_WHITELIST:
        allowed = allowed | F.col("source_id").startswith(prefix)
    return allowed


def _c_license_gate() -> Column:
    return F.col("license_class").isin(list(LICENSE_CLASSES))


def _c_nonempty_text() -> Column:
    return F.col("text").isNotNull() & (F.length(F.trim(F.col("text"))) > 0)


def _c_ingest_causality() -> Column:
    # ingest_ts must not precede the end of the event it describes. A small
    # negative tolerance absorbs producer clock skew; anything beyond that is
    # a corrupt stamp and would poison the lateness distribution the
    # watermark is derived from.
    skew = F.col("ingest_ts").cast("long") - F.col("ts_end").cast("long")
    return skew >= F.lit(-2)


CONTRACTS: tuple[Contract, ...] = (
    Contract(
        "TemporalOrdering",
        "ts_start < ts_end; a reversed interval yields a negative duration "
        "and an undefined window assignment.",
        _c_temporal_ordering,
    ),
    Contract(
        "BoundedChunking",
        f"0 < chunk_duration_sec <= {MAX_CHUNK_DURATION_SEC}; bounds how many "
        "60s windows a single record can span, which bounds state fan-out.",
        _c_bounded_chunking,
    ),
    Contract(
        "Provenance",
        "record_id and source_id present and non-blank; required for restart "
        "deduplication and for auditing an answer back to its source.",
        _c_provenance,
    ),
    Contract(
        "SourceWhitelist",
        "source_id carries an approved prefix (INV-10); unlisted sources fail "
        "the ingest precondition.",
        _c_source_whitelist,
    ),
    Contract(
        "LicenseGate",
        "license_class is an explicit approved value; 'unknown' is not "
        "admissible to the corpus.",
        _c_license_gate,
    ),
    Contract(
        "NonEmptyText",
        "text is present and non-blank; empty chunks consume window state "
        "while contributing no tokens.",
        _c_nonempty_text,
    ),
    Contract(
        "IngestCausality",
        "ingest_ts >= ts_end - 2s; guards the lateness distribution used to "
        "derive the watermark against corrupt producer clocks.",
        _c_ingest_causality,
    ),
)


def admission_filter() -> Column:
    """Conjunction of every contract. True == admissible.

    Wrapped in coalesce(..., False): Spark's AND is three-valued, so a null
    anywhere in the conjunction yields null rather than false. `where(null)`
    does not admit the row, but `where(not null)` does not reject it either —
    a row with a null in a predicate input would fall out of BOTH the admitted
    and quarantined branches and vanish without appearing in any count.
    Coalescing collapses the third value so the two branches partition the
    input exactly, which is what makes the reconciliation arithmetic hold.
    """
    gate = F.lit(True)
    for contract in CONTRACTS:
        gate = gate & contract.predicate()
    return F.coalesce(gate, F.lit(False))


def violation_reasons() -> Column:
    """Array of the names of every contract a record violates.

    Emitted alongside quarantined records so a failure is diagnosable without
    re-running the pipeline.

    Note on implementation: the obvious `array_remove(array(...), None)` does
    NOT work here. Spark's array_remove returns null when the element to
    remove is null, so the whole column comes back null and every violation
    reason is silently lost. `filter` with an isNotNull predicate is the
    correct construction. This was caught by reconciling the per-contract
    violation counts against the generator's ground-truth manifest.
    """
    candidates = F.array(
        *[
            F.when(~c.predicate() | c.predicate().isNull(), F.lit(c.name))
            for c in CONTRACTS
        ]
    )
    return F.filter(candidates, lambda x: x.isNotNull())


def describe() -> str:
    lines = ["Deal contracts (admission gates):"]
    for c in CONTRACTS:
        lines.append(f"  - {c.name}: {c.rationale}")
    return "\n".join(lines)


if __name__ == "__main__":
    print(CORPUS_CHUNK_SCHEMA.simpleString())
    print()
    print(describe())
