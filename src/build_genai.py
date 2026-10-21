from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                Table, TableStyle, HRFlowable)

OUT = "GenAI.pdf"
styles = getSampleStyleSheet()

title = ParagraphStyle("T", parent=styles["Title"], fontSize=15, leading=18, spaceAfter=2)
sub = ParagraphStyle("ST", parent=styles["Normal"], fontSize=10, leading=13, alignment=1,
                     textColor=colors.HexColor("#444444"), spaceAfter=10)
h = ParagraphStyle("H", parent=styles["Heading2"], fontSize=11.5, leading=14,
                   spaceBefore=11, spaceAfter=5, textColor=colors.HexColor("#1a1a1a"))
h3 = ParagraphStyle("H3", parent=styles["Heading3"], fontSize=10, leading=13,
                    spaceBefore=8, spaceAfter=3, textColor=colors.HexColor("#333333"))
body = ParagraphStyle("B", parent=styles["Normal"], fontSize=9.5, leading=13.2,
                      alignment=TA_JUSTIFY, spaceAfter=6)
small = ParagraphStyle("S", parent=styles["Normal"], fontSize=8.6, leading=11.6,
                       textColor=colors.HexColor("#555555"))

S = []
S.append(Paragraph("Generative AI Audit Log — MP 5", title))
S.append(Paragraph("CIS 531/731 — Fall 2026 — Structured Streaming", sub))

hdr = [
    ["Student", "Abdulla Mansoor (CIS 731) — abdulla03@ksu.edu"],
    ["Assignment", "MP 5: Data Analytics Pipelines (Structured Streaming)"],
    ["Track", "Track 2 — VDB & Trope Ontologies (no live ASR capture)"],
    ["Tool audited", "Claude (Anthropic) — conversational assistant"],
    ["Period covered", "19–20 October 2026"],
    ["Deliverable", "report.md, src/*.py, out/metrics.jsonl, "
                    "notebooks/mp5_reconciliation.ipynb"],
]
t = Table(hdr, colWidths=[1.15 * inch, 5.35 * inch])
t.setStyle(TableStyle([
    ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
    ("FONTSIZE", (0, 0), (-1, -1), 8.8),
    ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ("TOPPADDING", (0, 0), (-1, -1), 3.5),
    ("BOTTOMPADDING", (0, 0), (-1, -1), 3.5),
    ("LINEBELOW", (0, 0), (-1, -2), 0.25, colors.HexColor("#DDDDDD")),
]))
S.append(t)
S.append(Spacer(1, 5))
S.append(HRFlowable(width="100%", thickness=0.8, color=colors.HexColor("#999999")))

S.append(Paragraph("1. Scope and Policy Position", h))
S.append(Paragraph(
    "MP 5 Section 2 (\"Initiating Your Draft — The AI Kit Procedure\") directs students to use the "
    "Generative AI Kit for contract extraction, pipeline scaffolding, event-time calibration, fault "
    "injection, and run-report assembly. AI assistance on this milestone is therefore instructed rather "
    "than merely permitted, and this log records what it actually did. The same specification attaches "
    "a verification duty to each stage — \"then verify every nullability choice yourself\", \"audit it "
    "for schema inference, missing watermarks, unbounded state, and non-idempotent sinks\" — and that "
    "duty is where the substance of this entry lies.", body))
S.append(Paragraph(
    "I am disclosing the division of labour honestly, including the parts where the assistant did more "
    "than scaffold. Where a judgement in report.md is mine, I say so; where the assistant proposed "
    "something I adopted, I say that instead.", body))

S.append(Paragraph("2. Assistance by Stage", h))
rows = [
    ["Spec stage", "What the assistant did", "What I verified or decided"],
    ["1. Contract\nextraction",
     "Derived the StructType and an initial contract set\nfrom the MP 5 text (BoundedChunking,\nts_start < ts_end, whitelist, provenance).",
     "Reviewed each field's nullability\nagainst its downstream consumer;\nadded IngestCausality after\nreasoning about what a corrupt\nproducer clock does to the\nlateness distribution."],
    ["2. Pipeline\nscaffold",
     "Wrote the readStream / withWatermark /\ngroupBy(window(...)) skeleton with an\nexplicit schema and per-query checkpoints.",
     "Audited it as instructed. The audit\nfound the three-valued-logic hole in\nthe admission filter and the\nnon-idempotency risk in update mode."],
    ["3. Event-time\ncalibration",
     "Implemented the percentile measurement and\nproposed ceil(p99 / 30s) as the rounding rule.",
     "Chose p99 over max, and chose to\nmeasure on admitted records only.\nBoth are argued in report.md."],
    ["4. Fault\ninjection",
     "Implemented os._exit(1) kill, restart, and the\nreconciliation harness.",
     "Rejected the first reconciliation as\ninvalid (see §3) and required it be\nrebuilt on order-independent\ninvariants."],
    ["5. Run report",
     "Drafted report.md from the measured JSON\nartifacts and assembled metrics.jsonl.",
     "Every figure traced back to\nout/*.json. Re-ran run_all.sh from\nan empty directory to confirm\nreproducibility."],
]
t2 = Table(rows, colWidths=[0.95 * inch, 2.75 * inch, 2.8 * inch])
t2.setStyle(TableStyle([
    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
    ("FONTSIZE", (0, 0), (-1, -1), 7.5),
    ("LEADING", (0, 0), (-1, -1), 9.4),
    ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#EFEFEF")),
    ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#BBBBBB")),
    ("TOPPADDING", (0, 0), (-1, -1), 4),
    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ("LEFTPADDING", (0, 0), (-1, -1), 4),
]))
S.append(t2)

S.append(Paragraph("3. Defects Found by Running It, Not by Reading It", h))
S.append(Paragraph(
    "The spec's audit duty is the reason this submission is not the first thing that ran. Five defects "
    "were found and fixed, each by a measurement disagreeing with an expectation:", body))
S.append(Paragraph(
    "<b>(a) array_remove returns null.</b> violation_reasons() was first written as "
    "array_remove(array(...), None). Spark's array_remove returns null when the element to remove is "
    "null, so every violation reason came back null. The run reported 57 quarantined records and zero "
    "reasons; reconciling per-contract counts against the generator's manifest exposed it.", body))
S.append(Paragraph(
    "<b>(b) Three-valued logic in the admission filter.</b> A null in the conjunction yields null, and "
    "a null-gated row is admitted by neither where(gate) nor where(not gate) — it would vanish from "
    "every count. Fixed with coalesce(..., False) so the two branches partition the input exactly.", body))
S.append(Paragraph(
    "<b>(c) The watermark was decorative.</b> With files written in event-time order, the file source "
    "delivers records in roughly event-time order, so nothing ever arrives late and the job produces "
    "identical output with a 0 s watermark. The completeness-vs-latency trade-off could not be measured "
    "at all. Fixed by partitioning the fixture by ingest_ts, after which 372 of 1,000 records arrive out "
    "of order.", body))
S.append(Paragraph(
    "<b>(d) FileStreamSource guarantees no arrival order.</b> With every file present up front, unread "
    "files are taken in filesystem-listing order, so the drop set is not predictable from filenames. The "
    "first reconciliation could not account for 163 records and the engine's own drop counter disagreed "
    "with the per-window deficit by 129. I rejected that reconciliation rather than reporting it, and "
    "the replay was rebuilt as a staged feeder — a file that does not exist cannot be read — which made "
    "the accounting exact.", body))
S.append(Paragraph(
    "<b>(e) StreamingQueryProgress has no batchDuration field.</b> The spec names batchDuration; reading "
    "p[\"batchDuration\"] returns None. The correct field is durationMs.triggerExecution. The first "
    "metrics log had an all-null batchDuration column.", body))

S.append(Paragraph("4. What Was Not Resolved, and Is Not Hidden", h))
S.append(Paragraph(
    "One record of 943 (c000306) sits on the late-data filter's boundary: its window ends 14:16:00 "
    "against a reported watermark of 14:16:05, so the independent model predicts a drop and the engine "
    "admitted it. Four of five predicted drops are confirmed and the derived watermark matches the "
    "engine's reported value for all 40 non-empty batches, so the model is not broadly wrong. I "
    "considered adjusting the drop rule until the counts matched and did not, because that would be "
    "fitting a rule to a single observation. The check is asserted to within one record and the residual "
    "is stated in report.md and in out/reconciliation.json.", body))

S.append(Paragraph("5. Uses Explicitly Not Made", h))
S.append(Paragraph(
    "AI was not used to fabricate, estimate, or interpolate any measured value: every number in "
    "report.md is emitted by code in src/ and stored in out/. It was not used to invent citations. The "
    "landing-zone fixture is synthetic and is disclosed as synthetic in report.md, in the generator's "
    "module docstring, and in landing_manifest.json (\"synthetic\": true) — it is a seeded stand-in for "
    "the MP 3 corpus snapshot, not AI-generated data presented as real. No reconciliation result was "
    "accepted without the harness exiting zero.", body))

S.append(Paragraph("6. Attestation", h))
S.append(Paragraph(
    "I can explain and defend every design decision in report.md without assistance: why the watermark "
    "is p99 and not the maximum, why append mode is required rather than chosen, why the top-10 is "
    "computed at read time, why exactly-once needs both the checkpoint and the idempotent sink, and why "
    "the record accounting is a closed balance rather than a spot check. Where the assistant's "
    "contribution exceeded scaffolding, §2 and §3 say so.", body))
S.append(Spacer(1, 12))
S.append(Paragraph("Signed: ______________________________    Date: ______________", body))
S.append(Paragraph("Abdulla Mansoor — CIS 731", small))

doc = SimpleDocTemplate(OUT, pagesize=letter,
                        leftMargin=0.9 * inch, rightMargin=0.9 * inch,
                        topMargin=0.7 * inch, bottomMargin=0.7 * inch,
                        title="GenAI Audit Log MP5 - Abdulla Mansoor",
                        author="Abdulla Mansoor")
doc.build(S)
print("built", OUT)
