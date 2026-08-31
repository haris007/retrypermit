"""Rebuild the v3 source PDF from its reviewed JSON, without cloud calls.

Requires reportlab only for authoring, not application runtime or normal tests.
The canonical appendix prevents the PDF/fixture drift of the historical v2 demo.
"""

import json
import textwrap
from pathlib import Path
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    PageBreak,
    Preformatted,
)

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "fixtures/policies/runbook-v3.json"
OUTPUT = ROOT / "fixtures/dlq-runbook-v3.pdf"


def build():
    definition = json.loads(SOURCE.read_text())
    styles = getSampleStyleSheet()
    styles.add(
        ParagraphStyle("Meta", parent=styles["BodyText"], fontSize=9, leading=13)
    )
    styles.add(ParagraphStyle("PolicyCode", fontName="Courier", fontSize=7, leading=9))
    story = []

    def paragraph(text, style="BodyText"):
        story.append(Paragraph(escape(text), styles[style]))
        story.append(Spacer(1, 7))

    paragraph("RetryPermit", "Title")
    paragraph("Synthetic four-class DLQ runbook | v3", "Heading1")
    paragraph(
        "Source document for human review and Gemini extraction. Synthetic orders and simulated downstream effects only. Extraction is a proposal; approval and activation are separate authenticated actions."
    )
    for key in (
        "tenant_id",
        "version",
        "name",
        "auto_replay_cap",
        "approved_currencies",
        "retry_limit",
        "transient_recheck_seconds",
        "action_version",
    ):
        paragraph(f"{key}: {json.dumps(definition[key])}", "Meta")
    paragraph("Global constraints", "Heading2")
    paragraph(
        "Only USD is approved. The replay cap is USD 2,000.00, also bounded by the system maximum of USD 2,500.00. Up to 3 retry attempts. Transient rechecks occur after 300 seconds in production, using per-message Cloud Tasks; Cloud Scheduler is only the recovery net."
    )
    a = definition["clauses"][0]
    paragraph(f"{a['clause_id']} | {a['title']}", "Heading2")
    paragraph(a["text"])
    paragraph("failure_classes: schema_drift | authorized_actions: replay", "Meta")
    paragraph(
        "Allowed repairs (the entire migration_rules allowlist): rename_field customer_id -> customerId; coerce_type amount -> amount with target_type string_decimal_2. All default_value fields are null; rename_field target_type is null. No other repair is authorized.",
        "Meta",
    )
    story.append(PageBreak())
    paragraph("Defer, withhold, quarantine", "Heading1")
    for clause in definition["clauses"][1:]:
        paragraph(f"{clause['clause_id']} | {clause['title']}", "Heading2")
        paragraph(clause["text"])
        paragraph(
            "failure_classes: "
            + ", ".join(clause["failure_classes"])
            + " | authorized_actions: "
            + ", ".join(clause["authorized_actions"])
            + " | allowed_repairs: []",
            "Meta",
        )
    paragraph("Failure signatures", "Heading2")
    for signature in definition["failure_signatures"]:
        paragraph(signature["failure_class"] + ": " + signature["description"], "Meta")
    story.append(PageBreak())
    paragraph("Canonical policy definition", "Heading1")
    paragraph(
        "The JSON below is the complete definition of this synthetic runbook. Clause page references point to pages 1 and 2 above. Line wrapping is visual only. Do not infer additional clauses or repairs.",
        "Meta",
    )
    wrapped = "\n".join(
        "\n".join(
            textwrap.wrap(
                line,
                width=106,
                subsequent_indent="    ",
                replace_whitespace=False,
                drop_whitespace=False,
            )
        )
        if line
        else ""
        for line in json.dumps(definition, indent=2).splitlines()
    )
    story.append(Preformatted(wrapped, styles["PolicyCode"]))

    def footer(canvas, doc):
        canvas.saveState()
        canvas.setFillColor(colors.HexColor("#425466"))
        canvas.setFont("Helvetica", 8)
        canvas.drawString(44, 27, "RetryPermit | runbook-v3 | Synthetic demo policy")
        canvas.drawRightString(568, 27, str(doc.page))
        canvas.restoreState()

    doc = SimpleDocTemplate(
        str(OUTPUT),
        pagesize=(612, 792),
        leftMargin=44,
        rightMargin=44,
        topMargin=36,
        bottomMargin=44,
        title=definition["name"],
        author="RetryPermit",
        invariant=1,
        pageCompression=0,
    )
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    print(OUTPUT)


if __name__ == "__main__":
    build()
