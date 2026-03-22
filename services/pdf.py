"""
ATS-friendly PDF generation using ReportLab.

Single-column, Helvetica font, real text layer — parseable by all major ATS systems.
"""

import io

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import HRFlowable, Paragraph, SimpleDocTemplate, Spacer


# ── style helpers ─────────────────────────────────────────────────────────────

def _styles():
    base = getSampleStyleSheet()
    return {
        "name": ParagraphStyle(
            "Name", parent=base["Normal"],
            fontSize=16, fontName="Helvetica-Bold",
            alignment=TA_CENTER, spaceAfter=3,
        ),
        "contact": ParagraphStyle(
            "Contact", parent=base["Normal"],
            fontSize=10, fontName="Helvetica",
            alignment=TA_CENTER, spaceAfter=6,
        ),
        "section": ParagraphStyle(
            "Section", parent=base["Normal"],
            fontSize=11, fontName="Helvetica-Bold",
            spaceBefore=10, spaceAfter=3,
            textColor=colors.black,
        ),
        "body": ParagraphStyle(
            "Body", parent=base["Normal"],
            fontSize=10, fontName="Helvetica",
            spaceAfter=2, leading=14,
        ),
        "job_header": ParagraphStyle(
            "JobHeader", parent=base["Normal"],
            fontSize=10, fontName="Helvetica-Bold",
            spaceAfter=1, spaceBefore=6,
        ),
        "bullet": ParagraphStyle(
            "Bullet", parent=base["Normal"],
            fontSize=10, fontName="Helvetica",
            leftIndent=14, spaceAfter=2, leading=14,
        ),
    }


def _divider(thin=False):
    return HRFlowable(
        width="100%",
        thickness=0.5 if thin else 1,
        color=colors.grey if thin else colors.black,
        spaceAfter=4,
    )


# ── public API ────────────────────────────────────────────────────────────────

def generate_pdf(resume_data: dict, user: dict) -> bytes:
    """
    Generate an ATS-friendly PDF from resume_data.

    resume_data — the JSON produced by Claude (or manually created)
    user        — dict with keys: full_name, email
    Returns raw PDF bytes.
    """
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        leftMargin=0.65 * inch,
        rightMargin=0.65 * inch,
        topMargin=0.65 * inch,
        bottomMargin=0.65 * inch,
    )

    s = _styles()
    story = []

    # ── Header ────────────────────────────────────────────────────────────────
    full_name = (user.get("full_name") or "").strip() or user.get("email", "")
    story.append(Paragraph(full_name, s["name"]))

    contact_parts = [p for p in [user.get("email")] if p]
    if contact_parts:
        story.append(Paragraph(" | ".join(contact_parts), s["contact"]))

    story.append(_divider())

    # ── Summary ───────────────────────────────────────────────────────────────
    summary = (resume_data.get("professional_summary") or "").strip()
    if summary:
        story.append(Paragraph("PROFESSIONAL SUMMARY", s["section"]))
        story.append(_divider(thin=True))
        story.append(Paragraph(summary, s["body"]))

    # ── Work Experience ───────────────────────────────────────────────────────
    work_exp = resume_data.get("work_experience") or []
    if work_exp:
        story.append(Paragraph("WORK EXPERIENCE", s["section"]))
        story.append(_divider(thin=True))
        for exp in work_exp:
            title   = exp.get("title", "")
            company = exp.get("company", "")
            start   = exp.get("start_date", "")
            end     = exp.get("end_date") or "Present"
            story.append(
                Paragraph(f"<b>{title}</b> — {company} | {start} – {end}", s["job_header"])
            )
            for bullet in exp.get("bullets", []):
                story.append(Paragraph(f"• {bullet}", s["bullet"]))

    # ── Education ─────────────────────────────────────────────────────────────
    education = resume_data.get("education") or []
    if education:
        story.append(Paragraph("EDUCATION", s["section"]))
        story.append(_divider(thin=True))
        for edu in education:
            degree = edu.get("degree", "")
            field  = edu.get("field", "")
            school = edu.get("school", "")
            year   = edu.get("year", "")
            story.append(
                Paragraph(f"<b>{degree} in {field}</b> — {school} ({year})", s["job_header"])
            )

    # ── Skills ────────────────────────────────────────────────────────────────
    skills = resume_data.get("skills") or {}
    technical = skills.get("technical") or []
    soft      = skills.get("soft") or []
    if technical or soft:
        story.append(Paragraph("SKILLS", s["section"]))
        story.append(_divider(thin=True))
        if technical:
            story.append(Paragraph(f"<b>Technical:</b> {', '.join(technical)}", s["body"]))
        if soft:
            story.append(Paragraph(f"<b>Soft Skills:</b> {', '.join(soft)}", s["body"]))

    # ── Certifications ────────────────────────────────────────────────────────
    certs = resume_data.get("certifications") or []
    if certs:
        story.append(Paragraph("CERTIFICATIONS", s["section"]))
        story.append(_divider(thin=True))
        for cert in certs:
            story.append(Paragraph(f"• {cert}", s["bullet"]))

    doc.build(story)
    return buffer.getvalue()
