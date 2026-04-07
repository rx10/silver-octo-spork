"""
ATS-friendly PDF generation using ReportLab.

Single-column, Helvetica font, real text layer — parseable by all major ATS systems.
"""

import io
from typing import Optional

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    HRFlowable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


# ── style helpers ─────────────────────────────────────────────────────────────

def _styles():
    base = getSampleStyleSheet()
    return {
        "name": ParagraphStyle(
            "Name", parent=base["Normal"],
            fontSize=18, fontName="Helvetica-Bold",
            alignment=TA_CENTER, spaceAfter=4,
        ),
        "contact": ParagraphStyle(
            "Contact", parent=base["Normal"],
            fontSize=9.5, fontName="Helvetica",
            alignment=TA_CENTER, spaceAfter=2, textColor=colors.HexColor("#374151"),
        ),
        "section": ParagraphStyle(
            "Section", parent=base["Normal"],
            fontSize=10.5, fontName="Helvetica-Bold",
            spaceBefore=10, spaceAfter=2,
            textColor=colors.black,
        ),
        "body": ParagraphStyle(
            "Body", parent=base["Normal"],
            fontSize=10, fontName="Helvetica",
            spaceAfter=2, leading=14,
        ),
        "job_title": ParagraphStyle(
            "JobTitle", parent=base["Normal"],
            fontSize=10, fontName="Helvetica-BoldOblique",
            spaceAfter=1, spaceBefore=4,
        ),
        "job_header_left": ParagraphStyle(
            "JobHeaderLeft", parent=base["Normal"],
            fontSize=10, fontName="Helvetica-Bold",
            spaceAfter=0,
        ),
        "job_header_right": ParagraphStyle(
            "JobHeaderRight", parent=base["Normal"],
            fontSize=10, fontName="Helvetica",
            alignment=TA_RIGHT, spaceAfter=0,
            textColor=colors.HexColor("#4B5563"),
        ),
        "bullet": ParagraphStyle(
            "Bullet", parent=base["Normal"],
            fontSize=10, fontName="Helvetica",
            leftIndent=14, spaceAfter=2, leading=14,
        ),
        "skills_label": ParagraphStyle(
            "SkillsLabel", parent=base["Normal"],
            fontSize=10, fontName="Helvetica-Bold",
            spaceAfter=2, leading=14,
        ),
    }


def _divider(thin=False):
    return HRFlowable(
        width="100%",
        thickness=0.5 if thin else 1,
        color=colors.HexColor("#374151") if thin else colors.black,
        spaceAfter=4,
        spaceBefore=0,
    )


def _section_header(title: str, s: dict):
    """Bold ALL-CAPS section header followed by a thin rule."""
    return [Paragraph(title, s["section"]), _divider(thin=True)]


# ── public API ────────────────────────────────────────────────────────────────

def generate_pdf(resume_data: dict, personal: dict) -> bytes:
    """
    Generate an ATS-friendly PDF from resume_data.

    resume_data — JSON produced by Claude (professional_summary, work_experience, …)
    personal    — contact dict with any of: full_name, email, phone, location,
                  linkedin_url, portfolio_url
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

    s     = _styles()
    story = []

    # ── Header — name ─────────────────────────────────────────────────────────
    full_name = (personal.get("full_name") or "").strip() or personal.get("email", "Candidate")
    story.append(Paragraph(full_name, s["name"]))

    # Contact line — email | phone | location | linkedin
    contact_parts = [p for p in [
        personal.get("email"),
        personal.get("phone"),
        personal.get("location"),
        personal.get("linkedin_url"),
        personal.get("portfolio_url"),
    ] if p]
    if contact_parts:
        story.append(Paragraph(" &nbsp;|&nbsp; ".join(contact_parts), s["contact"]))

    story.append(Spacer(1, 4))
    story.append(_divider())

    # ── Professional Summary ──────────────────────────────────────────────────
    summary = (resume_data.get("professional_summary") or "").strip()
    if summary:
        story.extend(_section_header("PROFESSIONAL SUMMARY", s))
        story.append(Paragraph(summary, s["body"]))

    # ── Work Experience ───────────────────────────────────────────────────────
    work_exp = resume_data.get("work_experience") or []
    if work_exp:
        story.extend(_section_header("WORK EXPERIENCE", s))
        usable_width = (letter[0] - 1.3 * inch)  # page width minus margins
        for exp in work_exp:
            company    = exp.get("company", "")
            title      = exp.get("title", "")
            start      = exp.get("start_date", "")
            end        = exp.get("end_date") or "Present"
            date_range = f"{start} – {end}" if start else end

            # Company (bold left) | Date range (right) on the same row
            header_table = Table(
                [[Paragraph(company, s["job_header_left"]),
                  Paragraph(date_range, s["job_header_right"])]],
                colWidths=[usable_width * 0.65, usable_width * 0.35],
                hAlign="LEFT",
            )
            header_table.setStyle(TableStyle([
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING",  (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING",   (0, 0), (-1, -1), 2),
                ("BOTTOMPADDING",(0, 0), (-1, -1), 0),
            ]))
            story.append(header_table)
            story.append(Paragraph(title, s["job_title"]))

            for bullet in exp.get("bullets", []):
                story.append(Paragraph(f"• {bullet}", s["bullet"]))

            story.append(Spacer(1, 4))

    # ── Education ─────────────────────────────────────────────────────────────
    education = resume_data.get("education") or []
    if education:
        story.extend(_section_header("EDUCATION", s))
        usable_width = letter[0] - 1.3 * inch
        for edu in education:
            degree = edu.get("degree", "")
            field  = edu.get("field", "") or edu.get("field_of_study", "")
            school = edu.get("school", "")
            year   = edu.get("year", "") or edu.get("graduation_year", "")
            gpa    = edu.get("gpa", "")

            degree_text = f"{degree} in {field}" if field else degree
            edu_table = Table(
                [[Paragraph(school, s["job_header_left"]),
                  Paragraph(year, s["job_header_right"])]],
                colWidths=[usable_width * 0.65, usable_width * 0.35],
                hAlign="LEFT",
            )
            edu_table.setStyle(TableStyle([
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING",  (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING",   (0, 0), (-1, -1), 2),
                ("BOTTOMPADDING",(0, 0), (-1, -1), 0),
            ]))
            story.append(edu_table)
            story.append(Paragraph(degree_text, s["job_title"]))
            if gpa:
                story.append(Paragraph(f"GPA: {gpa}", s["body"]))
            story.append(Spacer(1, 4))

    # ── Skills ────────────────────────────────────────────────────────────────
    skills    = resume_data.get("skills") or {}
    technical = skills.get("technical") or []
    soft      = skills.get("soft") or []
    if technical or soft:
        story.extend(_section_header("SKILLS", s))
        if technical:
            story.append(Paragraph(
                f"<b>Technical:</b> {', '.join(technical)}", s["body"]
            ))
        if soft:
            story.append(Paragraph(
                f"<b>Soft Skills:</b> {', '.join(soft)}", s["body"]
            ))

    # ── Certifications ────────────────────────────────────────────────────────
    certs = resume_data.get("certifications") or []
    if certs:
        story.extend(_section_header("CERTIFICATIONS", s))
        for cert in certs:
            story.append(Paragraph(f"• {cert}", s["bullet"]))

    doc.build(story)
    return buffer.getvalue()
