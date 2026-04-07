"""
Claude API integration — resume generation.

Env vars required:
  ANTHROPIC_API_KEY   — your Anthropic key
"""

import json
import logging
import os
import re
from typing import Optional

import anthropic
from fastapi import HTTPException

logger = logging.getLogger(__name__)

MODEL_RESUME = "claude-sonnet-4-6"
MAX_RETRIES  = 2
MAX_JD_CHARS = 4000

SYSTEM_PROMPT = """You are an expert professional resume writer and ATS optimization specialist with 15 years of experience helping candidates land interviews at top companies.

YOUR TASK: Generate a tailored, ATS-optimized resume by rewriting the candidate's existing information to best match the target job description.

ABSOLUTE RULES — VIOLATION OF ANY RULE IS FAILURE:

RULE 1 — ZERO FABRICATION:
Use ONLY information explicitly provided in the CANDIDATE PROFILE section.
NEVER invent, assume, or fabricate:
- Company names, job titles, or employment dates
- Metrics, numbers, percentages, or dollar amounts NOT in the original
- Schools, degrees, GPAs, or graduation dates
- Skills the candidate has not listed
- Certifications or awards NOT provided
If a section has insufficient data, include it with minimal content. NEVER fill gaps with assumed information.

RULE 2 — PRESERVE FACTS, IMPROVE LANGUAGE:
You MAY rewrite bullet points to be more impactful and ATS-friendly.
You MUST preserve all factual claims in each bullet.
Use the X-Y-Z formula: "Accomplished [X] as measured by [Y], by doing [Z]"
Keep original metrics intact. Do NOT inflate or modify numbers.

RULE 3 — KEYWORD OPTIMIZATION (NOT STUFFING):
Naturally integrate relevant keywords from the Job Description into:
- Professional summary (2-3 sentences)
- Bullet point descriptions
- Skills section
Keywords must flow naturally in context. Never list keywords artificially.
Only include skills the candidate actually listed.

RULE 4 — STRUCTURED JSON OUTPUT:
Return ONLY a valid JSON object matching the schema below.
No markdown, no backticks, no explanations, no commentary.
The response must start with { and end with }.

RULE 5 — PROFESSIONAL SUMMARY:
Write 2-3 sentences tailored to THIS specific job.
Reference the target role and company if provided.
Highlight the candidate's strongest relevant qualifications.

RULE 6 — SKILLS PRIORITIZATION:
Reorder the candidate's skills so that skills mentioned in the JD appear first.
Split into "technical" and "soft" categories.
Do NOT add any skill the candidate has not listed.

OUTPUT JSON SCHEMA:
{
  "professional_summary": "string (2-3 sentences)",
  "work_experience": [
    {
      "company": "string (EXACT match from input)",
      "title": "string (EXACT match from input)",
      "start_date": "string (EXACT match from input)",
      "end_date": "string (EXACT match from input)",
      "bullets": ["string (rewritten, ATS-optimized, 1-6 bullets)"]
    }
  ],
  "education": [
    {
      "school": "string (EXACT match from input)",
      "degree": "string (EXACT match from input)",
      "field": "string (EXACT match from input)",
      "year": "string (EXACT match from input)",
      "gpa": "string or null (EXACT match from input)"
    }
  ],
  "skills": {
    "technical": ["string (from candidate list, JD-relevant first)"],
    "soft": ["string (from candidate list)"]
  },
  "certifications": ["string (EXACT match from input)"]
}"""


# ── JD preprocessing ──────────────────────────────────────────────────────────

def clean_jd(raw_jd: str) -> str:
    """Strip HTML, normalize whitespace, truncate to MAX_JD_CHARS at a sentence boundary."""
    # Strip HTML tags
    cleaned = re.sub(r"<[^>]+>", " ", raw_jd)
    # Decode common HTML entities
    for entity, char in [("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"),
                         ("&nbsp;", " "), ("&#39;", "'"), ("&quot;", '"')]:
        cleaned = cleaned.replace(entity, char)
    # Normalize whitespace
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    # Truncate at sentence boundary
    if len(cleaned) > MAX_JD_CHARS:
        cutoff = cleaned.rfind(". ", 0, MAX_JD_CHARS)
        cleaned = cleaned[:cutoff + 1] if cutoff > 0 else cleaned[:MAX_JD_CHARS]
    return cleaned


# ── prompt builder ────────────────────────────────────────────────────────────

def _build_prompt(profile: dict, job_description: str) -> str:
    lines = ["=== CANDIDATE PROFILE ==="]
    lines.append(f"Name: {profile.get('full_name') or 'N/A'}")
    lines.append(f"Email: {profile.get('email') or 'N/A'}")
    if profile.get("phone"):
        lines.append(f"Phone: {profile['phone']}")
    if profile.get("location"):
        lines.append(f"Location: {profile['location']}")
    if profile.get("linkedin_url"):
        lines.append(f"LinkedIn: {profile['linkedin_url']}")

    work = profile.get("work_experience") or []
    if work:
        lines.append("\n--- WORK EXPERIENCE ---")
        for exp in work:
            start = exp.get("start_date", "")
            end   = exp.get("end_date") or "Present"
            lines.append(f"Company: {exp.get('company')}")
            lines.append(f"Title: {exp.get('title')}")
            lines.append(f"Duration: {start} — {end}")
            lines.append("Achievements:")
            for b in exp.get("bullets", []):
                lines.append(f"  - {b}")

    edu = profile.get("education") or []
    if edu:
        lines.append("\n--- EDUCATION ---")
        for e in edu:
            field = e.get("field") or e.get("field_of_study", "")
            year  = e.get("year") or e.get("grad_year") or e.get("graduation_year", "")
            gpa   = e.get("gpa", "")
            lines.append(f"School: {e.get('school')}")
            lines.append(f"Degree: {e.get('degree')} in {field}")
            lines.append(f"Year: {year}")
            if gpa:
                lines.append(f"GPA: {gpa}")

    skills = profile.get("skills") or []
    if skills and isinstance(skills[0], dict):
        skills = [s.get("name", str(s)) for s in skills]
    if skills:
        lines.append(f"\n--- SKILLS ---\n{', '.join(skills)}")

    certs = profile.get("certifications") or []
    if certs and isinstance(certs[0], dict):
        certs = [c.get("name", str(c)) for c in certs]
    if certs:
        lines.append(f"\n--- CERTIFICATIONS ---\n{', '.join(certs)}")

    if job_description:
        lines.append(f"\n=== TARGET JOB DESCRIPTION ===\n{job_description}")

    lines.append(
        "\n=== INSTRUCTIONS ===\n"
        "Generate an ATS-optimized resume following ALL rules in your system prompt. "
        "Return ONLY the JSON object. No other text."
    )
    return "\n".join(lines)


# ── entity verification ───────────────────────────────────────────────────────

def _verify_entities(resume_data: dict, profile: dict) -> dict:
    """
    Anti-hallucination layer: ensure every company, school, and skill in the AI
    output exists in the original profile. Silently drops anything that doesn't.
    """
    input_companies = {
        exp.get("company", "").lower().strip()
        for exp in (profile.get("work_experience") or [])
    }
    input_schools = {
        edu.get("school", "").lower().strip()
        for edu in (profile.get("education") or [])
    }

    raw_skills = profile.get("skills") or []
    if raw_skills and isinstance(raw_skills[0], dict):
        raw_skills = [s.get("name", str(s)) for s in raw_skills]
    input_skills = {s.lower() for s in raw_skills}

    # Verify work experience
    verified_work = []
    for exp in resume_data.get("work_experience", []):
        co = exp.get("company", "").lower().strip()
        if input_companies and co not in input_companies:
            logger.error("HALLUCINATION: company '%s' not in profile — dropping", exp.get("company"))
            continue
        verified_work.append(exp)
    resume_data["work_experience"] = verified_work

    # Verify education
    verified_edu = []
    for edu in resume_data.get("education", []):
        school = edu.get("school", "").lower().strip()
        if input_schools and school not in input_schools:
            logger.error("HALLUCINATION: school '%s' not in profile — dropping", edu.get("school"))
            continue
        verified_edu.append(edu)
    resume_data["education"] = verified_edu

    # Filter skills to those the candidate actually listed
    if input_skills:
        skills = resume_data.get("skills", {})
        skills["technical"] = [s for s in (skills.get("technical") or []) if s.lower() in input_skills]
        skills["soft"]      = [s for s in (skills.get("soft") or []) if s.lower() in input_skills]
        resume_data["skills"] = skills

    return resume_data


# ── public API ────────────────────────────────────────────────────────────────

async def generate_resume(
    profile: dict,
    job: Optional[dict] = None,
    job_description: str = "",
) -> dict:
    """
    Call Claude to produce a tailored resume JSON.

    profile         — merged dict from User + UserProfile rows
    job             — Job row dict {title, company, description} (optional)
    job_description — raw JD text string (takes priority over job dict)
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY not configured")

    # Resolve JD text — inline string wins, then job dict description
    raw_jd = job_description or (job or {}).get("description", "")
    # Prepend job title/company when available so the AI can reference the role
    if job and not job_description:
        header = f"{job.get('title', '')} at {job.get('company', '')}\n\n"
        raw_jd = header + raw_jd
    cleaned_jd = clean_jd(raw_jd) if raw_jd else ""

    client     = anthropic.AsyncAnthropic(api_key=api_key)
    base_prompt = _build_prompt(profile, cleaned_jd)

    last_error: Optional[Exception] = None
    for attempt in range(MAX_RETRIES + 1):
        prompt = base_prompt
        if attempt > 0:
            prompt += (
                "\n\nPREVIOUS ATTEMPT RETURNED INVALID JSON. "
                "Return ONLY a valid JSON object. No markdown, no commentary."
            )

        try:
            message = await client.messages.create(
                model=MODEL_RESUME,
                max_tokens=4096,
                temperature=0.3,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
        except anthropic.APIError as e:
            logger.error("Anthropic API error: %s", e)
            raise HTTPException(status_code=502, detail=f"AI service error: {e}")

        raw = message.content[0].text.strip()

        # Strip markdown code fences if present
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw).strip()

        try:
            data = json.loads(raw)
            data = _verify_entities(data, profile)
            logger.info(
                "Resume generation OK — attempt=%d in=%d out=%d",
                attempt + 1,
                message.usage.input_tokens,
                message.usage.output_tokens,
            )
            return data
        except json.JSONDecodeError as exc:
            logger.warning("Attempt %d — invalid JSON from Claude: %s", attempt + 1, exc)
            last_error = exc

    raise HTTPException(
        status_code=500,
        detail=f"AI returned malformed JSON after {MAX_RETRIES + 1} attempts. Please try again.",
    )
