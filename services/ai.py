"""
Claude API integration — resume generation.

Env vars required:
  ANTHROPIC_API_KEY   — your Anthropic key
"""

import json
import logging
import os
from typing import Optional

import anthropic
from fastapi import HTTPException

logger = logging.getLogger(__name__)

MODEL_RESUME = "claude-sonnet-4-6"
MODEL_FAST   = "claude-haiku-4-5-20251001"

SYSTEM_PROMPT = """You are a professional resume writer and ATS optimization expert.

STRICT RULES:
1. Use ONLY the information provided in the candidate profile. NEVER invent or fabricate facts.
2. If information is missing, omit that section entirely — do not fill gaps.
3. Rewrite bullet points to be achievement-oriented using the X-Y-Z formula:
   "Accomplished [X] as measured by [Y], by doing [Z]."
4. Naturally integrate keywords from the Job Description into bullet points and
   the professional summary WITHOUT keyword stuffing.
5. Output ONLY valid JSON matching the schema below. No markdown, no commentary outside JSON.
6. Professional summary: 2-3 sentences, tailored to this specific role.
7. Skills section: only include skills the candidate actually listed."""


def _build_prompt(profile: dict, job: Optional[dict]) -> str:
    lines = ["=== CANDIDATE PROFILE ==="]
    lines.append(f"Name: {profile.get('full_name') or 'N/A'}")
    lines.append(f"Email: {profile.get('email') or 'N/A'}")

    if profile.get("professional_summary"):
        lines.append(f"Professional Summary (base): {profile['professional_summary']}")

    work = profile.get("work_experience") or []
    if work:
        lines.append("\nWork Experience:")
        for exp in work:
            start = exp.get("start_date", "")
            end   = exp.get("end_date") or "Present"
            lines.append(f"  - {exp.get('title')} at {exp.get('company')} ({start} – {end})")
            for b in exp.get("bullets", []):
                lines.append(f"    * {b}")

    edu = profile.get("education") or []
    if edu:
        lines.append("\nEducation:")
        for e in edu:
            lines.append(
                f"  - {e.get('degree')} in {e.get('field')}, "
                f"{e.get('school')} ({e.get('grad_year')})"
            )

    skills = profile.get("skills") or []
    if skills:
        lines.append(f"\nSkills: {', '.join(skills)}")

    certs = profile.get("certifications") or []
    if certs:
        if isinstance(certs[0], dict):
            certs = [c.get("name", str(c)) for c in certs]
        lines.append(f"Certifications: {', '.join(certs)}")

    if job:
        lines.append("\n=== TARGET JOB DESCRIPTION ===")
        lines.append(f"Title: {job.get('title', 'N/A')}")
        lines.append(f"Company: {job.get('company', 'N/A')}")
        desc = (job.get("description") or "")[:3000]   # cap to keep tokens reasonable
        lines.append(f"Description:\n{desc}")

    lines.append("""
=== OUTPUT INSTRUCTIONS ===
Return a JSON object with EXACTLY this structure (no extra keys):
{
  "professional_summary": "string",
  "work_experience": [
    {
      "company": "string",
      "title": "string",
      "start_date": "string",
      "end_date": "string",
      "bullets": ["achievement-oriented, ATS-optimised string"]
    }
  ],
  "education": [
    {"school": "string", "degree": "string", "field": "string", "year": "string"}
  ],
  "skills": {
    "technical": ["string"],
    "soft": ["string"]
  },
  "certifications": ["string"]
}""")

    return "\n".join(lines)


async def generate_resume(profile: dict, job: Optional[dict] = None) -> dict:
    """
    Call Claude to produce a tailored resume JSON.

    profile — merged dict from User + UserProfile rows
    job     — Job row dict (optional; improves tailoring when provided)
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY not configured")

    client = anthropic.AsyncAnthropic(api_key=api_key)
    prompt = _build_prompt(profile, job)

    try:
        message = await client.messages.create(
            model=MODEL_RESUME,
            max_tokens=4096,
            temperature=0.3,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
    except anthropic.APIError as e:
        logger.error(f"Anthropic API error: {e}")
        raise HTTPException(status_code=502, detail=f"AI service error: {e}")

    raw = message.content[0].text.strip()

    # Strip markdown fences if the model adds them
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.lower().startswith("json"):
            raw = raw[4:].strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error(f"Claude returned invalid JSON: {exc}\nRaw (first 500): {raw[:500]}")
        raise HTTPException(
            status_code=500,
            detail="AI returned malformed JSON. Please try again.",
        )
