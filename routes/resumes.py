"""Resume routes — /api/v1/resumes"""

import logging
import uuid
from datetime import datetime

import sentry_sdk
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import Response
from sqlalchemy.orm import Session

from auth import get_current_user
from database import get_db
from models import Job, Resume, User, UserProfile
from schemas import (
    ResumeCreate,
    ResumeGenerateRequest,
    ResumeOut,
    ResumeUpdate,
)
from services.ai import generate_resume
from services.pdf import generate_pdf

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/resumes", tags=["resumes"])

VALID_STATUSES = {"draft", "generated", "submitted", "archived"}


# ── helpers ───────────────────────────────────────────────────────────────────

def _get_resume_or_404(resume_id: str, user_id: str, db: Session) -> Resume:
    resume = db.query(Resume).filter(
        Resume.id == resume_id,
        Resume.user_id == user_id,
    ).first()
    if not resume:
        raise HTTPException(status_code=404, detail="Resume not found")
    return resume


def _build_profile_dict(user: User, profile: UserProfile | None) -> dict:
    """Merge User + UserProfile into a single dict for the AI service."""
    d: dict = {
        "full_name": user.full_name,
        "email":     user.email,
    }
    if profile:
        d.update({
            "professional_summary": profile.professional_summary,
            "work_experience":      profile.work_experience or [],
            "education":            profile.education or [],
            "skills":               profile.skills or [],
            "certifications":       profile.certifications or [],
            "projects":             profile.projects or [],
        })
    return d


# ── routes ────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[ResumeOut])
def list_resumes(
    status_filter: str | None = Query(None, alias="status"),
    limit:  int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    q = db.query(Resume).filter(Resume.user_id == current_user.id)
    if status_filter:
        q = q.filter(Resume.status == status_filter)
    return q.order_by(Resume.created_at.desc()).offset(offset).limit(limit).all()


@router.post("", response_model=ResumeOut, status_code=status.HTTP_201_CREATED)
def create_resume(
    body: ResumeCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    resume = Resume(
        id=str(uuid.uuid4()),
        user_id=current_user.id,
        job_id=body.job_id,
        resume_data=body.resume_data or {},
        template_id=body.template_id,
    )
    db.add(resume)
    db.commit()
    db.refresh(resume)
    return resume


@router.post("/generate", response_model=ResumeOut, status_code=status.HTTP_201_CREATED)
async def generate_resume_endpoint(
    body: ResumeGenerateRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    One-shot resume generation: accepts a full profile + job description inline,
    calls Claude, verifies entities, and returns the tailored resume.

    The generated resume is persisted to the database and available via
    GET /api/v1/resumes/{id} and GET /api/v1/resumes/{id}/pdf.
    """
    with sentry_sdk.start_transaction(op="resume.generate", name="Generate Resume"):
        sentry_sdk.set_user({"id": current_user.id, "email": current_user.email})

        # Build profile dict from the inline input
        p = body.profile
        profile_dict: dict = {
            "full_name":     p.personal.full_name,
            "email":         p.personal.email,
            "phone":         p.personal.phone,
            "location":      p.personal.location,
            "linkedin_url":  p.personal.linkedin_url,
            "portfolio_url": p.personal.portfolio_url,
            "work_experience": [e.model_dump() for e in p.work_experience],
            "education":       [e.model_dump() for e in p.education],
            "skills":          p.skills,
            "certifications":  p.certifications,
        }

        try:
            resume_data = await generate_resume(
                profile=profile_dict,
                job_description=body.job_description or "",
            )
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("Unexpected error during resume generation")
            sentry_sdk.capture_exception(exc)
            raise HTTPException(status_code=502, detail="AI generation failed. Please try again.")

        resume_id = str(uuid.uuid4())
        resume = Resume(
            id=resume_id,
            user_id=current_user.id,
            resume_data=resume_data,
            template_id=body.template_id,
            version=1,
            status="generated",
        )
        db.add(resume)
        db.commit()
        db.refresh(resume)

        logger.info("Resume generated — user=%s resume=%s", current_user.id, resume_id)
        return resume


@router.get("/{resume_id}", response_model=ResumeOut)
def get_resume(
    resume_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return _get_resume_or_404(resume_id, current_user.id, db)


@router.put("/{resume_id}", response_model=ResumeOut)
def update_resume(
    resume_id: str,
    body: ResumeUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    resume = _get_resume_or_404(resume_id, current_user.id, db)
    if body.resume_data is not None:
        resume.resume_data = body.resume_data
    if body.template_id is not None:
        resume.template_id = body.template_id
    if body.status is not None:
        if body.status not in VALID_STATUSES:
            raise HTTPException(status_code=400, detail=f"Invalid status. Choose from: {VALID_STATUSES}")
        resume.status = body.status
    db.commit()
    db.refresh(resume)
    return resume


@router.post("/{resume_id}/regenerate", response_model=ResumeOut)
async def regenerate_resume(
    resume_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Re-generate resume content from the user's stored profile and the linked job.
    Bumps version on each call.
    """
    with sentry_sdk.start_transaction(op="resume.regenerate", name="Regenerate Resume"):
        sentry_sdk.set_user({"id": current_user.id, "email": current_user.email})
        sentry_sdk.set_tag("resume_id", resume_id)

        logger.info("Resume regeneration started — user=%s resume=%s", current_user.id, resume_id)

        resume  = _get_resume_or_404(resume_id, current_user.id, db)
        profile = db.query(UserProfile).filter(UserProfile.user_id == current_user.id).first()
        profile_dict = _build_profile_dict(current_user, profile)

        job_dict = None
        if resume.job_id:
            job = db.query(Job).filter(Job.id == resume.job_id).first()
            if job:
                job_dict = {
                    "title":       job.title,
                    "company":     job.company,
                    "description": job.description,
                }

        try:
            new_data = await generate_resume(profile=profile_dict, job=job_dict)
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("Claude generation failed — resume=%s", resume_id)
            sentry_sdk.capture_exception(exc)
            raise HTTPException(status_code=502, detail="AI generation failed. Please try again.")

        resume.resume_data = new_data
        resume.version    += 1
        resume.status      = "generated"
        resume.pdf_url     = None
        resume.ats_score   = None
        db.commit()
        db.refresh(resume)

        logger.info("Resume regeneration complete — resume=%s version=%d", resume_id, resume.version)
        return resume


@router.get("/{resume_id}/pdf")
def download_pdf(
    resume_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Generate and stream a PDF for the given resume."""
    resume = _get_resume_or_404(resume_id, current_user.id, db)

    if not resume.resume_data:
        raise HTTPException(
            status_code=400,
            detail="Resume has no content yet. Run /generate or /regenerate first.",
        )

    # Fetch profile for richer contact header
    profile = db.query(UserProfile).filter(UserProfile.user_id == current_user.id).first()
    personal = {
        "full_name":    current_user.full_name,
        "email":        current_user.email,
        "phone":        None,
        "location":     None,
        "linkedin_url": None,
    }
    # UserProfile doesn't store contact info — only what's in User is available here.
    # The /generate endpoint path stores nothing to DB about contact details,
    # so we use what we have from the authenticated user.

    pdf_bytes = generate_pdf(resume.resume_data, personal)
    filename  = f"resume_v{resume.version}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.delete("/{resume_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_resume(
    resume_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Soft-delete by setting status to archived."""
    resume = _get_resume_or_404(resume_id, current_user.id, db)
    resume.status = "archived"
    db.commit()
