"""Resume routes — /api/v1/resumes"""

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from auth import get_current_user
from database import get_db
from models import Resume, User
from schemas import ResumeCreate, ResumeOut, ResumeUpdate

router = APIRouter(prefix="/api/v1/resumes", tags=["resumes"])

VALID_STATUSES = {"draft", "generated", "submitted", "archived"}


# ── routes ────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[ResumeOut])
def list_resumes(
    status_filter: str | None = Query(None, alias="status"),
    limit:  int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """List all resumes for the current user."""
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
    """Create a new resume (manual or as a basis for AI generation)."""
    resume = Resume(
        id=str(uuid.uuid4()),
        user_id=current_user.id,
        job_id=body.job_id,
        resume_data=body.resume_data,
        template_id=body.template_id,
    )
    db.add(resume)
    db.commit()
    db.refresh(resume)
    return resume


@router.get("/{resume_id}", response_model=ResumeOut)
def get_resume(
    resume_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    resume = db.query(Resume).filter(
        Resume.id == resume_id,
        Resume.user_id == current_user.id,
    ).first()
    if not resume:
        raise HTTPException(status_code=404, detail="Resume not found")
    return resume


@router.put("/{resume_id}", response_model=ResumeOut)
def update_resume(
    resume_id: str,
    body: ResumeUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Edit resume data, template, or status."""
    resume = db.query(Resume).filter(
        Resume.id == resume_id,
        Resume.user_id == current_user.id,
    ).first()
    if not resume:
        raise HTTPException(status_code=404, detail="Resume not found")
    if body.resume_data is not None:
        resume.resume_data = body.resume_data
    if body.template_id is not None:
        resume.template_id = body.template_id
    if body.status is not None:
        if body.status not in VALID_STATUSES:
            raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {VALID_STATUSES}")
        resume.status = body.status
    db.commit()
    db.refresh(resume)
    return resume


@router.post("/{resume_id}/regenerate", response_model=ResumeOut)
def regenerate_resume(
    resume_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Bump version and reset status to draft (ready for AI re-generation)."""
    resume = db.query(Resume).filter(
        Resume.id == resume_id,
        Resume.user_id == current_user.id,
    ).first()
    if not resume:
        raise HTTPException(status_code=404, detail="Resume not found")
    resume.version += 1
    resume.status = "draft"
    resume.pdf_url = None
    resume.ats_score = None
    db.commit()
    db.refresh(resume)
    return resume


@router.delete("/{resume_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_resume(
    resume_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Soft-delete by setting status to archived."""
    resume = db.query(Resume).filter(
        Resume.id == resume_id,
        Resume.user_id == current_user.id,
    ).first()
    if not resume:
        raise HTTPException(status_code=404, detail="Resume not found")
    resume.status = "archived"
    db.commit()
