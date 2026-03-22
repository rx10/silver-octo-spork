"""Application tracking routes — /api/v1/applications"""

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func
from sqlalchemy.orm import Session

from auth import get_current_user
from database import get_db
from models import Application, User
from schemas import ApplicationCreate, ApplicationOut, ApplicationStatusUpdate

router = APIRouter(prefix="/api/v1/applications", tags=["applications"])

VALID_STATUSES = {"pending", "submitted", "viewed", "interview", "rejected", "offer"}


# ── routes ────────────────────────────────────────────────────────────────────

@router.get("/stats")
def application_stats(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return application counts grouped by status."""
    rows = (
        db.query(Application.status, func.count(Application.id))
        .filter(Application.user_id == current_user.id)
        .group_by(Application.status)
        .all()
    )
    by_status = {s: c for s, c in rows}
    return {"total": sum(by_status.values()), "by_status": by_status}


@router.get("", response_model=list[ApplicationOut])
def list_applications(
    status_filter: str | None = Query(None, alias="status"),
    limit:  int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    q = db.query(Application).filter(Application.user_id == current_user.id)
    if status_filter:
        q = q.filter(Application.status == status_filter)
    return q.order_by(Application.created_at.desc()).offset(offset).limit(limit).all()


@router.post("", response_model=ApplicationOut, status_code=status.HTTP_201_CREATED)
def create_application(
    body: ApplicationCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Log a new job application."""
    application = Application(
        id=str(uuid.uuid4()),
        user_id=current_user.id,
        job_id=body.job_id,
        resume_id=body.resume_id,
        notes=body.notes,
        status="pending",
    )
    db.add(application)
    db.commit()
    db.refresh(application)
    return application


@router.patch("/{application_id}/status", response_model=ApplicationOut)
def update_application_status(
    application_id: str,
    body: ApplicationStatusUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Update the status of an application (e.g. interview, offer, rejected)."""
    if body.status not in VALID_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status. Must be one of: {sorted(VALID_STATUSES)}",
        )
    application = db.query(Application).filter(
        Application.id == application_id,
        Application.user_id == current_user.id,
    ).first()
    if not application:
        raise HTTPException(status_code=404, detail="Application not found")

    application.status = body.status
    if body.status == "submitted" and not application.submitted_at:
        application.submitted_at = datetime.utcnow()
    db.commit()
    db.refresh(application)
    return application
