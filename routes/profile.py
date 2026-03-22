"""Profile routes — GET/PUT /api/v1/profile and sub-routes."""

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from auth import get_current_user
from database import get_db
from models import User, UserProfile
from schemas import (
    EducationEntry,
    ExperienceEntry,
    PreferencesUpdate,
    ProfileOut,
    ProfileUpdate,
    SkillsUpdate,
)

router = APIRouter(prefix="/api/v1/profile", tags=["profile"])


# ── helper ────────────────────────────────────────────────────────────────────

def _get_or_create_profile(user: User, db: Session) -> UserProfile:
    profile = db.query(UserProfile).filter(UserProfile.user_id == user.id).first()
    if not profile:
        profile = UserProfile(
            id=str(uuid.uuid4()),
            user_id=user.id,
            education=[],
            work_experience=[],
            skills=[],
            certifications=[],
            projects=[],
        )
        db.add(profile)
        db.commit()
        db.refresh(profile)
    return profile


# ── routes ────────────────────────────────────────────────────────────────────

@router.get("", response_model=ProfileOut)
def get_profile(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return the current user's profile (auto-creates if missing)."""
    return _get_or_create_profile(current_user, db)


@router.put("", response_model=ProfileOut)
def update_profile(
    body: ProfileUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Replace any subset of profile fields."""
    profile = _get_or_create_profile(current_user, db)
    for field, value in body.model_dump(exclude_none=True).items():
        setattr(profile, field, value)
    profile.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(profile)
    return profile


@router.patch("/skills", response_model=ProfileOut)
def update_skills(
    body: SkillsUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Replace the skills list."""
    profile = _get_or_create_profile(current_user, db)
    profile.skills = body.skills
    profile.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(profile)
    return profile


@router.patch("/experience", response_model=ProfileOut)
def add_experience(
    body: ExperienceEntry,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Append a single work experience entry."""
    profile = _get_or_create_profile(current_user, db)
    experience = list(profile.work_experience or [])
    experience.append(body.model_dump())
    profile.work_experience = experience
    profile.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(profile)
    return profile


@router.patch("/education", response_model=ProfileOut)
def add_education(
    body: EducationEntry,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Append a single education entry."""
    profile = _get_or_create_profile(current_user, db)
    education = list(profile.education or [])
    education.append(body.model_dump())
    profile.education = education
    profile.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(profile)
    return profile


@router.put("/preferences", response_model=ProfileOut)
def update_preferences(
    body: PreferencesUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Update job search preferences."""
    profile = _get_or_create_profile(current_user, db)
    for field, value in body.model_dump(exclude_none=True).items():
        setattr(profile, field, value)
    profile.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(profile)
    return profile
