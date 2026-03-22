from datetime import datetime
from typing import Any, Optional
from pydantic import BaseModel


# ── Jobs ──────────────────────────────────────────────────────────────────────

class JobOut(BaseModel):
    id:          str
    title:       str
    company:     str
    location:    str
    posted_date: Optional[str]
    description: Optional[str]
    salary:      Optional[str]
    url:         str
    source:      str
    scraped_at:  datetime

    class Config:
        from_attributes = True


# ── Auth ──────────────────────────────────────────────────────────────────────

class UserOut(BaseModel):
    id:                str
    email:             str
    full_name:         Optional[str]
    subscription_tier: str
    created_at:        datetime

    class Config:
        from_attributes = True


class SignupRequest(BaseModel):
    email:     str
    password:  str
    full_name: Optional[str] = None


class LoginRequest(BaseModel):
    email:    str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type:   str = "bearer"
    user:         UserOut


# ── Profile ───────────────────────────────────────────────────────────────────

class ProfileOut(BaseModel):
    id:                   str
    user_id:              str
    professional_summary: Optional[str]
    education:            list
    work_experience:      list
    skills:               list
    certifications:       list
    projects:             list
    target_roles:         Optional[list]
    target_locations:     Optional[list]
    salary_min:           Optional[int]
    is_auto_apply:        bool

    class Config:
        from_attributes = True


class ProfileUpdate(BaseModel):
    professional_summary: Optional[str]  = None
    education:            Optional[list] = None
    work_experience:      Optional[list] = None
    skills:               Optional[list] = None
    certifications:       Optional[list] = None
    projects:             Optional[list] = None
    target_roles:         Optional[list] = None
    target_locations:     Optional[list] = None
    salary_min:           Optional[int]  = None
    is_auto_apply:        Optional[bool] = None


class SkillsUpdate(BaseModel):
    skills: list[str]


class ExperienceEntry(BaseModel):
    company:    str
    title:      str
    start_date: Optional[str]      = None
    end_date:   Optional[str]      = None
    bullets:    list[str]          = []


class EducationEntry(BaseModel):
    school:    str
    degree:    str
    field:     Optional[str] = None
    grad_year: Optional[str] = None
    gpa:       Optional[str] = None


class PreferencesUpdate(BaseModel):
    target_roles:     Optional[list[str]] = None
    target_locations: Optional[list[str]] = None
    salary_min:       Optional[int]       = None
    is_auto_apply:    Optional[bool]      = None


# ── Resumes ───────────────────────────────────────────────────────────────────

class ResumeCreate(BaseModel):
    job_id:      Optional[str] = None
    resume_data: dict          = {}
    template_id: str           = "classic"


class ResumeUpdate(BaseModel):
    resume_data: Optional[dict] = None
    template_id: Optional[str]  = None
    status:      Optional[str]  = None


class ResumeOut(BaseModel):
    id:          str
    user_id:     str
    job_id:      Optional[str]
    resume_data: dict
    pdf_url:     Optional[str]
    ats_score:   Optional[str]
    template_id: str
    version:     int
    status:      str
    created_at:  datetime

    class Config:
        from_attributes = True


# ── Applications ──────────────────────────────────────────────────────────────

class ApplicationCreate(BaseModel):
    job_id:    str
    resume_id: Optional[str] = None
    notes:     Optional[str] = None


class ApplicationStatusUpdate(BaseModel):
    status: str


class ApplicationOut(BaseModel):
    id:           str
    user_id:      str
    job_id:       str
    resume_id:    Optional[str]
    status:       str
    submitted_at: Optional[datetime]
    notes:        Optional[str]
    created_at:   datetime

    class Config:
        from_attributes = True


# ── Scraper ───────────────────────────────────────────────────────────────────

class ScrapeRequest(BaseModel):
    role:     str = "Software Developer"
    location: str = "California"


class ScrapeResponse(BaseModel):
    scraped: int
    new:     int
    message: str
