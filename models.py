from datetime import datetime
from sqlalchemy import Column, String, Text, DateTime, Index, Boolean, Integer
from sqlalchemy.dialects.postgresql import JSONB
from database import Base


class Job(Base):
    __tablename__ = "jobs"

    id           = Column(String(64), primary_key=True)
    title        = Column(String(255), nullable=False)
    company      = Column(String(255), nullable=False)
    location     = Column(String(255), nullable=False)
    posted_date  = Column(String(32),  nullable=True)
    description  = Column(Text,        nullable=True)
    salary       = Column(String(128), nullable=True)
    url          = Column(Text,        nullable=False, unique=True)
    source       = Column(String(32),  nullable=False)
    scraped_at   = Column(DateTime,    default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_jobs_title",    "title"),
        Index("ix_jobs_location", "location"),
        Index("ix_jobs_source",   "source"),
    )


class User(Base):
    __tablename__ = "users"

    id                = Column(String(36),  primary_key=True)
    email             = Column(String(255), nullable=False, unique=True, index=True)
    hashed_pw         = Column(String(255), nullable=True)   # nullable for future OAuth users
    full_name         = Column(String(200), nullable=True)
    subscription_tier = Column(String(20),  nullable=False, default="free")
    auth_provider     = Column(String(20),  nullable=False, default="local")
    created_at        = Column(DateTime,    default=datetime.utcnow, nullable=False)
    updated_at        = Column(DateTime,    default=datetime.utcnow, onupdate=datetime.utcnow)


class UserProfile(Base):
    __tablename__ = "user_profiles"

    id                   = Column(String(36), primary_key=True)
    user_id              = Column(String(36), nullable=False, unique=True, index=True)
    professional_summary = Column(Text,    nullable=True)
    education            = Column(JSONB,   nullable=False, server_default="[]")
    work_experience      = Column(JSONB,   nullable=False, server_default="[]")
    skills               = Column(JSONB,   nullable=False, server_default="[]")
    certifications       = Column(JSONB,   nullable=False, server_default="[]")
    projects             = Column(JSONB,   nullable=False, server_default="[]")
    target_roles         = Column(JSONB,   nullable=True)
    target_locations     = Column(JSONB,   nullable=True)
    salary_min           = Column(Integer, nullable=True)
    is_auto_apply        = Column(Boolean, nullable=False, default=False)
    updated_at           = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Resume(Base):
    __tablename__ = "resumes"

    id          = Column(String(36),   primary_key=True)
    user_id     = Column(String(36),   nullable=False, index=True)
    job_id      = Column(String(64),   nullable=True)   # references jobs.id
    resume_data = Column(JSONB,        nullable=False, server_default="{}")
    pdf_url     = Column(String(1000), nullable=True)
    ats_score   = Column(String(10),   nullable=True)
    template_id = Column(String(50),   nullable=False, default="classic")
    version     = Column(Integer,      nullable=False, default=1)
    status      = Column(String(20),   nullable=False, default="draft")
    created_at  = Column(DateTime,     default=datetime.utcnow, nullable=False)


class OTPToken(Base):
    __tablename__ = "otp_tokens"

    id         = Column(String(36), primary_key=True)
    email      = Column(String(255), nullable=False, index=True)
    code       = Column(String(6),   nullable=False)
    expires_at = Column(DateTime,    nullable=False)
    used       = Column(Boolean,     nullable=False, default=False)
    created_at = Column(DateTime,    default=datetime.utcnow, nullable=False)


class Application(Base):
    __tablename__ = "applications"

    id           = Column(String(36), primary_key=True)
    user_id      = Column(String(36), nullable=False, index=True)
    job_id       = Column(String(64), nullable=False)
    resume_id    = Column(String(36), nullable=True)
    status       = Column(String(20), nullable=False, default="pending")
    submitted_at = Column(DateTime,   nullable=True)
    notes        = Column(Text,       nullable=True)
    created_at   = Column(DateTime,   default=datetime.utcnow, nullable=False)
