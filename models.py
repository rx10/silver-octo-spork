from datetime import datetime
from sqlalchemy import Column, String, Text, DateTime, Index, Boolean
from database import Base


class Job(Base):
    __tablename__ = "jobs"

    # Unique ID derived from a hash of the job URL — prevents duplicates
    id           = Column(String(64), primary_key=True)
    title        = Column(String(255), nullable=False)
    company      = Column(String(255), nullable=False)
    location     = Column(String(255), nullable=False)
    posted_date  = Column(String(32), nullable=True)   # stored as ISO string e.g. "2026-03-10"
    description  = Column(Text, nullable=True)
    salary       = Column(String(128), nullable=True)
    url          = Column(Text, nullable=False, unique=True)
    source       = Column(String(32), nullable=False)  # "LinkedIn" | "Dice"
    scraped_at   = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_jobs_title",    "title"),
        Index("ix_jobs_location", "location"),
        Index("ix_jobs_source",   "source"),
    )


class User(Base):
    __tablename__ = "users"

    id         = Column(String(36), primary_key=True)   # UUID
    email      = Column(String(255), nullable=False, unique=True, index=True)
    hashed_pw  = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
