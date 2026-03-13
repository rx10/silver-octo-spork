"""
Job Board API — FastAPI backend
Endpoints:
  GET  /api/jobs          — list / search jobs
  POST /api/scrape        — trigger a fresh scrape
  GET  /health            — health check
Scheduler: APScheduler runs a scrape every 24 hours automatically.
"""

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from database import Base, engine, get_db
from models import Job
from schemas import JobOut, ScrapeRequest, ScrapeResponse
from scraper import run_scrape

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default scrape parameters — override via env vars
DEFAULT_ROLE     = os.getenv("SCRAPE_ROLE",     "Software Developer")
DEFAULT_LOCATION = os.getenv("SCRAPE_LOCATION", "California")

scheduler = AsyncIOScheduler()


# ── startup / shutdown ────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables ready")

    # Schedule automatic scrape every 24 hours
    scheduler.add_job(
        scheduled_scrape,
        trigger="interval",
        hours=24,
        id="daily_scrape",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started — daily scrape active")

    yield

    scheduler.shutdown()
    logger.info("Scheduler shut down")


app = FastAPI(title="Job Board API", version="1.0.0", lifespan=lifespan)

# Allow Next.js dev server and production domain
ALLOWED_ORIGINS = os.getenv(
    "ALLOWED_ORIGINS",
    "http://localhost:3000,http://localhost:3001"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── helpers ───────────────────────────────────────────────────────────────────

def upsert_jobs(db: Session, raw_jobs: list[dict]) -> int:
    """
    Insert new jobs, skip duplicates (by primary key / URL hash).
    Returns count of newly inserted rows.
    """
    new_count = 0
    for data in raw_jobs:
        existing = db.get(Job, data["id"])
        if existing:
            continue
        db.add(Job(**data, scraped_at=datetime.utcnow()))
        new_count += 1
    db.commit()
    return new_count


async def scheduled_scrape():
    """Called by APScheduler every 24 hours."""
    from database import SessionLocal
    logger.info("Scheduled scrape starting…")
    raw = run_scrape(DEFAULT_ROLE, DEFAULT_LOCATION)
    db  = SessionLocal()
    try:
        new = upsert_jobs(db, raw)
        logger.info(f"Scheduled scrape done — {len(raw)} scraped, {new} new")
    finally:
        db.close()


# ── routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.get("/api/jobs", response_model=list[JobOut])
def list_jobs(
    title:    str | None = Query(None, description="Filter by job title or company"),
    location: str | None = Query(None, description="Filter by location"),
    source:   str | None = Query(None, description="Filter by source: LinkedIn or Dice"),
    limit:    int        = Query(100, ge=1, le=500),
    offset:   int        = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    q = db.query(Job)

    if title:
        pattern = f"%{title}%"
        q = q.filter(
            Job.title.ilike(pattern) | Job.company.ilike(pattern)
        )
    if location:
        q = q.filter(Job.location.ilike(f"%{location}%"))
    if source:
        q = q.filter(Job.source.ilike(source))

    q = q.order_by(Job.scraped_at.desc())
    return q.offset(offset).limit(limit).all()


@app.post("/api/scrape", response_model=ScrapeResponse)
def trigger_scrape(
    body: ScrapeRequest = ScrapeRequest(),
    db: Session = Depends(get_db),
):
    """Manually trigger a fresh scrape with optional role / location override."""
    try:
        raw = run_scrape(body.role, body.location)
    except Exception as e:
        logger.error(f"Scrape failed: {e}")
        raise HTTPException(status_code=500, detail=f"Scrape error: {str(e)}")

    new = upsert_jobs(db, raw)

    return ScrapeResponse(
        scraped=len(raw),
        new=new,
        message=f"Scraped {len(raw)} jobs, {new} new added to database.",
    )
