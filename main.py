"""
Job Board API — FastAPI backend
Endpoints:
  GET  /api/jobs          — list / search jobs
  POST /api/scrape        — trigger a fresh scrape (runs in background)
  GET  /api/scrape/status — check if a scrape is running
  GET  /health            — health check
Scheduler: APScheduler runs a scrape every 24 hours automatically.
"""

import logging
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
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

# ── scrape state (lightweight in-memory tracker) ─────────────────────────────

_scrape_lock = threading.Lock()
_scrape_status: dict = {
    "running": False,
    "last_run": None,
    "last_result": None,
}


# ── startup / shutdown ────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables ready")

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

# Custom CORS — always sends headers, even on 500/502
class CORSAlways(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.method == "OPTIONS":
            return Response(status_code=200, headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Max-Age": "86400",
            })
        try:
            response = await call_next(request)
        except Exception:
            response = Response(status_code=500, content="Internal Server Error")
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "*"
        return response

app.add_middleware(CORSAlways)


# ── helpers ───────────────────────────────────────────────────────────────────

def refresh_jobs(db: Session, raw_jobs: list[dict]) -> int:
    """Clear all existing jobs and insert the fresh batch."""
    deleted = db.query(Job).delete()
    logger.info(f"Cleared {deleted} old jobs from database")

    now = datetime.utcnow()
    for data in raw_jobs:
        db.add(Job(**data, scraped_at=now))

    db.commit()
    logger.info(f"Inserted {len(raw_jobs)} fresh jobs")
    return len(raw_jobs)


def _do_scrape(role: str, location: str):
    """Run the scrape synchronously and persist results."""
    global _scrape_status

    if not _scrape_lock.acquire(blocking=False):
        logger.info("Scrape already running — skipping")
        return

    try:
        _scrape_status["running"] = True
        logger.info(f"Scrape starting — role={role!r}, location={location!r}")

        raw = run_scrape(role, location)

        from database import SessionLocal
        db = SessionLocal()
        try:
            new = refresh_jobs(db, raw)
        finally:
            db.close()

        _scrape_status.update({
            "running": False,
            "last_run": datetime.utcnow().isoformat(),
            "last_result": {
                "scraped": len(raw),
                "new": new,
                "message": f"Refreshed database with {len(raw)} latest jobs.",
            },
        })
        logger.info(f"Scrape done — {len(raw)} scraped, {new} new")

    except Exception as e:
        _scrape_status.update({
            "running": False,
            "last_run": datetime.utcnow().isoformat(),
            "last_result": {"error": str(e)},
        })
        logger.error(f"Scrape failed: {e}", exc_info=True)

    finally:
        _scrape_lock.release()


async def scheduled_scrape():
    """Called by APScheduler every 24 hours."""
    import asyncio
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _do_scrape, DEFAULT_ROLE, DEFAULT_LOCATION)


# ── routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

print("Hello, World!")


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
    background_tasks: BackgroundTasks = BackgroundTasks(),
):
    """Trigger a fresh scrape. Runs in background to avoid HTTP timeouts."""
    if _scrape_status["running"]:
        return ScrapeResponse(
            scraped=0,
            new=0,
            message="A scrape is already running. Check /api/scrape/status for progress.",
        )

    background_tasks.add_task(_do_scrape, body.role, body.location)

    return ScrapeResponse(
        scraped=0,
        new=0,
        message="Scrape started in background. Check /api/scrape/status for results.",
    )


@app.get("/api/scrape/status")
def scrape_status():
    """Check the current scrape status and last result."""
    return _scrape_status