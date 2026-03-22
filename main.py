"""
Job Board API — FastAPI backend
Auth:
  POST /api/auth/signup       — register, returns JWT + user
  POST /api/auth/login        — login,    returns JWT + user
  GET  /api/auth/me           — current user (requires Bearer token)

Profile:
  GET  /api/v1/profile                  — get profile
  PUT  /api/v1/profile                  — update profile
  PATCH /api/v1/profile/skills          — replace skills list
  PATCH /api/v1/profile/experience      — append experience entry
  PATCH /api/v1/profile/education       — append education entry
  PUT  /api/v1/profile/preferences      — update job preferences

Resumes:
  GET  /api/v1/resumes                  — list resumes
  POST /api/v1/resumes                  — create resume
  GET  /api/v1/resumes/{id}             — get resume
  PUT  /api/v1/resumes/{id}             — update resume
  POST /api/v1/resumes/{id}/regenerate  — bump version
  DELETE /api/v1/resumes/{id}           — soft-delete (archived)

Applications:
  GET  /api/v1/applications             — list applications
  POST /api/v1/applications             — log application
  GET  /api/v1/applications/stats       — counts by status
  PATCH /api/v1/applications/{id}/status — update status

Jobs:
  GET  /api/jobs              — list / search jobs
  POST /api/scrape            — trigger a fresh scrape
  GET  /api/scrape/status     — scrape progress
  GET  /health                — health check
"""

import logging
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from sqlalchemy.orm import Session

from database import Base, engine, get_db
from models import Job
from schemas import (
    JobOut,
    LoginRequest,
    ScrapeRequest,
    ScrapeResponse,
    TokenResponse,
    UserOut,
)
from auth import authenticate_user, get_current_user
from scraper import run_scrape
from routes.profile import router as profile_router
from routes.resumes import router as resumes_router
from routes.applications import router as applications_router
from routes.oauth import router as oauth_router
from routes.billing import router as billing_router
from routes.otp import router as otp_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_ROLE     = os.getenv("SCRAPE_ROLE",     "Software Developer")
DEFAULT_LOCATION = os.getenv("SCRAPE_LOCATION", "California")

scheduler = AsyncIOScheduler()

_scrape_lock = threading.Lock()
_scrape_status: dict = {
    "running":     False,
    "last_run":    None,
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


app = FastAPI(title="Socratic.pro API", version="1.0.0", lifespan=lifespan)

# ── routers ───────────────────────────────────────────────────────────────────

app.include_router(profile_router)
app.include_router(resumes_router)
app.include_router(applications_router)
app.include_router(oauth_router)
app.include_router(billing_router)
app.include_router(otp_router)

# ── CORS ──────────────────────────────────────────────────────────────────────

ALLOWED_ORIGINS = [
    "https://socratic.pro",
]

class CORSAlways(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        origin = request.headers.get("origin", "")
        allow_origin = origin if origin in ALLOWED_ORIGINS else ""

        if request.method == "OPTIONS":
            return Response(status_code=200, headers={
                "Access-Control-Allow-Origin":  allow_origin,
                "Access-Control-Allow-Methods": "GET, POST, PUT, PATCH, DELETE, OPTIONS",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Max-Age":       "86400",
            })
        try:
            response = await call_next(request)
        except Exception:
            response = Response(status_code=500, content="Internal Server Error")
        response.headers["Access-Control-Allow-Origin"]  = allow_origin
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "*"
        return response

app.add_middleware(CORSAlways)


# ── scrape helpers ────────────────────────────────────────────────────────────

def refresh_jobs(db: Session, raw_jobs: list[dict]) -> int:
    from sqlalchemy.dialects.postgresql import insert
    now = datetime.utcnow()
    for data in raw_jobs:
        stmt = insert(Job).values(**data, scraped_at=now)
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={c: stmt.excluded[c] for c in data if c != "id"},
        )
        db.execute(stmt)
    db.commit()
    return len(raw_jobs)


def _do_scrape(role: str, location: str):
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
            "running":     False,
            "last_run":    datetime.utcnow().isoformat(),
            "last_result": {
                "scraped": len(raw),
                "new":     new,
                "message": f"Refreshed database with {len(raw)} latest jobs.",
            },
        })
        logger.info(f"Scrape done — {len(raw)} scraped, {new} new")

    except Exception as e:
        _scrape_status.update({
            "running":     False,
            "last_run":    datetime.utcnow().isoformat(),
            "last_result": {"error": str(e)},
        })
        logger.error(f"Scrape failed: {e}", exc_info=True)

    finally:
        _scrape_lock.release()


async def scheduled_scrape():
    import asyncio
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _do_scrape, DEFAULT_ROLE, DEFAULT_LOCATION)


# ── health ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


# ── auth routes ───────────────────────────────────────────────────────────────

@app.post("/api/auth/signup", response_model=TokenResponse)
def signup():
    """Registration is currently closed (pitch mode)."""
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Registration is currently closed. We're in early access — stay tuned!",
    )


@app.post("/api/auth/login", response_model=TokenResponse)
def login(body: LoginRequest, db: Session = Depends(get_db)):
    """Authenticate an existing account. Returns a JWT + user on success."""
    token, user = authenticate_user(body.email, body.password, db)
    return TokenResponse(access_token=token, user=UserOut.model_validate(user))


@app.get("/api/auth/me", response_model=UserOut)
def me(current_user=Depends(get_current_user)):
    """Return the current authenticated user."""
    return current_user


# ── job routes ────────────────────────────────────────────────────────────────

@app.get("/api/jobs", response_model=list[JobOut])
def list_jobs(
    title:    str | None = Query(None, description="Filter by job title or company"),
    location: str | None = Query(None, description="Filter by location"),
    source:   str | None = Query(None, description="Filter by source"),
    limit:    int        = Query(100, ge=1, le=500),
    offset:   int        = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    q = db.query(Job)
    if title:
        pattern = f"%{title}%"
        q = q.filter(Job.title.ilike(pattern) | Job.company.ilike(pattern))
    if location:
        q = q.filter(Job.location.ilike(f"%{location}%"))
    if source:
        q = q.filter(Job.source.ilike(source))
    return q.order_by(Job.scraped_at.desc()).offset(offset).limit(limit).all()


@app.post("/api/scrape", response_model=ScrapeResponse)
def trigger_scrape(
    body: ScrapeRequest = ScrapeRequest(),
    background_tasks: BackgroundTasks = BackgroundTasks(),
):
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
    return _scrape_status
