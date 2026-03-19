"""
FastAPI backend for the job board.

Routes
------
POST /api/scrape          — trigger a scrape (cache-aware)
GET  /api/scrape/status   — poll scrape progress
GET  /api/jobs            — fetch jobs (from Postgres)
GET  /api/health          — DB + Redis health check
DELETE /api/cache         — manually bust cache for a search

Cache flow
----------
POST /api/scrape(role, location):
  1. Compute cache_key = jobs:search:{role}:{location}
  2. Redis HIT  → get_jobs_by_ids(cached_ids) from Postgres → return immediately
  3. Redis MISS → start background scrape thread:
       on_batch callback:  upsert_jobs(batch) + append_ids(key, batch_ids)
       on finish:          finalize_cache(key, ttl=6h)
  4. Client polls /api/scrape/status every 2s
  5. /api/jobs reads Postgres directly → progressive results appear

GET /api/jobs(role?, location?):
  Reads Postgres filtered by role (full-text) + location (ILIKE).
  No limit — returns everything found.
"""

import threading
import logging
import time
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import db
import cache
from scraper import run_scrape

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Job Board API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
def startup():
    db.init_db()
    logger.info(f"DB ready — {db.count_jobs()} jobs in store")
    if cache.is_healthy():
        logger.info("Redis ready")
    else:
        logger.warning("Redis unavailable — running without cache")


# ── Scrape state (in-process; replace with Redis pub/sub for multi-worker) ────

class _ScrapeState:
    def __init__(self):
        self.lock       = threading.Lock()
        self.running    = False
        self.role       = ""
        self.location   = ""
        self.jobs_found = 0          # incremental count
        self.cache_key  = ""
        self.last_result: dict = {}

_state = _ScrapeState()


# ── Models ────────────────────────────────────────────────────────────────────

class ScrapeRequest(BaseModel):
    role:     str = "Software Developer"
    location: str = "Hyderabad"


# ── Background scrape worker ──────────────────────────────────────────────────

def _scrape_worker(role: str, location: str, key: str) -> None:
    """Runs in a background thread. Writes to Postgres + Redis incrementally."""
    with _state.lock:
        _state.running    = True
        _state.role       = role
        _state.location   = location
        _state.jobs_found = 0
        _state.cache_key  = key
        _state.last_result = {}

    def on_batch(batch: list[dict]) -> None:
        ids = db.upsert_jobs(batch)
        cache.append_ids(key, ids)
        with _state.lock:
            _state.jobs_found += len(ids)
        logger.info(f"on_batch: +{len(ids)} jobs (total {_state.jobs_found})")

    try:
        all_jobs = run_scrape(role=role, location=location, on_batch=on_batch)
        # Final upsert (catches any jobs not covered by on_batch)
        db.upsert_jobs(all_jobs)
        final_ids = cache.finalize_cache(key)
        with _state.lock:
            _state.jobs_found = len(final_ids) or len(all_jobs)
            _state.last_result = {"total": _state.jobs_found}
    except Exception as e:
        logger.error(f"Scrape worker error: {e}", exc_info=True)
        with _state.lock:
            _state.last_result = {"error": str(e)}
    finally:
        with _state.lock:
            _state.running = False


# ── Routes ────────────────────────────────────────────────────────────────────

@app.post("/api/scrape")
def start_scrape(req: ScrapeRequest):
    """
    Trigger a scrape. Returns immediately.
    - Cache HIT:  returns {cached: true, count: N} — no background work needed.
    - Cache MISS: starts background thread, returns {cached: false}.
    """
    role     = req.role.strip()     or "Software Developer"
    location = req.location.strip() or "Hyderabad"
    key      = cache.cache_key(role, location)

    # ── Cache hit ─────────────────────────────────────────────────────────────
    cached_ids = cache.get_cached(key)
    if cached_ids:
        ttl = cache.ttl_seconds(key)
        logger.info(f"Cache HIT for '{role}' @ '{location}' ({len(cached_ids)} jobs, TTL {ttl}s)")
        return {
            "cached": True,
            "count":  len(cached_ids),
            "ttl_seconds": ttl,
        }

    # ── Cache miss — start scrape ─────────────────────────────────────────────
    with _state.lock:
        if _state.running:
            raise HTTPException(409, "A scrape is already running. Poll /api/scrape/status.")

    # Clear any stale partial cache entry from a previous aborted run
    cache.invalidate(key)

    thread = threading.Thread(
        target=_scrape_worker,
        args=(role, location, key),
        daemon=True,
        name=f"scraper-{role[:20]}-{location[:20]}",
    )
    thread.start()
    logger.info(f"Cache MISS — started scrape for '{role}' @ '{location}'")
    return {"cached": False, "message": "Scraping started"}


@app.get("/api/scrape/status")
def scrape_status():
    """Poll this while scraping. Returns running flag + incremental job count."""
    with _state.lock:
        return {
            "running":     _state.running,
            "jobs_found":  _state.jobs_found,
            "role":        _state.role,
            "location":    _state.location,
            "last_result": _state.last_result,
        }


@app.get("/api/jobs")
def get_jobs(
    title:    Optional[str] = Query(None, description="Full-text role search"),
    location: Optional[str] = Query(None, description="Location substring filter"),
):
    """
    Return all jobs matching filters, newest first.
    No hard limit — returns everything in Postgres that matches.
    Reads directly from Postgres so progressive results appear during a scrape.
    """
    jobs = db.get_jobs(role=title, location=location)
    return jobs


@app.delete("/api/cache")
def bust_cache(
    role:     str = Query(..., description="Role to invalidate"),
    location: str = Query(..., description="Location to invalidate"),
):
    """Manually invalidate a specific cache entry to force a fresh scrape."""
    key = cache.cache_key(role.strip(), location.strip())
    cache.invalidate(key)
    return {"invalidated": key}


@app.delete("/api/cache/all")
def bust_all_cache():
    """Invalidate all cached searches."""
    n = cache.invalidate_all()
    return {"invalidated_keys": n}


@app.get("/api/health")
def health():
    """Health check — verifies DB and Redis connectivity."""
    try:
        job_count = db.count_jobs()
        db_ok = True
    except Exception as e:
        job_count = -1
        db_ok = False

    redis_ok = cache.is_healthy()

    return {
        "db":        "ok" if db_ok     else "error",
        "redis":     "ok" if redis_ok  else "unavailable",
        "jobs_in_db": job_count,
        "scraping":  _state.running,
    }
