"""
PostgreSQL persistence layer.

Uses psycopg2 with a ThreadedConnectionPool so multiple scraper threads
can safely write simultaneously.

Environment variables:
  DATABASE_URL  — postgres://user:pass@host:5432/dbname   (preferred)
  PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE         (fallback)
"""

import os
import json
import logging
from contextlib import contextmanager
from typing import Optional

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

logger = logging.getLogger(__name__)

# ── Connection pool ───────────────────────────────────────────────────────────

_pool: Optional[ThreadedConnectionPool] = None

def _dsn() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        # Heroku-style postgres:// → postgresql://
        return url.replace("postgres://", "postgresql://", 1)
    return (
        f"host={os.getenv('PGHOST','localhost')} "
        f"port={os.getenv('PGPORT','5432')} "
        f"dbname={os.getenv('PGDATABASE','jobboard')} "
        f"user={os.getenv('PGUSER','postgres')} "
        f"password={os.getenv('PGPASSWORD','')}"
    )

def get_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(minconn=1, maxconn=10, dsn=_dsn())
        logger.info("Postgres pool created")
    return _pool

@contextmanager
def get_conn():
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


# ── Schema ────────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id           TEXT        PRIMARY KEY,
    title        TEXT        NOT NULL DEFAULT '',
    company      TEXT        NOT NULL DEFAULT '',
    location     TEXT        NOT NULL DEFAULT '',
    posted_date  DATE,
    description  TEXT,
    salary       TEXT,
    url          TEXT        NOT NULL UNIQUE,
    source       TEXT        NOT NULL DEFAULT '',
    scraped_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- full-text search vector, auto-updated by trigger
    search_vec   TSVECTOR
);

-- GIN index for full-text search
CREATE INDEX IF NOT EXISTS idx_jobs_search_vec
    ON jobs USING GIN(search_vec);

-- Indexes for common filters
CREATE INDEX IF NOT EXISTS idx_jobs_source      ON jobs(source);
CREATE INDEX IF NOT EXISTS idx_jobs_posted_date ON jobs(posted_date DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_scraped_at  ON jobs(scraped_at  DESC);

-- Trigger: keep search_vec in sync with title + company + location + description
CREATE OR REPLACE FUNCTION jobs_search_vec_update() RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vec :=
        setweight(to_tsvector('english', coalesce(NEW.title,       '')), 'A') ||
        setweight(to_tsvector('english', coalesce(NEW.company,     '')), 'B') ||
        setweight(to_tsvector('english', coalesce(NEW.location,    '')), 'C') ||
        setweight(to_tsvector('english', coalesce(NEW.description, '')), 'D');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_jobs_search_vec ON jobs;
CREATE TRIGGER trg_jobs_search_vec
    BEFORE INSERT OR UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION jobs_search_vec_update();
"""

def init_db() -> None:
    """Create schema on startup. Safe to call multiple times (all IF NOT EXISTS)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(_SCHEMA)
    logger.info("DB schema ready")


# ── Write ─────────────────────────────────────────────────────────────────────

_UPSERT_SQL = """
INSERT INTO jobs
    (id, title, company, location, posted_date, description, salary, url, source, scraped_at)
VALUES (
    %(id)s, %(title)s, %(company)s, %(location)s,
    %(posted_date)s, %(description)s, %(salary)s, %(url)s, %(source)s, NOW()
)
ON CONFLICT (id) DO UPDATE SET
    title        = EXCLUDED.title,
    company      = EXCLUDED.company,
    location     = EXCLUDED.location,
    posted_date  = EXCLUDED.posted_date,
    description  = COALESCE(EXCLUDED.description, jobs.description),
    salary       = COALESCE(EXCLUDED.salary,      jobs.salary),
    source       = EXCLUDED.source,
    scraped_at   = NOW()
RETURNING id;
"""

def upsert_jobs(jobs: list[dict]) -> list[str]:
    """
    Insert or update a batch of job dicts. Returns list of upserted IDs.
    Safe to call from multiple threads simultaneously.
    """
    if not jobs:
        return []

    rows = []
    for j in jobs:
        rows.append({
            "id":          j["id"],
            "title":       j.get("title")       or "",
            "company":     j.get("company")     or "",
            "location":    j.get("location")    or "",
            "posted_date": j.get("posted_date"),   # None → SQL NULL → DATE
            "description": j.get("description"),
            "salary":      j.get("salary"),
            "url":         j["url"],
            "source":      j.get("source")      or "",
        })

    returned_ids: list[str] = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, _UPSERT_SQL, rows, page_size=100)
            # Collect returned ids
            # (execute_batch doesn't return rows; use execute_values for that)
    # Just return the input ids — they're deterministic hashes
    returned_ids = [r["id"] for r in rows]
    logger.debug(f"Upserted {len(returned_ids)} jobs")
    return returned_ids


# ── Read ──────────────────────────────────────────────────────────────────────

_SELECT_COLS = """
    id, title, company, location,
    posted_date::text AS posted_date,
    description, salary, url, source,
    scraped_at::text  AS scraped_at
"""

def get_jobs(
    role: Optional[str]     = None,
    location: Optional[str] = None,
    limit: int              = 5000,
) -> list[dict]:
    """
    Fetch jobs from Postgres.
    - role:     full-text search against title/company/description
    - location: case-insensitive substring match on location column
    - limit:    max rows (default 5000 — effectively unlimited for normal use)
    Returns newest-first by scraped_at.
    """
    conditions: list[str] = []
    params: list          = []

    if role and role.strip():
        # Full-text search on search_vec
        conditions.append("search_vec @@ plainto_tsquery('english', %s)")
        params.append(role.strip())

    if location and location.strip():
        conditions.append("location ILIKE %s")
        params.append(f"%{location.strip()}%")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    sql = f"""
        SELECT {_SELECT_COLS}
        FROM   jobs
        {where}
        ORDER  BY scraped_at DESC, posted_date DESC NULLS LAST
        LIMIT  %s
    """
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    return [dict(r) for r in rows]


def get_jobs_by_ids(ids: list[str]) -> list[dict]:
    """Fetch specific jobs by their IDs (used for cache-hit path)."""
    if not ids:
        return []

    sql = f"""
        SELECT {_SELECT_COLS}
        FROM   jobs
        WHERE  id = ANY(%s)
        ORDER  BY scraped_at DESC, posted_date DESC NULLS LAST
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (ids,))
            rows = cur.fetchall()

    return [dict(r) for r in rows]


def count_jobs() -> int:
    """Total jobs in DB — useful for health checks."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM jobs")
            return cur.fetchone()[0]
