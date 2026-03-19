"""
Redis caching layer for job search results.

Cache key:  jobs:search:{role_slug}:{location_slug}
Cache value: JSON-encoded list of job IDs
TTL:         Configurable, default 6 hours

Environment variables:
  REDIS_URL   — redis://[:password@]host:6379/0   (preferred)
  REDIS_HOST  — default localhost
  REDIS_PORT  — default 6379
  REDIS_DB    — default 0
  REDIS_PASSWORD — optional

Degrades gracefully: if Redis is unavailable, all cache ops are no-ops
and scraping proceeds normally.
"""

import os
import json
import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_client = None  # module-level singleton


# ── Connection ────────────────────────────────────────────────────────────────

def _get_client():
    global _client
    if _client is not None:
        return _client

    try:
        import redis

        url = os.getenv("REDIS_URL")
        if url:
            _client = redis.from_url(url, decode_responses=True, socket_timeout=3)
        else:
            _client = redis.Redis(
                host=os.getenv("REDIS_HOST", "localhost"),
                port=int(os.getenv("REDIS_PORT", 6379)),
                db=int(os.getenv("REDIS_DB", 0)),
                password=os.getenv("REDIS_PASSWORD") or None,
                decode_responses=True,
                socket_timeout=3,
            )

        _client.ping()
        logger.info("Redis connected")
    except Exception as e:
        logger.warning(f"Redis unavailable — caching disabled ({e})")
        _client = None

    return _client


# ── Key helpers ───────────────────────────────────────────────────────────────

def _slugify(text: str) -> str:
    """Normalize a search term into a stable cache key segment."""
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "any"


def cache_key(role: str, location: str) -> str:
    return f"jobs:search:{_slugify(role)}:{_slugify(location)}"


# ── Cache ops ─────────────────────────────────────────────────────────────────

DEFAULT_TTL_HOURS = int(os.getenv("CACHE_TTL_HOURS", "6"))


def get_cached(key: str) -> Optional[list[str]]:
    """
    Return cached job ID list for key, or None on miss / error.
    Also returns None if the cached list is empty (treat as miss).
    """
    r = _get_client()
    if r is None:
        return None
    try:
        raw = r.get(key)
        if raw is None:
            return None
        ids: list[str] = json.loads(raw)
        return ids if ids else None
    except Exception as e:
        logger.warning(f"Redis get error for {key}: {e}")
        return None


def set_cached(key: str, job_ids: list[str], ttl_hours: int = DEFAULT_TTL_HOURS) -> None:
    """Store a job ID list with TTL. Overwrites existing key."""
    r = _get_client()
    if r is None:
        return
    try:
        r.set(key, json.dumps(job_ids), ex=ttl_hours * 3600)
        logger.debug(f"Cached {len(job_ids)} IDs at {key} (TTL {ttl_hours}h)")
    except Exception as e:
        logger.warning(f"Redis set error for {key}: {e}")


def append_ids(key: str, new_ids: list[str]) -> None:
    """
    Atomically append new IDs to an in-progress cache entry (no TTL yet).
    Called during scraping so partial results accumulate.
    TTL is set at the end via set_cached() / finalize_cache().
    """
    r = _get_client()
    if r is None:
        return
    if not new_ids:
        return
    try:
        # Use a pipeline for atomic read-modify-write
        with r.pipeline() as pipe:
            pipe.get(key)
            current_raw, = pipe.execute()

        current: list[str] = json.loads(current_raw) if current_raw else []
        existing_set = set(current)
        to_add = [i for i in new_ids if i not in existing_set]
        if to_add:
            updated = current + to_add
            # Store without TTL during accumulation
            r.set(key, json.dumps(updated))
            logger.debug(f"Appended {len(to_add)} IDs to {key}")
    except Exception as e:
        logger.warning(f"Redis append error for {key}: {e}")


def finalize_cache(key: str, ttl_hours: int = DEFAULT_TTL_HOURS) -> list[str]:
    """
    Set the TTL on an accumulated cache key and return the final ID list.
    Call this after scraping completes.
    """
    r = _get_client()
    if r is None:
        return []
    try:
        raw = r.get(key)
        ids: list[str] = json.loads(raw) if raw else []
        if ids:
            r.expire(key, ttl_hours * 3600)
            logger.info(f"Finalized cache {key}: {len(ids)} jobs, TTL {ttl_hours}h")
        return ids
    except Exception as e:
        logger.warning(f"Redis finalize error for {key}: {e}")
        return []


def invalidate(key: str) -> None:
    """Delete a cache entry (e.g. for manual refresh)."""
    r = _get_client()
    if r is None:
        return
    try:
        r.delete(key)
        logger.info(f"Invalidated cache key: {key}")
    except Exception as e:
        logger.warning(f"Redis delete error for {key}: {e}")


def invalidate_all() -> int:
    """Delete all job search cache keys. Returns number deleted."""
    r = _get_client()
    if r is None:
        return 0
    try:
        keys = r.keys("jobs:search:*")
        if keys:
            return r.delete(*keys)
        return 0
    except Exception as e:
        logger.warning(f"Redis invalidate_all error: {e}")
        return 0


def ttl_seconds(key: str) -> int:
    """Return remaining TTL in seconds, or -1 if key missing/no TTL."""
    r = _get_client()
    if r is None:
        return -1
    try:
        return r.ttl(key)
    except Exception:
        return -1


def is_healthy() -> bool:
    """Ping Redis — used in health-check endpoints."""
    r = _get_client()
    if r is None:
        return False
    try:
        return r.ping()
    except Exception:
        return False
