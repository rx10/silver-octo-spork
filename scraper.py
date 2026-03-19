#!/usr/bin/env python3
"""
Multi-Source Job Scraper (Dice, LinkedIn, Indeed)

Architecture:
  - Dice: Playwright intercepts API key ONCE -> httpx API scraping.
  - LinkedIn: httpx HTML scraping (residential proxy bypasses blocks).
  - Indeed: curl_cffi impersonates Chrome TLS to bypass Cloudflare perfectly.
  - Caching: Saves Dice API key and scraped Job URLs to `scraper_cache.json`.
"""

import json
import logging
import os
import random
import re
import string
import time
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import quote_plus

import httpx
from bs4 import BeautifulSoup
from curl_cffi import requests as curl_requests

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

CACHE_FILE = "scraper_cache.json"
MAX_RETRIES = 3

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# ── Cache Management ──────────────────────────────────────────────────────────

_cache = {"dice_api_key": None, "seen_urls": []}


def _load_cache():
    global _cache
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                _cache = json.load(f)
                _cache.setdefault("seen_urls", [])
        except Exception as e:
            logger.warning(f"Failed to load cache: {e}")


def _save_cache():
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(_cache, f, indent=2)
    except Exception as e:
        logger.warning(f"Failed to save cache: {e}")


_load_cache()


# ── Proxy Configuration ───────────────────────────────────────────────────────

_OXYLABS_ENDPOINT = "pr.oxylabs.io:7777"


def _get_base_proxy_url() -> Optional[str]:
    full = os.getenv("PROXY_URL", "").strip()
    if full:
        return full
    user = os.getenv("OXYLABS_USER", "").strip()
    pwd = os.getenv("OXYLABS_PASS", "").strip()
    if user and pwd:
        return f"http://{user}:{pwd}@{_OXYLABS_ENDPOINT}"
    return None


def _get_httpx_client(**kwargs) -> httpx.Client:
    proxy = _get_base_proxy_url()
    if proxy:
        kwargs.setdefault("proxy", proxy)
    return httpx.Client(**kwargs)


def _get_curl_cffi_session() -> curl_requests.Session:
    """Returns a curl_cffi session with a sticky proxy IP for Indeed pagination."""
    proxy = _get_base_proxy_url()
    proxies = {}
    if proxy:
        # If Oxylabs, inject a random sessid to lock the IP during pagination
        if "customer-" in proxy and "-sessid-" not in proxy:
            sessid = "".join(
                random.choices(string.ascii_lowercase + string.digits, k=10)
            )
            proxy = proxy.replace("customer-", f"customer-", 1).replace(
                ":", f"-sessid-{sessid}:", 1
            )
        proxies = {"http": proxy, "https": proxy}

    return curl_requests.Session(impersonate="chrome124", proxies=proxies)


# ── Utilities ─────────────────────────────────────────────────────────────────

def delay(lo=1.0, hi=3.0):
    time.sleep(random.uniform(lo, hi))


def hdr():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }


def backoff(attempt, base=5.0):
    time.sleep(min(base * 2**attempt + random.uniform(0, 2), 60))


def trunc(text: str, n=500) -> str:
    if not text or len(text) <= n:
        return text or ""
    return text[:n].rsplit(" ", 1)[0] + " …"


def parse_date(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip().lower()
    today = datetime.utcnow().date()
    if any(w in s for w in ("today", "just", "hour", "moment")):
        return today.isoformat()
    if "yesterday" in s:
        return (today - timedelta(days=1)).isoformat()
    for unit, fn in [
        ("day", lambda n: timedelta(days=n)),
        ("week", lambda n: timedelta(weeks=n)),
        ("month", lambda n: timedelta(days=n * 30)),
    ]:
        if unit in s:
            try:
                n = int("".join(filter(str.isdigit, s)) or "1")
                return (today - fn(n)).isoformat()
            except ValueError:
                pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return today.isoformat()


def _is_us(location: str) -> bool:
    loc = location.strip().lower()
    us_kw = {"united states", "usa", "us", "remote"}
    return any(kw in loc for kw in us_kw) or "," in loc


# ══════════════════════════════════════════════════════════════════════════════
#  DICE (Playwright interception + httpx API)
# ══════════════════════════════════════════════════════════════════════════════

def _intercept_dice_key() -> Optional[str]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None

    key = None

    def on_req(req):
        nonlocal key
        if key:
            return
        if "dhigroupinc.com" in req.url.lower():
            k = req.headers.get("x-api-key")
            if k and len(k) >= 30:
                key = k

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(user_agent=random.choice(USER_AGENTS))
            page.on("request", on_req)
            page.goto(
                "https://www.dice.com/jobs?q=developer&location=United+States",
                wait_until="domcontentloaded",
                timeout=45000,
            )
            page.wait_for_timeout(4000)
            browser.close()
    except Exception as e:
        logger.warning(f"Dice key interception failed: {e}")

    return key


def _get_dice_key() -> str:
    if _cache.get("dice_api_key"):
        return _cache["dice_api_key"]

    key = _intercept_dice_key() or os.getenv("DICE_API_KEY")
    if not key:
        key = "1YAt0R9wBg4WfsF9VB2778F5CHLAPMVW3WAZcKd8"  # Fallback

    _cache["dice_api_key"] = key
    _save_cache()
    return key


def scrape_dice(role: str, location: str, max_pages=3) -> list[dict]:
    api_key = _get_dice_key()
    jobs = []
    dice_loc = location if _is_us(location) else "Remote"
    is_remote = dice_loc != location

    with _get_httpx_client(timeout=15, follow_redirects=True) as c:
        for page in range(1, max_pages + 1):
            url = (
                f"https://job-search-api.svc.dhigroupinc.com/v1/dice/jobs/search"
                f"?q={quote_plus(role)}&location={quote_plus(dice_loc)}"
                f"&pageSize=20&page={page}&language=en"
            )
            for attempt in range(MAX_RETRIES):
                try:
                    delay(0.5, 1.5)
                    resp = c.get(
                        url,
                        headers={
                            **hdr(),
                            "Accept": "application/json",
                            "x-api-key": api_key,
                        },
                    )
                    if resp.status_code == 429:
                        backoff(attempt)
                        continue
                    if resp.status_code in (401, 403):
                        logger.error("Dice auth error. Clearing cached API key.")
                        _cache["dice_api_key"] = None
                        _save_cache()
                        return jobs
                    resp.raise_for_status()
                    data = resp.json()
                    break
                except httpx.HTTPError:
                    backoff(attempt)
            else:
                break

            hits = data.get("data", [])
            if not hits:
                break

            for item in hits:
                job_url = item.get("detailsPageUrl") or f"https://www.dice.com/job-detail/{item.get('guid', '')}"
                raw_loc = item.get("jobLocation", {}).