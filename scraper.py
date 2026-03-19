#!/usr/bin/env python3
"""
Multi-Source Job Scraper (Dice, LinkedIn, Indeed)
Fixed for LinkedIn 999 errors using curl_cffi + Oxylabs sticky sessions.
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

# ── Cache ─────────────────────────────────────────────────────────────────────

_cache = {"dice_api_key": None, "seen_urls": set()}


def _load_cache():
    global _cache
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                _cache["dice_api_key"] = data.get("dice_api_key")
                _cache["seen_urls"] = set(data.get("seen_urls", []))
        except Exception as e:
            logger.warning(f"Cache load failed: {e}")


def _save_cache():
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "dice_api_key": _cache["dice_api_key"],
                    "seen_urls": list(_cache["seen_urls"]),
                },
                f,
                indent=2,
            )
    except Exception as e:
        logger.warning(f"Cache save failed: {e}")


_load_cache()


# ── Proxy ─────────────────────────────────────────────────────────────────────

def _get_base_proxy_url() -> Optional[str]:
    full = os.getenv("PROXY_URL", "").strip()
    if full:
        return full
    user = os.getenv("OXYLABS_USER", "").strip()
    pwd = os.getenv("OXYLABS_PASS", "").strip()
    if user and pwd:
        return f"http://{user}:{pwd}@pr.oxylabs.io:7777"
    return None


def _get_curl_session() -> curl_requests.Session:
    """curl_cffi session with sticky Oxylabs IP (critical for pagination)."""
    base = _get_base_proxy_url()
    if not base:
        return curl_requests.Session(impersonate="chrome124")

    # Add sticky session ID
    sessid = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    if "customer-" in base and "-sessid-" not in base:
        # Force US + sticky session
        username_part = base.split("://")[1].split(":")[0]
        if "customer-" not in username_part:
            username_part = f"customer-{username_part}"
        username_part = f"{username_part}-cc-us-sessid-{sessid}"
        proxy_url = f"http://{username_part}:{base.split(':', 3)[-1]}"
    else:
        proxy_url = base

    return curl_requests.Session(
        impersonate="chrome124",
        proxies={"http": proxy_url, "https": proxy_url},
    )


def _get_httpx_client(**kwargs) -> httpx.Client:
    proxy = _get_base_proxy_url()
    if proxy:
        kwargs.setdefault("proxy", proxy)
    return httpx.Client(**kwargs)


# ── Utils ─────────────────────────────────────────────────────────────────────

def delay(lo=2.0, hi=5.0):
    time.sleep(random.uniform(lo, hi))


def hdr():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
    }


def backoff(attempt, base=8.0):
    time.sleep(min(base * (2**attempt) + random.uniform(0, 3), 45))


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
    # ... (keep your original parse_date logic)
    return today.isoformat()


def _is_us(location: str) -> bool:
    loc = location.strip().lower()
    return any(k in loc for k in ("united states", "usa", "us", "remote", "california", "new york", "texas"))


# ══════════════════════════════════════════════════════════════════════════════
#  DICE
# ══════════════════════════════════════════════════════════════════════════════

def _intercept_dice_key() -> Optional[str]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None

    key = None
    def on_req(req):
        nonlocal key
        if "dhigroupinc.com" in req.url.lower():
            k = req.headers.get("x-api-key")
            if k and len(k) > 30:
                key = k

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(user_agent=random.choice(USER_AGENTS))
            page.on("request", on_req)
            page.goto("https://www.dice.com/jobs?q=developer&location=United+States",
                      wait_until="domcontentloaded", timeout=45000)
            page.wait_for_timeout(5000)
            browser.close()
    except Exception as e:
        logger.warning(f"Dice key interception failed: {e}")
    return key


def _get_dice_key() -> str:
    if _cache.get("dice_api_key"):
        return _cache["dice_api_key"]

    key = _intercept_dice_key() or os.getenv("DICE_API_KEY")
    if not key:
        key = "1YAt0R9wBg4WfsF9VB2778F5CHLAPMVW3WAZcKd8"

    _cache["dice_api_key"] = key
    _save_cache()
    return key


def scrape_dice(role: str, location: str, max_pages=3) -> list[dict]:
    api_key = _get_dice_key()
    jobs = []
    dice_loc = location if _is_us(location) else "Remote"

    with _get_httpx_client(timeout=15) as c:
        for page in range(1, max_pages + 1):
            url = f"https://job-search-api.svc.dhigroupinc.com/v1/dice/jobs/search?q={quote_plus(role)}&location={quote_plus(dice_loc)}&pageSize=20&page={page}&language=en"
            for attempt in range(MAX_RETRIES):
                try:
                    delay(0.8, 1.8)
                    resp = c.get(url, headers={**hdr(), "x-api-key": api_key, "Accept": "application/json"})
                    if resp.status_code in (401, 403):
                        _cache["dice_api_key"] = None
                        _save_cache()
                        return jobs
                    resp.raise_for_status()
                    data = resp.json()
                    for item in data.get("data", []):
                        url = item.get("detailsPageUrl") or f"https://www.dice.com/job-detail/{item.get('guid')}"
                        if url in _cache["seen_urls"]:
                            continue
                        _cache["seen_urls"].add(url)
                        jobs.append({
                            "id": hash(url) % 10**12,
                            "title": item.get("title"),
                            "company": item.get("companyName", "Unknown"),
                            "location": item.get("jobLocation", {}).get("displayName", dice_loc),
                            "posted_date": parse_date(item.get("postedDate")),
                            "description": trunc(item.get("summary", "")),
                            "url": url,
                            "source": "Dice",
                        })
                    logger.info(f"Dice page {page}: {len(data.get('data', []))} jobs")
                    break
                except Exception:
                    backoff(attempt)
    return jobs


# ══════════════════════════════════════════════════════════════════════════════
#  LINKEDIN — FIXED WITH CURL_CFFI
# ══════════════════════════════════════════════════════════════════════════════

def scrape_linkedin(role: str, location: str, max_pages=2, max_details=10) -> list[dict]:
    jobs = []
    session = _get_curl_session()

    for pg in range(max_pages):
        url = f"https://www.linkedin.com/jobs/search/?keywords={quote_plus(role)}&location={quote_plus(location)}&start={pg*25}"
        for attempt in range(MAX_RETRIES):
            try:
                delay(3, 6)
                resp = session.get(url, headers=hdr(), timeout=25)
                
                if resp.status_code == 999:
                    logger.warning(f"LinkedIn 999 — rotating session")
                    session = _get_curl_session()  # new sticky IP
                    backoff(attempt, base=12)
                    continue
                    
                if resp.status_code != 200:
                    backoff(attempt)
                    continue
                    
                break
            except Exception as e:
                logger.error(f"LinkedIn error: {e}")
                backoff(attempt)

        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("div.base-card")

        for card in cards:
            title = card.select_one("h3.base-search-card__title")
            link = card.select_one("a.base-card__full-link")
            if not title or not link:
                continue
            href = link.get("href", "").split("?")[0]
            if not href.startswith("http"):
                continue
            if href in _cache["seen_urls"]:
                continue
            _cache["seen_urls"].add(href)

            jobs.append({
                "id": hash(href) % 10**12,
                "title": title.get_text(strip=True),
                "company": (card.select_one("h4") or card.select_one("span")).get_text(strip=True) if card.select_one("h4") else "Unknown",
                "location": (card.select_one("span.job-search-card__location") or card.select_one("span")).get_text(strip=True),
                "url": href,
                "source": "LinkedIn",
                "description": None,
            })

        logger.info(f"LinkedIn page {pg}: {len(cards)} cards found")

    _save_cache()
    return jobs


# ══════════════════════════════════════════════════════════════════════════════
#  INDEED (curl_cffi)
# ══════════════════════════════════════════════════════════════════════════════

def scrape_indeed(role: str, location: str, max_pages=3) -> list[dict]:
    jobs = []
    session = _get_curl_session()

    for page in range(max_pages):
        url = f"https://www.indeed.com/jobs?q={quote_plus(role)}&l={quote_plus(location)}&start={page*10}&sort=date"
        resp = session.get(url, headers=hdr(), timeout=20)
        
        if resp.status_code != 200:
            logger.warning(f"Indeed blocked with status {resp.status_code}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("div.job_seen_beacon, div.css-5lfssm")

        for card in cards:
            title = card.select_one("h2.jobTitle span")
            link = card.select_one("h2.jobTitle a")
            if not title or not link:
                continue
            href = link.get("href", "")
            if href.startswith("/"):
                href = "https://www.indeed.com" + href
            if href in _cache["seen_urls"]:
                continue
            _cache["seen_urls"].add(href)

            jobs.append({
                "id": hash(href) % 10**12,
                "title": title.get_text(strip=True),
                "company": card.select_one("span[data-testid='company-name']").get_text(strip=True) if card.select_one("span[data-testid='company-name']") else "Unknown",
                "location": card.select_one("div[data-testid='text-location']").get_text(strip=True) if card.select_one("div[data-testid='text-location']") else location,
                "url": href,
                "source": "Indeed",
            })

    _save_cache()
    return jobs


# ══════════════════════════════════════════════════════════════════════════════
#  RUN
# ══════════════════════════════════════════════════════════════════════════════

def run_scrape(role="Software Developer", location="San Diego"):
    all_jobs = []
    for source, func in [
        ("Dice", scrape_dice),
        ("LinkedIn", scrape_linkedin),
        ("Indeed", scrape_indeed),
    ]:
        try:
            jobs = func(role, location)
            all_jobs.extend(jobs)
            logger.info(f"{source}: collected {len(jobs)} jobs")
        except Exception as e:
            logger.error(f"{source} failed: {e}")

    logger.info(f"Total jobs collected: {len(all_jobs)}")
    return all_jobs


if __name__ == "__main__":
    run_scrape(role="delivery manager", location="San Diego")