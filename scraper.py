"""
Scraper module — Dice.com + LinkedIn
Uses httpx for requests and BeautifulSoup for parsing.
Playwright is available as a fallback for JS-heavy pages (see comments).

Rate limiting:
  - Random delay between requests (1–3 s)
  - Rotates User-Agent strings
  - Respects HTTP 429 with exponential backoff
"""
import re
import os
import hashlib
import random
import time
import logging
from datetime import datetime, timedelta
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


def make_id(url: str) -> str:
    """Deterministic job ID from URL hash — natural dedup key."""
    return hashlib.sha256(url.encode()).hexdigest()[:32]


def random_delay():
    time.sleep(random.uniform(1.0, 3.0))


def get_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }


def parse_dice_date(date_str: Optional[str]) -> Optional[str]:
    """Convert Dice relative dates ('2 days ago') to ISO date strings."""
    if not date_str:
        return None
    date_str = date_str.strip().lower()
    today = datetime.utcnow().date()
    if "today" in date_str or "just" in date_str or "hour" in date_str:
        return today.isoformat()
    if "yesterday" in date_str:
        return (today - timedelta(days=1)).isoformat()
    if "day" in date_str:
        try:
            n = int("".join(filter(str.isdigit, date_str)))
            return (today - timedelta(days=n)).isoformat()
        except ValueError:
            pass
    return today.isoformat()


# ── Dice scraper ──────────────────────────────────────────────────────────────

def scrape_dice(role: str, location: str, max_pages: int = 3) -> list[dict]:
    api_key = get_dice_api_key()  # ← dynamic
    """
    Scrape Dice.com using their internal search API — more reliable than HTML parsing.
    """
    jobs = []

    with httpx.Client(timeout=15, follow_redirects=True) as client:
        for page in range(1, max_pages + 1):
            url = (
                f"https://job-search-api.svc.dhigroupinc.com/v1/dice/jobs/search"
                f"?q={role.replace(' ', '+')}&countryCode=US&location={location.replace(' ', '+')}"
                f"&pageSize=20&page={page}&language=en"
            )
            try:
                random_delay()
                resp = client.get(url, headers={
                    "User-Agent": random.choice(USER_AGENTS),
                    "Accept": "application/json",
                    "x-api-key": api_key,
                })
                if resp.status_code == 429:
                    logger.warning("Dice API rate-limited — backing off 30s")
                    time.sleep(30)
                    continue
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(f"Dice API error on page {page}: {e}")
                break

            hits = data.get("data", [])
            if not hits:
                logger.info(f"Dice page {page}: no results, stopping")
                break

            for item in hits:
                try:
                    job_url = item.get("detailsPageUrl") or f"https://www.dice.com/job-detail/{item.get('guid', '')}"
                    jobs.append({
                        "id":          make_id(job_url),
                        "title":       item.get("title", ""),
                        "company":     item.get("companyName", "Unknown"),
                        "location":    item.get("jobLocation", {}).get("displayName") or location,
                        "posted_date": parse_dice_date(item.get("postedDate")),
                        "description": (item.get("summary") or "")[:500],
                        "salary":      item.get("salary"),
                        "url":         job_url,
                        "source":      "Dice",
                    })
                except Exception as e:
                    logger.warning(f"Dice item parse error: {e}")
                    continue

            logger.info(f"Dice page {page}: scraped {len(hits)} jobs")

    logger.info(f"Dice total: {len(jobs)} jobs")
    return jobs



import re

def get_dice_api_key() -> str:
    """Fetch Dice's API key dynamically from their JS bundle."""
    try:
        with httpx.Client(timeout=10, follow_redirects=True) as client:
            resp = client.get("https://www.dice.com", headers=get_headers())
            soup = BeautifulSoup(resp.text, "html.parser")

            scripts = soup.find_all("script", src=True)
            bundle_url = None
            for s in scripts:
                src = s["src"]
                if "_app" in src or "main" in src or "webpack" in src:
                    bundle_url = src if src.startswith("http") else f"https://www.dice.com{src}"
                    break

if not bundle_url:
    logger.warning("Dice API key: no JS bundle found — using fallback")
else:
    logger.info(f"Dice API key: found bundle URL: {bundle_url}")
    js = client.get(bundle_url, headers=get_headers()).text
    logger.info(f"Dice API key: JS bundle size: {len(js)} chars")
    match = re.search(r'x-api-key["\s:]+([A-Za-z0-9]{32,})', js)
    if match:
        ...
    else:
        logger.warning("Dice API key: regex found no match — trying alternate patterns")
        # try alternate patterns
        for pattern in [r'"apiKey"\s*:\s*"([A-Za-z0-9]{32,})"', r'apiKey=([A-Za-z0-9]{32,})']:
            m = re.search(pattern, js)
            if m:
                logger.info(f"Dice API key: found with alternate pattern: {m.group(1)[:8]}...")
                return m.group(1)
                
    except Exception as e:
        logger.warning(f"Dice API key: dynamic fetch failed ({e}) — using fallback")

    env_key = os.getenv("DICE_API_KEY")
    if env_key:
        logger.info("Dice API key: using DICE_API_KEY env var")
        return env_key

    logger.warning("Dice API key: using hardcoded default — consider setting DICE_API_KEY env var")
    return "1YAt0R9wBg4WfsF9VB2778F5CHLAPMVW3WAZcKd8"
# ── LinkedIn scraper ──────────────────────────────────────────────────────────

def scrape_linkedin(role: str, location: str, max_pages: int = 3) -> list[dict]:
    """
    Scrape LinkedIn public job listings (no login required for browsing).

    LinkedIn is more aggressive with bot detection. This uses:
      - Randomised User-Agent rotation
      - Longer random delays
      - The /jobs/search endpoint which returns static HTML

    If LinkedIn blocks requests consistently, switch to Playwright with
    stealth mode (playwright-stealth pip package).
    """
    jobs = []
    role_slug = role.replace(" ", "%20")
    loc_slug  = location.replace(" ", "%20")

    with httpx.Client(timeout=20, follow_redirects=True) as client:
        for page in range(max_pages):
            start = page * 25
            url = (
                f"https://www.linkedin.com/jobs/search/"
                f"?keywords={role_slug}&location={loc_slug}&start={start}"
            )
            try:
                time.sleep(random.uniform(2.0, 4.5))   # LinkedIn needs longer delays
                resp = client.get(url, headers=get_headers())
                if resp.status_code in (429, 999):
                    logger.warning("LinkedIn rate-limited — backing off 60s")
                    time.sleep(60)
                    resp = client.get(url, headers=get_headers())
                resp.raise_for_status()
            except httpx.HTTPError as e:
                logger.error(f"LinkedIn HTTP error on page {page}: {e}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            cards = soup.select("div.base-card") or \
                    soup.select("li.jobs-search__results-list > div")

            if not cards:
                logger.info(f"LinkedIn page {page}: no cards found, stopping")
                break

            for card in cards:
                try:
                    title_el   = card.select_one("h3.base-search-card__title, h3")
                    company_el = card.select_one("h4.base-search-card__subtitle, h4")
                    loc_el     = card.select_one("span.job-search-card__location, span.location")
                    date_el    = card.select_one("time")
                    link_el    = card.select_one("a.base-card__full-link, a")

                    if not title_el or not link_el:
                        continue

                    href = link_el.get("href", "").split("?")[0]
                    if not href.startswith("http"):
                        continue

                    # date from <time datetime="2026-03-10">
                    posted = None
                    if date_el:
                        posted = date_el.get("datetime") or parse_dice_date(date_el.get_text())

                    jobs.append({
                        "id":          make_id(href),
                        "title":       title_el.get_text(strip=True),
                        "company":     company_el.get_text(strip=True) if company_el else "Unknown",
                        "location":    loc_el.get_text(strip=True)     if loc_el     else location,
                        "posted_date": posted,
                        "description": None,   # LinkedIn hides full desc behind login
                        "salary":      None,
                        "url":         href,
                        "source":      "LinkedIn",
                    })
                except Exception as e:
                    logger.warning(f"LinkedIn card parse error: {e}")
                    continue

            logger.info(f"LinkedIn page {page}: scraped {len(cards)} cards")

    logger.info(f"LinkedIn total: {len(jobs)} jobs")
    return jobs


# ── combined entry point ──────────────────────────────────────────────────────

def run_scrape(role: str = "Software Developer", location: str = "California") -> list[dict]:
    """Run both scrapers and merge results. Dedup by URL within this batch."""
    all_jobs: list[dict] = []
    seen_urls: set[str]  = set()

    for job in scrape_dice(role, location) + scrape_linkedin(role, location):
        if job["url"] not in seen_urls:
            seen_urls.add(job["url"])
            all_jobs.append(job)

    logger.info(f"run_scrape total unique: {len(all_jobs)}")
    return all_jobs