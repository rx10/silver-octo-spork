"""
Scraper module — Dice.com + LinkedIn
Uses httpx for requests and BeautifulSoup for parsing.

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
    return hashlib.sha256(url.encode()).hexdigest()[:32]


def random_delay(lo: float = 1.0, hi: float = 3.0):
    time.sleep(random.uniform(lo, hi))


def get_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }


def truncate_text(text: str, max_len: int = 500) -> str:
    """Truncate text at a word boundary, adding ellipsis if needed."""
    if not text or len(text) <= max_len:
        return text or ""
    truncated = text[:max_len].rsplit(" ", 1)[0]
    return truncated.rstrip(".,;:!?") + " …"


def parse_relative_date(date_str: Optional[str]) -> Optional[str]:
    """
    Parse relative date strings like 'today', '3 days ago', 'yesterday',
    as well as ISO-format date strings. Returns ISO date string or None.
    """
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
    if "week" in date_str:
        try:
            n = int("".join(filter(str.isdigit, date_str)) or "1")
            return (today - timedelta(weeks=n)).isoformat()
        except ValueError:
            pass
    if "month" in date_str:
        try:
            n = int("".join(filter(str.isdigit, date_str)) or "1")
            return (today - timedelta(days=n * 30)).isoformat()
        except ValueError:
            pass

    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        pass
    return today.isoformat()


def exponential_backoff(attempt: int, base: float = 5.0, cap: float = 120.0):
    """Sleep with jittered exponential backoff."""
    delay = min(base * (2 ** attempt) + random.uniform(0, 2), cap)
    logger.info(f"Backing off {delay:.1f}s (attempt {attempt + 1})")
    time.sleep(delay)


# ── Dice API key ──────────────────────────────────────────────────────────────

DICE_API_KEY_HARDCODED = "1YAt0R9wBg4WfsF9VB2778F5CHLAPMVW3WAZcKd8"

# Patterns ordered from most specific to least specific.
# Each is tried against every JS bundle found on the Dice homepage.
_API_KEY_PATTERNS = [
    # Exact header assignment:  "x-api-key": "KEY"  or  'x-api-key': 'KEY'
    re.compile(r"""["']x-api-key["']\s*[:=]\s*["']([A-Za-z0-9]{30,})["']"""),
    # Generic config key:  apiKey: "KEY"  or  "apiKey": "KEY"
    re.compile(r"""["']?apiKey["']?\s*[:=]\s*["']([A-Za-z0-9]{30,})["']"""),
    # Query-param style:  apiKey=KEY  or  x-api-key=KEY
    re.compile(r"""(?:apiKey|x-api-key)=([A-Za-z0-9]{30,})"""),
    # Object property:  {api_key: "KEY"}  or  {API_KEY: "KEY"}
    re.compile(r"""["']?(?:api_key|API_KEY)["']?\s*[:=]\s*["']([A-Za-z0-9]{30,})["']"""),
    # Catch-all for long alphanumeric strings assigned near "api" or "key" context
    re.compile(r"""api[^"']{0,30}["']([A-Za-z0-9]{35,45})["']""", re.IGNORECASE),
]


def _find_bundle_urls(soup: BeautifulSoup) -> list[str]:
    """
    Extract candidate JS bundle URLs from a Dice homepage.
    Returns all script srcs that look like app/main/chunk bundles.
    """
    candidates = []
    for tag in soup.find_all("script", src=True):
        src = tag["src"]
        # Skip analytics / third-party / tiny vendor scripts
        if any(skip in src.lower() for skip in ("gtm", "analytics", "google", "facebook", "hotjar")):
            continue
        # Prefer anything that looks like an app or chunk bundle
        if any(hint in src.lower() for hint in ("_app", "main", "webpack", "chunk", "bundle", "index")):
            full = src if src.startswith("http") else f"https://www.dice.com{src}"
            candidates.append(full)

    # If no obvious bundles, try all first-party scripts as a last resort
    if not candidates:
        for tag in soup.find_all("script", src=True):
            src = tag["src"]
            if "dice.com" in src or src.startswith("/"):
                full = src if src.startswith("http") else f"https://www.dice.com{src}"
                candidates.append(full)

    return candidates


def _extract_key_from_js(js_text: str) -> Optional[str]:
    """Try every regex pattern against the JS text, return first match or None."""
    for pattern in _API_KEY_PATTERNS:
        match = pattern.search(js_text)
        if match:
            return match.group(1)
    return None


def _extract_key_from_inline_scripts(soup: BeautifulSoup) -> Optional[str]:
    """Check inline <script> blocks on the page (some SPAs inject config there)."""
    for tag in soup.find_all("script", src=False):
        text = tag.string or ""
        if len(text) < 50:
            continue
        key = _extract_key_from_js(text)
        if key:
            return key
    return None


def _extract_key_from_nextdata(soup: BeautifulSoup) -> Optional[str]:
    """Next.js apps often embed config in a __NEXT_DATA__ script tag."""
    tag = soup.find("script", id="__NEXT_DATA__")
    if tag and tag.string:
        key = _extract_key_from_js(tag.string)
        if key:
            return key
    return None


def get_dice_api_key() -> str:
    """
    Attempt to dynamically extract Dice's API key from their frontend.

    Strategy (in order):
      1. Fetch dice.com homepage.
      2. Check __NEXT_DATA__ / inline scripts for the key.
      3. Fetch every candidate JS bundle and regex-search for the key.
      4. Fall back to DICE_API_KEY env var.
      5. Fall back to hardcoded default (with loud warning).
    """
    try:
        with httpx.Client(timeout=15, follow_redirects=True) as client:
            resp = client.get("https://www.dice.com", headers=get_headers())
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # ── Step 1: inline / __NEXT_DATA__ ────────────────────────────
            key = _extract_key_from_nextdata(soup)
            if key:
                logger.info(f"Dice API key: found in __NEXT_DATA__ ({key[:8]}…)")
                return key

            key = _extract_key_from_inline_scripts(soup)
            if key:
                logger.info(f"Dice API key: found in inline script ({key[:8]}…)")
                return key

            # ── Step 2: external JS bundles ────────────────────────────────
            bundle_urls = _find_bundle_urls(soup)
            if not bundle_urls:
                logger.warning("Dice API key: no JS bundles found on homepage")
            else:
                logger.info(f"Dice API key: checking {len(bundle_urls)} JS bundle(s)")

            for burl in bundle_urls:
                try:
                    random_delay(0.3, 1.0)
                    js_resp = client.get(burl, headers=get_headers())
                    js_resp.raise_for_status()
                    js_text = js_resp.text
                    logger.debug(f"  bundle {burl} — {len(js_text)} chars")

                    key = _extract_key_from_js(js_text)
                    if key:
                        logger.info(f"Dice API key: extracted from bundle ({key[:8]}…)")
                        return key
                except httpx.HTTPError as e:
                    logger.debug(f"  bundle fetch failed: {burl} — {e}")
                    continue

            logger.warning("Dice API key: exhausted all bundles, no key found")

    except Exception as e:
        logger.warning(f"Dice API key: dynamic fetch failed ({e})")

    # ── Fallbacks ──────────────────────────────────────────────────────────
    env_key = os.getenv("DICE_API_KEY")
    if env_key:
        logger.info("Dice API key: using DICE_API_KEY env var")
        return env_key

    logger.warning(
        "Dice API key: falling back to HARDCODED default. "
        "This key may be expired — set DICE_API_KEY env var or fix dynamic extraction!"
    )
    return DICE_API_KEY_HARDCODED


# ── Dice scraper ──────────────────────────────────────────────────────────────

MAX_RETRIES = 3


def scrape_dice(role: str, location: str, max_pages: int = 3) -> list[dict]:
    """Scrape Dice.com using their internal search API."""
    api_key = get_dice_api_key()
    jobs: list[dict] = []

    with httpx.Client(timeout=15, follow_redirects=True) as client:
        page = 1
        while page <= max_pages:
            url = (
                f"https://job-search-api.svc.dhigroupinc.com/v1/dice/jobs/search"
                f"?q={role.replace(' ', '+')}&countryCode=US"
                f"&location={location.replace(' ', '+')}"
                f"&pageSize=20&page={page}&language=en"
            )

            success = False
            for attempt in range(MAX_RETRIES):
                try:
                    random_delay()
                    resp = client.get(url, headers={
                        "User-Agent": random.choice(USER_AGENTS),
                        "Accept": "application/json",
                        "x-api-key": api_key,
                    })

                    if resp.status_code == 429:
                        logger.warning(f"Dice API rate-limited on page {page}")
                        exponential_backoff(attempt)
                        continue

                    if resp.status_code in (401, 403):
                        logger.error(
                            f"Dice API auth error ({resp.status_code}) — "
                            f"API key is likely expired"
                        )
                        return jobs

                    resp.raise_for_status()
                    data = resp.json()
                    success = True
                    break

                except httpx.HTTPError as e:
                    logger.error(f"Dice API HTTP error on page {page}, attempt {attempt + 1}: {e}")
                    exponential_backoff(attempt)

            if not success:
                logger.error(f"Dice page {page}: all {MAX_RETRIES} retries exhausted, stopping")
                break

            hits = data.get("data", [])
            if not hits:
                logger.info(f"Dice page {page}: no results, stopping")
                break

            for item in hits:
                try:
                    job_url = (
                        item.get("detailsPageUrl")
                        or f"https://www.dice.com/job-detail/{item.get('guid', '')}"
                    )
                    jobs.append({
                        "id":          make_id(job_url),
                        "title":       item.get("title", ""),
                        "company":     item.get("companyName", "Unknown"),
                        "location":    (
                            item.get("jobLocation", {}).get("displayName") or location
                        ),
                        "posted_date": parse_relative_date(item.get("postedDate")),
                        "description": truncate_text(item.get("summary") or "", 500),
                        "salary":      item.get("salary"),
                        "url":         job_url,
                        "source":      "Dice",
                    })
                except Exception as e:
                    logger.warning(f"Dice item parse error: {e}")
                    continue

            logger.info(f"Dice page {page}: scraped {len(hits)} jobs")
            page += 1

    logger.info(f"Dice total: {len(jobs)} jobs")
    return jobs


# ── LinkedIn scraper ──────────────────────────────────────────────────────────

def scrape_linkedin(role: str, location: str, max_pages: int = 3) -> list[dict]:
    """Scrape LinkedIn public job listings (no login required)."""
    jobs: list[dict] = []
    role_slug = role.replace(" ", "%20")
    loc_slug  = location.replace(" ", "%20")

    with httpx.Client(timeout=20, follow_redirects=True) as client:
        page = 0
        while page < max_pages:
            start = page * 25
            url = (
                f"https://www.linkedin.com/jobs/search/"
                f"?keywords={role_slug}&location={loc_slug}&start={start}"
            )

            success = False
            for attempt in range(MAX_RETRIES):
                try:
                    random_delay(2.0, 4.5)
                    resp = client.get(url, headers=get_headers())

                    if resp.status_code in (429, 999):
                        logger.warning(f"LinkedIn rate-limited on page {page}")
                        exponential_backoff(attempt, base=15.0, cap=180.0)
                        continue

                    resp.raise_for_status()
                    success = True
                    break

                except httpx.HTTPError as e:
                    logger.error(f"LinkedIn HTTP error on page {page}, attempt {attempt + 1}: {e}")
                    exponential_backoff(attempt, base=10.0)

            if not success:
                logger.error(f"LinkedIn page {page}: all retries exhausted, stopping")
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            cards = (
                soup.select("div.base-card")
                or soup.select("li.jobs-search__results-list > div")
            )

            if not cards:
                logger.info(f"LinkedIn page {page}: no cards found, stopping")
                break

            for card in cards:
                try:
                    title_el   = card.select_one("h3.base-search-card__title, h3")
                    company_el = card.select_one("h4.base-search-card__subtitle, h4")
                    loc_el     = card.select_one(
                        "span.job-search-card__location, span.location"
                    )
                    date_el    = card.select_one("time")
                    link_el    = card.select_one("a.base-card__full-link, a")

                    if not title_el or not link_el:
                        continue

                    href = link_el.get("href", "").split("?")[0]
                    if not href.startswith("http"):
                        continue

                    posted = None
                    if date_el:
                        posted = (
                            date_el.get("datetime")
                            or parse_relative_date(date_el.get_text())
                        )

                    jobs.append({
                        "id":          make_id(href),
                        "title":       title_el.get_text(strip=True),
                        "company":     (
                            company_el.get_text(strip=True) if company_el else "Unknown"
                        ),
                        "location":    (
                            loc_el.get_text(strip=True) if loc_el else location
                        ),
                        "posted_date": posted,
                        "description": None,
                        "salary":      None,
                        "url":         href,
                        "source":      "LinkedIn",
                    })
                except Exception as e:
                    logger.warning(f"LinkedIn card parse error: {e}")
                    continue

            logger.info(f"LinkedIn page {page}: scraped {len(cards)} cards")
            page += 1

    logger.info(f"LinkedIn total: {len(jobs)} jobs")
    return jobs


# ── combined entry point ──────────────────────────────────────────────────────

def run_scrape(
    role: str = "Software Developer",
    location: str = "California",
) -> list[dict]:
    """Run both scrapers and merge results. Dedup by URL within this batch."""
    all_jobs: list[dict] = []
    seen_urls: set[str]  = set()

    for job in scrape_dice(role, location) + scrape_linkedin(role, location):
        if job["url"] not in seen_urls:
            seen_urls.add(job["url"])
            all_jobs.append(job)

    logger.info(f"run_scrape total unique: {len(all_jobs)}")
    return all_jobs