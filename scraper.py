"""
Scraper module — Dice.com + LinkedIn
Uses httpx for requests, BeautifulSoup for parsing,
and (optionally) Playwright to intercept Dice's live API key.

Setup (choose one):

  # Option A — venv (recommended, avoids system conflicts)
  python -m venv .venv
  source .venv/bin/activate          # Linux/Mac
  pip install httpx beautifulsoup4 lxml playwright
  playwright install chromium

  # Option B — system-wide (Arch / externally-managed envs)
  pip install httpx beautifulsoup4 lxml playwright --break-system-packages
  playwright install chromium

  Playwright is optional — without it, the scraper still works via
  static JS extraction + HTML fallback.

Rate limiting:
  - Random delay between requests (1–3 s)
  - Rotates User-Agent strings
  - HTTP 429 → jittered exponential backoff

Dice strategy:
  1. Intercept the real x-api-key from Dice's own XHR via headless browser.
  2. Fall back to regex extraction from JS bundles / inline scripts.
  3. Fall back to DICE_API_KEY env var → hardcoded default.
  4. If the API fails entirely, scrape Dice search HTML directly.

LinkedIn strategy:
  1. Scrape public search result cards for title/company/location.
  2. Fetch individual job detail pages for description & salary.
  3. Extract salary from JSON-LD structured data when available.
"""

import json
import re
import os
import hashlib
import random
import time
import logging
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import quote_plus

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ── shared utilities ──────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

MAX_RETRIES = 3


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
    """Truncate at a word boundary, adding ellipsis if trimmed."""
    if not text or len(text) <= max_len:
        return text or ""
    truncated = text[:max_len].rsplit(" ", 1)[0]
    return truncated.rstrip(".,;:!?") + " …"


def parse_relative_date(date_str: Optional[str]) -> Optional[str]:
    """
    Parse relative date strings ('today', '3 days ago', 'yesterday')
    and ISO-format dates. Returns ISO date string or None.
    """
    if not date_str:
        return None
    date_str = date_str.strip().lower()
    today = datetime.utcnow().date()

    if "today" in date_str or "just" in date_str or "hour" in date_str:
        return today.isoformat()
    if "yesterday" in date_str:
        return (today - timedelta(days=1)).isoformat()

    for unit, delta_fn in [
        ("day",   lambda n: timedelta(days=n)),
        ("week",  lambda n: timedelta(weeks=n)),
        ("month", lambda n: timedelta(days=n * 30)),
    ]:
        if unit in date_str:
            try:
                n = int("".join(filter(str.isdigit, date_str)) or "1")
                return (today - delta_fn(n)).isoformat()
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


# ── Dice API key extraction ───────────────────────────────────────────────────

DICE_API_KEY_HARDCODED = "1YAt0R9wBg4WfsF9VB2778F5CHLAPMVW3WAZcKd8"

# Module-level cache so we only sniff the key once per process.
_cached_dice_api_key: Optional[str] = None
_cached_dice_key_source: Optional[str] = None  # tracks which strategy won

# Regex patterns for static JS extraction (fallback to browser interception).
_API_KEY_PATTERNS = [
    re.compile(r"""["']x-api-key["']\s*[:=]\s*["']([A-Za-z0-9]{30,})["']"""),
    re.compile(r"""["']?apiKey["']?\s*[:=]\s*["']([A-Za-z0-9]{30,})["']"""),
    re.compile(r"""(?:apiKey|x-api-key)=([A-Za-z0-9]{30,})"""),
    re.compile(r"""["']?(?:api_key|API_KEY)["']?\s*[:=]\s*["']([A-Za-z0-9]{30,})["']"""),
    re.compile(r"""api[^"']{0,30}["']([A-Za-z0-9]{35,45})["']""", re.IGNORECASE),
]


# ── Strategy 1: headless browser interception ─────────────────────────────────

def _intercept_key_via_browser(timeout_sec: int = 45) -> Optional[str]:
    """
    Launch a headless browser, navigate to a Dice search page,
    and capture the x-api-key header from the XHR to their search API.

    Requires:
        pip install playwright && playwright install chromium
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.info(
            "Dice API key: playwright not installed — skipping browser interception. "
            "Install with: pip install playwright && playwright install chromium"
        )
        return None

    captured_key: Optional[str] = None

    def _on_request(request):
        nonlocal captured_key
        if captured_key:
            return
        url_lower = request.url.lower()
        if "job-search-api" in url_lower or "dhigroupinc.com" in url_lower:
            key = request.headers.get("x-api-key")
            if key and len(key) >= 30:
                captured_key = key
                logger.info(f"Dice API key: captured from request to {request.url[:80]}")

    def _on_response(response):
        nonlocal captured_key
        if captured_key:
            return
        url_lower = response.url.lower()
        if "job-search-api" in url_lower or "dhigroupinc.com" in url_lower:
            # Sometimes the key is echoed back in response headers
            key = response.headers.get("x-api-key")
            if key and len(key) >= 30:
                captured_key = key
                logger.info(f"Dice API key: captured from response headers")

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={"width": 1280, "height": 800},
            )
            page = context.new_page()
            page.on("request", _on_request)
            page.on("response", _on_response)

            # Navigate to search page
            search_url = "https://www.dice.com/jobs?q=software+engineer&location=United+States"
            logger.info(f"Dice API key: loading {search_url} in headless browser")
            page.goto(search_url, wait_until="domcontentloaded", timeout=timeout_sec * 1000)

            # Wait for page to settle
            page.wait_for_timeout(3000)

            if not captured_key:
                # Interact to trigger client-side API calls:
                # Scroll down to trigger lazy loading
                logger.debug("Dice API key: scrolling to trigger API calls")
                page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
                page.wait_for_timeout(2000)
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(2000)

            if not captured_key:
                # Try clicking on a search/filter to force a new API call
                logger.debug("Dice API key: trying search interaction")
                try:
                    search_input = page.locator(
                        "input[placeholder*='Search'], "
                        "input[name*='q'], "
                        "input[data-cy*='search'], "
                        "input[aria-label*='Search']"
                    ).first
                    if search_input.is_visible(timeout=3000):
                        search_input.click()
                        search_input.fill("developer")
                        page.keyboard.press("Enter")
                        page.wait_for_timeout(5000)
                except Exception:
                    pass

            if not captured_key:
                # Try clicking pagination / "next page"
                logger.debug("Dice API key: trying pagination click")
                try:
                    next_btn = page.locator(
                        "a[aria-label*='next'], "
                        "button[aria-label*='next'], "
                        "a[data-cy*='page'], "
                        "li.pagination-next a"
                    ).first
                    if next_btn.is_visible(timeout=2000):
                        next_btn.click()
                        page.wait_for_timeout(5000)
                except Exception:
                    pass

            if not captured_key:
                # Last resort: wait longer for any late network calls
                page.wait_for_timeout(3000)

            browser.close()

    except Exception as e:
        logger.warning(f"Dice API key: browser interception failed ({e})")
        return None

    if captured_key:
        logger.info(f"Dice API key: intercepted via browser ({captured_key[:8]}…)")
    else:
        logger.warning("Dice API key: browser loaded but no API request captured")

    return captured_key


# ── Strategy 2: static regex extraction from JS ──────────────────────────────

def _extract_key_from_js(js_text: str) -> Optional[str]:
    """Try every regex pattern against JS text, return first match or None."""
    for pattern in _API_KEY_PATTERNS:
        match = pattern.search(js_text)
        if match:
            return match.group(1)
    return None


def _extract_key_static(client: httpx.Client) -> Optional[str]:
    """
    Fetch the Dice homepage and try to extract the API key from:
      - __NEXT_DATA__ JSON blob
      - Inline <script> tags
      - External JS bundles
    """
    try:
        resp = client.get("https://www.dice.com", headers=get_headers())
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Dice API key: could not fetch homepage ({e})")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # __NEXT_DATA__
    nd = soup.find("script", id="__NEXT_DATA__")
    if nd and nd.string:
        key = _extract_key_from_js(nd.string)
        if key:
            logger.info(f"Dice API key: found in __NEXT_DATA__ ({key[:8]}…)")
            return key

    # Inline scripts
    for tag in soup.find_all("script", src=False):
        text = tag.string or ""
        if len(text) < 50:
            continue
        key = _extract_key_from_js(text)
        if key:
            logger.info(f"Dice API key: found in inline script ({key[:8]}…)")
            return key

    # External JS bundles
    skip_keywords = ("gtm", "analytics", "google", "facebook", "hotjar")
    bundle_hints = ("_app", "main", "webpack", "chunk", "bundle", "index")

    bundle_urls: list[str] = []
    for tag in soup.find_all("script", src=True):
        src = tag["src"]
        if any(s in src.lower() for s in skip_keywords):
            continue
        if any(h in src.lower() for h in bundle_hints):
            full = src if src.startswith("http") else f"https://www.dice.com{src}"
            bundle_urls.append(full)

    if not bundle_urls:
        for tag in soup.find_all("script", src=True):
            src = tag["src"]
            if "dice.com" in src or src.startswith("/"):
                full = src if src.startswith("http") else f"https://www.dice.com{src}"
                bundle_urls.append(full)

    logger.info(f"Dice API key: checking {len(bundle_urls)} JS bundle(s)")
    for burl in bundle_urls:
        try:
            random_delay(0.3, 1.0)
            js_resp = client.get(burl, headers=get_headers())
            js_resp.raise_for_status()
            key = _extract_key_from_js(js_resp.text)
            if key:
                logger.info(f"Dice API key: found in bundle ({key[:8]}…)")
                return key
        except httpx.HTTPError:
            continue

    logger.warning("Dice API key: static extraction found nothing")
    return None


# ── Combined key getter ───────────────────────────────────────────────────────

def get_dice_api_key(*, force_refresh: bool = False) -> str:
    """
    Get the Dice API key, trying (in order):
      1. Module-level cache (skip with force_refresh=True)
      2. Headless browser interception (most reliable)
      3. Static regex extraction from JS bundles
      4. DICE_API_KEY environment variable
      5. Hardcoded fallback (loud warning)
    """
    global _cached_dice_api_key, _cached_dice_key_source

    if _cached_dice_api_key and not force_refresh:
        logger.info(
            f"Dice API key: using cached key from [{_cached_dice_key_source}] "
            f"({_cached_dice_api_key[:8]}…)"
        )
        return _cached_dice_api_key

    # Strategy 1: browser interception
    key = _intercept_key_via_browser()
    if key:
        _cached_dice_api_key = key
        _cached_dice_key_source = "browser_interception"
        return key

    # Strategy 2: static extraction
    try:
        with httpx.Client(timeout=15, follow_redirects=True) as client:
            key = _extract_key_static(client)
            if key:
                _cached_dice_api_key = key
                _cached_dice_key_source = "static_js_extraction"
                return key
    except Exception as e:
        logger.warning(f"Dice API key: static extraction error ({e})")

    # Strategy 3: env var
    env_key = os.getenv("DICE_API_KEY")
    if env_key:
        logger.info("Dice API key: using DICE_API_KEY env var")
        _cached_dice_api_key = env_key
        _cached_dice_key_source = "env_var"
        return env_key

    # Strategy 4: hardcoded
    logger.warning(
        "Dice API key: ALL dynamic methods failed. "
        "Falling back to HARDCODED key — it is likely expired! "
        "Set DICE_API_KEY env var or install playwright "
        "(pip install playwright && playwright install chromium)."
    )
    _cached_dice_key_source = "hardcoded_fallback"
    return DICE_API_KEY_HARDCODED


# ── Dice HTML scraper (no API key needed) ─────────────────────────────────────

def _dig_for_jobs(obj, depth: int = 0) -> Optional[list]:
    """
    Recursively search a nested dict/list (parsed __NEXT_DATA__) for
    what looks like a list of job result objects.
    """
    if depth > 8:
        return None
    if isinstance(obj, list) and len(obj) > 2:
        if all(isinstance(x, dict) and "title" in x for x in obj[:3]):
            return obj
    if isinstance(obj, dict):
        for key in ("data", "jobs", "results", "searchResults", "jobResults"):
            if key in obj:
                found = _dig_for_jobs(obj[key], depth + 1)
                if found:
                    return found
        for val in obj.values():
            found = _dig_for_jobs(val, depth + 1)
            if found:
                return found
    return None


def _scrape_dice_html(
    role: str, location: str, max_pages: int, client: httpx.Client,
) -> list[dict]:
    """
    Scrape Dice job search results directly from the rendered HTML.
    Works without an API key — uses the public search page.
    """
    jobs: list[dict] = []
    logger.info("Dice HTML fallback: starting")

    for page_num in range(1, max_pages + 1):
        url = (
            f"https://www.dice.com/jobs"
            f"?q={quote_plus(role)}&location={quote_plus(location)}"
            f"&page={page_num}&countryCode=US&language=en"
        )

        success = False
        resp = None
        for attempt in range(MAX_RETRIES):
            try:
                random_delay()
                resp = client.get(url, headers=get_headers())
                if resp.status_code == 429:
                    logger.warning(f"Dice HTML rate-limited on page {page_num}")
                    exponential_backoff(attempt)
                    continue
                resp.raise_for_status()
                success = True
                break
            except httpx.HTTPError as e:
                logger.error(f"Dice HTML page {page_num}, attempt {attempt + 1}: {e}")
                exponential_backoff(attempt)

        if not success or resp is None:
            logger.error(f"Dice HTML page {page_num}: retries exhausted, stopping")
            break

        soup = BeautifulSoup(resp.text, "html.parser")

        # ── Try __NEXT_DATA__ for structured job data ─────────────────
        next_data = soup.find("script", id="__NEXT_DATA__")
        if next_data and next_data.string:
            try:
                nd_json = json.loads(next_data.string)
                search_results = _dig_for_jobs(nd_json)
                if search_results:
                    for item in search_results:
                        try:
                            job_url = (
                                item.get("detailsPageUrl")
                                or item.get("url")
                                or f"https://www.dice.com/job-detail/"
                                   f"{item.get('id', item.get('guid', ''))}"
                            )
                            if not job_url.startswith("http"):
                                job_url = f"https://www.dice.com{job_url}"
                            jobs.append({
                                "id":          make_id(job_url),
                                "title":       item.get("title", ""),
                                "company":     item.get("companyName", "Unknown"),
                                "location":    (
                                    item.get("jobLocation", {}).get("displayName")
                                    if isinstance(item.get("jobLocation"), dict)
                                    else item.get("location", location)
                                ),
                                "posted_date": parse_relative_date(
                                    item.get("postedDate")
                                ),
                                "description": truncate_text(
                                    item.get("summary") or "", 500
                                ),
                                "salary":      item.get("salary"),
                                "url":         job_url,
                                "source":      "Dice",
                            })
                        except Exception as e:
                            logger.warning(
                                f"Dice HTML __NEXT_DATA__ item error: {e}"
                            )
                    logger.info(
                        f"Dice HTML page {page_num}: {len(search_results)} "
                        f"jobs from __NEXT_DATA__"
                    )
                    continue
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                logger.debug(f"Dice HTML __NEXT_DATA__ parse failed: {e}")

        # ── Parse visible HTML job cards ──────────────────────────────
        cards = (
            soup.select("a[data-cy='card-title-link']")
            or soup.select("a.card-title-link")
            or soup.select("[class*='JobCard'] a")
            or soup.select("div.search-card a[href*='/job-detail/']")
        )

        if not cards:
            cards = soup.select("a[href*='/job-detail/']")

        if not cards:
            logger.info(f"Dice HTML page {page_num}: no job cards found, stopping")
            break

        seen_on_page: set[str] = set()
        for link in cards:
            try:
                href = link.get("href", "")
                if "/job-detail/" not in href:
                    continue
                if not href.startswith("http"):
                    href = f"https://www.dice.com{href}"
                href = href.split("?")[0]
                if href in seen_on_page:
                    continue
                seen_on_page.add(href)

                title = link.get_text(strip=True)
                if not title:
                    continue

                parent = link.find_parent("div") or link.find_parent("li")
                company = "Unknown"
                loc_text = location
                if parent:
                    co_el = (
                        parent.select_one(
                            "[data-cy='search-result-company-name']"
                        )
                        or parent.select_one("[class*='company']")
                    )
                    if co_el:
                        company = co_el.get_text(strip=True)
                    loc_el = (
                        parent.select_one(
                            "[data-cy='search-result-location']"
                        )
                        or parent.select_one("[class*='location']")
                    )
                    if loc_el:
                        loc_text = loc_el.get_text(strip=True)

                jobs.append({
                    "id":          make_id(href),
                    "title":       title,
                    "company":     company,
                    "location":    loc_text,
                    "posted_date": None,
                    "description": None,
                    "salary":      None,
                    "url":         href,
                    "source":      "Dice",
                })
            except Exception as e:
                logger.warning(f"Dice HTML card parse error: {e}")
                continue

        logger.info(f"Dice HTML page {page_num}: scraped {len(seen_on_page)} jobs")

    logger.info(f"Dice HTML fallback total: {len(jobs)} jobs")
    return jobs


# ── Dice API scraper ──────────────────────────────────────────────────────────

def _scrape_dice_api(
    role: str, location: str, max_pages: int,
    api_key: str, client: httpx.Client,
) -> list[dict]:
    """Scrape Dice via their internal search API. Returns [] on auth failure."""
    jobs: list[dict] = []

    page = 1
    while page <= max_pages:
        url = (
            f"https://job-search-api.svc.dhigroupinc.com/v1/dice/jobs/search"
            f"?q={quote_plus(role)}&countryCode=US"
            f"&location={quote_plus(location)}"
            f"&pageSize=20&page={page}&language=en"
        )

        success = False
        data = None
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
                        f"key expired, switching to HTML fallback"
                    )
                    return jobs  # caller will try HTML fallback

                resp.raise_for_status()
                data = resp.json()
                success = True
                break

            except httpx.HTTPError as e:
                logger.error(
                    f"Dice API error page {page}, attempt {attempt + 1}: {e}"
                )
                exponential_backoff(attempt)

        if not success:
            logger.error(
                f"Dice API page {page}: retries exhausted, stopping"
            )
            break

        hits = data.get("data", [])
        if not hits:
            logger.info(f"Dice API page {page}: no results, stopping")
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
                        item.get("jobLocation", {}).get("displayName")
                        or location
                    ),
                    "posted_date": parse_relative_date(item.get("postedDate")),
                    "description": truncate_text(
                        item.get("summary") or "", 500
                    ),
                    "salary":      item.get("salary"),
                    "url":         job_url,
                    "source":      "Dice",
                })
            except Exception as e:
                logger.warning(f"Dice API item parse error: {e}")
                continue

        logger.info(f"Dice API page {page}: scraped {len(hits)} jobs")
        page += 1

    logger.info(f"Dice API total: {len(jobs)} jobs")
    return jobs


# ── Dice entry point (API → HTML fallback) ────────────────────────────────────

def scrape_dice(role: str, location: str, max_pages: int = 3) -> list[dict]:
    """
    Scrape Dice.com. Tries the JSON API first; if the key is bad or the
    API returns zero results, falls back to scraping the HTML search page.
    """
    with httpx.Client(timeout=15, follow_redirects=True) as client:
        api_key = get_dice_api_key()
        jobs = _scrape_dice_api(role, location, max_pages, api_key, client)

        if jobs:
            return jobs

        logger.info("Dice API returned 0 jobs — trying HTML fallback")
        return _scrape_dice_html(role, location, max_pages, client)


# ── LinkedIn helpers ───────────────────────────────────────────────────────────

def _extract_linkedin_salary(soup: BeautifulSoup) -> Optional[str]:
    """
    Extract salary information from a LinkedIn job page or card.
    LinkedIn puts salary data in several possible locations.
    """
    # ── HTML selectors (ordered by specificity) ───────────────────────
    selectors = [
        "div.salary-main-rail__data-body",
        "span.compensation__salary",
        "div.compensation__salary",
        "div.job-details-jobs-unified-top-card__job-insight span",
        "div[class*='salary']",
        "span[class*='salary']",
        "span[class*='compensation']",
        "div[class*='Salary']",
        "span[class*='Salary']",
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(strip=True)
            if "$" in text or any(c.isdigit() for c in text):
                logger.debug(f"LinkedIn salary: found via selector '{sel}': {text[:60]}")
                return text

    # ── JSON-LD structured data ───────────────────────────────────────
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            ld = json.loads(script.string or "")

            # Handle both single object and @graph format
            items = [ld] if not isinstance(ld, list) else ld
            if "@graph" in ld if isinstance(ld, dict) else False:
                items = ld["@graph"]

            for item in items:
                if not isinstance(item, dict):
                    continue
                salary = (
                    item.get("baseSalary")
                    or item.get("estimatedSalary")
                    or item.get("salary")
                )
                if not salary:
                    continue

                # Normalize to list
                salary_list = salary if isinstance(salary, list) else [salary]
                for sal in salary_list:
                    if not isinstance(sal, dict):
                        continue
                    value = sal.get("value", sal)
                    if isinstance(value, dict):
                        currency = sal.get("currency", "USD")
                        min_val = value.get("minValue") or value.get("value")
                        max_val = value.get("maxValue")
                        unit = value.get("unitText", "")
                        suffix = f"/{unit}" if unit else ""
                        if min_val and max_val:
                            result = f"{currency} {min_val:,.0f}–{max_val:,.0f}{suffix}"
                            logger.debug(f"LinkedIn salary: found in JSON-LD: {result}")
                            return result
                        elif min_val:
                            result = f"{currency} {min_val:,.0f}{suffix}"
                            logger.debug(f"LinkedIn salary: found in JSON-LD: {result}")
                            return result
        except (json.JSONDecodeError, TypeError, AttributeError, ValueError):
            continue

    # ── Fallback: scan all text for salary patterns ───────────────────
    page_text = soup.get_text()
    salary_pattern = re.search(
        r'\$[\d,]+(?:\.\d{2})?\s*[-–/to]+\s*\$[\d,]+(?:\.\d{2})?'
        r'(?:\s*(?:per\s+)?(?:year|yr|hour|hr|month|annually))?',
        page_text, re.IGNORECASE,
    )
    if salary_pattern:
        result = salary_pattern.group(0).strip()
        logger.debug(f"LinkedIn salary: found via regex: {result}")
        return result

    return None


def _extract_linkedin_description(soup: BeautifulSoup) -> Optional[str]:
    """Extract the job description text from a LinkedIn job detail page."""
    selectors = [
        # Public (non-authenticated) job pages
        "div.show-more-less-html__markup",
        "div.description__text",
        "section.show-more-less-html",
        # Newer markup
        "article.jobs-description__container",
        "div.jobs-description-content__text",
        "div.jobs-description__content",
        # Generic fallbacks
        "section.description div",
        "div[class*='description'] div.core-section-container__content",
        "div[class*='Description']",
        "div[class*='description__text']",
        "div[class*='job-description']",
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(separator=" ", strip=True)
            if len(text) > 50:
                logger.debug(
                    f"LinkedIn description: found via '{sel}' "
                    f"({len(text)} chars)"
                )
                return truncate_text(text, 500)

    # ── JSON-LD fallback ──────────────────────────────────────────────
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            ld = json.loads(script.string or "")
            desc = None
            if isinstance(ld, dict):
                desc = ld.get("description")
            if isinstance(ld, list):
                for item in ld:
                    if isinstance(item, dict) and "description" in item:
                        desc = item["description"]
                        break
            if desc and len(desc) > 50:
                # Strip HTML tags if present
                clean = BeautifulSoup(desc, "html.parser").get_text(
                    separator=" ", strip=True
                )
                logger.debug(
                    f"LinkedIn description: found in JSON-LD ({len(clean)} chars)"
                )
                return truncate_text(clean, 500)
        except (json.JSONDecodeError, TypeError, AttributeError):
            continue

    logger.debug("LinkedIn description: no match found on page")
    return None


def _fetch_linkedin_job_details(
    client: httpx.Client, job: dict,
) -> dict:
    """
    Fetch a single LinkedIn job detail page to fill in description and salary.
    Returns the updated job dict. On failure, returns the original unchanged.
    """
    url = job["url"]
    try:
        random_delay(2.0, 5.0)
        resp = client.get(url, headers=get_headers())

        if resp.status_code in (429, 999):
            logger.warning(f"LinkedIn detail rate-limited: {url}")
            return job
        resp.raise_for_status()

    except httpx.HTTPError as e:
        logger.warning(f"LinkedIn detail fetch failed: {url} — {e}")
        return job

    soup = BeautifulSoup(resp.text, "html.parser")

    if not job.get("description"):
        desc = _extract_linkedin_description(soup)
        if desc:
            job["description"] = desc
        else:
            logger.info(f"LinkedIn detail: no description found at {url.split('/')[-1]}")

    if not job.get("salary"):
        salary = _extract_linkedin_salary(soup)
        if salary:
            job["salary"] = salary

    return job


# ── LinkedIn scraper ──────────────────────────────────────────────────────────

def scrape_linkedin(
    role: str,
    location: str,
    max_pages: int = 3,
    fetch_details: bool = True,
    max_detail_fetches: int = 15,
) -> list[dict]:
    """
    Scrape LinkedIn public job listings (no login required).

    Args:
        role:               Job title / keywords.
        location:           Location string.
        max_pages:          Max search result pages to scrape.
        fetch_details:      If True, fetch individual job pages for
                            description & salary (slower but richer data).
        max_detail_fetches: Cap on how many detail pages to fetch to
                            avoid hammering LinkedIn.
    """
    jobs: list[dict] = []

    with httpx.Client(timeout=20, follow_redirects=True) as client:
        page = 0
        while page < max_pages:
            start = page * 25
            url = (
                f"https://www.linkedin.com/jobs/search/"
                f"?keywords={quote_plus(role)}"
                f"&location={quote_plus(location)}&start={start}"
            )

            success = False
            resp = None
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
                    logger.error(
                        f"LinkedIn HTTP error page {page}, "
                        f"attempt {attempt + 1}: {e}"
                    )
                    exponential_backoff(attempt, base=10.0)

            if not success or resp is None:
                logger.error(
                    f"LinkedIn page {page}: retries exhausted, stopping"
                )
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
                    title_el = card.select_one(
                        "h3.base-search-card__title, h3"
                    )
                    company_el = card.select_one(
                        "h4.base-search-card__subtitle, h4"
                    )
                    loc_el = card.select_one(
                        "span.job-search-card__location, span.location"
                    )
                    date_el = card.select_one("time")
                    link_el = card.select_one("a.base-card__full-link, a")

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

                    # Try to grab salary from the search card itself
                    card_salary = None
                    salary_el = (
                        card.select_one("span.job-search-card__salary-info")
                        or card.select_one("div[class*='salary']")
                        or card.select_one("span[class*='compensation']")
                    )
                    if salary_el:
                        card_salary = salary_el.get_text(strip=True)

                    jobs.append({
                        "id":          make_id(href),
                        "title":       title_el.get_text(strip=True),
                        "company":     (
                            company_el.get_text(strip=True)
                            if company_el else "Unknown"
                        ),
                        "location":    (
                            loc_el.get_text(strip=True)
                            if loc_el else location
                        ),
                        "posted_date": posted,
                        "description": None,
                        "salary":      card_salary,
                        "url":         href,
                        "source":      "LinkedIn",
                    })
                except Exception as e:
                    logger.warning(f"LinkedIn card parse error: {e}")
                    continue

            logger.info(f"LinkedIn page {page}: scraped {len(cards)} cards")
            page += 1

        # ── Fetch individual detail pages for description & salary ────
        if fetch_details and jobs:
            to_fetch = [
                j for j in jobs
                if not j.get("description") or not j.get("salary")
            ][:max_detail_fetches]

            logger.info(
                f"LinkedIn: fetching details for {len(to_fetch)} / "
                f"{len(jobs)} jobs"
            )

            for i, job in enumerate(to_fetch):
                logger.debug(
                    f"LinkedIn detail {i + 1}/{len(to_fetch)}: {job['url']}"
                )
                _fetch_linkedin_job_details(client, job)

                # If we get rate-limited, stop fetching details
                # (we already have the basic info from search cards)

            filled = sum(1 for j in jobs if j.get("description"))
            logger.info(
                f"LinkedIn: {filled}/{len(jobs)} jobs now have descriptions"
            )

    logger.info(f"LinkedIn total: {len(jobs)} jobs")
    return jobs


# ── combined entry point ──────────────────────────────────────────────────────

def run_scrape(
    role: str = "Software Developer",
    location: str = "California",
) -> list[dict]:
    """Run both scrapers and merge results. Dedup by URL."""
    all_jobs: list[dict] = []
    seen_urls: set[str]  = set()

    for job in scrape_dice(role, location) + scrape_linkedin(role, location):
        if job["url"] not in seen_urls:
            seen_urls.add(job["url"])
            all_jobs.append(job)

    logger.info(f"run_scrape total unique: {len(all_jobs)}")
    return all_jobs


# ── Diagnostics ───────────────────────────────────────────────────────────────

def verify_dice_api_key() -> dict:
    """
    Diagnostic: test each API key extraction strategy independently
    and report which ones work. Run this to debug key issues.

    Returns a dict with the result of each strategy.

    Usage:
        python scraper.py --verify
        # or in code:
        from scraper import verify_dice_api_key
        print(verify_dice_api_key())
    """
    results: dict = {
        "browser_interception": None,
        "static_js_extraction": None,
        "env_var": None,
        "hardcoded": DICE_API_KEY_HARDCODED,
        "active_key": None,
        "active_source": None,
    }

    # Test browser interception
    logger.info("── Testing browser interception ──")
    key = _intercept_key_via_browser()
    if key:
        results["browser_interception"] = f"{key[:8]}…{key[-4:]}"
        logger.info(f"  ✓ Browser interception: {key[:8]}…{key[-4:]}")
    else:
        logger.info("  ✗ Browser interception: FAILED")

    # Test static extraction
    logger.info("── Testing static JS extraction ──")
    try:
        with httpx.Client(timeout=15, follow_redirects=True) as client:
            key = _extract_key_static(client)
            if key:
                results["static_js_extraction"] = f"{key[:8]}…{key[-4:]}"
                logger.info(f"  ✓ Static extraction: {key[:8]}…{key[-4:]}")
            else:
                logger.info("  ✗ Static extraction: FAILED")
    except Exception as e:
        logger.info(f"  ✗ Static extraction: ERROR ({e})")

    # Test env var
    logger.info("── Testing env var ──")
    env_key = os.getenv("DICE_API_KEY")
    if env_key:
        results["env_var"] = f"{env_key[:8]}…{env_key[-4:]}"
        logger.info(f"  ✓ DICE_API_KEY env var: {env_key[:8]}…{env_key[-4:]}")
    else:
        logger.info("  ✗ DICE_API_KEY env var: NOT SET")

    # Test the actual key that would be used
    logger.info("── Testing active key against Dice API ──")
    active_key = get_dice_api_key(force_refresh=True)
    results["active_key"] = f"{active_key[:8]}…{active_key[-4:]}"
    results["active_source"] = _cached_dice_key_source
    logger.info(
        f"  Active key: {active_key[:8]}…{active_key[-4:]} "
        f"(source: {_cached_dice_key_source})"
    )

    # Validate the active key with a test API call
    logger.info("── Validating key with a test API call ──")
    try:
        with httpx.Client(timeout=10, follow_redirects=True) as client:
            resp = client.get(
                "https://job-search-api.svc.dhigroupinc.com/v1/dice/jobs/search"
                "?q=test&countryCode=US&pageSize=1&page=1&language=en",
                headers={
                    "User-Agent": random.choice(USER_AGENTS),
                    "Accept": "application/json",
                    "x-api-key": active_key,
                },
            )
            results["api_test_status"] = resp.status_code
            if resp.status_code == 200:
                data = resp.json()
                count = len(data.get("data", []))
                results["api_test_jobs"] = count
                logger.info(f"  ✓ API call succeeded: HTTP 200, {count} job(s)")
            elif resp.status_code in (401, 403):
                logger.warning(f"  ✗ API call REJECTED: HTTP {resp.status_code} — key is expired/invalid")
            else:
                logger.warning(f"  ? API call returned HTTP {resp.status_code}")
    except Exception as e:
        results["api_test_status"] = f"ERROR: {e}"
        logger.warning(f"  ✗ API call failed: {e}")

    return results


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)-7s %(name)s: %(message)s",
    )

    if "--verify" in sys.argv:
        print("\n🔍 Running Dice API key diagnostics...\n")
        results = verify_dice_api_key()
        print("\n── Summary ──")
        for k, v in results.items():
            print(f"  {k}: {v}")
    else:
        print("Usage:")
        print("  python scraper.py --verify    # Test API key extraction")
        print()
        print("Or import and use in your code:")
        print("  from scraper import run_scrape, verify_dice_api_key")

