"""
Job scraper for Dice, LinkedIn, Indeed, ZipRecruiter, RemoteOK.
- Non-US searches automatically include a parallel "Remote" pass.
- Hybrid jobs are excluded from Remote results.
- run_scrape() accepts an optional on_batch(jobs) callback so the caller
  can persist results incrementally (enabling progressive UI loading).
"""

import json, re, os, hashlib, random, time, logging
from datetime import datetime, timedelta
from typing import Optional, Callable
from urllib.parse import quote_plus

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

MAX_RETRIES = 3
_cached_api_key: Optional[str] = None


# ── Utilities ─────────────────────────────────────────────────────────────────

def make_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:32]

def delay(lo: float = 1.0, hi: float = 3.0) -> None:
    time.sleep(random.uniform(lo, hi))

def headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

def backoff(attempt: int, base: float = 5.0) -> None:
    time.sleep(min(base * 2 ** attempt + random.uniform(0, 2), 120))

def truncate(text: str, n: int = 500) -> str:
    if not text or len(text) <= n:
        return text or ""
    return text[:n].rsplit(" ", 1)[0] + " …"

def parse_date(s: Optional[str]) -> Optional[str]:
    """Parse '3 days ago', 'yesterday', ISO dates, etc."""
    if not s:
        return None
    s = s.strip().lower()
    today = datetime.utcnow().date()
    if any(w in s for w in ("today", "just", "hour", "moment")):
        return today.isoformat()
    if "yesterday" in s:
        return (today - timedelta(days=1)).isoformat()
    for unit, fn in [
        ("day",   lambda n: timedelta(days=n)),
        ("week",  lambda n: timedelta(weeks=n)),
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


# ── Location helpers ──────────────────────────────────────────────────────────

_US_ABBREVS = {
    "al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id","il","in","ia",
    "ks","ky","la","me","md","ma","mi","mn","ms","mo","mt","ne","nv","nh","nj",
    "nm","ny","nc","nd","oh","ok","or","pa","ri","sc","sd","tn","tx","ut","vt",
    "va","wa","wv","wi","wy","dc",
}
_US_STATES = {
    "alabama","alaska","arizona","arkansas","california","colorado","connecticut",
    "delaware","florida","georgia","hawaii","idaho","illinois","indiana","iowa",
    "kansas","kentucky","louisiana","maine","maryland","massachusetts","michigan",
    "minnesota","mississippi","missouri","montana","nebraska","nevada",
    "new hampshire","new jersey","new mexico","new york","north carolina",
    "north dakota","ohio","oklahoma","oregon","pennsylvania","rhode island",
    "south carolina","south dakota","tennessee","texas","utah","vermont",
    "virginia","washington","west virginia","wisconsin","wyoming",
}
_US_KEYWORDS = {"united states", "usa", "remote"}

def _is_us_location(location: str) -> bool:
    loc = location.strip().lower()
    if any(kw in loc for kw in _US_KEYWORDS):
        return True
    if loc in _US_STATES:
        return True
    parts = [p.strip().rstrip(".") for p in loc.replace(",", " ").split()]
    # Check last token is a state abbrev (e.g. "San Francisco, CA")
    if parts and parts[-1] in _US_ABBREVS:
        return True
    # Check any multi-word state name appears in loc (e.g. "New York")
    return any(state in loc for state in _US_STATES)

_HYBRID_TERMS = (
    "hybrid", "on-site", "onsite", "on site",
    "in-office", "in office", "in-person", "in person",
)

def _is_hybrid(job: dict) -> bool:
    """Return True if the job is hybrid / on-site (not fully remote)."""
    loc  = (job.get("location")    or "").lower()
    desc = (job.get("description") or "").lower()[:300]
    return any(t in loc or t in desc for t in _HYBRID_TERMS)


# ── Dice API Key ──────────────────────────────────────────────────────────────

def _intercept_key_browser() -> Optional[str]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.info("Playwright not installed — skipping browser key interception")
        return None

    key = None

    def on_request(req):
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
            page.on("request", on_request)
            page.goto(
                "https://www.dice.com/jobs?q=software+engineer&location=United+States",
                wait_until="domcontentloaded", timeout=45000,
            )
            page.wait_for_timeout(3000)
            if not key:
                try:
                    inp = page.locator("input[placeholder*='Search'], input[name*='q']").first
                    if inp.is_visible(timeout=3000):
                        inp.click()
                        inp.fill("developer")
                        page.keyboard.press("Enter")
                        page.wait_for_timeout(5000)
                except Exception:
                    pass
            browser.close()
    except Exception as e:
        logger.warning(f"Browser interception failed: {e}")
        return None

    if key:
        logger.info(f"Dice API key intercepted: {key[:8]}…")
    return key


def get_dice_key() -> str:
    global _cached_api_key
    if _cached_api_key:
        return _cached_api_key
    key = _intercept_key_browser()
    if not key:
        key = os.getenv("DICE_API_KEY")
        if key:
            logger.info("Using DICE_API_KEY env var")
    if not key:
        key = "1YAt0R9wBg4WfsF9VB2778F5CHLAPMVW3WAZcKd8"
        logger.warning("Using hardcoded Dice key — may be expired!")
    _cached_api_key = key
    return key


# ── Dice ──────────────────────────────────────────────────────────────────────

def scrape_dice(role: str, location: str, max_pages: int = 5) -> list[dict]:
    api_key = get_dice_key()
    jobs: list[dict] = []

    with httpx.Client(timeout=15, follow_redirects=True) as client:
        for page in range(1, max_pages + 1):
            url = (
                f"https://job-search-api.svc.dhigroupinc.com/v1/dice/jobs/search"
                f"?q={quote_plus(role)}&location={quote_plus(location)}"
                f"&pageSize=20&page={page}&language=en"
            )
            for attempt in range(MAX_RETRIES):
                try:
                    delay()
                    resp = client.get(
                        url,
                        headers={**headers(), "Accept": "application/json", "x-api-key": api_key},
                    )
                    if resp.status_code == 429:
                        backoff(attempt)
                        continue
                    if resp.status_code in (401, 403):
                        logger.error(f"Dice API auth error {resp.status_code}")
                        return jobs
                    resp.raise_for_status()
                    data = resp.json()
                    break
                except httpx.HTTPError as e:
                    logger.error(f"Dice page {page} attempt {attempt + 1}: {e}")
                    backoff(attempt)
            else:
                break

            hits = data.get("data", [])
            if not hits:
                break

            for item in hits:
                job_url = item.get("detailsPageUrl") or f"https://www.dice.com/job-detail/{item.get('guid','')}"
                raw_loc = item.get("jobLocation", {}).get("displayName") or location
                jobs.append({
                    "id":          make_id(job_url),
                    "title":       item.get("title", ""),
                    "company":     item.get("companyName", "Unknown"),
                    "location":    raw_loc,
                    "posted_date": parse_date(item.get("postedDate")),
                    "description": truncate(item.get("summary") or ""),
                    "salary":      item.get("salary"),
                    "url":         job_url,
                    "source":      "Dice",
                })
            logger.info(f"Dice page {page}: {len(hits)} jobs")

    logger.info(f"Dice total: {len(jobs)}")
    return jobs


# ── LinkedIn ──────────────────────────────────────────────────────────────────

def _extract_li_description(soup: BeautifulSoup) -> Optional[str]:
    for sel in [
        "div.show-more-less-html__markup",
        "div.description__text",
        "section.show-more-less-html",
        "div[class*='description__text']",
    ]:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(separator=" ", strip=True)
            if len(text) > 50:
                return truncate(text)
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            ld = json.loads(script.string or "")
            desc = ld.get("description") if isinstance(ld, dict) else None
            if desc and len(desc) > 50:
                return truncate(BeautifulSoup(desc, "html.parser").get_text(separator=" ", strip=True))
        except (json.JSONDecodeError, TypeError):
            pass
    return None


def _extract_li_salary(soup: BeautifulSoup) -> Optional[str]:
    for sel in [
        "div.salary-main-rail__data-body",
        "span.compensation__salary",
        "div[class*='salary']",
        "span[class*='salary']",
    ]:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(strip=True)
            if "$" in text or any(c.isdigit() for c in text):
                return text
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            ld = json.loads(script.string or "")
            sal = (ld.get("baseSalary") or ld.get("estimatedSalary")) if isinstance(ld, dict) else None
            if isinstance(sal, list):
                sal = sal[0] if sal else None
            if isinstance(sal, dict):
                v = sal.get("value", sal)
                if isinstance(v, dict):
                    lo, hi = v.get("minValue"), v.get("maxValue")
                    cur = sal.get("currency", "USD")
                    if lo and hi:
                        return f"{cur} {lo:,.0f}–{hi:,.0f}"
                    elif lo:
                        return f"{cur} {lo:,.0f}"
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
    m = re.search(r'\$[\d,]+\s*[-–/]+\s*\$[\d,]+', soup.get_text())
    return m.group(0).strip() if m else None


def scrape_linkedin(role: str, location: str, max_pages: int = 5, max_details: int = 40) -> list[dict]:
    jobs: list[dict] = []

    with httpx.Client(timeout=20, follow_redirects=True) as client:
        for page in range(max_pages):
            url = (
                f"https://www.linkedin.com/jobs/search/"
                f"?keywords={quote_plus(role)}&location={quote_plus(location)}&start={page * 25}"
            )
            for attempt in range(MAX_RETRIES):
                try:
                    delay(2, 4.5)
                    resp = client.get(url, headers=headers())
                    if resp.status_code in (429, 999):
                        backoff(attempt, base=15)
                        continue
                    resp.raise_for_status()
                    break
                except httpx.HTTPError:
                    backoff(attempt, base=10)
            else:
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            cards = soup.select("div.base-card")
            if not cards:
                break

            for card in cards:
                title_el = card.select_one("h3.base-search-card__title, h3")
                link_el  = card.select_one("a.base-card__full-link, a")
                if not title_el or not link_el:
                    continue
                href = link_el.get("href", "").split("?")[0]
                if not href.startswith("http"):
                    continue

                company_el = card.select_one("h4.base-search-card__subtitle, h4")
                loc_el     = card.select_one("span.job-search-card__location, span.location")
                date_el    = card.select_one("time")
                salary_el  = card.select_one("span.job-search-card__salary-info")

                jobs.append({
                    "id":          make_id(href),
                    "title":       title_el.get_text(strip=True),
                    "company":     company_el.get_text(strip=True) if company_el else "Unknown",
                    "location":    loc_el.get_text(strip=True) if loc_el else location,
                    "posted_date": (date_el.get("datetime") or parse_date(date_el.get_text())) if date_el else None,
                    "description": None,
                    "salary":      salary_el.get_text(strip=True) if salary_el else None,
                    "url":         href,
                    "source":      "LinkedIn",
                })
            logger.info(f"LinkedIn page {page}: {len(cards)} cards")

        # Fetch detail pages for descriptions
        to_fetch = [j for j in jobs if not j.get("description")][:max_details]
        logger.info(f"LinkedIn: fetching details for {len(to_fetch)}/{len(jobs)}")
        for job in to_fetch:
            try:
                delay(2, 5)
                resp = client.get(job["url"], headers=headers())
                if resp.status_code in (429, 999):
                    continue
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "html.parser")
                job["description"] = job.get("description") or _extract_li_description(soup)
                job["salary"]      = job.get("salary")      or _extract_li_salary(soup)
            except httpx.HTTPError:
                continue

    logger.info(f"LinkedIn total: {len(jobs)}")
    return jobs


# ── Indeed ────────────────────────────────────────────────────────────────────

def scrape_indeed(role: str, location: str, max_pages: int = 5) -> list[dict]:
    jobs: list[dict] = []
    # Indeed remote filter GUID
    remote_param = "&remotejob=032b3046-06a3-4876-8dfd-474eb5e7ed11" if location.lower() == "remote" else ""

    with httpx.Client(timeout=20, follow_redirects=True) as client:
        for page in range(max_pages):
            url = (
                f"https://www.indeed.com/jobs"
                f"?q={quote_plus(role)}&l={quote_plus(location)}&start={page * 10}{remote_param}"
            )
            for attempt in range(MAX_RETRIES):
                try:
                    delay(2, 5)
                    resp = client.get(url, headers=headers())
                    if resp.status_code in (429, 403):
                        backoff(attempt, base=20)
                        continue
                    resp.raise_for_status()
                    break
                except httpx.HTTPError:
                    backoff(attempt, base=10)
            else:
                break

            soup = BeautifulSoup(resp.text, "html.parser")

            # Try extracting embedded JSON job data first
            mosaic_jobs = _parse_indeed_mosaic(soup, location)
            if mosaic_jobs:
                jobs.extend(mosaic_jobs)
                logger.info(f"Indeed page {page} (mosaic): {len(mosaic_jobs)} jobs")
                continue

            # Fall back to HTML card parsing
            cards = soup.select("div.job_seen_beacon, div.jobsearch-SerpJobCard, li.css-5lfssm")
            if not cards:
                logger.info(f"Indeed page {page}: no cards found, stopping")
                break

            for card in cards:
                title_el  = card.select_one("h2.jobTitle a span, h2.jobTitle span[title], h2 a span")
                link_el   = card.select_one("h2.jobTitle a, a.jcs-JobTitle, h2 a")
                if not title_el or not link_el:
                    continue

                href = link_el.get("href", "")
                if href.startswith("/"):
                    href = f"https://www.indeed.com{href}"
                # Keep query string for Indeed (job ID is in ?jk= param)
                if not href.startswith("http"):
                    continue

                company_el = card.select_one(
                    "span[data-testid='company-name'], span.companyName, "
                    "a[data-testid='company-name']"
                )
                loc_el     = card.select_one(
                    "div[data-testid='text-location'], div.companyLocation, "
                    "span[data-testid='text-location']"
                )
                salary_el  = card.select_one(
                    "div.salary-snippet-container, div[data-testid='attribute_snippet_testid'], "
                    "span.estimated-salary"
                )
                date_el    = card.select_one("span[data-testid='myJobsStateDate'], span.date")
                snippet_el = card.select_one("div.job-snippet, ul.jobCardShelfContainer")

                salary_text = salary_el.get_text(strip=True) if salary_el else None
                # Only keep if it looks like a salary
                if salary_text and not re.search(r'[\d$£€₹]', salary_text):
                    salary_text = None

                jobs.append({
                    "id":          make_id(href),
                    "title":       title_el.get_text(strip=True),
                    "company":     company_el.get_text(strip=True) if company_el else "Unknown",
                    "location":    loc_el.get_text(strip=True) if loc_el else location,
                    "posted_date": parse_date(date_el.get_text(strip=True)) if date_el else None,
                    "description": truncate(snippet_el.get_text(separator=" ", strip=True)) if snippet_el else None,
                    "salary":      salary_text,
                    "url":         href,
                    "source":      "Indeed",
                })

            logger.info(f"Indeed page {page} (HTML): {len(cards)} cards")

    logger.info(f"Indeed total: {len(jobs)}")
    return jobs


def _parse_indeed_mosaic(soup: BeautifulSoup, fallback_location: str) -> list[dict]:
    """Try to pull job data from Indeed's embedded JS mosaic object."""
    jobs: list[dict] = []
    for script in soup.select("script"):
        text = script.string or ""
        if "mosaic-provider-jobcards" not in text:
            continue
        # Extract the JSON blob
        m = re.search(r'"mosaic-provider-jobcards"\s*:\s*(\{.*?"jobs"\s*:\s*\[.*?\].*?\})', text, re.DOTALL)
        if not m:
            continue
        try:
            blob = json.loads(m.group(1))
            raw_jobs = blob.get("metaData", {}).get("mosaicProviderJobCardsModel", {}).get("results", [])
            if not raw_jobs:
                raw_jobs = blob.get("jobs", [])
            for item in raw_jobs:
                job_key = item.get("jobkey", "")
                job_url = f"https://www.indeed.com/viewjob?jk={job_key}" if job_key else ""
                if not job_url:
                    continue
                salary_info = item.get("extractedSalary") or {}
                salary = None
                if salary_info:
                    lo = salary_info.get("min")
                    hi = salary_info.get("max")
                    typ = salary_info.get("type", "year")
                    if lo and hi:
                        salary = f"${lo:,.0f}–${hi:,.0f}/{typ}"
                    elif lo:
                        salary = f"${lo:,.0f}/{typ}"

                jobs.append({
                    "id":          make_id(job_url),
                    "title":       item.get("title", ""),
                    "company":     item.get("company", "Unknown"),
                    "location":    item.get("formattedLocation") or fallback_location,
                    "posted_date": parse_date(item.get("pubDate") or item.get("formattedRelativeTime")),
                    "description": truncate(
                        BeautifulSoup(item.get("snippet", ""), "html.parser").get_text(separator=" ", strip=True)
                    ),
                    "salary":      salary,
                    "url":         job_url,
                    "source":      "Indeed",
                })
        except (json.JSONDecodeError, TypeError, ValueError, KeyError):
            continue
    return jobs


# ── ZipRecruiter ──────────────────────────────────────────────────────────────

def scrape_ziprecruiter(role: str, location: str, max_pages: int = 5) -> list[dict]:
    jobs: list[dict] = []
    remote_flag = "&remote=1" if location.lower() == "remote" else ""

    with httpx.Client(timeout=20, follow_redirects=True) as client:
        for page in range(1, max_pages + 1):
            url = (
                f"https://www.ziprecruiter.com/candidate/search"
                f"?search={quote_plus(role)}&location={quote_plus(location)}&page={page}{remote_flag}"
            )
            for attempt in range(MAX_RETRIES):
                try:
                    delay(2, 5)
                    resp = client.get(url, headers=headers())
                    if resp.status_code in (429, 403):
                        backoff(attempt, base=20)
                        continue
                    resp.raise_for_status()
                    break
                except httpx.HTTPError:
                    backoff(attempt, base=10)
            else:
                break

            soup = BeautifulSoup(resp.text, "html.parser")

            # Try JSON-in-page extraction first
            json_jobs = _parse_ziprecruiter_json(soup, location)
            if json_jobs:
                jobs.extend(json_jobs)
                logger.info(f"ZipRecruiter page {page} (JSON): {len(json_jobs)} jobs")
                continue

            # HTML fallback
            cards = soup.select(
                "article.job_result, div[class*='jobList_item'], "
                "li[class*='job-listing'], div[data-testid='job-card']"
            )
            if not cards:
                logger.info(f"ZipRecruiter page {page}: no cards, stopping")
                break

            for card in cards:
                title_el   = card.select_one("h2 a, h3 a, a[class*='job_link'], a[data-testid='job-title']")
                company_el = card.select_one(
                    "a[class*='company'], span[class*='company'], p[class*='company']"
                )
                loc_el     = card.select_one(
                    "span[class*='location'], p[class*='location'], div[class*='location']"
                )
                salary_el  = card.select_one(
                    "span[class*='salary'], div[class*='salary'], p[class*='compensation']"
                )
                snippet_el = card.select_one(
                    "p[class*='job_description'], div[class*='snippet'], ul[class*='bullets']"
                )

                if not title_el:
                    continue
                href = title_el.get("href", "")
                if href.startswith("/"):
                    href = f"https://www.ziprecruiter.com{href}"
                if not href.startswith("http"):
                    continue

                jobs.append({
                    "id":          make_id(href),
                    "title":       title_el.get_text(strip=True),
                    "company":     company_el.get_text(strip=True) if company_el else "Unknown",
                    "location":    loc_el.get_text(strip=True) if loc_el else location,
                    "posted_date": None,
                    "description": truncate(snippet_el.get_text(separator=" ", strip=True)) if snippet_el else None,
                    "salary":      salary_el.get_text(strip=True) if salary_el else None,
                    "url":         href,
                    "source":      "ZipRecruiter",
                })

            logger.info(f"ZipRecruiter page {page} (HTML): {len(cards)} cards")

    logger.info(f"ZipRecruiter total: {len(jobs)}")
    return jobs


def _parse_ziprecruiter_json(soup: BeautifulSoup, fallback_location: str) -> list[dict]:
    """Extract jobs from ZipRecruiter's embedded __NEXT_DATA__ or window.__data JSON."""
    jobs: list[dict] = []
    for script in soup.select("script[id='__NEXT_DATA__'], script[type='application/json']"):
        try:
            data = json.loads(script.string or "")
            # Navigate the nested structure
            job_list = (
                data.get("props", {}).get("pageProps", {}).get("jobListings", [])
                or data.get("props", {}).get("pageProps", {}).get("jobs", [])
                or data.get("jobListings", [])
                or data.get("jobs", [])
            )
            for item in job_list:
                href = item.get("job_url") or item.get("url") or ""
                if not href:
                    continue
                salary_min = item.get("salary_min_annual") or item.get("compensation_min")
                salary_max = item.get("salary_max_annual") or item.get("compensation_max")
                salary = None
                if salary_min and salary_max:
                    salary = f"${salary_min:,.0f}–${salary_max:,.0f}"
                elif salary_min:
                    salary = f"${salary_min:,.0f}+"

                jobs.append({
                    "id":          make_id(href),
                    "title":       item.get("title") or item.get("name") or "",
                    "company":     item.get("hiring_company", {}).get("name") or item.get("company") or "Unknown",
                    "location":    item.get("location") or item.get("city") or fallback_location,
                    "posted_date": parse_date(item.get("posted_time") or item.get("posted_at")),
                    "description": truncate(
                        BeautifulSoup(item.get("snippet") or item.get("description") or "", "html.parser")
                        .get_text(separator=" ", strip=True)
                    ),
                    "salary":      salary,
                    "url":         href,
                    "source":      "ZipRecruiter",
                })
        except (json.JSONDecodeError, TypeError, AttributeError):
            continue
    return jobs


# ── RemoteOK (public JSON API) ────────────────────────────────────────────────

def scrape_remoteok(role: str) -> list[dict]:
    """
    RemoteOK has a public, no-auth JSON API.
    Returns fully-remote jobs only — ideal complement for non-US searches.
    """
    jobs: list[dict] = []
    tag = role.lower().replace(" ", "-")

    urls = [
        f"https://remoteok.com/api?tag={quote_plus(tag)}",
        "https://remoteok.com/api",  # fallback: all jobs, filter client-side
    ]

    with httpx.Client(timeout=20, follow_redirects=True) as client:
        for url in urls:
            try:
                delay(1, 2)
                resp = client.get(
                    url,
                    headers={
                        **headers(),
                        "Accept": "application/json",
                        "Referer": "https://remoteok.com/",
                    },
                )
                if resp.status_code == 429:
                    backoff(0, base=10)
                    continue
                resp.raise_for_status()
                data = resp.json()
                break
            except (httpx.HTTPError, json.JSONDecodeError) as e:
                logger.error(f"RemoteOK error: {e}")
                continue
        else:
            return jobs

    role_words = set(role.lower().split())

    for item in data:
        # First item is usually a legal/meta dict, skip non-job entries
        if not isinstance(item, dict) or not item.get("position"):
            continue

        title: str = item.get("position", "")

        # If we fetched all jobs, filter by role relevance
        if "tag=" not in url:
            tags_str = " ".join(item.get("tags") or []).lower()
            title_lower = title.lower()
            if not any(w in title_lower or w in tags_str for w in role_words):
                continue

        job_url = item.get("url") or f"https://remoteok.com/remote-jobs/{item.get('id','')}"
        salary_min = item.get("salary_min")
        salary_max = item.get("salary_max")
        salary = None
        if salary_min and salary_max:
            salary = f"${salary_min:,.0f}–${salary_max:,.0f}"
        elif salary_min:
            salary = f"${salary_min:,.0f}+"

        desc_raw = item.get("description") or ""
        desc = truncate(BeautifulSoup(desc_raw, "html.parser").get_text(separator=" ", strip=True))

        jobs.append({
            "id":          make_id(job_url),
            "title":       title,
            "company":     item.get("company", "Unknown"),
            "location":    "Remote",
            "posted_date": parse_date(item.get("date")),
            "description": desc,
            "salary":      salary,
            "url":         job_url,
            "source":      "RemoteOK",
        })

    logger.info(f"RemoteOK total: {len(jobs)}")
    return jobs


# ── Entry point ───────────────────────────────────────────────────────────────

def run_scrape(
    role: str = "Software Developer",
    location: str = "Hyderabad",
    on_batch: Optional[Callable[[list[dict]], None]] = None,
) -> list[dict]:
    """
    Scrape all sources for the given role + location.

    For non-US locations, automatically also runs a "Remote" pass and includes
    RemoteOK results. Hybrid jobs are excluded from Remote results.

    on_batch(jobs): called with each new unique batch as sources finish,
    enabling progressive DB saves and UI streaming. Pass None to skip.
    """
    is_non_us = not _is_us_location(location)

    # Sources to scrape: list of (label, callable)
    sources: list[tuple[str, Callable]] = [
        ("Dice",          lambda loc: scrape_dice(role, loc)),
        ("LinkedIn",      lambda loc: scrape_linkedin(role, loc)),
        ("Indeed",        lambda loc: scrape_indeed(role, loc)),
        ("ZipRecruiter",  lambda loc: scrape_ziprecruiter(role, loc)),
    ]

    # Locations to search per source
    locations_to_search = [location]
    if is_non_us:
        locations_to_search.append("Remote")

    seen: set[str] = set()
    results: list[dict] = []

    def _add_batch(batch: list[dict], is_remote_search: bool) -> list[dict]:
        new_jobs: list[dict] = []
        for job in batch:
            # For remote-pass jobs, drop hybrid listings
            if is_remote_search and _is_hybrid(job):
                continue
            if job["url"] not in seen:
                seen.add(job["url"])
                results.append(job)
                new_jobs.append(job)
        return new_jobs

    for source_label, scraper_fn in sources:
        for loc in locations_to_search:
            is_remote = loc.lower() == "remote"
            try:
                logger.info(f"Starting {source_label} @ {loc}")
                batch = scraper_fn(loc)
                new_jobs = _add_batch(batch, is_remote_search=is_remote)
                if new_jobs and on_batch:
                    on_batch(new_jobs)
                logger.info(f"{source_label} @ {loc}: +{len(new_jobs)} new jobs")
            except Exception as e:
                logger.error(f"{source_label} @ {loc} failed: {e}")

    # RemoteOK for non-US searches (fully remote, no hybrid possible)
    if is_non_us:
        try:
            logger.info("Starting RemoteOK")
            batch = scrape_remoteok(role)
            new_jobs = _add_batch(batch, is_remote_search=False)  # already all-remote
            if new_jobs and on_batch:
                on_batch(new_jobs)
            logger.info(f"RemoteOK: +{len(new_jobs)} new jobs")
        except Exception as e:
            logger.error(f"RemoteOK failed: {e}")

    logger.info(f"Total unique jobs: {len(results)}")
    return results