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
    """
    Scrape Indeed via RSS feed (primary) — public, no auth, not blocked.
    Falls back to HTML scraping if RSS returns nothing.

    Indeed's RSS endpoint:
        https://www.indeed.com/rss?q=<role>&l=<location>&start=<offset>
    Returns standard RSS/XML with <item> elements, each containing:
        <title>, <link>, <source>, <pubDate>, <description> (HTML snippet)
    The location and salary are embedded in the description HTML.
    """
    jobs: list[dict] = []
    is_remote = location.lower() == "remote"
    # Indeed remote RSS filter
    remote_param = "&remotejob=032b3046-06a3-4876-8dfd-474eb5e7ed11" if is_remote else ""

    rss_headers = {
        **headers(),
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
    }

    with httpx.Client(timeout=20, follow_redirects=True) as client:

        # ── Phase 1: RSS (primary — not blocked by Indeed) ────────────────────
        for page in range(max_pages):
            rss_url = (
                f"https://www.indeed.com/rss"
                f"?q={quote_plus(role)}&l={quote_plus(location)}"
                f"&start={page * 10}{remote_param}"
            )
            for attempt in range(MAX_RETRIES):
                try:
                    delay(1, 2.5)
                    resp = client.get(rss_url, headers=rss_headers)
                    if resp.status_code in (429, 403):
                        logger.warning(f"Indeed RSS page {page} blocked ({resp.status_code}), backing off")
                        backoff(attempt, base=10)
                        continue
                    resp.raise_for_status()
                    break
                except httpx.HTTPError as e:
                    logger.error(f"Indeed RSS page {page} attempt {attempt + 1}: {e}")
                    backoff(attempt, base=5)
            else:
                logger.warning("Indeed RSS: all retries failed, stopping")
                break

            rss_jobs = _parse_indeed_rss(resp.text, location)
            if not rss_jobs:
                logger.info(f"Indeed RSS page {page}: no items, stopping")
                break

            jobs.extend(rss_jobs)
            logger.info(f"Indeed RSS page {page}: {len(rss_jobs)} jobs")

            # RSS returns exactly 10 items per page; if fewer, we're at the end
            if len(rss_jobs) < 10:
                break

        if jobs:
            logger.info(f"Indeed RSS total: {len(jobs)}")
            return jobs

        # ── Phase 2: HTML fallback (if RSS returned nothing) ──────────────────
        logger.info("Indeed RSS returned 0 jobs — trying HTML fallback")
        for page in range(max_pages):
            url = (
                f"https://www.indeed.com/jobs"
                f"?q={quote_plus(role)}&l={quote_plus(location)}"
                f"&start={page * 10}{remote_param}"
            )
            for attempt in range(MAX_RETRIES):
                try:
                    delay(3, 6)
                    resp = client.get(url, headers=headers())
                    if resp.status_code in (429, 403):
                        logger.warning(f"Indeed HTML page {page} blocked ({resp.status_code})")
                        backoff(attempt, base=20)
                        continue
                    resp.raise_for_status()
                    break
                except httpx.HTTPError:
                    backoff(attempt, base=10)
            else:
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            html_jobs = _parse_indeed_html(soup, location)
            if not html_jobs:
                logger.info(f"Indeed HTML page {page}: no cards, stopping")
                break
            jobs.extend(html_jobs)
            logger.info(f"Indeed HTML page {page}: {len(html_jobs)} jobs")

    logger.info(f"Indeed total: {len(jobs)}")
    return jobs


def _parse_indeed_rss(xml_text: str, fallback_location: str) -> list[dict]:
    """
    Parse Indeed RSS XML. Each <item> looks like:
        <title>Job Title - Company Name - Location</title>
        <link>https://www.indeed.com/viewjob?jk=...</link>
        <pubDate>Mon, 18 Mar 2024 12:00:00 GMT</pubDate>
        <description><![CDATA[ ... HTML snippet ... ]]></description>
        <source>Company Name</source>
    """
    jobs: list[dict] = []
    try:
        soup = BeautifulSoup(xml_text, "xml")   # lxml xml parser
    except Exception:
        soup = BeautifulSoup(xml_text, "html.parser")

    items = soup.find_all("item")
    if not items:
        # Try case-insensitive fallback
        items = soup.select("item")

    for item in items:
        # ── URL ───────────────────────────────────────────────────────────────
        link_el = item.find("link")
        # In RSS, <link> text is sometimes in a sibling text node
        href = ""
        if link_el:
            href = link_el.get_text(strip=True) or link_el.get("href", "")
        if not href or not href.startswith("http"):
            # guid often has the real URL
            guid_el = item.find("guid")
            href = guid_el.get_text(strip=True) if guid_el else ""
        if not href.startswith("http"):
            continue

        # ── Title / company / location ────────────────────────────────────────
        raw_title = item.find("title")
        raw_title = raw_title.get_text(strip=True) if raw_title else ""

        # Indeed RSS title format: "Job Title - Company - City, State"
        # Split on " - " (en dash or hyphen)
        parts = re.split(r"\s+[-–]\s+", raw_title)
        title   = parts[0].strip() if parts else raw_title
        company = parts[1].strip() if len(parts) > 1 else "Unknown"
        loc_raw = parts[2].strip() if len(parts) > 2 else fallback_location

        # <source> tag sometimes has company name
        source_el = item.find("source")
        if source_el and source_el.get_text(strip=True):
            company = source_el.get_text(strip=True)

        # ── Date ──────────────────────────────────────────────────────────────
        pub_el = item.find("pubDate")
        posted = parse_date(pub_el.get_text(strip=True)) if pub_el else None

        # ── Description (HTML snippet inside CDATA) ───────────────────────────
        desc_el = item.find("description")
        desc_html = desc_el.get_text(strip=True) if desc_el else ""
        desc_text = BeautifulSoup(desc_html, "html.parser").get_text(separator=" ", strip=True)

        # Extract salary from description if present
        salary = None
        sal_match = re.search(
            r'([$£€₹]\s*[\d,]+(?:\s*[-–]\s*[$£€₹]?\s*[\d,]+)?(?:\s*/\s*(?:yr|year|hr|hour|mo|month))?)',
            desc_text, re.IGNORECASE
        )
        if sal_match:
            salary = sal_match.group(1).strip()

        # Extract location from description if it's more specific than fallback
        loc_in_desc = re.search(r'location:\s*([^\n<]+)', desc_text, re.IGNORECASE)
        if loc_in_desc:
            loc_raw = loc_in_desc.group(1).strip()

        jobs.append({
            "id":          make_id(href),
            "title":       title,
            "company":     company,
            "location":    loc_raw or fallback_location,
            "posted_date": posted,
            "description": truncate(desc_text),
            "salary":      salary,
            "url":         href,
            "source":      "Indeed",
        })

    return jobs


def _parse_indeed_html(soup: BeautifulSoup, fallback_location: str) -> list[dict]:
    """HTML card fallback for Indeed — used only when RSS is unavailable."""
    jobs: list[dict] = []
    cards = soup.select("div.job_seen_beacon, div.jobsearch-SerpJobCard, li.css-5lfssm")

    for card in cards:
        title_el  = card.select_one("h2.jobTitle a span, h2.jobTitle span[title], h2 a span")
        link_el   = card.select_one("h2.jobTitle a, a.jcs-JobTitle, h2 a")
        if not title_el or not link_el:
            continue

        href = link_el.get("href", "")
        if href.startswith("/"):
            href = f"https://www.indeed.com{href}"
        if not href.startswith("http"):
            continue

        company_el = card.select_one("span[data-testid='company-name'], span.companyName")
        loc_el     = card.select_one("div[data-testid='text-location'], div.companyLocation")
        salary_el  = card.select_one("div.salary-snippet-container, span.estimated-salary")
        date_el    = card.select_one("span[data-testid='myJobsStateDate'], span.date")
        snippet_el = card.select_one("div.job-snippet, ul.jobCardShelfContainer")

        salary_text = salary_el.get_text(strip=True) if salary_el else None
        if salary_text and not re.search(r'[\d$£€₹]', salary_text):
            salary_text = None

        jobs.append({
            "id":          make_id(href),
            "title":       title_el.get_text(strip=True),
            "company":     company_el.get_text(strip=True) if company_el else "Unknown",
            "location":    loc_el.get_text(strip=True) if loc_el else fallback_location,
            "posted_date": parse_date(date_el.get_text(strip=True)) if date_el else None,
            "description": truncate(snippet_el.get_text(separator=" ", strip=True)) if snippet_el else None,
            "salary":      salary_text,
            "url":         href,
            "source":      "Indeed",
        })

    return jobs


# ── ZipRecruiter ──────────────────────────────────────────────────────────────

def scrape_ziprecruiter(role: str, location: str, max_pages: int = 5) -> list[dict]:
    """
    Scrape ZipRecruiter via RSS feed (primary) — public, not blocked.
    Falls back to their job-search JSON API (no auth needed for basic queries).

    RSS endpoint:
        https://www.ziprecruiter.com/jobs/search?q=<role>&l=<location>&format=rss
    JSON API endpoint (unofficial but stable):
        https://www.ziprecruiter.com/jobs-search?search=<role>&location=<location>
        &form=jobs-landing&is_remote_job=<0|1>&page=<n>  → returns JSON
    """
    jobs: list[dict] = []
    is_remote  = location.lower() == "remote"
    remote_rss = "&remote=1" if is_remote else ""

    rss_headers = {
        **headers(),
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
    }

    with httpx.Client(timeout=20, follow_redirects=True) as client:

        # ── Phase 1: RSS (primary) ─────────────────────────────────────────────
        for page in range(1, max_pages + 1):
            rss_url = (
                f"https://www.ziprecruiter.com/jobs/search"
                f"?q={quote_plus(role)}&l={quote_plus(location)}"
                f"&format=rss&page={page}{remote_rss}"
            )
            for attempt in range(MAX_RETRIES):
                try:
                    delay(1, 2.5)
                    resp = client.get(rss_url, headers=rss_headers)
                    if resp.status_code in (403, 429):
                        logger.warning(f"ZipRecruiter RSS page {page} blocked ({resp.status_code})")
                        backoff(attempt, base=10)
                        continue
                    resp.raise_for_status()
                    break
                except httpx.HTTPError as e:
                    logger.error(f"ZipRecruiter RSS page {page} attempt {attempt + 1}: {e}")
                    backoff(attempt, base=5)
            else:
                break

            rss_jobs = _parse_ziprecruiter_rss(resp.text, location)
            if not rss_jobs:
                logger.info(f"ZipRecruiter RSS page {page}: no items, stopping")
                break

            jobs.extend(rss_jobs)
            logger.info(f"ZipRecruiter RSS page {page}: {len(rss_jobs)} jobs")

            if len(rss_jobs) < 10:
                break  # last page

        if jobs:
            logger.info(f"ZipRecruiter RSS total: {len(jobs)}")
            return jobs

        # ── Phase 2: JSON API fallback ─────────────────────────────────────────
        logger.info("ZipRecruiter RSS returned 0 — trying JSON API fallback")
        for page in range(1, max_pages + 1):
            api_url = (
                f"https://www.ziprecruiter.com/jobs-search"
                f"?search={quote_plus(role)}&location={quote_plus(location)}"
                f"&form=jobs-landing&is_remote_job={'1' if is_remote else '0'}"
                f"&page={page}"
            )
            for attempt in range(MAX_RETRIES):
                try:
                    delay(2, 4)
                    resp = client.get(
                        api_url,
                        headers={**headers(), "Accept": "application/json, text/javascript, */*"},
                    )
                    if resp.status_code in (403, 429):
                        backoff(attempt, base=15)
                        continue
                    resp.raise_for_status()
                    break
                except httpx.HTTPError:
                    backoff(attempt, base=10)
            else:
                break

            api_jobs = _parse_ziprecruiter_json_api(resp.text, location)
            if not api_jobs:
                logger.info(f"ZipRecruiter JSON API page {page}: no jobs, stopping")
                break
            jobs.extend(api_jobs)
            logger.info(f"ZipRecruiter JSON API page {page}: {len(api_jobs)} jobs")

    logger.info(f"ZipRecruiter total: {len(jobs)}")
    return jobs


def _parse_ziprecruiter_rss(xml_text: str, fallback_location: str) -> list[dict]:
    """
    Parse ZipRecruiter RSS. Each <item> contains:
        <title>Job Title at Company Name</title>
        <link>https://www.ziprecruiter.com/...</link>
        <pubDate>...</pubDate>
        <description><![CDATA[ HTML snippet ]]></description>
        <location>City, State</location>   ← ZR-specific extension
        <salary>$X – $Y</salary>           ← ZR-specific extension (sometimes present)
    """
    jobs: list[dict] = []
    try:
        soup = BeautifulSoup(xml_text, "xml")
    except Exception:
        soup = BeautifulSoup(xml_text, "html.parser")

    items = soup.find_all("item")
    if not items:
        items = soup.select("item")

    for item in items:
        # URL
        link_el = item.find("link")
        href = link_el.get_text(strip=True) if link_el else ""
        if not href or not href.startswith("http"):
            guid_el = item.find("guid")
            href = guid_el.get_text(strip=True) if guid_el else ""
        if not href.startswith("http"):
            continue

        # Title — ZipRecruiter RSS: "Job Title at Company Name"
        raw_title = ""
        title_el = item.find("title")
        if title_el:
            raw_title = title_el.get_text(strip=True)

        # Split on " at " to separate title and company
        if " at " in raw_title:
            idx     = raw_title.rfind(" at ")
            title   = raw_title[:idx].strip()
            company = raw_title[idx + 4:].strip()
        else:
            title   = raw_title
            company = "Unknown"

        # ZipRecruiter RSS often includes <location> tag
        loc_el  = item.find("location") or item.find("job:location")
        loc     = loc_el.get_text(strip=True) if loc_el else fallback_location

        # Salary — ZR sometimes includes <salary> or it's in description
        salary_el = item.find("salary") or item.find("job:salary")
        salary    = salary_el.get_text(strip=True) if salary_el else None

        # Date
        pub_el = item.find("pubDate")
        posted = parse_date(pub_el.get_text(strip=True)) if pub_el else None

        # Description
        desc_el   = item.find("description")
        desc_html = desc_el.get_text(strip=True) if desc_el else ""
        desc_text = BeautifulSoup(desc_html, "html.parser").get_text(separator=" ", strip=True)

        # Extract salary from description if not already found
        if not salary:
            sal_m = re.search(
                r'([$£€₹]\s*[\d,]+(?:\s*[-–]\s*[$£€₹]?\s*[\d,]+)?'
                r'(?:\s*/\s*(?:yr|year|hr|hour|mo|month|annum))?)',
                desc_text, re.IGNORECASE,
            )
            if sal_m:
                salary = sal_m.group(1).strip()

        jobs.append({
            "id":          make_id(href),
            "title":       title,
            "company":     company,
            "location":    loc,
            "posted_date": posted,
            "description": truncate(desc_text),
            "salary":      salary,
            "url":         href,
            "source":      "ZipRecruiter",
        })

    return jobs


def _parse_ziprecruiter_json_api(response_text: str, fallback_location: str) -> list[dict]:
    """
    Parse ZipRecruiter's jobs-search endpoint which returns JSON or HTML with
    embedded JSON. Handles both cases.
    """
    jobs: list[dict] = []

    # Try direct JSON parse first
    try:
        data = json.loads(response_text)
        job_list = (
            data.get("jobs")
            or data.get("job_results", {}).get("jobs", [])
            or []
        )
    except (json.JSONDecodeError, AttributeError):
        # Embedded JSON in HTML — look for window.__data or __NEXT_DATA__
        job_list = []
        for pattern in [
            r'window\.__data\s*=\s*({.*?});',
            r'<script[^>]+id="__NEXT_DATA__"[^>]*>({.*?})</script>',
        ]:
            m = re.search(pattern, response_text, re.DOTALL)
            if m:
                try:
                    blob = json.loads(m.group(1))
                    job_list = (
                        blob.get("jobs")
                        or blob.get("props", {}).get("pageProps", {}).get("jobs", [])
                        or []
                    )
                    if job_list:
                        break
                except (json.JSONDecodeError, AttributeError):
                    continue

    for item in job_list:
        href = item.get("job_url") or item.get("url") or ""
        if not href or not href.startswith("http"):
            continue

        salary_min = item.get("salary_min_annual") or item.get("compensation_min")
        salary_max = item.get("salary_max_annual") or item.get("compensation_max")
        salary = None
        if salary_min and salary_max:
            salary = f"${float(salary_min):,.0f}–${float(salary_max):,.0f}"
        elif salary_min:
            salary = f"${float(salary_min):,.0f}+"

        desc_raw = item.get("snippet") or item.get("description") or ""
        desc = truncate(BeautifulSoup(desc_raw, "html.parser").get_text(separator=" ", strip=True))

        jobs.append({
            "id":          make_id(href),
            "title":       item.get("title") or item.get("name") or "",
            "company":     (item.get("hiring_company") or {}).get("name") or item.get("company") or "Unknown",
            "location":    item.get("location") or item.get("city") or fallback_location,
            "posted_date": parse_date(item.get("posted_time") or item.get("posted_at")),
            "description": desc,
            "salary":      salary,
            "url":         href,
            "source":      "ZipRecruiter",
        })

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