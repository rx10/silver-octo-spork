"""
Job scraper for Dice.com and LinkedIn.
Dice: intercepts API key via headless browser, falls back to env var.
LinkedIn: scrapes public search pages + fetches detail pages for descriptions.
"""

import json, re, os, hashlib, random, time, logging
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import quote_plus

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

MAX_RETRIES = 3
_cached_api_key: Optional[str] = None


# ── Utilities ─────────────────────────────────────────────────────────────────

def make_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:32]

def delay(lo=1.0, hi=3.0):
    time.sleep(random.uniform(lo, hi))

def headers():
    return {"User-Agent": random.choice(USER_AGENTS), "Accept-Language": "en-US,en;q=0.9"}

def backoff(attempt, base=5.0):
    time.sleep(min(base * 2**attempt + random.uniform(0, 2), 120))

def truncate(text: str, n=500) -> str:
    if not text or len(text) <= n:
        return text or ""
    return text[:n].rsplit(" ", 1)[0] + " …"

def parse_date(s: Optional[str]) -> Optional[str]:
    """Parse '3 days ago', 'yesterday', ISO dates, etc."""
    if not s:
        return None
    s = s.strip().lower()
    today = datetime.utcnow().date()
    if any(w in s for w in ("today", "just", "hour")):
        return today.isoformat()
    if "yesterday" in s:
        return (today - timedelta(days=1)).isoformat()
    for unit, fn in [("day", timedelta), ("week", lambda n: timedelta(weeks=n)), ("month", lambda n: timedelta(days=n*30))]:
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


# ── Dice API Key ──────────────────────────────────────────────────────────────

def _intercept_key_browser() -> Optional[str]:
    """Launch headless Chromium, trigger a Dice search, capture the x-api-key from network traffic."""
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
            page.goto("https://www.dice.com/jobs?q=software+engineer&location=United+States",
                       wait_until="domcontentloaded", timeout=45000)
            page.wait_for_timeout(3000)

            # type into search to trigger an API call
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
    """Get Dice API key: cached → browser → env var → hardcoded fallback."""
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


# ── Dice Scraper ──────────────────────────────────────────────────────────────

# US states and common US location keywords
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
_US_ABBREVS = {
    "al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id","il","in","ia",
    "ks","ky","la","me","md","ma","mi","mn","ms","mo","mt","ne","nv","nh","nj",
    "nm","ny","nc","nd","oh","ok","or","pa","ri","sc","sd","tn","tx","ut","vt",
    "va","wa","wv","wi","wy","dc",
}
_US_KEYWORDS = {"united states", "usa", "us", "remote"}

def _is_us_location(location: str) -> bool:
    loc = location.strip().lower()
    if any(kw in loc for kw in _US_KEYWORDS):
        return True
    if loc in _US_STATES or loc in _US_ABBREVS:
        return True
    # Check if location ends with a state abbrev like "San Francisco, CA"
    parts = [p.strip().rstrip(".") for p in loc.replace(",", " ").split()]
    if parts and parts[-1] in _US_ABBREVS:
        return True
    return False


def scrape_dice(role: str, location: str, max_pages=3) -> list[dict]:
    api_key = get_dice_key()
    jobs = []

    # Dice only supports US locations — use "Remote" for non-US searches
    dice_location = location if _is_us_location(location) else "Remote"
    if dice_location != location:
        logger.info(f"Dice: non-US location '{location}' → searching '{dice_location}' instead")

    with httpx.Client(timeout=15, follow_redirects=True) as client:
        for page in range(1, max_pages + 1):
            url = (f"https://job-search-api.svc.dhigroupinc.com/v1/dice/jobs/search"
                   f"?q={quote_plus(role)}&location={quote_plus(dice_location)}"
                   f"&pageSize=20&page={page}&language=en")

            for attempt in range(MAX_RETRIES):
                try:
                    delay()
                    resp = client.get(url, headers={**headers(), "Accept": "application/json", "x-api-key": api_key})

                    if resp.status_code == 429:
                        backoff(attempt)
                        continue
                    if resp.status_code in (401, 403):
                        logger.error(f"Dice API auth error {resp.status_code} — key likely expired")
                        return jobs

                    resp.raise_for_status()
                    data = resp.json()
                    break
                except httpx.HTTPError as e:
                    logger.error(f"Dice page {page} attempt {attempt+1}: {e}")
                    backoff(attempt)
            else:
                break  # all retries failed

            hits = data.get("data", [])
            if not hits:
                break

            for item in hits:
                job_url = item.get("detailsPageUrl") or f"https://www.dice.com/job-detail/{item.get('guid','')}"
                jobs.append({
                    "id":          make_id(job_url),
                    "title":       item.get("title", ""),
                    "company":     item.get("companyName", "Unknown"),
                    "location":    item.get("jobLocation", {}).get("displayName") or location,
                    "posted_date": parse_date(item.get("postedDate")),
                    "description": truncate(item.get("summary") or ""),
                    "salary":      item.get("salary"),
                    "url":         job_url,
                    "source":      "Dice",
                })

            logger.info(f"Dice page {page}: {len(hits)} jobs")

    logger.info(f"Dice total: {len(jobs)}")
    return jobs


# ── LinkedIn Scraper ──────────────────────────────────────────────────────────

def _extract_description(soup: BeautifulSoup) -> Optional[str]:
    """Pull job description from LinkedIn detail page via selectors or JSON-LD."""
    for sel in ["div.show-more-less-html__markup", "div.description__text",
                "section.show-more-less-html", "div[class*='description__text']"]:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(separator=" ", strip=True)
            if len(text) > 50:
                return truncate(text)

    # JSON-LD fallback
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            ld = json.loads(script.string or "")
            desc = ld.get("description") if isinstance(ld, dict) else None
            if desc and len(desc) > 50:
                return truncate(BeautifulSoup(desc, "html.parser").get_text(separator=" ", strip=True))
        except (json.JSONDecodeError, TypeError):
            pass
    return None


def _extract_salary(soup: BeautifulSoup) -> Optional[str]:
    """Pull salary from LinkedIn detail page via selectors, JSON-LD, or regex."""
    for sel in ["div.salary-main-rail__data-body", "span.compensation__salary",
                "div[class*='salary']", "span[class*='salary']"]:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(strip=True)
            if "$" in text or any(c.isdigit() for c in text):
                return text

    # JSON-LD
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

    # regex fallback
    m = re.search(r'\$[\d,]+\s*[-–/]+\s*\$[\d,]+', soup.get_text())
    return m.group(0).strip() if m else None


def scrape_linkedin(role: str, location: str, max_pages=3, max_details=15) -> list[dict]:
    jobs = []

    with httpx.Client(timeout=20, follow_redirects=True) as client:
        # Phase 1: collect cards from search pages
        for page in range(max_pages):
            url = (f"https://www.linkedin.com/jobs/search/"
                   f"?keywords={quote_plus(role)}&location={quote_plus(location)}&start={page*25}")

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

        # Phase 2: fetch detail pages for description & salary
        to_fetch = [j for j in jobs if not j.get("description")][:max_details]
        logger.info(f"LinkedIn: fetching details for {len(to_fetch)}/{len(jobs)} jobs")

        for job in to_fetch:
            try:
                delay(2, 5)
                resp = client.get(job["url"], headers=headers())
                if resp.status_code in (429, 999):
                    continue
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "html.parser")
                job["description"] = job.get("description") or _extract_description(soup)
                job["salary"]      = job.get("salary") or _extract_salary(soup)
            except httpx.HTTPError:
                continue

        filled = sum(1 for j in jobs if j.get("description"))
        logger.info(f"LinkedIn: {filled}/{len(jobs)} have descriptions")

    logger.info(f"LinkedIn total: {len(jobs)}")
    return jobs


# ── Entry point ───────────────────────────────────────────────────────────────

def run_scrape(role="Software Developer", location="California") -> list[dict]:
    """Scrape Dice + LinkedIn, deduplicate by URL."""
    seen = set()
    results = []
    for job in scrape_dice(role, location) + scrape_linkedin(role, location):
        if job["url"] not in seen:
            seen.add(job["url"])
            results.append(job)
    logger.info(f"Total unique: {len(results)}")
    return results