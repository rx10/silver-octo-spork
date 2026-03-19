"""
Job scraper — Dice, LinkedIn, Indeed.

Dice:     Playwright intercepts x-api-key once → httpx API calls
LinkedIn: curl_cffi (Chrome TLS fingerprint) + Oxylabs sticky sessions
Indeed:   httpx RSS feed → HTML fallback

Proxy (Oxylabs):
  OXYLABS_USER=customer-YOUR_USERNAME
  OXYLABS_PASS=YOUR_PASSWORD
  — or —
  PROXY_URL=http://customer-USER:PASS@pr.oxylabs.io:7777
"""

import json, re, os, hashlib, random, string, time, logging
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import quote_plus, urlparse

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
_cached_api_key: Optional[str] = None

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]


# ══════════════════════════════════════════════════════════════════════════════
#  PROXY — Oxylabs with sticky sessions
# ══════════════════════════════════════════════════════════════════════════════

_proxy_user: Optional[str] = None
_proxy_pass: Optional[str] = None
_proxy_host: str = "pr.oxylabs.io"
_proxy_port: str = "7777"
_proxy_loaded = False


def _load_proxy():
    """Parse proxy credentials from env vars. Called once."""
    global _proxy_user, _proxy_pass, _proxy_host, _proxy_port, _proxy_loaded
    if _proxy_loaded:
        return
    _proxy_loaded = True

    # Option 1: Full URL
    full = os.getenv("PROXY_URL", "").strip()
    if full:
        parsed = urlparse(full)
        _proxy_user = parsed.username
        _proxy_pass = parsed.password
        _proxy_host = parsed.hostname or "pr.oxylabs.io"
        _proxy_port = str(parsed.port or 7777)
        logger.info(f"Proxy: {_proxy_user}@{_proxy_host}:{_proxy_port}")
        return

    # Option 2: Separate credentials
    user = os.getenv("OXYLABS_USER", "").strip()
    pwd  = os.getenv("OXYLABS_PASS", "").strip()
    if user and pwd:
        _proxy_user = user
        _proxy_pass = pwd
        logger.info(f"Oxylabs: {user}@{_proxy_host}:{_proxy_port}")
        return

    logger.warning("No proxy configured — will likely get blocked by LinkedIn/Indeed")

from urllib.parse import quote

def _proxy_url(sticky_session: Optional[str] = None) -> Optional[str]:
    _load_proxy()
    if not _proxy_user or not _proxy_pass:
        return None

    user = _proxy_user
    if sticky_session:
        user = f"{_proxy_user}-sessid-{sticky_session}"

    # URL-encode both user and password so +, @, etc. don't break the URL
    return (
        f"http://{quote(user, safe='')}:{quote(_proxy_pass, safe='')}"
        f"@{_proxy_host}:{_proxy_port}"
    )

def _httpx_client(sticky: Optional[str] = None, **kwargs) -> httpx.Client:
    """Create httpx Client with proxy."""
    proxy = _proxy_url(sticky)
    if proxy:
        kwargs.setdefault("proxy", proxy)
    return httpx.Client(**kwargs)


def _new_session_id() -> str:
    """Random session ID for Oxylabs sticky sessions."""
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=10))


# ══════════════════════════════════════════════════════════════════════════════
#  UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def make_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:32]

def delay(lo=1.0, hi=3.0):
    time.sleep(random.uniform(lo, hi))

def hdr():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }

def backoff(attempt, base=5.0):
    time.sleep(min(base * 2 ** attempt + random.uniform(0, 2), 60))

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


# ── US Location Detection ─────────────────────────────────────────────────────

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

def _is_us(location: str) -> bool:
    loc = location.strip().lower()
    if any(kw in loc for kw in ("united states", "usa", "us", "remote")):
        return True
    if loc in _US_STATES or loc in _US_ABBREVS:
        return True
    parts = [p.strip().rstrip(".") for p in loc.replace(",", " ").split()]
    return bool(parts and parts[-1] in _US_ABBREVS)


# ══════════════════════════════════════════════════════════════════════════════
#  DICE — Playwright key interception + httpx API calls
# ══════════════════════════════════════════════════════════════════════════════

def _intercept_dice_key() -> Optional[str]:
    """One-time Playwright launch to capture Dice's x-api-key."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.info("Playwright not installed — skipping Dice key interception")
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
                wait_until="domcontentloaded", timeout=45000,
            )
            page.wait_for_timeout(3000)
            if not key:
                try:
                    inp = page.locator("input[placeholder*='Search'], input[name*='q']").first
                    if inp.is_visible(timeout=3000):
                        inp.click()
                        inp.fill("engineer")
                        page.keyboard.press("Enter")
                        page.wait_for_timeout(5000)
                except Exception:
                    pass
            browser.close()
    except Exception as e:
        logger.warning(f"Dice key interception failed: {e}")
        return None

    if key:
        logger.info(f"Dice API key intercepted: {key[:8]}…")
    return key


def _get_dice_key() -> str:
    global _cached_api_key
    if _cached_api_key:
        return _cached_api_key
    key = _intercept_dice_key()
    if not key:
        key = os.getenv("DICE_API_KEY")
        if key:
            logger.info("Using DICE_API_KEY env var")
    if not key:
        key = "1YAt0R9wBg4WfsF9VB2778F5CHLAPMVW3WAZcKd8"
        logger.warning("Using hardcoded Dice key — may be expired!")
    _cached_api_key = key
    return key


def scrape_dice(role: str, location: str, max_pages=5) -> list[dict]:
    """Pure httpx API calls using intercepted key."""
    api_key = _get_dice_key()
    jobs = []
    dice_loc = location if _is_us(location) else "Remote"
    is_remote = dice_loc != location

    if is_remote:
        logger.info(f"Dice: non-US '{location}' → 'Remote'")

    with _httpx_client(timeout=15, follow_redirects=True) as c:
        for page in range(1, max_pages + 1):
            url = (
                f"https://job-search-api.svc.dhigroupinc.com/v1/dice/jobs/search"
                f"?q={quote_plus(role)}&location={quote_plus(dice_loc)}"
                f"&pageSize=20&page={page}&language=en"
            )
            for attempt in range(MAX_RETRIES):
                try:
                    delay(0.5, 1.5)
                    resp = c.get(url, headers={
                        **hdr(), "Accept": "application/json", "x-api-key": api_key,
                    })
                    if resp.status_code == 429:
                        backoff(attempt)
                        continue
                    if resp.status_code in (401, 403):
                        logger.error(f"Dice auth error {resp.status_code}")
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
                job_url = item.get("detailsPageUrl") or f"https://www.dice.com/job-detail/{item.get('guid', '')}"
                raw_loc = item.get("jobLocation", {}).get("displayName") or dice_loc
                jobs.append({
                    "id":          make_id(job_url),
                    "title":       item.get("title", ""),
                    "company":     item.get("companyName", "Unknown"),
                    "location":    "Remote" if is_remote else raw_loc,
                    "posted_date": parse_date(item.get("postedDate")),
                    "description": trunc(item.get("summary") or ""),
                    "salary":      item.get("salary"),
                    "url":         job_url,
                    "source":      "Dice",
                })
            logger.info(f"Dice page {page}: {len(hits)} jobs")

    logger.info(f"Dice total: {len(jobs)}")
    return jobs


# ══════════════════════════════════════════════════════════════════════════════
#  LINKEDIN — curl_cffi (Chrome TLS) + Oxylabs sticky sessions
# ══════════════════════════════════════════════════════════════════════════════

def _li_curl_session(session_id: str):
    """
    Create curl_cffi session with:
    - Chrome 124 TLS fingerprint (bypasses JA3 fingerprinting)
    - Oxylabs sticky session (same IP across all requests in this session)
    """
    from curl_cffi import requests as curl_requests

    proxy = _proxy_url(sticky_session=session_id)
    proxies = {"http": proxy, "https": proxy} if proxy else None

    session = curl_requests.Session(impersonate="chrome124", proxies=proxies)
    return session


def _li_description(soup: BeautifulSoup) -> Optional[str]:
    for sel in ["div.show-more-less-html__markup", "div.description__text",
                "section.show-more-less-html", "div[class*='description__text']"]:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(separator=" ", strip=True)
            if len(text) > 50:
                return trunc(text)
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            ld = json.loads(script.string or "")
            desc = ld.get("description") if isinstance(ld, dict) else None
            if desc and len(desc) > 50:
                return trunc(BeautifulSoup(desc, "html.parser").get_text(separator=" ", strip=True))
        except (json.JSONDecodeError, TypeError):
            pass
    return None


def _li_salary(soup: BeautifulSoup) -> Optional[str]:
    for sel in ["div.salary-main-rail__data-body", "span.compensation__salary",
                "div[class*='salary']", "span[class*='salary']"]:
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


def scrape_linkedin(role: str, location: str, max_pages=3, max_details=15) -> list[dict]:
    """
    curl_cffi with Chrome TLS fingerprint + Oxylabs sticky session.
    Sticky session = same residential IP for all pages in this scrape.
    If 999, rotate to a new sticky session (new IP) and retry.
    """
    try:
        from curl_cffi import requests as curl_requests
    except ImportError:
        logger.error("curl_cffi not installed — pip install curl_cffi --break-system-packages")
        return _scrape_linkedin_httpx_fallback(role, location, max_pages, max_details)

    jobs = []
    session_id = _new_session_id()
    session = _li_curl_session(session_id)
    retries_left = 3  # max session rotations

    # Phase 1: Warm up — visit LinkedIn homepage to get cookies
    try:
        logger.info("LinkedIn: warming up session")
        resp = session.get("https://www.linkedin.com/", headers=hdr(), timeout=20)
        logger.info(f"LinkedIn homepage: {resp.status_code}")
        delay(2, 4)
    except Exception as e:
        logger.warning(f"LinkedIn warmup failed: {e}")

    # Phase 2: Search pages
    pg = 0
    while pg < max_pages:
        url = (
            f"https://www.linkedin.com/jobs/search/"
            f"?keywords={quote_plus(role)}&location={quote_plus(location)}&start={pg * 25}"
        )

        success = False
        for attempt in range(MAX_RETRIES):
            try:
                delay(3, 6)
                resp = session.get(url, headers=hdr(), timeout=30)

                if resp.status_code in (999, 429):
                    logger.warning(f"LinkedIn page {pg}: {resp.status_code} — rotating session")
                    retries_left -= 1
                    if retries_left <= 0:
                        logger.error("LinkedIn: exhausted session rotations")
                        break

                    # New sticky session = new residential IP
                    session_id = _new_session_id()
                    session = _li_curl_session(session_id)
                    logger.info(f"LinkedIn: new session {session_id}")

                    # Warm up new session
                    try:
                        session.get("https://www.linkedin.com/", headers=hdr(), timeout=15)
                        delay(3, 5)
                    except:
                        pass

                    backoff(attempt, base=10)
                    continue

                if resp.status_code == 200:
                    success = True
                    break

                logger.warning(f"LinkedIn page {pg}: unexpected {resp.status_code}")
                backoff(attempt)

            except Exception as e:
                logger.error(f"LinkedIn page {pg} error: {e}")
                backoff(attempt)

        if not success:
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("div.base-card")
        if not cards:
            logger.info(f"LinkedIn page {pg}: no cards, stopping")
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

        logger.info(f"LinkedIn page {pg}: {len(cards)} cards")
        pg += 1

    # Phase 3: Detail pages for descriptions (use same session)
    if jobs:
        to_fetch = [j for j in jobs if not j.get("description")][:max_details]
        logger.info(f"LinkedIn: fetching {len(to_fetch)}/{len(jobs)} detail pages")

        for job in to_fetch:
            try:
                delay(2, 5)
                resp = session.get(job["url"], headers=hdr(), timeout=20)
                if resp.status_code in (429, 999):
                    continue
                if resp.status_code != 200:
                    continue
                soup = BeautifulSoup(resp.text, "html.parser")
                job["description"] = job.get("description") or _li_description(soup)
                job["salary"]      = job.get("salary") or _li_salary(soup)
            except Exception:
                continue

        filled = sum(1 for j in jobs if j.get("description"))
        logger.info(f"LinkedIn: {filled}/{len(jobs)} have descriptions")

    logger.info(f"LinkedIn total: {len(jobs)}")
    return jobs


def _scrape_linkedin_httpx_fallback(role, location, max_pages, max_details):
    """Fallback if curl_cffi is not installed."""
    logger.info("LinkedIn: using httpx fallback (curl_cffi not available)")
    jobs = []
    session_id = _new_session_id()

    with _httpx_client(sticky=session_id, timeout=20, follow_redirects=True) as c:
        for pg in range(max_pages):
            url = (
                f"https://www.linkedin.com/jobs/search/"
                f"?keywords={quote_plus(role)}&location={quote_plus(location)}&start={pg * 25}"
            )
            for attempt in range(MAX_RETRIES):
                try:
                    delay(3, 6)
                    resp = c.get(url, headers=hdr())
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
            logger.info(f"LinkedIn httpx page {pg}: {len(cards)} cards")

    logger.info(f"LinkedIn httpx total: {len(jobs)}")
    return jobs


# ══════════════════════════════════════════════════════════════════════════════
#  INDEED — httpx RSS → HTML fallback
# ══════════════════════════════════════════════════════════════════════════════

def scrape_indeed(role: str, location: str, max_pages=5) -> list[dict]:
    """HTML scrape with proxy. RSS endpoint is defunct (404)."""
    jobs = []
    session_id = _new_session_id()

    with _httpx_client(sticky=session_id, timeout=20, follow_redirects=True) as c:
        for pg in range(max_pages):
            url = (
                f"https://www.indeed.com/jobs"
                f"?q={quote_plus(role)}&l={quote_plus(location)}&start={pg * 10}"
            )
            resp = None
            for attempt in range(MAX_RETRIES):
                try:
                    delay(2, 5)
                    resp = c.get(url, headers=hdr())
                    if resp.status_code in (403, 429):
                        logger.warning(
                            f"Indeed page {pg}: {resp.status_code}"
                        )
                        backoff(attempt, base=15)
                        continue
                    resp.raise_for_status()
                    break
                except httpx.HTTPError as e:
                    logger.error(f"Indeed page {pg}: {e}")
                    backoff(attempt, base=10)
            else:
                break

            if not resp or resp.status_code != 200:
                break

            batch = _parse_indeed_html(
                BeautifulSoup(resp.text, "html.parser"), location
            )
            if not batch:
                logger.info(f"Indeed page {pg}: no cards, stopping")
                break
            jobs.extend(batch)
            logger.info(f"Indeed page {pg}: {len(batch)} jobs")

    logger.info(f"Indeed total: {len(jobs)}")
    return jobs

def _parse_indeed_rss(xml_text: str, fallback_loc: str) -> list[dict]:
    jobs = []
    try:
        soup = BeautifulSoup(xml_text, "xml")
    except Exception:
        soup = BeautifulSoup(xml_text, "html.parser")

    for item in soup.find_all("item"):
        link_el = item.find("link")
        href = ""
        if link_el:
            href = link_el.get_text(strip=True) or link_el.get("href", "")
        if not href or not href.startswith("http"):
            guid_el = item.find("guid")
            href = guid_el.get_text(strip=True) if guid_el else ""
        if not href.startswith("http"):
            continue

        raw_title = item.find("title")
        raw_title = raw_title.get_text(strip=True) if raw_title else ""
        parts = re.split(r"\s+[-–]\s+", raw_title)
        title   = parts[0].strip() if parts else raw_title
        company = parts[1].strip() if len(parts) > 1 else "Unknown"
        loc     = parts[2].strip() if len(parts) > 2 else fallback_loc

        source_el = item.find("source")
        if source_el and source_el.get_text(strip=True):
            company = source_el.get_text(strip=True)

        pub_el = item.find("pubDate")
        posted = parse_date(pub_el.get_text(strip=True)) if pub_el else None

        desc_el   = item.find("description")
        desc_html = desc_el.get_text(strip=True) if desc_el else ""
        desc_text = BeautifulSoup(desc_html, "html.parser").get_text(separator=" ", strip=True)

        salary = None
        sal_m = re.search(
            r'([$£€₹]\s*[\d,]+(?:\s*[-–]\s*[$£€₹]?\s*[\d,]+)?(?:\s*/\s*(?:yr|year|hr|hour))?)',
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
            "description": trunc(desc_text),
            "salary":      salary,
            "url":         href,
            "source":      "Indeed",
        })
    return jobs


def _parse_indeed_html(soup: BeautifulSoup, fallback_loc: str) -> list[dict]:
    jobs = []
    cards = soup.select("div.job_seen_beacon, div.jobsearch-SerpJobCard, li.css-5lfssm")

    for card in cards:
        title_el = card.select_one("h2.jobTitle a span, h2.jobTitle span[title], h2 a span")
        link_el  = card.select_one("h2.jobTitle a, a.jcs-JobTitle, h2 a")
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

        salary = salary_el.get_text(strip=True) if salary_el else None
        if salary and not re.search(r'[\d$£€₹]', salary):
            salary = None

        jobs.append({
            "id":          make_id(href),
            "title":       title_el.get_text(strip=True),
            "company":     company_el.get_text(strip=True) if company_el else "Unknown",
            "location":    loc_el.get_text(strip=True) if loc_el else fallback_loc,
            "posted_date": parse_date(date_el.get_text(strip=True)) if date_el else None,
            "description": trunc(snippet_el.get_text(separator=" ", strip=True)) if snippet_el else None,
            "salary":      salary,
            "url":         href,
            "source":      "Indeed",
        })
    return jobs


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def run_scrape(role="Software Developer", location="California", on_batch=None) -> list[dict]:
    """Scrape Dice + LinkedIn + Indeed, deduplicate by URL."""
    seen = set()
    results = []

    for label, fn in [("Dice", scrape_dice), ("LinkedIn", scrape_linkedin), ("Indeed", scrape_indeed)]:
        try:
            batch = fn(role, location)
            new = [j for j in batch if j["url"] not in seen]
            for j in new:
                seen.add(j["url"])
            results.extend(new)

            if on_batch and new:
                on_batch(new)

            logger.info(f"{label}: {len(new)} new unique jobs")
        except Exception as e:
            logger.error(f"{label} failed: {e}")

    logger.info(f"Total unique: {len(results)}")
    return results