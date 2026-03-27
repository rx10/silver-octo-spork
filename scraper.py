"""
Job scraper — Dice, LinkedIn, Indeed, ZipRecruiter, RemoteOK, Glassdoor.

Dice:         Playwright intercepts x-api-key once → httpx API calls
LinkedIn:     curl_cffi (Chrome TLS fingerprint) + Bright Data sticky sessions
Indeed:       curl_cffi + proxy + Bright Data geo-targeting → HTML parse
ZipRecruiter: curl_cffi + proxy → HTML parse
RemoteOK:     Public JSON API (no proxy needed)
Glassdoor:    curl_cffi + proxy → HTML parse
"""

import re, os, hashlib, random, string, time, logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from urllib.parse import quote_plus, quote, urlparse
import base64
from playwright.sync_api import sync_playwright, Playwright

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

import os
import requests
from bs4 import BeautifulSoup

BRIGHTDATA_API_KEY = os.environ.get("BRIGHTDATA_API_KEY")
UNLOCKER_ZONE_NAME = os.environ.get("UNLOCKER_ZONE_NAME")



UNLOCKER_ENDPOINT = "https://api.brightdata.com/request"

def _unlocker_get_html(url: str, country: str | None = None) -> str:
    """
    Fetch a URL via Bright Data Unlocker API and return HTML as text.
    """
    if not BRIGHTDATA_API_KEY:
        raise RuntimeError("Missing BRIGHTDATA_API_KEY env var")
    if not UNLOCKER_ZONE_NAME:
        raise RuntimeError("Missing UNLOCKER_ZONE_NAME env var")

    headers = {
        "Authorization": f"Bearer {BRIGHTDATA_API_KEY}",
        "Content-Type": "application/json",
    }

    payload: dict = {
        "zone": UNLOCKER_ZONE_NAME,
        "url": url,
        "format": "raw",  # raw HTML from target site
    }
    if country:
        payload["country"] = country.lower()

    resp = requests.post(UNLOCKER_ENDPOINT, json=payload, headers=headers, timeout=120)

    if not resp.ok:
        logger.error(
            "Unlocker HTTP error %s for %s: %r",
            resp.status_code,
            url,
            resp.text[:500],
        )
        resp.raise_for_status()

    # For format="raw", body is the HTML itself
    html = resp.text
    if not html.strip():
        logger.error("Unlocker returned empty body for %s", url)
        raise RuntimeError(f"Unlocker returned empty body for {url}")

    return html




MAX_RETRIES = 3

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

# ═══════════════════════════════════════════════════════════════════════
#  UTILITIES
# ═══════════════════════════════════════════════════════════════════════

def make_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:32]

def delay(lo=1.0, hi=3.0):
    time.sleep(random.uniform(lo, hi))

def backoff(attempt: int, base=5.0):
    time.sleep(min(base * 2 ** attempt + random.uniform(0, 2), 60))

def trunc(text: str, n=500) -> str:
    if not text or len(text) <= n:
        return text or ""
    return text[:n].rsplit(" ", 1)[0] + " …"

def hdr(accept="text/html") -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": accept,
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }

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
            n = int("".join(filter(str.isdigit, s)) or "1")
            return (today - fn(n)).isoformat()
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return today.isoformat()


# ═══════════════════════════════════════════════════════════════════════
#  INDEED DOMAIN + GEO DETECTION — country-aware routing
# ═══════════════════════════════════════════════════════════════════════

INDEED_COUNTRY_MAP = {
    "in.indeed.com": [
        "hyderabad", "bangalore", "bengaluru", "mumbai", "delhi",
        "new delhi", "chennai", "pune", "kolkata", "noida",
        "gurgaon", "gurugram", "ahmedabad", "jaipur", "lucknow",
        "chandigarh", "indore", "nagpur", "coimbatore", "kochi",
        "cochin", "thiruvananthapuram", "trivandrum", "visakhapatnam",
        "vizag", "bhubaneswar", "mysore", "mysuru", "mangalore",
        "mangaluru", "madurai", "vadodara", "surat", "rajkot",
        "patna", "ranchi", "bhopal", "guwahati", "dehradun",
        "agra", "varanasi", "kanpur", "allahabad", "prayagraj",
        "amritsar", "ludhiana", "jalandhar", "jodhpur", "udaipur",
        "nashik", "aurangabad", "thane", "navi mumbai",
        "faridabad", "ghaziabad", "greater noida", "mohali",
        "panchkula", "hubli", "belgaum", "belagavi", "salem",
        "tiruchirappalli", "trichy", "tiruppur", "erode",
        "vijayawada", "guntur", "warangal", "karimnagar",
        "secunderabad", "madhapur", "hitec city", "hitech city",
        "gachibowli", "kondapur", "kukatpally", "ameerpet",
        "whitefield", "electronic city", "marathahalli",
        "koramangala", "indiranagar", "hsr layout",
        "andheri", "powai", "bandra", "lower parel",
        "telangana", "karnataka", "maharashtra", "tamil nadu",
        "tamilnadu", "kerala", "andhra pradesh", "west bengal",
        "uttar pradesh", "rajasthan", "gujarat", "madhya pradesh",
        "bihar", "odisha", "orissa", "punjab", "haryana",
        "jharkhand", "chhattisgarh", "uttarakhand", "himachal",
        "assam", "goa",
        "india",
    ],
    "uk.indeed.com": [
        "london", "manchester", "birmingham", "leeds", "glasgow",
        "edinburgh", "liverpool", "bristol", "sheffield", "cardiff",
        "belfast", "nottingham", "newcastle", "southampton",
        "cambridge", "oxford", "reading", "brighton", "leicester",
        "coventry", "aberdeen", "dundee", "swansea", "york",
        "bath", "exeter", "norwich", "plymouth", "portsmouth",
        "milton keynes", "luton", "wolverhampton", "derby",
        "stoke", "sunderland", "middlesbrough", "warwick",
        "england", "scotland", "wales", "northern ireland",
        "united kingdom", "uk", "britain", "great britain",
    ],
    "de.indeed.com": [
        "berlin", "munich", "münchen", "hamburg", "frankfurt",
        "cologne", "köln", "düsseldorf", "stuttgart", "dortmund",
        "essen", "leipzig", "bremen", "dresden", "hannover",
        "nuremberg", "nürnberg", "duisburg", "bochum", "wuppertal",
        "bielefeld", "bonn", "karlsruhe", "mannheim", "augsburg",
        "wiesbaden", "aachen", "freiburg", "heidelberg",
        "germany", "deutschland",
    ],
    "ca.indeed.com": [
        "toronto", "vancouver", "montreal", "montréal", "calgary",
        "edmonton", "ottawa", "winnipeg", "quebec", "hamilton",
        "kitchener", "waterloo", "london ontario", "victoria",
        "halifax", "saskatoon", "regina", "st. john", "kelowna",
        "barrie", "oshawa", "guelph", "kingston", "thunder bay",
        "ontario", "british columbia", "alberta", "quebec province",
        "manitoba", "saskatchewan", "nova scotia", "new brunswick",
        "canada",
    ],
    "au.indeed.com": [
        "sydney", "melbourne", "brisbane", "perth", "adelaide",
        "gold coast", "canberra", "hobart", "darwin", "newcastle nsw",
        "wollongong", "geelong", "townsville", "cairns", "toowoomba",
        "ballarat", "bendigo", "launceston", "mackay", "rockhampton",
        "new south wales", "nsw", "victoria au", "queensland",
        "western australia", "south australia", "tasmania",
        "northern territory", "act",
        "australia",
    ],
    "sg.indeed.com": ["singapore"],
    "jp.indeed.com": [
        "tokyo", "osaka", "kyoto", "yokohama", "nagoya", "sapporo",
        "fukuoka", "kobe", "sendai", "hiroshima", "japan",
    ],
    "ae.indeed.com": [
        "dubai", "abu dhabi", "sharjah", "ajman", "ras al khaimah",
        "fujairah", "al ain", "uae", "united arab emirates",
    ],
    "nl.indeed.com": [
        "amsterdam", "rotterdam", "the hague", "den haag", "utrecht",
        "eindhoven", "tilburg", "groningen", "almere", "breda",
        "netherlands", "holland",
    ],
    "fr.indeed.com": [
        "paris", "lyon", "marseille", "toulouse", "nice",
        "nantes", "strasbourg", "montpellier", "bordeaux", "lille",
        "rennes", "reims", "toulon", "grenoble", "dijon",
        "france",
    ],
    "ie.indeed.com": [
        "dublin", "cork", "galway", "limerick", "waterford",
        "ireland",
    ],
}

# Country code + optional city for Bright Data geo-targeting per Indeed domain
INDEED_GEO_MAP = {
    "in.indeed.com": ("IN", {
        "hyderabad": "hyderabad", "secunderabad": "hyderabad",
        "madhapur": "hyderabad", "hitec city": "hyderabad",
        "hitech city": "hyderabad", "gachibowli": "hyderabad",
        "kondapur": "hyderabad", "kukatpally": "hyderabad",
        "ameerpet": "hyderabad",
        "bangalore": "bangalore", "bengaluru": "bangalore",
        "whitefield": "bangalore", "electronic city": "bangalore",
        "marathahalli": "bangalore", "koramangala": "bangalore",
        "indiranagar": "bangalore", "hsr layout": "bangalore",
        "mumbai": "mumbai", "navi mumbai": "mumbai",
        "andheri": "mumbai", "powai": "mumbai",
        "bandra": "mumbai", "lower parel": "mumbai",
        "thane": "mumbai",
        "delhi": "delhi", "new delhi": "delhi",
        "noida": "delhi", "greater noida": "delhi",
        "gurgaon": "delhi", "gurugram": "delhi",
        "faridabad": "delhi", "ghaziabad": "delhi",
        "chennai": "chennai",
        "pune": "pune",
        "kolkata": "kolkata",
        "ahmedabad": "ahmedabad",
        "jaipur": "jaipur",
        "lucknow": "lucknow",
        "chandigarh": "chandigarh", "mohali": "chandigarh",
        "panchkula": "chandigarh",
        "indore": "indore",
        "nagpur": "nagpur",
        "coimbatore": "coimbatore",
        "kochi": "kochi", "cochin": "kochi",
        "thiruvananthapuram": "thiruvananthapuram",
        "trivandrum": "thiruvananthapuram",
        "visakhapatnam": "visakhapatnam", "vizag": "visakhapatnam",
        "bhubaneswar": "bhubaneswar",
        "mysore": "mysore", "mysuru": "mysore",
        "vadodara": "vadodara",
        "surat": "surat",
        "patna": "patna",
        "ranchi": "ranchi",
        "bhopal": "bhopal",
        "guwahati": "guwahati",
        "dehradun": "dehradun",
    }),
    "uk.indeed.com": ("GB", {
        "london": "london", "manchester": "manchester",
        "birmingham": "birmingham", "leeds": "leeds",
        "glasgow": "glasgow", "edinburgh": "edinburgh",
        "liverpool": "liverpool", "bristol": "bristol",
        "sheffield": "sheffield", "cardiff": "cardiff",
        "belfast": "belfast", "nottingham": "nottingham",
        "newcastle": "newcastle", "southampton": "southampton",
        "cambridge": "cambridge", "oxford": "oxford",
        "brighton": "brighton", "leicester": "leicester",
    }),
    "de.indeed.com": ("DE", {
        "berlin": "berlin", "munich": "munich", "münchen": "munich",
        "hamburg": "hamburg", "frankfurt": "frankfurt",
        "cologne": "cologne", "köln": "cologne",
        "düsseldorf": "dusseldorf", "stuttgart": "stuttgart",
        "leipzig": "leipzig", "dresden": "dresden",
        "hannover": "hannover", "nuremberg": "nuremberg",
    }),
    "ca.indeed.com": ("CA", {
        "toronto": "toronto", "vancouver": "vancouver",
        "montreal": "montreal", "montréal": "montreal",
        "calgary": "calgary", "edmonton": "edmonton",
        "ottawa": "ottawa", "winnipeg": "winnipeg",
        "quebec": "quebec", "hamilton": "hamilton",
        "halifax": "halifax",
    }),
    "au.indeed.com": ("AU", {
        "sydney": "sydney", "melbourne": "melbourne",
        "brisbane": "brisbane", "perth": "perth",
        "adelaide": "adelaide", "gold coast": "gold_coast",
        "canberra": "canberra", "hobart": "hobart",
        "darwin": "darwin",
    }),
    "sg.indeed.com": ("SG", {
        "singapore": "singapore",
    }),
    "jp.indeed.com": ("JP", {
        "tokyo": "tokyo", "osaka": "osaka",
        "kyoto": "kyoto", "yokohama": "yokohama",
        "nagoya": "nagoya", "fukuoka": "fukuoka",
    }),
    "ae.indeed.com": ("AE", {
        "dubai": "dubai", "abu dhabi": "abu_dhabi",
        "sharjah": "sharjah",
    }),
    "nl.indeed.com": ("NL", {
        "amsterdam": "amsterdam", "rotterdam": "rotterdam",
        "the hague": "the_hague", "den haag": "the_hague",
        "utrecht": "utrecht", "eindhoven": "eindhoven",
    }),
    "fr.indeed.com": ("FR", {
        "paris": "paris", "lyon": "lyon",
        "marseille": "marseille", "toulouse": "toulouse",
        "nice": "nice", "bordeaux": "bordeaux",
        "lille": "lille", "strasbourg": "strasbourg",
    }),
    "ie.indeed.com": ("IE", {
        "dublin": "dublin", "cork": "cork",
        "galway": "galway", "limerick": "limerick",
    }),
}


def _get_indeed_domain(location: str) -> str:
    loc = location.lower().strip()
    for domain, keywords in INDEED_COUNTRY_MAP.items():
        if any(kw in loc for kw in keywords):
            return domain
    return "www.indeed.com"


def _get_indeed_geo(location: str, domain: str) -> tuple[Optional[str], Optional[str]]:
    """Return (country_code, city) for Bright Data geo-targeting."""
    geo = INDEED_GEO_MAP.get(domain)
    if not geo:
        return None, None
    country_code, city_map = geo
    loc = location.lower().strip()
    city = None
    for keyword, city_name in city_map.items():
        if keyword in loc:
            city = city_name
            break
    return country_code, city


# ═══════════════════════════════════════════════════════════════════════
#  PROXY — Bright Data sticky sessions + geo-targeting
# ═══════════════════════════════════════════════════════════════════════

class ProxyConfig:
    """Lazy-loaded singleton for proxy settings."""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._loaded = False
        return cls._instance

    def _load(self):
        if self._loaded:
            return
        self._loaded = True

        self.customer_id = None
        self.password = None
        self.zone = None
        self.host = "brd.superproxy.io"
        self.port = "33335"
        self.raw_proxy_url = None

        full = os.getenv("PROXY_URL", "").strip()
        if full:
            self.raw_proxy_url = full
            p = urlparse(full)
            self.password = p.password
            self.host = p.hostname or self.host
            self.port = str(p.port or 33335)

            username = p.username or ""
            if username.startswith("brd-customer-"):
                m_customer = re.search(r"brd-customer-([^-]+)", username)
                m_zone = re.search(r"-zone-([^-]+)", username)
                if m_customer:
                    self.customer_id = m_customer.group(1)
                if m_zone:
                    self.zone = m_zone.group(1)
        else:
            self.customer_id = os.getenv("BRD_CUSTOMER_ID", "").strip() or None
            self.password = os.getenv("BRD_PASSWORD", "").strip() or None
            self.zone = os.getenv("BRD_ZONE", "").strip() or None

        if self.customer_id and self.zone:
            logger.info(f"Proxy: brd-customer-{self.customer_id}-zone-{self.zone}@{self.host}:{self.port}")
        elif self.raw_proxy_url:
            logger.info(f"Proxy: custom PROXY_URL @ {self.host}:{self.port}")
        else:
            logger.warning("No Bright Data proxy configured — may get blocked on some sites")

    @staticmethod
    def _normalize_city(city: str) -> str:
        return city.strip().lower().replace(" ", "").replace("_", "")

    def url(
        self,
        sticky_session: Optional[str] = None,
        country: Optional[str] = None,
        city: Optional[str] = None,
    ) -> Optional[str]:
        """
        Build Bright Data proxy URL with optional geo-targeting.
        Username format:
        brd-customer-{CUSTOMER_ID}-zone-{ZONE}-country-{cc}-city-{city}-session-{id}
        """
        self._load()

        if self.customer_id and self.password and self.zone:
            user = f"brd-customer-{self.customer_id}-zone-{self.zone}"
            if country:
                user += f"-country-{country.lower()}"
            if city:
                user += f"-city-{self._normalize_city(city)}"
            if sticky_session:
                user += f"-session-{sticky_session}"
            return f"http://{quote(user, safe='')}:{quote(self.password, safe='')}@{self.host}:{self.port}"

        if self.raw_proxy_url:
            return self.raw_proxy_url

        return None

_proxy = ProxyConfig()


def _new_sid() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=10))


# ═══════════════════════════════════════════════════════════════════════
#  SESSION FACTORY — curl_cffi preferred, httpx fallback
# ═══════════════════════════════════════════════════════════════════════

_has_curl_cffi: Optional[bool] = None

def _check_curl():
    global _has_curl_cffi
    if _has_curl_cffi is None:
        try:
            import curl_cffi  # noqa: F401
            _has_curl_cffi = True
        except ImportError:
            _has_curl_cffi = False
            logger.warning("curl_cffi not installed — using httpx (less stealthy)")
    return _has_curl_cffi


def _make_session(
    sid: Optional[str] = None,
    country: Optional[str] = None,
    city: Optional[str] = None,
):
    """Return (session, is_curl). Session has .get()/.close(). Supports Bright Data geo-targeting."""
    proxy = _proxy.url(sticky_session=sid, country=country, city=city)
    if _check_curl():
        from curl_cffi import requests as curl_requests
        proxies = {"http": proxy, "https": proxy} if proxy else None
        return curl_requests.Session(impersonate="chrome124", proxies=proxies), True
    client = httpx.Client(
        proxy=proxy, timeout=20, follow_redirects=True,
    ) if proxy else httpx.Client(timeout=20, follow_redirects=True)
    return client, False


# ═══════════════════════════════════════════════════════════════════════
#  GENERIC PAGINATED SCRAPER
# ═══════════════════════════════════════════════════════════════════════

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

def _paginated_scrape_unlocker(
    warmup_url: str,
    url_fn,
    parse_fn,
    location: str,
    max_pages: int,
    country: str | None = None,
) -> list[dict]:
    results: list[dict] = []

    # Optional warmup – not strictly required, but you can keep it
    try:
        _ = _unlocker_get_html(warmup_url, country=country)
    except Exception as e:
        logger.warning(f"Warmup failed for {warmup_url}: {e}")

    for pg in range(max_pages):
        url = url_fn(pg)
        logger.info(f"Page {pg}: {url}")

        try:
            html = _unlocker_get_html(url, country=country)
        except Exception as e:
            logger.warning(f"Page {pg}: Unlocker request failed, stopping pagination: {e}")
            break

        soup = BeautifulSoup(html, "html.parser")
        page_jobs = parse_fn(soup, fallback_location=location or "")
        logger.info(f"Page {pg}: parsed {len(page_jobs)} jobs")

        if not page_jobs:
            break

        results.extend(page_jobs)

    return results


from urllib.parse import quote_plus

from urllib.parse import quote_plus

from urllib.parse import quote_plus
from typing import List, Dict



def scrape_indeed(query: str, location: str = "", max_pages: int = 3) -> List[Dict]:
    """
    Indeed scraper with country-aware domain routing + Bright Data Unlocker API.
    """
    domain = _get_indeed_domain(location or "")
    country, city = _get_indeed_geo(location or "", domain)

    q = quote_plus(query)
    l = quote_plus(location or "")

    warmup_url = f"https://{domain}/"
    base_url = f"https://{domain}/jobs?q={q}&l={l}"

    def url_fn(pg: int) -> str:
        start = pg * 10
        return f"{base_url}&start={start}"

    def parse_fn(soup: BeautifulSoup, fallback_location: str) -> List[Dict]:
        jobs = []
        seen = set()

        cards = soup.select(
            "div.job_seen_beacon, "
            "div.slider_container div[data-jk], "
            "table.jobCard_mainContent, "
            "a.tapItem"
        )

        for card in cards:
            title_el = (
                card.select_one("h2.jobTitle span[title]") or
                card.select_one("h2.jobTitle") or
                card.select_one("[data-testid='job-title']") or
                card.select_one("a[data-jk]")
            )
            title = title_el.get_text(" ", strip=True) if title_el else None
            if title and title.lower().startswith("new"):
                title = title[3:].strip()

            company_el = (
                card.select_one("[data-testid='company-name']") or
                card.select_one("span.companyName") or
                card.select_one("div.heading6.company_location span")
            )
            company = company_el.get_text(" ", strip=True) if company_el else None

            loc_el = (
                card.select_one("[data-testid='job-location']") or
                card.select_one("div.companyLocation") or
                card.select_one("div.heading6.company_location div")
            )
            job_location = loc_el.get_text(" ", strip=True) if loc_el else (fallback_location or "")

            link_el = card.select_one("a[href*='/rc/clk'], a[href*='/viewjob'], a[data-jk]")
            url = None
            if link_el:
                href = link_el.get("href")
                if href:
                    url = href if href.startswith("http") else f"https://{domain}{href}"

            if not url:
                jk = card.get("data-jk")
                if jk:
                    url = f"https://{domain}/viewjob?jk={jk}"

            if not title or not company or not url:
                continue

            if url in seen:
                continue
            seen.add(url)

            salary_el = (
                card.select_one(".salary-snippet") or
                card.select_one("[data-testid='attribute_snippet_testid']") or
                card.select_one(".estimated-salary")
            )
            salary = salary_el.get_text(" ", strip=True) if salary_el else None

            snippet_el = (
                card.select_one("[data-testid='job-snippet']") or
                card.select_one(".job-snippet") or
                card.select_one(".slider-snippet")
            )
            snippet = snippet_el.get_text(" ", strip=True) if snippet_el else ""

            date_el = (
                card.select_one("span.date") or
                card.select_one(".date") or
                card.select_one("[data-testid='myJobsStateDate']")
            )
            posted_at = parse_date(date_el.get_text(" ", strip=True)) if date_el else None

            jobs.append({
                "id": make_id(url),
                "title": title,
                "company": company,
                "location": job_location,
                "url": url,
                "description": trunc(snippet, 400),
                "salary": salary,
                "posted_at": posted_at,
                "source": "indeed",
            })

        return jobs

    try:
        jobs = _paginated_scrape_unlocker(
            warmup_url=warmup_url,
            url_fn=url_fn,
            parse_fn=parse_fn,
            location=location,
            max_pages=max_pages,
            country=country,  # optional geo hint to Unlocker
        )
        logger.info(f"Total jobs scraped: {len(jobs)}")
        return jobs
    except Exception:
        logger.exception("scrape_indeed failed")
        return []
    