"""
Job scraper — Dice, LinkedIn, Indeed, ZipRecruiter, RemoteOK, Glassdoor.

Dice:         Playwright intercepts x-api-key once → httpx API calls
LinkedIn:     curl_cffi (Chrome TLS fingerprint) + Bright Data sticky sessions
Indeed:       Bright Data Unlocker API + geo-targeting → HTML parse
ZipRecruiter: curl_cffi + proxy → HTML parse
RemoteOK:     Public JSON API (no proxy needed)
Glassdoor:    curl_cffi + proxy → HTML parse
"""

import re, os, hashlib, random, string, time, logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from urllib.parse import quote_plus, quote, urlparse

import httpx
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BRIGHTDATA_API_KEY = os.environ.get("BRIGHTDATA_API_KEY")
UNLOCKER_ZONE_NAME = os.environ.get("UNLOCKER_ZONE_NAME")
UNLOCKER_ENDPOINT = "https://api.brightdata.com/request"

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
#  BRIGHT DATA UNLOCKER — with proper retry + backoff
# ═══════════════════════════════════════════════════════════════════════

def _unlocker_get_html(url: str, country: str | None = None, retries: int = 3) -> str:
    """
    Fetch a URL via Bright Data Unlocker API and return HTML as text.
    Retries with increasing backoff on empty body or CAPTCHA.
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
        "format": "raw",
    }
    if country:
        payload["country"] = country.lower()

    last_err = None
    last_body = "(no response yet)"

    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(
                UNLOCKER_ENDPOINT,
                json=payload,
                headers=headers,
                timeout=120,
            )

            if not resp.ok:
                last_body = resp.text[:500]
                logger.error(
                    "Unlocker HTTP %s for %s (attempt %d/%d): %r",
                    resp.status_code, url, attempt, retries, last_body,
                )
                resp.raise_for_status()

            html = resp.text
            last_body = html[:500] if html else "(empty)"

            if not html.strip():
                raise RuntimeError("Unlocker returned empty body")

            # Check for CAPTCHA/block pages
            if len(html) < 2000 and ("captcha" in html.lower() or "unusual traffic" in html.lower()):
                logger.warning(
                    "Unlocker returned CAPTCHA/block for %s (attempt %d/%d)",
                    url, attempt, retries,
                )
                raise RuntimeError("Unlocker returned CAPTCHA page")

            return html

        except Exception as e:
            last_err = e
            logger.warning(
                "Unlocker failed for %s (attempt %d/%d): %s",
                url, attempt, retries, e,
            )
            if attempt < retries:
                wait = 3 * (2 ** (attempt - 1)) + random.uniform(0, 2)
                logger.info(f"Retrying in {wait:.1f}s...")
                time.sleep(wait)

    logger.error(
        "Unlocker exhausted %d attempts for %s, last body prefix=%r",
        retries, url, last_body,
    )
    raise RuntimeError(f"Unlocker failed after {retries} attempts for {url}") from last_err


# ═══════════════════════════════════════════════════════════════════════
#  INDEED DOMAIN + GEO DETECTION
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
        "chennai": "chennai", "pune": "pune",
        "kolkata": "kolkata", "ahmedabad": "ahmedabad",
        "jaipur": "jaipur", "lucknow": "lucknow",
        "chandigarh": "chandigarh", "mohali": "chandigarh",
        "panchkula": "chandigarh",
        "indore": "indore", "nagpur": "nagpur",
        "coimbatore": "coimbatore",
        "kochi": "kochi", "cochin": "kochi",
        "thiruvananthapuram": "thiruvananthapuram",
        "trivandrum": "thiruvananthapuram",
        "visakhapatnam": "visakhapatnam", "vizag": "visakhapatnam",
        "bhubaneswar": "bhubaneswar",
        "mysore": "mysore", "mysuru": "mysore",
        "vadodara": "vadodara", "surat": "surat",
        "patna": "patna", "ranchi": "ranchi",
        "bhopal": "bhopal", "guwahati": "guwahati",
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
    "sg.indeed.com": ("SG", {"singapore": "singapore"}),
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
#  GENERIC PAGINATED SCRAPER (for non-Unlocker sources)
# ═══════════════════════════════════════════════════════════════════════

def _paginated_scrape(
    *,
    warmup_url: str,
    url_fn,
    parse_fn,
    location: str,
    max_pages: int,
    block_codes: tuple = (403, 429),
    delay_range: tuple = (2, 5),
    backoff_base: float = 10.0,
    max_rotations: int = 3,
    country: Optional[str] = None,
    city: Optional[str] = None,
) -> list[dict]:
    sid = _new_sid()
    session, _ = _make_session(sid, country=country, city=city)
    jobs: list[dict] = []
    rotations_left = max_rotations

    try:
        session.get(warmup_url, headers=hdr(), timeout=15)
        delay(2, 4)
    except Exception as e:
        logger.warning(f"Warmup failed for {warmup_url}: {e}")

    for pg in range(max_pages):
        url = url_fn(pg)
        success = False
        for attempt in range(MAX_RETRIES):
            try:
                delay(*delay_range)
                resp = session.get(url, headers=hdr(), timeout=30)
                if resp.status_code in block_codes:
                    rotations_left -= 1
                    if rotations_left <= 0:
                        logger.error(f"Exhausted session rotations at page {pg}")
                        return jobs
                    sid = _new_sid()
                    try:
                        session.close()
                    except Exception:
                        pass
                    session, _ = _make_session(sid, country=country, city=city)
                    try:
                        session.get(warmup_url, headers=hdr(), timeout=15)
                        delay(2, 4)
                    except Exception:
                        pass
                    backoff(attempt, base=backoff_base)
                    continue
                if resp.status_code == 200:
                    success = True
                    break
                backoff(attempt)
            except Exception as e:
                logger.error(f"Page {pg}: {e}")
                backoff(attempt)
        if not success:
            break

        batch = parse_fn(BeautifulSoup(resp.text, "html.parser"), location)
        if not batch:
            logger.info(f"Page {pg}: no results, stopping")
            break
        jobs.extend(batch)
        logger.info(f"Page {pg}: {len(batch)} jobs")

    try:
        session.close()
    except Exception:
        pass
    return jobs


# ═══════════════════════════════════════════════════════════════════════
#  DICE — Playwright key interception + httpx API calls
# ═══════════════════════════════════════════════════════════════════════

_cached_dice_key: Optional[str] = None

def _get_dice_key() -> str:
    global _cached_dice_key
    if _cached_dice_key:
        return _cached_dice_key

    try:
        from playwright.sync_api import sync_playwright
        key = None
        def on_req(req):
            nonlocal key
            if key:
                return
            if "dhigroupinc.com" in req.url.lower():
                k = req.headers.get("x-api-key")
                if k and len(k) >= 30:
                    key = k
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(user_agent=random.choice(USER_AGENTS))
            page.on("request", on_req)
            page.goto(
                "https://www.dice.com/jobs?q=developer&location=United+States",
                wait_until="domcontentloaded", timeout=45000,
            )
            page.wait_for_timeout(4000)
            browser.close()
        if key:
            logger.info(f"Dice API key intercepted: {key[:8]}…")
            _cached_dice_key = key
            return key
    except Exception as e:
        logger.warning(f"Dice key interception failed: {e}")

    key = os.getenv("DICE_API_KEY", "").strip()
    if key:
        logger.info("Dice: using DICE_API_KEY env var")
        _cached_dice_key = key
        return key

    raise RuntimeError(
        "No Dice API key available. Set DICE_API_KEY or install playwright."
    )


def scrape_dice(role: str, location: str, max_pages=5) -> list[dict]:
    api_key = _get_dice_key()
    jobs = []
    proxy = _proxy.url()
    kwargs = {"timeout": 15, "follow_redirects": True}
    if proxy:
        kwargs["proxy"] = proxy

    with httpx.Client(**kwargs) as c:
        for page in range(1, max_pages + 1):
            url = (
                f"https://job-search-api.svc.dhigroupinc.com/v1/dice/jobs/search"
                f"?q={quote_plus(role)}&location={quote_plus(location)}"
                f"&pageSize=20&page={page}&language=en&countryCode=US"
            )
            for attempt in range(MAX_RETRIES):
                try:
                    delay(0.5, 1.5)
                    resp = c.get(url, headers={**hdr("application/json"), "x-api-key": api_key})
                    if resp.status_code == 429:
                        backoff(attempt)
                        continue
                    if resp.status_code in (401, 403):
                        logger.error(f"Dice auth error {resp.status_code}")
                        return jobs
                    resp.raise_for_status()
                    break
                except httpx.HTTPError as e:
                    logger.error(f"Dice page {page}: {e}")
                    backoff(attempt)
            else:
                break

            hits = resp.json().get("data", [])
            if not hits:
                break
            for item in hits:
                job_url = item.get("detailsPageUrl") or \
                    f"https://www.dice.com/job-detail/{item.get('guid', '')}"
                jobs.append({
                    "id":          make_id(job_url),
                    "title":       item.get("title", ""),
                    "company":     item.get("companyName", "Unknown"),
                    "location":    item.get("jobLocation", {}).get("displayName") or location,
                    "posted_date": parse_date(item.get("postedDate")),
                    "description": trunc(item.get("summary") or ""),
                    "salary":      item.get("salary"),
                    "url":         job_url,
                    "source":      "Dice",
                })
            logger.info(f"Dice page {page}: {len(hits)} jobs")
    logger.info(f"Dice total: {len(jobs)}")
    return jobs


# ═══════════════════════════════════════════════════════════════════════
#  LINKEDIN
# ═══════════════════════════════════════════════════════════════════════

def _parse_linkedin(soup: BeautifulSoup, location: str) -> list[dict]:
    jobs = []
    for card in soup.select("div.base-card"):
        title_el = card.select_one("h3.base-search-card__title, h3")
        link_el  = card.select_one("a.base-card__full-link, a")
        if not title_el or not link_el:
            continue
        href = link_el.get("href", "").split("?")[0]
        if not href.startswith("http"):
            continue
        company_el = card.select_one("h4.base-search-card__subtitle, h4")
        loc_el     = card.select_one("span.job-search-card__location")
        date_el    = card.select_one("time")
        salary_el  = card.select_one("span.job-search-card__salary-info")
        jobs.append({
            "id":          make_id(href),
            "title":       title_el.get_text(strip=True),
            "company":     company_el.get_text(strip=True) if company_el else "Unknown",
            "location":    loc_el.get_text(strip=True) if loc_el else location,
            "posted_date": (date_el.get("datetime") or parse_date(date_el.get_text()))
                           if date_el else None,
            "description": None,
            "salary":      salary_el.get_text(strip=True) if salary_el else None,
            "url":         href,
            "source":      "LinkedIn",
        })
    return jobs


def scrape_linkedin(role: str, location: str, max_pages=3, max_details=15) -> list[dict]:
    jobs = _paginated_scrape(
        warmup_url="https://www.linkedin.com/",
        url_fn=lambda pg: (
            f"https://www.linkedin.com/jobs/search/"
            f"?keywords={quote_plus(role)}&location={quote_plus(location)}&start={pg * 25}"
        ),
        parse_fn=_parse_linkedin,
        location=location,
        max_pages=max_pages,
        block_codes=(429, 999),
        delay_range=(3, 6),
    )

    sid = _new_sid()
    session, _ = _make_session(sid)
    for job in [j for j in jobs if not j.get("description")][:max_details]:
        try:
            delay(2, 5)
            resp = session.get(job["url"], headers=hdr(), timeout=20)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for sel in ("div.show-more-less-html__markup", "div.description__text"):
                el = soup.select_one(sel)
                if el:
                    job["description"] = trunc(el.get_text(separator=" ", strip=True))
                    break
        except Exception:
            continue
    try:
        session.close()
    except Exception:
        pass

    logger.info(f"LinkedIn total: {len(jobs)}")
    return jobs


# ═══════════════════════════════════════════════════════════════════════
#  INDEED — Bright Data Unlocker + adaptive pagination + dedup
# ═══════════════════════════════════════════════════════════════════════

def _parse_indeed_page(
    soup: BeautifulSoup,
    fallback_location: str,
    domain: str = "in.indeed.com",
) -> List[Dict]:
    """Parse Indeed search results page. Returns list of job dicts."""
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

        # Extract URL
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
            "posted_date": posted_at,
            "source": "Indeed",
        })

    return jobs


def scrape_indeed(query: str, location: str = "", max_pages: int = 5) -> List[Dict]:
    domain = _get_indeed_domain(location or "")
    country, city = _get_indeed_geo(location or "", domain)

    # Multiple query variants to get more results without pagination
    queries = [query]
    if "developer" in query.lower():
        queries.extend([f"{query} junior", f"{query} senior", f"{query} full stack"])
    elif "sde" in query.lower():
        queries.extend(["software developer", "software engineer", "SDE 1", "SDE 2"])

    all_jobs = []
    seen_urls = set()

    for q in queries:
        url = f"https://{domain}/jobs?q={quote_plus(q)}&l={quote_plus(location)}&start=0"
        logger.info(f"Indeed query: {url}")

        if all_jobs:  # delay between queries
            time.sleep(random.uniform(4, 8))

        try:
            html = _unlocker_get_html(url, country=country)
        except Exception as e:
            logger.warning(f"Indeed query '{q}' failed: {e}")
            continue

        soup = BeautifulSoup(html, "html.parser")
        page_jobs = _parse_indeed_page(soup, fallback_location=location, domain=domain)

        new = [j for j in page_jobs if j["url"] not in seen_urls]
        seen_urls.update(j["url"] for j in new)
        all_jobs.extend(new)
        logger.info(f"Indeed query '{q}': {len(page_jobs)} parsed, {len(new)} new")

    logger.info(f"Indeed total: {len(all_jobs)} unique jobs")
    return all_jobs

# ═══════════════════════════════════════════════════════════════════════
#  ZIPRECRUITER
# ═══════════════════════════════════════════════════════════════════════

def _parse_ziprecruiter(soup: BeautifulSoup, fallback_loc: str) -> list[dict]:
    jobs = []
    cards = soup.select(
        "article.job_result, div.job_result_two_pane, li[class*='job-listing'],"
        "div[data-testid='job-card']"
    )
    for card in cards:
        title_el = card.select_one(
            "h2.title a, a.job_link, h2[class*='title'], a[data-testid='job-title']"
        )
        if not title_el:
            continue
        href = title_el.get("href", "")
        if href.startswith("/"):
            href = f"https://www.ziprecruiter.com{href}"
        if not href.startswith("http"):
            continue
        company_el = card.select_one(
            "a.t_org_link, span[class*='company'], p[class*='company'],"
            "a[data-testid='company-name']"
        )
        loc_el = card.select_one(
            "p.location, span[class*='location'], div[class*='location'],"
            "span[data-testid='location']"
        )
        salary_el = card.select_one("span[class*='salary'], div[class*='salary'], p[class*='salary']")
        date_el   = card.select_one("div[class*='date'], span[class*='date'], time")
        snippet_el = card.select_one("p[class*='snippet'], div[class*='snippet'], ul[class*='bullets']")
        salary = salary_el.get_text(strip=True) if salary_el else None
        if salary and not re.search(r"[\d$£€₹]", salary):
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
            "source":      "ZipRecruiter",
        })
    return jobs


def scrape_ziprecruiter(role: str, location: str, max_pages=5) -> list[dict]:
    jobs = _paginated_scrape(
        warmup_url="https://www.ziprecruiter.com/",
        url_fn=lambda pg: (
            f"https://www.ziprecruiter.com/jobs-search"
            f"?search={quote_plus(role)}&location={quote_plus(location)}&page={pg + 1}"
        ),
        parse_fn=_parse_ziprecruiter,
        location=location,
        max_pages=max_pages,
    )
    logger.info(f"ZipRecruiter total: {len(jobs)}")
    return jobs


# ═══════════════════════════════════════════════════════════════════════
#  REMOTEOK — Public JSON API (no proxy needed)
# ═══════════════════════════════════════════════════════════════════════

def scrape_remoteok(role: str, location: str = "Remote", max_results=60) -> list[dict]:
    try:
        with httpx.Client(timeout=15, follow_redirects=True) as c:
            delay(1, 2)
            resp = c.get("https://remoteok.com/api", headers={
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/json",
            })
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.error(f"RemoteOK API error: {e}")
        return []

    jobs = []
    role_lower = role.lower()
    for item in data:
        if not isinstance(item, dict) or not item.get("position"):
            continue
        title = item.get("position", "")
        tags = " ".join(item.get("tags") or []).lower()
        if role_lower not in title.lower() and role_lower not in tags:
            continue

        job_url = item.get("url") or f"https://remoteok.com/remote-jobs/{item.get('id', '')}"

        sal_min, sal_max = item.get("salary_min"), item.get("salary_max")
        salary = None
        if sal_min and sal_max:
            salary = f"USD {int(sal_min):,} – {int(sal_max):,}"
        elif sal_min:
            salary = f"USD {int(sal_min):,}+"

        posted = None
        if epoch := item.get("epoch"):
            try:
                posted = datetime.utcfromtimestamp(int(epoch)).date().isoformat()
            except (ValueError, OSError):
                pass

        jobs.append({
            "id":          make_id(job_url),
            "title":       title,
            "company":     item.get("company", "Unknown"),
            "location":    "Remote",
            "posted_date": posted,
            "description": trunc(
                BeautifulSoup(item.get("description") or "", "html.parser")
                .get_text(separator=" ", strip=True)
            ),
            "salary":      salary,
            "url":         job_url,
            "source":      "RemoteOK",
        })
        if len(jobs) >= max_results:
            break

    logger.info(f"RemoteOK total: {len(jobs)}")
    return jobs


# ═══════════════════════════════════════════════════════════════════════
#  GLASSDOOR
# ═══════════════════════════════════════════════════════════════════════

def _parse_glassdoor(soup: BeautifulSoup, fallback_loc: str) -> list[dict]:
    import json
    jobs = []

    for script in soup.select('script[type="application/ld+json"]'):
        try:
            ld = json.loads(script.string or "")
            for item in (ld if isinstance(ld, list) else [ld]):
                if item.get("@type") != "JobPosting":
                    continue
                href = item.get("url", "")
                if not href.startswith("http"):
                    continue
                salary = None
                sal = item.get("baseSalary")
                if isinstance(sal, dict):
                    v = sal.get("value", {})
                    if isinstance(v, dict):
                        lo, hi = v.get("minValue"), v.get("maxValue")
                        cur = sal.get("currency", "USD")
                        if lo and hi:
                            salary = f"{cur} {lo:,.0f} – {hi:,.0f}"
                loc = item.get("jobLocation", {})
                if isinstance(loc, list):
                    loc = loc[0] if loc else {}
                addr = loc.get("address", {})
                loc_str = addr.get("addressLocality") or addr.get("addressRegion") or fallback_loc
                jobs.append({
                    "id":          make_id(href),
                    "title":       item.get("title", ""),
                    "company":     item.get("hiringOrganization", {}).get("name", "Unknown"),
                    "location":    loc_str,
                    "posted_date": parse_date(item.get("datePosted")),
                    "description": trunc(
                        BeautifulSoup(item.get("description") or "", "html.parser")
                        .get_text(separator=" ", strip=True)
                    ),
                    "salary":      salary,
                    "url":         href,
                    "source":      "Glassdoor",
                })
        except (json.JSONDecodeError, TypeError):
            continue
    if jobs:
        return jobs

    cards = soup.select(
        "li.react-job-listing, div[data-test='jobListing'],"
        "li[class*='JobsList_jobListItem'], article[class*='JobCard']"
    )
    for card in cards:
        title_el = card.select_one(
            "a[data-test='job-title'], div[class*='JobCard_jobTitle'], a[class*='jobTitle']"
        )
        if not title_el:
            continue
        href = title_el.get("href", "")
        if href.startswith("/"):
            href = f"https://www.glassdoor.com{href}"
        if not href.startswith("http"):
            continue
        company_el = card.select_one(
            "span[class*='EmployerProfile_compactEmployerName'],"
            "div[data-test='employer-name'], span[class*='jobEmpolyerName']"
        )
        loc_el    = card.select_one("div[data-test='emp-location'], span[class*='jobLocation'], div[class*='JobCard_location']")
        salary_el = card.select_one("div[data-test='detailSalary'], span[class*='salary'], div[class*='JobCard_salaryEstimate']")
        date_el   = card.select_one("div[data-test='job-age'], span[class*='listing-age']")
        jobs.append({
            "id":          make_id(href),
            "title":       title_el.get_text(strip=True),
            "company":     company_el.get_text(strip=True) if company_el else "Unknown",
            "location":    loc_el.get_text(strip=True) if loc_el else fallback_loc,
            "posted_date": parse_date(date_el.get_text(strip=True)) if date_el else None,
            "description": None,
            "salary":      salary_el.get_text(strip=True) if salary_el else None,
            "url":         href,
            "source":      "Glassdoor",
        })
    return jobs


def scrape_glassdoor(role: str, location: str, max_pages=5) -> list[dict]:
    jobs = _paginated_scrape(
        warmup_url="https://www.glassdoor.com/",
        url_fn=lambda pg: (
            f"https://www.glassdoor.com/Job/jobs.htm"
            f"?sc.keyword={quote_plus(role)}&locT=C&locId=1147401"
            f"&jobType=all&fromAge=-1&minSalary=0&includeNoSalaryJobs=true"
            f"&radius=100&p={pg + 1}"
        ),
        parse_fn=_parse_glassdoor,
        location=location,
        max_pages=max_pages,
        block_codes=(403, 429, 503),
        delay_range=(3, 6),
    )
    logger.info(f"Glassdoor total: {len(jobs)}")
    return jobs


# ═══════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════

SCRAPERS = {
    "dice":          scrape_dice,
    "linkedin":      scrape_linkedin,
    "indeed":        scrape_indeed,
    "ziprecruiter":  scrape_ziprecruiter,
    "remoteok":      scrape_remoteok,
    "glassdoor":     scrape_glassdoor,
}


def run_scrape(
    role: str = "Software Developer",
    location: str = "Hyderabad",
    sources: Optional[list[str]] = None,
    on_batch=None,
) -> list[dict]:
    active = sources or list(SCRAPERS.keys())
    seen: set[str] = set()
    results: list[dict] = []

    for name in active:
        fn = SCRAPERS.get(name)
        if not fn:
            logger.warning(f"Unknown source: {name}")
            continue
        try:
            logger.info(f"Starting {name} scrape")
            batch = fn(role, location)
            new = [j for j in batch if j["url"] not in seen]
            seen.update(j["url"] for j in new)
            results.extend(new)
            if on_batch and new:
                on_batch(new)
            logger.info(f"{name}: {len(new)} new unique jobs")
        except Exception as e:
            logger.error(f"{name} scraper failed: {e}")

    logger.info(f"run_scrape complete — {len(results)} total unique jobs")
    return results