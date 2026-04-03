"""
Tests for RemoteOK and ZipRecruiter scrapers.

Run:
    python test_remoteok_zip.py

Live tests (hit real network):
    RUN_LIVE_TEST=1 python test_remoteok_zip.py
"""

import os, sys, json, unittest
from unittest.mock import patch, MagicMock
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(__file__))
import scraper


LIVE = os.getenv("RUN_LIVE_TEST") == "1"


# ─────────────────────────────────────────────────────────────────────────────
# REMOTEOK
# ─────────────────────────────────────────────────────────────────────────────

REMOTEOK_FIXTURE = [
    {"legal": True},   # first item is always the legal notice dict
    {
        "id": "123",
        "position": "Senior Python Developer",
        "company": "Acme Corp",
        "tags": ["python", "django", "remote"],
        "url": "https://remoteok.com/remote-jobs/123",
        "salary_min": 90000,
        "salary_max": 130000,
        "epoch": 1700000000,
        "description": "<p>Build cool stuff with <b>Python</b>.</p>",
    },
    {
        "id": "456",
        "position": "React Frontend Engineer",
        "company": "Beta Inc",
        "tags": ["react", "javascript"],
        "url": "https://remoteok.com/remote-jobs/456",
        "salary_min": None,
        "salary_max": None,
        "epoch": None,
        "description": "",
    },
]


def _mock_remoteok_response(data=REMOTEOK_FIXTURE, status=200):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = data
    resp.raise_for_status = MagicMock()
    return resp


class TestRemoteOK(unittest.TestCase):

    def _call(self, role, data=REMOTEOK_FIXTURE):
        mock_resp = _mock_remoteok_response(data)
        with patch("scraper.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get.return_value = mock_resp
            mock_client_cls.return_value = mock_client
            with patch("scraper.delay"):
                return scraper.scrape_remoteok(role)

    def test_filters_by_role(self):
        jobs = self._call("python")
        titles = [j["title"] for j in jobs]
        self.assertIn("Senior Python Developer", titles)
        self.assertNotIn("React Frontend Engineer", titles)

    def test_role_matched_via_tags(self):
        jobs = self._call("django")
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["title"], "Senior Python Developer")

    def test_salary_formatted(self):
        jobs = self._call("python")
        self.assertEqual(jobs[0]["salary"], "USD 90,000 – 130,000")

    def test_no_salary_is_none(self):
        jobs = self._call("react")
        self.assertIsNone(jobs[0]["salary"])

    def test_posted_date_from_epoch(self):
        jobs = self._call("python")
        self.assertIsNotNone(jobs[0]["posted_date"])
        self.assertRegex(jobs[0]["posted_date"], r"\d{4}-\d{2}-\d{2}")

    def test_description_strips_html(self):
        jobs = self._call("python")
        self.assertNotIn("<p>", jobs[0]["description"])
        self.assertIn("Python", jobs[0]["description"])

    def test_source_is_remoteok(self):
        jobs = self._call("python")
        self.assertTrue(all(j["source"] == "RemoteOK" for j in jobs))

    def test_location_is_remote(self):
        jobs = self._call("python")
        self.assertTrue(all(j["location"] == "Remote" for j in jobs))

    def test_api_error_returns_empty(self):
        with patch("scraper.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get.side_effect = Exception("connection refused")
            mock_client_cls.return_value = mock_client
            with patch("scraper.delay"):
                jobs = scraper.scrape_remoteok("python")
        self.assertEqual(jobs, [])

    def test_max_results_respected(self):
        many = [{"id": str(i), "position": "Python Dev", "company": "X",
                 "tags": ["python"], "url": f"https://remoteok.com/remote-jobs/{i}",
                 "salary_min": None, "salary_max": None, "epoch": None, "description": ""}
                for i in range(100)]
        jobs = self._call("python", data=[{"legal": True}] + many)
        self.assertLessEqual(len(jobs), 60)

    @unittest.skipUnless(LIVE, "set RUN_LIVE_TEST=1 to run")
    def test_live(self):
        jobs = scraper.scrape_remoteok("python", max_results=5)
        self.assertGreater(len(jobs), 0)
        j = jobs[0]
        self.assertIn("title", j)
        self.assertEqual(j["source"], "RemoteOK")
        self.assertEqual(j["location"], "Remote")


# ─────────────────────────────────────────────────────────────────────────────
# ZIPRECRUITER — _parse_ziprecruiter
# ─────────────────────────────────────────────────────────────────────────────

def _zip_html(cards_html: str) -> BeautifulSoup:
    return BeautifulSoup(f"<html><body>{cards_html}</body></html>", "html.parser")


CARD_FULL = """
<article class="job_result">
  <a class="job_link" href="https://www.ziprecruiter.com/jobs/acme-123">Backend Engineer</a>
  <a class="t_org_link">Acme Corp</a>
  <p class="location">Austin, TX</p>
  <span class="salary_text">$120,000/yr</span>
  <div class="date_text">2 days ago</div>
  <p class="snippet_text">Build scalable APIs.</p>
</article>
"""

CARD_MINIMAL = """
<article class="job_result">
  <a class="job_link" href="https://www.ziprecruiter.com/jobs/beta-456">Data Scientist</a>
</article>
"""

CARD_RELATIVE_URL = """
<article class="job_result">
  <a class="job_link" href="/jobs/gamma-789">DevOps Engineer</a>
  <a class="t_org_link">Gamma LLC</a>
</article>
"""


class TestParseZipRecruiter(unittest.TestCase):

    def test_full_card_parsed(self):
        soup = _zip_html(CARD_FULL)
        jobs = scraper._parse_ziprecruiter(soup, "Remote")
        self.assertEqual(len(jobs), 1)
        j = jobs[0]
        self.assertEqual(j["title"], "Backend Engineer")
        self.assertEqual(j["company"], "Acme Corp")
        self.assertEqual(j["location"], "Austin, TX")
        self.assertEqual(j["salary"], "$120,000/yr")
        self.assertEqual(j["source"], "ZipRecruiter")

    def test_minimal_card_uses_fallback_location(self):
        soup = _zip_html(CARD_MINIMAL)
        jobs = scraper._parse_ziprecruiter(soup, "United States")
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["location"], "United States")
        self.assertEqual(jobs[0]["company"], "Unknown")

    def test_relative_url_made_absolute(self):
        soup = _zip_html(CARD_RELATIVE_URL)
        jobs = scraper._parse_ziprecruiter(soup, "Remote")
        self.assertTrue(jobs[0]["url"].startswith("https://www.ziprecruiter.com"))

    def test_non_salary_text_excluded(self):
        html = """
        <article class="job_result">
          <a class="job_link" href="https://www.ziprecruiter.com/jobs/x">Dev</a>
          <span class="salary_text">Competitive</span>
        </article>
        """
        jobs = scraper._parse_ziprecruiter(_zip_html(html), "Remote")
        self.assertIsNone(jobs[0]["salary"])

    def test_no_cards_returns_empty(self):
        soup = _zip_html("<div>nothing here</div>")
        self.assertEqual(scraper._parse_ziprecruiter(soup, "Remote"), [])

    def test_id_is_stable(self):
        soup = _zip_html(CARD_FULL)
        j1 = scraper._parse_ziprecruiter(soup, "Remote")[0]
        j2 = scraper._parse_ziprecruiter(soup, "Remote")[0]
        self.assertEqual(j1["id"], j2["id"])

    @unittest.skipUnless(LIVE, "set RUN_LIVE_TEST=1 to run")
    def test_live(self):
        jobs = scraper.scrape_ziprecruiter("software engineer", "United States", max_pages=1)
        # ZipRecruiter may block without proxy — acceptable
        self.assertIsInstance(jobs, list)
        if jobs:
            self.assertIn("title", jobs[0])
            self.assertEqual(jobs[0]["source"], "ZipRecruiter")


if __name__ == "__main__":
    unittest.main(verbosity=2)
