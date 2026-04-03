"""
Test Dice API key interception and fallback logic.

Run:
    python test_dice_key.py
"""

import os, sys, logging, unittest
from unittest.mock import patch, MagicMock

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(name)s: %(message)s")

# Make sure scraper is importable from same directory
sys.path.insert(0, os.path.dirname(__file__))
import scraper


def _reset():
    """Clear the module-level key cache between tests."""
    scraper._cached_dice_key = None


# ─────────────────────────────────────────────────────────────────────────────
# CASE 1: env var fallback (no Playwright required)
# ─────────────────────────────────────────────────────────────────────────────
class TestEnvVarFallback(unittest.TestCase):

    def setUp(self):
        _reset()

    def test_env_key_returned_when_playwright_unavailable(self):
        """When Playwright raises, _get_dice_key() should fall back to DICE_API_KEY."""
        fake_key = "envfallbackkey" + "x" * 20   # ≥30 chars not required for env path

        def boom(*a, **kw):
            raise RuntimeError("playwright not installed")

        with patch.dict(os.environ, {"DICE_API_KEY": fake_key}):
            with patch("playwright.sync_api.sync_playwright", side_effect=boom):
                key = scraper._get_dice_key()

        self.assertEqual(key, fake_key)
        print(f"\n[PASS] env fallback → '{key}'")

    def test_env_key_cached_after_first_call(self):
        """Second call should return cached value without hitting Playwright."""
        fake_key = "cachedenvkey" + "y" * 20

        def boom(*a, **kw):
            raise RuntimeError("playwright not installed")

        with patch.dict(os.environ, {"DICE_API_KEY": fake_key}):
            with patch("playwright.sync_api.sync_playwright", side_effect=boom):
                k1 = scraper._get_dice_key()
                k2 = scraper._get_dice_key()   # should use cache, not call playwright again

        self.assertEqual(k1, k2)
        print(f"\n[PASS] cache hit → '{k2}'")


# ─────────────────────────────────────────────────────────────────────────────
# CASE 2: RuntimeError when nothing is configured
# ─────────────────────────────────────────────────────────────────────────────
class TestRuntimeErrorPath(unittest.TestCase):

    def setUp(self):
        _reset()

    def test_raises_when_no_key_and_no_playwright(self):
        """Should raise RuntimeError if Playwright fails and DICE_API_KEY is not set."""
        env = {k: v for k, v in os.environ.items() if k != "DICE_API_KEY"}

        def boom(*a, **kw):
            raise RuntimeError("playwright not installed")

        with patch("playwright.sync_api.sync_playwright", side_effect=boom):
            with patch.dict(os.environ, env, clear=True):
                with self.assertRaises(RuntimeError) as ctx:
                    scraper._get_dice_key()

        print(f"\n[PASS] RuntimeError raised: {ctx.exception}")


# ─────────────────────────────────────────────────────────────────────────────
# CASE 3: Playwright interception path (mocked — no real browser)
# ─────────────────────────────────────────────────────────────────────────────
class TestPlaywrightInterception(unittest.TestCase):

    def setUp(self):
        _reset()

    def _make_playwright_mock(self, intercepted_key: str | None):
        """
        Build a mock sync_playwright context manager that simulates the
        request interception loop in _get_dice_key().
        """
        # We need to capture the on_req callback and call it ourselves
        captured_callback = {}

        mock_page = MagicMock()
        mock_browser = MagicMock()
        mock_browser.new_page.return_value = mock_page

        def fake_on(event, cb):
            if event == "request":
                captured_callback["fn"] = cb

        mock_page.on.side_effect = fake_on

        def fake_goto(*a, **kw):
            # Simulate a request that carries the intercepted key
            if intercepted_key and "fn" in captured_callback:
                mock_req = MagicMock()
                mock_req.url = "https://job-search-api.svc.dhigroupinc.com/search"
                mock_req.headers = {"x-api-key": intercepted_key}
                captured_callback["fn"](mock_req)

        mock_page.goto.side_effect = fake_goto

        mock_chromium = MagicMock()
        mock_chromium.launch.return_value = mock_browser

        mock_pw_instance = MagicMock()
        mock_pw_instance.chromium = mock_chromium

        mock_ctx = MagicMock()
        mock_ctx.__enter__ = MagicMock(return_value=mock_pw_instance)
        mock_ctx.__exit__ = MagicMock(return_value=False)

        mock_sync_pw = MagicMock(return_value=mock_ctx)
        return mock_sync_pw

    def test_key_intercepted_from_browser_request(self):
        """Playwright intercepts a key ≥30 chars from dhigroupinc.com requests."""
        fake_key = "interceptedplaywrightkey" + "z" * 10   # 34 chars

        mock_sync_pw = self._make_playwright_mock(intercepted_key=fake_key)

        with patch("playwright.sync_api.sync_playwright", mock_sync_pw):
            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("DICE_API_KEY", None)
                key = scraper._get_dice_key()

        self.assertEqual(key, fake_key)
        print(f"\n[PASS] Playwright interception → '{key}'")

    def test_short_key_ignored_by_playwright(self):
        """Keys shorter than 30 chars should be ignored; falls back to env."""
        short_key = "tooshort"
        env_key = "validenvfallbackkey" + "x" * 15

        mock_sync_pw = self._make_playwright_mock(intercepted_key=short_key)

        with patch("playwright.sync_api.sync_playwright", mock_sync_pw):
            with patch.dict(os.environ, {"DICE_API_KEY": env_key}):
                key = scraper._get_dice_key()

        self.assertEqual(key, env_key)
        print(f"\n[PASS] Short key ignored → fell back to env key '{key}'")


# ─────────────────────────────────────────────────────────────────────────────
# CASE 4: LIVE — real Playwright browser (requires playwright installed)
# ─────────────────────────────────────────────────────────────────────────────
class TestLivePlaywrightInterception(unittest.TestCase):
    """
    Skipped unless RUN_LIVE_DICE_TEST=1 is set.
    Launches a real Chromium browser and hits dice.com.
    """

    def setUp(self):
        _reset()

    @unittest.skipUnless(os.getenv("RUN_LIVE_DICE_TEST") == "1", "set RUN_LIVE_DICE_TEST=1 to run")
    def test_live_interception(self):
        os.environ.pop("DICE_API_KEY", None)
        key = scraper._get_dice_key()
        self.assertIsNotNone(key)
        self.assertGreaterEqual(len(key), 30)
        print(f"\n[PASS] Live intercepted key (first 10 chars): {key[:10]}...")


if __name__ == "__main__":
    unittest.main(verbosity=2)
