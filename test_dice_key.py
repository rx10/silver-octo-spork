"""
Test Dice API key logic.

Run:
    python test_dice_key.py
"""

import os, sys, logging, unittest

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(name)s: %(message)s")

sys.path.insert(0, os.path.dirname(__file__))
import scraper


class TestDiceKey(unittest.TestCase):

    def test_returns_env_key(self):
        with unittest.mock.patch.dict(os.environ, {"DICE_API_KEY": "testkey123"}):
            self.assertEqual(scraper._get_dice_key(), "testkey123")

    def test_raises_when_not_set(self):
        env = {k: v for k, v in os.environ.items() if k != "DICE_API_KEY"}
        with unittest.mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaises(RuntimeError):
                scraper._get_dice_key()

    @unittest.skipUnless(os.getenv("RUN_LIVE_DICE_TEST") == "1", "set RUN_LIVE_DICE_TEST=1 to run")
    def test_live_scrape(self):
        jobs = scraper.scrape_dice("software engineer", "United States", max_pages=1)
        self.assertGreater(len(jobs), 0)
        self.assertIn("title", jobs[0])


import unittest.mock

if __name__ == "__main__":
    unittest.main(verbosity=2)
