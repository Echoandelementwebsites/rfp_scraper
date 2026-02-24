import sys
import os
import unittest
import asyncio

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from rfp_scraper_v2.core.models import Agency, Bid
from rfp_scraper_v2.core.database import DatabaseHandler
from rfp_scraper_v2.crawlers.engine import CrawlerEngine, engine

class TestBasics(unittest.TestCase):
    def test_models(self):
        a = Agency(name="Test City", state="TX", type="city", homepage_url="http://example.com")
        self.assertEqual(a.name, "Test City")

        b = Bid(
            title="Road Work",
            clientName="Dept of Public Works",
            deadline="2025-12-31",
            description="Fixing roads",
            link="http://example.com/bid/1",
            full_text="Some text",
            csi_divisions=["Division 01"],
            slug="123"
        )
        self.assertEqual(b.title, "Road Work")
        self.assertEqual(b.client_name, "Dept of Public Works")

    def test_database_sqlite(self):
        # Use in-memory DB or temp file
        db = DatabaseHandler(db_url=None)
        self.assertFalse(db.is_postgres)
        # We can't easily test connection without creating a file, which DatabaseHandler does.
        # It creates rfp_scraper_v2/rfp_scraper_v2.db
        self.assertTrue(os.path.exists(db.db_path))

    def test_engine_config(self):
        conf = engine.get_run_config()
        self.assertTrue(conf.process_iframes)
        self.assertTrue(conf.magic)

        llm = engine.get_llm_config()
        self.assertEqual(llm.provider, "openai/deepseek-chat")

if __name__ == '__main__':
    unittest.main()
