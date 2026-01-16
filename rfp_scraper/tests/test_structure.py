import unittest
import sys
import os
import datetime
from unittest.mock import MagicMock

# Ensure we can import the module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from rfp_scraper.factory import ScraperFactory
from rfp_scraper.scrapers.base import BaseScraper
from rfp_scraper.scrapers.california import CaliforniaScraper

class TestArchitecture(unittest.TestCase):
    def test_factory_loading(self):
        factory = ScraperFactory()
        states = factory.get_available_states()
        print(f"Loaded states: {states}")
        self.assertIn("California", states)
        self.assertIn("Texas", states)
        self.assertIn("Florida", states)
        self.assertIn("New York", states)
        self.assertIn("Illinois", states)
        self.assertIn("Connecticut", states)

    def test_factory_get_scraper(self):
        factory = ScraperFactory()
        scraper = factory.get_scraper("California")
        self.assertIsInstance(scraper, CaliforniaScraper)
        self.assertIsInstance(scraper, BaseScraper)

    def test_date_qualification(self):
        scraper = CaliforniaScraper()

        # Today
        today = datetime.datetime.now()

        # Deadline = Today + 4 days -> Qualified
        deadline_qualified = today + datetime.timedelta(days=4)
        self.assertTrue(scraper.is_qualified(deadline_qualified), "Deadline 4 days away should be qualified")

        # Deadline = Today + 3 days -> Not Qualified
        deadline_unqualified = today + datetime.timedelta(days=3)
        self.assertFalse(scraper.is_qualified(deadline_unqualified), "Deadline 3 days away should NOT be qualified")

        # Deadline = Today -> Not Qualified
        self.assertFalse(scraper.is_qualified(today))

    def test_city_inference(self):
        # Test CT logic if imported
        from rfp_scraper.scrapers.connecticut import ConnecticutScraper
        ct_scraper = ConnecticutScraper()

        city = ct_scraper.infer_city("Paving in Hartford", "Description text")
        self.assertEqual(city, "Hartford")

        city = ct_scraper.infer_city("Paving in Unknown", "Description text")
        self.assertEqual(city, "Connecticut")

if __name__ == '__main__':
    unittest.main()
