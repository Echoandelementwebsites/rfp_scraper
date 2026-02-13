import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from rfp_scraper.scrapers.hierarchical import HierarchicalScraper

class TestHierarchicalScraperOptimization(unittest.TestCase):
    def setUp(self):
        # Mock dependencies
        self.mock_page = MagicMock()
        self.mock_db = MagicMock()
        self.mock_ai_parser = MagicMock()
        self.mock_compliance = MagicMock()

        # Patch dependencies in the module
        patcher_db = patch('rfp_scraper.scrapers.hierarchical.DatabaseHandler', return_value=self.mock_db)
        patcher_ai = patch('rfp_scraper.scrapers.hierarchical.DeepSeekClient', return_value=self.mock_ai_parser)
        patcher_comp = patch('rfp_scraper.scrapers.hierarchical.ComplianceManager', return_value=self.mock_compliance)

        self.addCleanup(patcher_db.stop)
        self.addCleanup(patcher_ai.stop)
        self.addCleanup(patcher_comp.stop)

        self.mock_db_cls = patcher_db.start()
        self.mock_ai_cls = patcher_ai.start()
        self.mock_comp_cls = patcher_comp.start()

        # Initialize scraper
        self.scraper = HierarchicalScraper(state_name="TestState")

        # Setup common mock returns
        self.mock_db.get_all_states.return_value = pd.DataFrame([{'id': 1, 'name': 'TestState'}])
        self.mock_db.get_agencies_by_state.return_value = pd.DataFrame([{
            'organization_name': 'Test Agency',
            'url': 'http://example.com'
        }])
        self.mock_db.url_already_scraped.return_value = False
        self.mock_compliance.can_fetch.return_value = True

        # Mock behavior functions to avoid actual sleeps/scrolls
        self.patcher_behavior = patch('rfp_scraper.scrapers.hierarchical.mimic_human_arrival')
        self.patcher_scroll = patch('rfp_scraper.scrapers.hierarchical.smooth_scroll')
        self.mock_mimic = self.patcher_behavior.start()
        self.mock_scroll = self.patcher_scroll.start()
        self.addCleanup(self.patcher_behavior.stop)
        self.addCleanup(self.patcher_scroll.stop)

    def test_find_better_url_calls_evaluate(self):
        """Verify _find_better_url calls page.evaluate with a JS function string."""
        self.mock_page.evaluate.return_value = "http://example.com/bids"

        result = self.scraper._find_better_url(self.mock_page)

        self.assertEqual(result, "http://example.com/bids")
        self.mock_page.evaluate.assert_called()
        args, _ = self.mock_page.evaluate.call_args
        self.assertIn("const keywords =", args[0])
        self.assertIn("document.querySelectorAll('a')", args[0])

    def test_scrape_deep_scan_flow(self):
        """Verify the deep scan loop flow with the new logic."""
        # 1. Setup mocks for the flow

        # _find_better_url returns a better URL
        # We need to mock page.evaluate carefully because it's used multiple times
        # 1st call: _find_better_url (returns better URL)
        # 2nd call: EXTRACT_MAIN_CONTENT_JS (returns content)
        def evaluate_side_effect(script):
            if "const keywords =" in script:
                return "http://example.com/bids"
            if "const clone = document.body.cloneNode(true)" in script:
                return "<html><body>RFP for Construction</body></html>" + "A" * 100
            return None

        self.mock_page.evaluate.side_effect = evaluate_side_effect
        self.mock_page.url = "http://example.com" # Start at main page

        # Mock AI parser
        self.mock_ai_parser.parse_rfp_content.return_value = [{
            'title': 'Construction RFP',
            'clientName': 'Test Agency',
            'deadline': '2030-12-31',
            'description': 'Building a bridge'
        }]
        self.mock_ai_parser.classify_csi_divisions.return_value = ['03 - Concrete']

        # 2. Run scrape
        results_df = self.scraper.scrape(self.mock_page)

        # 3. Assertions

        # Verify navigation
        # Should navigate to main URL first
        self.mock_mimic.assert_any_call(self.mock_page, 'http://example.com', referrer_url='https://www.google.com/', timeout=20000)
        # Should navigate to better URL
        self.mock_mimic.assert_any_call(self.mock_page, 'http://example.com/bids', referrer_url='http://example.com', timeout=15000)

        # Verify extraction
        self.mock_ai_parser.parse_rfp_content.assert_called()

        # Verify DB insert
        self.mock_db.insert_bid.assert_called()
        args, kwargs = self.mock_db.insert_bid.call_args
        # insert_bid(slug, client, title, ...)
        # title is the 3rd positional argument (index 2)
        # However, checking if it was passed positionally
        self.assertEqual(args[2], 'Construction Rfp') # Note: clean_text converts to Title Case
        self.assertEqual(kwargs['matching_trades'], '03 - Concrete')

    def test_scrape_skips_file_url(self):
        """Verify that file URLs detected by _find_better_url are skipped."""
        # Setup _find_better_url to return a PDF
        def evaluate_side_effect(script):
            if "const keywords =" in script:
                return "http://example.com/bids.pdf"
            if "const clone = document.body.cloneNode(true)" in script:
                return "Content" # Should strictly not be reached if we skip, but for safety in mock
            return None
        self.mock_page.evaluate.side_effect = evaluate_side_effect
        self.mock_page.url = "http://example.com"

        self.scraper.scrape(self.mock_page)

        # Verify we did NOT navigate to the PDF
        # We check all calls to mimic_human_arrival
        # Calls: (page, url, ...)
        navigated_urls = [call.args[1] for call in self.mock_mimic.call_args_list]
        self.assertNotIn("http://example.com/bids.pdf", navigated_urls)

if __name__ == '__main__':
    unittest.main()
