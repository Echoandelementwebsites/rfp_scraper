import unittest
from unittest.mock import MagicMock, patch
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from rfp_scraper.discovery import DiscoveryEngine
from rfp_scraper.ai_parser import DeepSeekClient
from rfp_scraper.config_loader import get_domain_patterns

class TestDeepSearch(unittest.TestCase):

    @patch('rfp_scraper.discovery.DDGS')
    def test_search_and_rank_candidates(self, mock_ddgs):
        # Mock DDGS context manager
        mock_instance = MagicMock()
        mock_ddgs.return_value.__enter__.return_value = mock_instance

        # Mock results
        mock_instance.text.return_value = [
            {'title': 'Res 1', 'href': 'http://url1.com', 'body': 'Snippet 1'},
            {'title': 'Res 2', 'href': 'http://url2.com', 'body': 'Snippet 2'}
        ]

        engine = DiscoveryEngine()
        candidates = engine.search_and_rank_candidates("query")

        self.assertEqual(len(candidates), 2)
        self.assertEqual(candidates[0]['url'], 'http://url1.com')
        self.assertEqual(candidates[0]['snippet'], 'Snippet 1')

    @patch('rfp_scraper.ai_parser.OpenAI')
    def test_identify_best_agency_url(self, mock_openai):
        client = DeepSeekClient(api_key="fake-key")

        # Mock chat completion
        mock_response = MagicMock()
        mock_response.choices[0].message.content = '{"url": "http://verified.gov"}'
        client.client.chat.completions.create.return_value = mock_response

        candidates = [{'title': 'T', 'url': 'U', 'snippet': 'S'}]
        url = client.identify_best_agency_url(candidates, "Agency", ["pattern"])

        self.assertEqual(url, "http://verified.gov")

    def test_get_domain_patterns(self):
        # This tests loading the real JSON file, which is fine
        patterns = get_domain_patterns("county")
        self.assertIsInstance(patterns, list)
        # Assuming at least one pattern exists for county
        self.assertTrue(len(patterns) > 0)
        self.assertIn("[countyname]county.gov", patterns)

if __name__ == '__main__':
    unittest.main()
