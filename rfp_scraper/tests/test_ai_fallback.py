import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from rfp_scraper.discovery import DiscoveryEngine
from rfp_scraper.ai_parser import DeepSeekClient

class TestAIFallback(unittest.TestCase):

    @patch('rfp_scraper.discovery.DDGS')
    def test_get_raw_candidates(self, mock_ddgs_cls):
        # Mock DDGS context manager
        mock_ddgs = MagicMock()
        mock_ddgs_cls.return_value.__enter__.return_value = mock_ddgs

        # Mock text search results
        mock_ddgs.text.return_value = [
            {'title': 'Res 1', 'href': 'http://url1.com', 'body': 'Snippet 1'},
            {'title': 'Res 2', 'href': 'http://url2.com', 'body': 'Snippet 2'}
        ]

        engine = DiscoveryEngine()
        candidates = engine.get_raw_candidates("query", limit=2)

        self.assertEqual(len(candidates), 2)
        self.assertEqual(candidates[0]['title'], 'Res 1')
        self.assertEqual(candidates[0]['url'], 'http://url1.com')
        self.assertEqual(candidates[0]['snippet'], 'Snippet 1')

        # Verify call arguments
        mock_ddgs.text.assert_called_with("query", max_results=2)

    @patch('rfp_scraper.ai_parser.OpenAI')
    def test_find_agency_in_search_results(self, mock_openai_cls):
        # Mock OpenAI client
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        client = DeepSeekClient(api_key="fake_key")

        # Mock response
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "http://official-site.gov"
        mock_client.chat.completions.create.return_value = mock_response

        candidates = [{'title': 'T', 'url': 'http://official-site.gov', 'snippet': 'S'}]
        domain_rules = ['.gov']

        result = client.find_agency_in_search_results("Agency", "Jurisdiction", candidates, domain_rules)

        self.assertEqual(result, "http://official-site.gov")

        # Verify Prompt construction (roughly)
        call_args = mock_client.chat.completions.create.call_args
        messages = call_args[1]['messages']
        user_content = messages[0]['content']

        self.assertIn("I am looking for the official website for Agency in Jurisdiction", user_content)
        self.assertIn(".gov", user_content)
        self.assertIn("http://official-site.gov", user_content) # Candidates are in prompt

    @patch('rfp_scraper.ai_parser.OpenAI')
    def test_find_agency_returns_none(self, mock_openai_cls):
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client
        client = DeepSeekClient(api_key="fake_key")

        mock_response = MagicMock()
        mock_response.choices[0].message.content = "None"
        mock_client.chat.completions.create.return_value = mock_response

        candidates = [{'title': 'T', 'url': 'http://fake.com', 'snippet': 'S'}]

        result = client.find_agency_in_search_results("Agency", "Jurisdiction", candidates, [])
        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()
