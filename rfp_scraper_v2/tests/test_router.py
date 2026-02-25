import unittest
from unittest.mock import AsyncMock, patch, MagicMock
import asyncio
import sys
import os

# Add project root to path. Assuming test is in rfp_scraper_v2/tests/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from rfp_scraper_v2.crawlers import pipeline
from rfp_scraper_v2.core.models import BidExtractionSchema

class TestRouter: # Not inheriting from unittest.TestCase for manual async run simplicity
    def setUp(self):
        self.crawler = MagicMock()

    async def test_router_deterministic_bonfire(self):
        # Setup mock return for deterministic
        mock_bids = [BidExtractionSchema(title="Bonfire Bid", clientName="Client", deadline="2023-01-01", description="Desc", link="http://bonfirehub.com/bid/1")]

        # Patching inside the module where extract_deterministic is defined
        with patch('rfp_scraper_v2.crawlers.pipeline.extract_deterministic', new_callable=AsyncMock) as mock_det:
            mock_det.return_value = mock_bids

            # Call router with Bonfire URL
            bids = await pipeline.extract_bids(self.crawler, "https://example.bonfirehub.com/opportunities")

            # Verify deterministic was called
            mock_det.assert_called_once_with(self.crawler, "https://example.bonfirehub.com/opportunities", "Bonfire")
            # Verify result matches
            assert bids == mock_bids, f"Expected {mock_bids}, got {bids}"
            print("  [PASS] Bonfire Router Test")

    async def test_router_fallback_ai_unknown_domain(self):
        # Setup mock return for AI
        mock_bids = [BidExtractionSchema(title="AI Bid", clientName="Client", deadline="2023-01-01", description="Desc", link="http://unknown.com/bid/1")]

        with patch('rfp_scraper_v2.crawlers.pipeline.extract_bids_ai', new_callable=AsyncMock) as mock_ai:
            mock_ai.return_value = mock_bids

            # Call router with unknown URL
            bids = await pipeline.extract_bids(self.crawler, "https://unknown.com/bids")

            # Verify AI was called
            mock_ai.assert_called_once_with(self.crawler, "https://unknown.com/bids")
            assert bids == mock_bids
            print("  [PASS] AI Fallback (Unknown Domain) Test")

    async def test_router_fallback_ai_deterministic_empty(self):
        # Deterministic returns empty
        with patch('rfp_scraper_v2.crawlers.pipeline.extract_deterministic', new_callable=AsyncMock) as mock_det, \
             patch('rfp_scraper_v2.crawlers.pipeline.extract_bids_ai', new_callable=AsyncMock) as mock_ai:

            mock_det.return_value = []
            mock_bids = [BidExtractionSchema(title="AI Bid Fallback", clientName="Client", deadline="2023-01-01", description="Desc", link="http://bonfirehub.com/bid/1")]
            mock_ai.return_value = mock_bids

            # Call router with Bonfire URL
            bids = await pipeline.extract_bids(self.crawler, "https://example.bonfirehub.com/opportunities")

            # Verify deterministic called first, then AI
            mock_det.assert_called_once()
            mock_ai.assert_called_once_with(self.crawler, "https://example.bonfirehub.com/opportunities")
            assert bids == mock_bids
            print("  [PASS] AI Fallback (Empty Deterministic) Test")

async def main():
    t = TestRouter()
    t.setUp()
    await t.test_router_deterministic_bonfire()
    await t.test_router_fallback_ai_unknown_domain()
    await t.test_router_fallback_ai_deterministic_empty()
    print("\nALL ROUTER TESTS PASSED.")

if __name__ == '__main__':
    asyncio.run(main())
