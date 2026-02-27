import unittest
from unittest.mock import AsyncMock, patch, MagicMock
import asyncio
import sys
import os
import pytest

# Add project root to path. Assuming test is in rfp_scraper_v2/tests/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from rfp_scraper_v2.crawlers import pipeline
from rfp_scraper_v2.core.models import BidExtractionSchema
from rfp_scraper_v2.crawlers.schemas import BONFIRE_SCHEMA

@pytest.mark.asyncio
class TestRouter:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.crawler = MagicMock()
        self.api_key = "dummy_key"
        self.agency_name = "Test Agency"

    async def test_router_deterministic_bonfire(self):
        # Setup mock return for deterministic
        mock_bids = [BidExtractionSchema(title="Bonfire Bid", clientName="Client", deadline="2023-01-01", description="Desc", link="http://bonfirehub.com/bid/1")]

        # Patching inside the module where extract_deterministic is defined
        with patch('rfp_scraper_v2.crawlers.pipeline.extract_deterministic', new_callable=AsyncMock) as mock_det:
            mock_det.return_value = mock_bids

            # Call router with Bonfire URL
            bids = await pipeline.extract_bids(self.crawler, "https://example.bonfirehub.com/opportunities", self.agency_name, self.api_key)

            # Verify deterministic was called with agency_name
            mock_det.assert_called_once_with(self.crawler, "https://example.bonfirehub.com/opportunities", self.agency_name, BONFIRE_SCHEMA)
            # Verify result matches
            assert bids == mock_bids, f"Expected {mock_bids}, got {bids}"
            print("  [PASS] Bonfire Router Test")

    async def test_router_fallback_ai_unknown_domain(self):
        # Setup mock return for AI
        mock_bids = [BidExtractionSchema(title="AI Bid", clientName="Client", deadline="2023-01-01", description="Desc", link="http://unknown.com/bid/1")]

        with patch('rfp_scraper_v2.crawlers.pipeline.extract_bids_ai', new_callable=AsyncMock) as mock_ai:
            mock_ai.return_value = mock_bids

            # Call router with unknown URL
            bids = await pipeline.extract_bids(self.crawler, "https://unknown.com/bids", self.agency_name, self.api_key)

            # Verify AI was called with agency_name
            mock_ai.assert_called_once_with(self.crawler, "https://unknown.com/bids", self.agency_name, self.api_key)
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
            bids = await pipeline.extract_bids(self.crawler, "https://example.bonfirehub.com/opportunities", self.agency_name, self.api_key)

            # Verify deterministic called first, then AI
            mock_det.assert_called_once()
            mock_ai.assert_called_once_with(self.crawler, "https://example.bonfirehub.com/opportunities", self.agency_name, self.api_key)
            assert bids == mock_bids
            print("  [PASS] AI Fallback (Empty Deterministic) Test")

if __name__ == '__main__':
    # Manual run fallback
    t = TestRouter()
    loop = asyncio.new_event_loop()
    t.setup()
    loop.run_until_complete(t.test_router_deterministic_bonfire())
    loop.run_until_complete(t.test_router_fallback_ai_unknown_domain())
    loop.run_until_complete(t.test_router_fallback_ai_deterministic_empty())
    print("\nALL ROUTER TESTS PASSED.")
