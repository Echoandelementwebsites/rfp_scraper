
import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import datetime
import json
import logging
import asyncio

# Need to import these to ensure patching works correctly
from rfp_scraper_v2.core.database import DatabaseHandler
from rfp_scraper_v2.core.models import Bid

class TestDatabaseHandler(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Patch init to avoid connecting to real DB
        self.patcher = patch('rfp_scraper_v2.core.database.DatabaseHandler._init_postgres')
        self.mock_init = self.patcher.start()

        # Patch logger where it is used in database.py
        self.logger_patcher = patch('rfp_scraper_v2.core.database.logger')
        self.mock_logger = self.logger_patcher.start()

        # Mock environment
        with patch.dict('os.environ', {'DATABASE_URL': 'postgresql://u:p@h:5432/d'}):
            self.db = DatabaseHandler()

    async def asyncTearDown(self):
        self.patcher.stop()
        self.logger_patcher.stop()

    async def test_async_save_bid_strict_types(self):
        # Setup mock pool and connection
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        # Mocking the async context manager for pool.acquire()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        self.db.async_pool = mock_pool

        # Create a test bid
        test_bid = Bid(
            title="Road Construction",
            clientName="City of Test",
            deadline="2025-12-31",
            description="Paving roads",
            link="http://test.com/bid/123",
            full_text="Full text content",
            csi_divisions=["030000", "040000"],
            slug="city-of-test-road-construction"
        )

        # Call the method
        await self.db.async_save_bid(test_bid, "CA")

        # Verify execute was called
        self.assertTrue(mock_conn.execute.called)

        # Get arguments
        args, _ = mock_conn.execute.call_args
        query = args[0]
        params = args[1:]

        # 1. Verify JSONB casting syntax in query
        self.assertIn("$8::jsonb", query)

        # 2. Verify CSI divisions passed as JSON string
        csi_param = params[7] # index 7 corresponds to $8 (slug is $1 at index 0)
        self.assertEqual(csi_param, '["030000", "040000"]')

        # 3. Verify Deadline is a date object
        deadline_param = params[3] # $4
        self.assertIsInstance(deadline_param, datetime.date)
        self.assertEqual(deadline_param, datetime.date(2025, 12, 31))

    async def test_async_save_bid_error_logging(self):
        # Setup mock that raises exception
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.execute.side_effect = Exception("DB Connection Lost")
        self.db.async_pool = mock_pool

        test_bid = Bid(
            title="Fail Bid", clientName="Fail Client", link="http://fail.com",
            full_text="", csi_divisions=[], slug="fail-bid"
        )

        # Must await to trigger exception handling inside the method
        await self.db.async_save_bid(test_bid, "NY")

        # Verify logger.error was called
        # We need to ensure we are asserting on the correct mock object.
        # self.mock_logger mocks 'rfp_scraper_v2.core.database.logger'
        self.assertTrue(self.mock_logger.error.called, "Logger error not called")

        call_args = self.mock_logger.error.call_args
        # call_args[0] is positional args tuple, call_args[0][0] is the message string
        self.assertIn("Error saving bid fail-bid", call_args[0][0])
        # call_args[1] is keyword args dict
        self.assertTrue(call_args[1].get('exc_info'))

if __name__ == '__main__':
    unittest.main()
