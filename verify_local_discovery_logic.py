import os
import sys
import unittest
from unittest.mock import MagicMock
import shutil

# Add project root to path
sys.path.append(os.getcwd())

from rfp_scraper.db import DatabaseHandler
from rfp_scraper.config_loader import get_local_search_scope

class TestLocalDiscovery(unittest.TestCase):
    def setUp(self):
        # Use a temp db
        self.db_path = "test_rfp_scraper.db"
        self.db = DatabaseHandler(self.db_path)

        # Setup Test Data
        self.db.add_state("TestState")
        self.states = self.db.get_all_states()
        self.state_id = int(self.states.iloc[0]['id'])

        # Add a local jurisdiction
        self.city_name = "TestCity"
        self.juris_type = "city"
        self.juris_id = self.db.append_local_jurisdiction(self.state_id, self.city_name, self.juris_type)

    def tearDown(self):
        # Cleanup
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_local_discovery_flow(self):
        """
        Simulate the AI-Native logic in app.py Phase 2
        """
        print("\nTesting Local Discovery Flow...")

        # 1. Fetch Scope
        categories = get_local_search_scope(self.juris_type)
        self.assertIsInstance(categories, list)
        self.assertIn("Main Office", categories)
        self.assertIn("Public Works", categories)

        # 2. Mock Discovery Engine & AI Client
        mock_discovery = MagicMock()
        mock_ai_client = MagicMock()

        # Mock Search Results
        def fetch_side_effect(query, num_results=10):
            return [{"title": "Result", "url": "http://example.com", "snippet": "Snippet"}]
        mock_discovery.fetch_search_context.side_effect = fetch_side_effect

        # Mock AI Analysis
        def analyze_side_effect(jurisdiction, category, results):
            if category == "Main Office":
                return "http://testcity.gov"
            if category == "Public Works":
                return "http://testcity.gov/pw"
            return None
        mock_ai_client.analyze_serp_results.side_effect = analyze_side_effect

        # 3. Simulate Loop (from app.py)
        tasks = []
        for category in categories:
            tasks.append({
                "state_id": self.state_id,
                "name": self.city_name,
                "category": category,
                "phase": "ai_native_local",
                "jurisdiction_id": self.juris_id
            })

        print(f"Generated {len(tasks)} tasks.")

        for task in tasks:
            # Only process relevant categories for test speed/clarity
            if task["category"] not in ["Main Office", "Public Works"]:
                continue

            juris_name = task["name"]
            category = task["category"]

            # 1. Construct Query
            query = f"Official website for {juris_name} {category}"

            # 2. Fetch Raw Results
            raw_results = mock_discovery.fetch_search_context(query, num_results=8)

            # 3. AI Analysis
            found_url = mock_ai_client.analyze_serp_results(juris_name, category, raw_results)

            if found_url:
                # Display Name logic
                display_name = f"{juris_name} {category}"
                if category == "Main Office":
                    display_name = juris_name

                # Save
                if not self.db.agency_exists(task["state_id"], url=found_url, category=category, local_jurisdiction_id=task["jurisdiction_id"]):
                    self.db.add_agency(
                        state_id=task["state_id"],
                        name=display_name,
                        url=found_url,
                        verified=True,
                        category=category,
                        local_jurisdiction_id=task["jurisdiction_id"]
                    )

        # 4. Verify Results
        agencies = self.db.get_agencies_by_state(self.state_id)

        # Check Main Office
        main_office = agencies[agencies['category'] == 'Main Office']
        self.assertFalse(main_office.empty, "Main Office not found")
        self.assertEqual(main_office.iloc[0]['organization_name'], "TestCity")
        self.assertEqual(main_office.iloc[0]['url'], "http://testcity.gov")

        # Check Public Works
        pw = agencies[agencies['category'] == 'Public Works']
        self.assertFalse(pw.empty, "Public Works not found")
        self.assertEqual(pw.iloc[0]['organization_name'], "TestCity Public Works")
        self.assertEqual(pw.iloc[0]['url'], "http://testcity.gov/pw")

        print("Verification Successful!")

if __name__ == "__main__":
    unittest.main()
