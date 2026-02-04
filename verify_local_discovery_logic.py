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
        Simulate the logic in app.py Phase 2
        """
        print("\nTesting Local Discovery Flow...")

        # 1. Fetch Scope
        patterns_map = get_local_search_scope(self.juris_type)
        self.assertIn("Main Office", patterns_map)
        self.assertIn("Public Works", patterns_map)

        # 2. Mock Discovery Engine
        mock_discovery = MagicMock()

        def side_effect(query):
            if "TestCity" in query and "official site" in query:
                # Main Office Specifics
                if "City of TestCity" in query or "City Government" in query or "Municipal Government" in query:
                    return "http://testcity.gov"

                # Public Works Specifics
                # We want to ensure that if ANY Public Works pattern is hit, it returns the PW URL
                # OR returns None so the loop continues to the next pattern.
                if "Public Works" in query or "Water Department" in query:
                    return "http://testcity.gov/pw"

                # For other patterns in Public Works (like Sanitation, Waste, etc.)
                # If we don't define them here, they return None (implicit),
                # causing the loop to try the next pattern until it hits one of the above.
                return None
            return None

        mock_discovery.find_url_by_query.side_effect = side_effect

        # 3. Simulate Loop
        tasks = []
        for category_key, patterns in patterns_map.items():
            tasks.append({
                "state_id": self.state_id,
                "name": self.city_name,
                "category": category_key,
                "patterns": patterns,
                "jurisdiction_id": self.juris_id
            })

        print(f"Generated {len(tasks)} tasks.")

        for task in tasks:
            category = task["category"]
            juris_name = task["name"]
            patterns = task["patterns"]

            # Simulate Browser Query Loop
            found_url = None
            for pat in patterns:
                query_name = pat
                # Replace placeholders
                query_name = query_name.replace("[City Name]", juris_name)
                query_name = query_name.replace("[Jurisdiction]", juris_name)

                query = f"{query_name} official site"
                found_url = mock_discovery.find_url_by_query(query)
                if found_url:
                    break

            if found_url:
                # Construct Display Name logic
                display_name = f"{juris_name} {category}"
                if category == 'Main Office':
                    display_name = juris_name

                # Save
                if not self.db.agency_exists(self.state_id, url=found_url, name=display_name, category=category, local_jurisdiction_id=task["jurisdiction_id"]):
                    self.db.add_agency(
                        state_id=self.state_id,
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
