import sys
import os
import sqlite3
import pandas as pd
from typing import List, Dict

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from rfp_scraper_v2.core.database import DatabaseHandler
from rfp_scraper_v2.orchestrator import get_agencies_for_scraping, get_jurisdictions_for_discovery, load_json

def setup_test_db(db_path: str):
    if os.path.exists(db_path):
        os.remove(db_path)

    class TestDatabaseHandler(DatabaseHandler):
        def __init__(self, db_path):
             self.db_url = None
             self.is_postgres = False
             self.db_path = db_path
             if os.path.dirname(self.db_path):
                 os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
             self._init_sqlite()

    db = TestDatabaseHandler(db_path)

    # Insert Data manually for control
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # States
    cursor.execute("INSERT INTO states (name) VALUES ('California')")
    state_id = cursor.lastrowid

    # Local Jurisdictions
    cursor.execute("INSERT INTO local_jurisdictions (state_id, name, type) VALUES (?, 'San Francisco', 'city')", (state_id,))
    lj_id = cursor.lastrowid

    # Agencies
    # 1. Agency with procurement URL (should be preferred)
    cursor.execute("""
        INSERT INTO agencies (state_id, organization_name, url, procurement_url, category, local_jurisdiction_id)
        VALUES (?, 'SF Public Works', 'https://sfpublicworks.org', 'https://sfpublicworks.org/bids', 'city', ?)
    """, (state_id, lj_id))

    # 2. Agency with only homepage URL (fallback)
    cursor.execute("""
        INSERT INTO agencies (state_id, organization_name, url, category, local_jurisdiction_id)
        VALUES (?, 'SF Recreation', 'https://sfrecpark.org', 'city', ?)
    """, (state_id, lj_id))

    # 3. Agency with neither (should be skipped)
    cursor.execute("""
        INSERT INTO agencies (state_id, organization_name, category, local_jurisdiction_id)
        VALUES (?, 'Empty Agency', 'city', ?)
    """, (state_id, lj_id))

    conn.commit()
    conn.close()

    return db

def test_get_agencies_for_scraping():
    db_path = "test_rfp_scraper.db"
    db = setup_test_db(db_path)

    print("Testing get_agencies_for_scraping...")
    agencies = get_agencies_for_scraping(db, ["California"])

    assert len(agencies) == 2, f"Expected 2 agencies, got {len(agencies)}"

    # Check SF Public Works
    sf_pw = next(a for a in agencies if a.name == 'SF Public Works')
    assert sf_pw.procurement_url == 'https://sfpublicworks.org/bids', f"Expected procurement URL, got {sf_pw.procurement_url}"
    assert sf_pw.type == 'city', f"Expected type 'city', got {sf_pw.type}"

    # Check SF Recreation
    sf_rec = next(a for a in agencies if a.name == 'SF Recreation')
    assert sf_rec.procurement_url == 'https://sfrecpark.org', f"Expected homepage URL as fallback, got {sf_rec.procurement_url}"

    print("✅ get_agencies_for_scraping passed!")

    # Clean up
    if os.path.exists(db_path):
        os.remove(db_path)

def test_get_jurisdictions_for_discovery():
    db_path = "test_rfp_scraper_discovery.db"

    class TestDatabaseHandler(DatabaseHandler):
        def __init__(self, db_path):
             self.db_url = None
             self.is_postgres = False
             self.db_path = db_path
             if os.path.dirname(self.db_path):
                 os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
             self._init_sqlite()

    if os.path.exists(db_path):
        os.remove(db_path)

    db = TestDatabaseHandler(db_path)

    # Insert Data
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO states (name) VALUES ('California')")
    state_id = cursor.lastrowid
    cursor.execute("INSERT INTO local_jurisdictions (state_id, name, type) VALUES (?, 'San Diego', 'city')", (state_id,))
    conn.commit()
    conn.close()

    # Load patterns from real file
    local_data = load_json("cities_towns_dictionary.json")
    if not local_data:
        print("Warning: cities_towns_dictionary.json not found by load_json, trying hardcoded path")
        import json
        with open("cities_towns_dictionary.json", "r") as f:
            local_data = json.load(f)

    domain_patterns = local_data.get("domain_patterns", [])

    print("Testing get_jurisdictions_for_discovery...")
    agencies = get_jurisdictions_for_discovery(db, ["California"], domain_patterns)

    assert len(agencies) == 1, f"Expected 1 agency, got {len(agencies)}"
    sd = agencies[0]
    assert sd.name == 'San Diego', f"Expected San Diego, got {sd.name}"
    # Check if URL generation worked (should contain sandiego and ca)
    assert "sandiego" in sd.homepage_url or "sandiegoca" in sd.homepage_url, f"Unexpected URL: {sd.homepage_url}"

    print("✅ get_jurisdictions_for_discovery passed!")

    if os.path.exists(db_path):
        os.remove(db_path)

if __name__ == "__main__":
    try:
        test_get_agencies_for_scraping()
        test_get_jurisdictions_for_discovery()
        print("🎉 All verification tests passed!")
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
