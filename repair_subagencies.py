import sys
import os
import sqlite3
import argparse
from typing import List, Optional

# Ensure the project root is in the Python path
sys.path.append(os.getcwd())

from rfp_scraper.db import DatabaseHandler
from rfp_scraper.discovery import DiscoveryEngine, is_root_domain
from rfp_scraper.ai_parser import DeepSeekClient

# Configuration
KEYWORDS = ["Housing Authority", "School District", "Water District", "Transit", "Development Authority"]
DOMAIN_RULES = ['.gov', '.org', '.edu', '.us']

def repair_agencies(db_handler: DatabaseHandler, discovery: DiscoveryEngine, ai_client: DeepSeekClient, commit: bool = False):
    """
    Core logic for repairing sub-agencies.
    """
    print(f"Starting Sub-Agency Repair Script...")
    print(f"Mode: {'COMMIT' if commit else 'DRY RUN'}")
    print("-" * 60)

    # Custom SQL Query to fetch agencies with state names
    conn = sqlite3.connect(db_handler.db_path)
    cursor = conn.cursor()

    query = """
        SELECT a.id, a.organization_name, a.url, s.name as state_name
        FROM agencies a
        JOIN states s ON a.state_id = s.id
    """

    try:
        cursor.execute(query)
        rows = cursor.fetchall()
    except Exception as e:
        print(f"Error executing query: {e}")
        conn.close()
        return

    conn.close()

    repair_count = 0

    for row in rows:
        agency_id, name, url, state_name = row

        # 1. Check Keywords
        has_keyword = any(k.lower() in name.lower() for k in KEYWORDS)
        if not has_keyword:
            continue

        # 2. Check if URL is a Root Domain
        if not is_root_domain(url):
            continue

        # Flagged for repair
        print(f"⚠️ Repairing: {name}")
        print(f"   Current URL: {url} (Root Domain)")
        print(f"   State: {state_name}")

        # 3. Force Re-Discovery
        search_query = f"{name} {state_name} official site"
        print(f"   🔍 Searching: '{search_query}'")

        candidates = discovery.fetch_search_context(search_query)

        if not candidates:
            print("   ❌ No search candidates found.")
            continue

        # 4. AI Identification
        best_url = ai_client.identify_best_agency_url(candidates, name, DOMAIN_RULES)

        if not best_url:
            print("   ❌ AI could not identify a specific URL.")
            continue

        # 5. Restore Specificity Logic
        # Must be different from old URL
        if best_url == url:
             print(f"   ⏭️  AI returned same URL ({best_url}). Skipping.")
             continue

        if is_root_domain(best_url):
             print(f"   ⚠️  New URL is also a Root Domain ({best_url}). Skipping based on rules.")
             continue

        print(f"   ✅ Found Better URL: {best_url}")

        if commit:
            db_handler.update_agency_url(agency_id, best_url)
            print("   💾 Database Updated.")
        else:
            print("   [DRY RUN] Would update database.")

        repair_count += 1
        print("-" * 30)

    print(f"\nFinished. Repaired {repair_count} agencies.")

def main():
    parser = argparse.ArgumentParser(description="Repair sub-agency URLs that were incorrectly set to generic town root domains.")
    parser.add_argument("--commit", action="store_true", help="Commit changes to the database. Defaults to Dry Run.")
    args = parser.parse_args()

    # Initialize components
    db_handler = DatabaseHandler()
    discovery = DiscoveryEngine()
    ai_client = DeepSeekClient()

    repair_agencies(db_handler, discovery, ai_client, commit=args.commit)

if __name__ == "__main__":
    main()
