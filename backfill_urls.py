import os
import pandas as pd
from dotenv import load_dotenv
from rfp_scraper_v2.core.database import DatabaseHandler
from rfp_scraper.cisa_manager import CisaManager
from rfp_scraper.utils import get_state_abbreviation

def run_backfill():
    print("Loading environment and connecting to DB...")
    load_dotenv()
    db = DatabaseHandler()

    print("Loading CISA Registry...")
    cisa = CisaManager()
    cisa._load_data()

    states_df = db.get_all_states()
    state_map = {row['id']: row['name'] for _, row in states_df.iterrows()}

    agencies_df = db.get_all_agencies()

    # Filter for agencies missing a URL
    missing_urls = agencies_df[agencies_df['url'].isnull() | (agencies_df['url'] == '')]
    print(f"Found {len(missing_urls)} agencies missing URLs. Starting backfill...")

    updated_count = 0

    for _, agency in missing_urls.iterrows():
        agency_id = agency['id']
        name = agency['organization_name']
        state_id = agency['state_id']
        state_name = state_map.get(state_id)

        if not state_name:
            continue

        state_abbr = get_state_abbreviation(state_name)

        # Parse the clean jurisdiction name from our strict UI conventions
        clean_name = name
        if " - " in name:
            clean_name = name.split(" - ")[0]
        if clean_name.startswith("City of "):
            clean_name = clean_name.replace("City of ", "")
        elif clean_name.startswith("Town of "):
            clean_name = clean_name.replace("Town of ", "")

        # Lookup in CISA
        cisa_url = cisa.get_agency_url(clean_name, state_abbr)

        # Fallback for counties if it didn't find "X County"
        if not cisa_url and " County" in clean_name:
            cisa_url = cisa.get_agency_url(clean_name.replace(" County", ""), state_abbr)

        if cisa_url:
            db.update_agency_url(agency_id, cisa_url)
            print(f"✅ Updated: {name} -> {cisa_url}")
            updated_count += 1
        else:
            print(f"❌ Not in CISA: {name} (Searched: {clean_name})")

    print(f"\n🎉 Backfill complete! Successfully updated {updated_count} URLs.")

if __name__ == "__main__":
    run_backfill()
