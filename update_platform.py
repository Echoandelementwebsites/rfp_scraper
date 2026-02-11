import json
import os
import re
import sys

# Ensure project root is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from rfp_scraper.db import DatabaseHandler
from rfp_scraper.discovery import discover_agency_url, is_better_url
from rfp_scraper.utils import get_state_abbreviation
from rfp_scraper.cisa_manager import CisaManager

STATE_SOURCES = {
    "Alabama": "https://purchasing.alabama.gov/",
    "Alaska": "https://iris-vss.alaska.gov/webapp/PRDVSS1X1/AltSelfService",
    "Arizona": "https://spo.az.gov/",
    "Arkansas": "https://www.arkansasedc.com/business-resources/small-business-entrepreneurship-development/procurement-opportunities",
    "California": "https://caleprocure.ca.gov/pages/Events-BS3/event-search.aspx",
    "Colorado": "https://vss.state.co.us/",
    "Connecticut": "https://portal.ct.gov/das/ctsource/bidboard",
    "Delaware": "https://mymarketplace.delaware.gov/",
    "District of Columbia": "https://ocp.dc.gov/service/ocp-solicitations",
    "Florida": "https://www.myflorida.com/apps/vbs/vbs_www.search_r1.crit1",
    "Georgia": "https://ssl.doas.state.ga.us/gpr/",
    "Hawaii": "https://hands.ehawaii.gov/hands/opportunities",
    "Idaho": "https://purchasing.idaho.gov/",
    "Illinois": "https://www.bidbuy.illinois.gov/bso/",
    "Indiana": "https://www.in.gov/idoa/procurement/",
    "Iowa": "https://vss.iowa.gov/webapp/VSS_ON/AltSelfService",
    "Kansas": "https://admin.ks.gov/offices/procurement-and-contracts",
    "Kentucky": "https://finance.ky.gov/office-of-the-controller/office-of-procurement-services/Pages/default.aspx",
    "Louisiana": "https://wwwcfprd.doa.louisiana.gov/osp/lapac/pubMain.cfm",
    "Maine": "https://www.maine.gov/dafs/bbm/procurementservices/",
    "Maryland": "https://procurement.maryland.gov/",
    "Massachusetts": "https://www.commbuys.com/bso/",
    "Michigan": "https://sigma.michigan.gov/webapp/PRDVSS2X1/AltSelfService",
    "Minnesota": "http://www.mmd.admin.state.mn.us/process/admin/postings.asp",
    "Mississippi": "https://www.ms.gov/dfa/contract_bid_search/",
    "Missouri": "https://archive.oa.mo.gov/purch/bids/",
    "Montana": "https://spb.mt.gov/",
    "Nebraska": "https://das.nebraska.gov/materiel/purchasing.html",
    "Nevada": "https://purchasing.nv.gov/",
    "New Hampshire": "https://apps.das.nh.gov/bidscontracts/bids.aspx",
    "New Jersey": "https://www.nj.gov/treasury/purchase/",
    "New Mexico": "https://www.generalservices.state.nm.us/state-purchasing/",
    "New York": "https://ogs.ny.gov/design-construction-bid-openings",
    "North Carolina": "https://evp.nc.gov/",
    "North Dakota": "https://apps.nd.gov/csd/spo/services/bidder/main.htm",
    "Ohio": "https://ohiobuys.ohio.gov/page.aspx/en/rfp/request_browse_public",
    "Oklahoma": "https://oklahoma.gov/omes/services/purchasing.html",
    "Oregon": "https://oregonbuys.gov/bso/",
    "Pennsylvania": "https://www.emarketplace.state.pa.us/Search.aspx",
    "Rhode Island": "https://www.ridop.ri.gov/",
    "South Carolina": "https://procurement.sc.gov/",
    "South Dakota": "https://boa.sd.gov/central-services/procurement-management/",
    "Tennessee": "https://www.tn.gov/generalservices/procurement.html",
    "Texas": "https://www.txsmartbuy.gov/esbd",
    "Utah": "https://purchasing.utah.gov/",
    "Vermont": "https://bgs.vermont.gov/purchasing",
    "Virginia": "https://eva.virginia.gov/",
    "Washington": "https://pr-webs-vendor.des.wa.gov/",
    "West Virginia": "https://www.state.wv.us/admin/purchase/",
    "Wisconsin": "https://esupplier.wi.gov/",
    "Wyoming": "https://ai.wyo.gov/divisions/general-services/purchasing",
    "Puerto Rico": "https://recovery.pr.gov/en/cor3-rfps-and-contracts",
    "Guam": "https://gsa.doa.guam.gov/",
    "Virgin Islands": "https://dpp.vi.gov/",
    "American Samoa": "https://procurement.as.gov/",
    "Northern Mariana Islands": "https://finance.gov.mp/procurement-services.php"
}

CONFIG_PATH = os.path.join("rfp_scraper", "config.json")
SCRAPERS_DIR = os.path.join("rfp_scraper", "scrapers")

def update_config():
    """Reads rfp_scraper/config.json, merge in missing states, save back."""
    if not os.path.exists(CONFIG_PATH):
        print(f"Error: {CONFIG_PATH} not found.")
        return

    try:
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)
    except Exception as e:
        print(f"Error loading config.json: {e}")
        return

    updated = False
    for state, url in STATE_SOURCES.items():
        if state not in config:
            print(f"Adding {state} to config.")
            config[state] = url
            updated = True
        else:
            # print(f"Skipping {state} (already exists).")
            pass

    if updated:
        try:
            with open(CONFIG_PATH, "w") as f:
                json.dump(config, f, indent=4)
            print("Config updated successfully.")
        except Exception as e:
            print(f"Error saving config.json: {e}")
    else:
        print("No config changes needed.")

def generate_scrapers():
    """Iterate state list and generate missing scraper modules."""
    if not os.path.exists(SCRAPERS_DIR):
        print(f"Error: {SCRAPERS_DIR} not found.")
        return

    for state in STATE_SOURCES.keys():
        # Convert "North Dakota" -> "north_dakota"
        filename = state.lower().replace(" ", "_").replace(".", "") + ".py"
        filepath = os.path.join(SCRAPERS_DIR, filename)

        # Convert "North Dakota" -> "NorthDakotaScraper"
        # Also handle "District of Columbia" -> "DistrictOfColumbiaScraper"
        class_name_base = state.title().replace(" ", "")
        # Remove any non-alphanumeric chars if present (though STATE_SOURCES keys are clean)
        class_name_base = re.sub(r'[^a-zA-Z0-9]', '', class_name_base)
        class_name = f"{class_name_base}Scraper"

        if os.path.exists(filepath):
            # print(f"Scraper for {state} already exists at {filepath}.")
            continue

        print(f"Generating scraper for {state} at {filepath}...")

        content = f"""from rfp_scraper.scrapers.generic import GenericScraper

class {class_name}(GenericScraper):
    pass
"""
        try:
            with open(filepath, "w") as f:
                f.write(content)
        except Exception as e:
            print(f"Error writing {filepath}: {e}")

def run_cisa_sync():
    """
    Downloads CISA registry and syncs all states in the database.
    """
    print("\n--- Starting CISA Registry Sync ---")
    try:
        db = DatabaseHandler()
        states = db.get_all_states()
        if states.empty:
            print("No states found in database. Skipping CISA sync.")
            return

        cisa_manager = CisaManager()

        total_added = 0
        total_updated = 0

        for _, row in states.iterrows():
            state_id = row['id']
            state_name = row['name']
            state_abbr = get_state_abbreviation(state_name)

            if not state_abbr:
                continue

            print(f"Syncing {state_name} ({state_abbr})...")
            stats = cisa_manager.sync_state_database(db, state_id, state_abbr)
            total_added += stats['added']
            total_updated += stats['updated']

        print(f"CISA Sync Complete. Added {total_added}, Updated {total_updated} agencies.")

    except Exception as e:
        print(f"Error during CISA Sync: {e}")

def sync_and_repair_agencies():
    """
    Iterates through all agencies in the database.
    1. Verifies existing URL (Dead link check).
    2. Discovers potential new URL (Smart Discovery).
    3. Updates database if a better URL is found.
    """
    print("Starting Agency Sync & Repair...")
    try:
        db = DatabaseHandler()
        agencies = db.get_all_agencies()
    except Exception as e:
        print(f"Error accessing database: {e}")
        return

    if agencies.empty:
        print("No agencies found in database.")
        return

    total = len(agencies)
    print(f"Processing {total} agencies...")

    updated_count = 0
    checked_count = 0

    for index, row in agencies.iterrows():
        checked_count += 1
        agency_id = row['id']
        name = row['organization_name']
        state_name = row['state_name']
        current_url = row['url']
        category = row['category']

        # Log progress every 10
        if checked_count % 10 == 0:
            print(f"Checked {checked_count}/{total}...")

        # Parse Clean Name from "Name (ST) Category" format if possible
        # Example: "Milford (CT) Public Works"
        state_abbr = get_state_abbreviation(state_name)

        clean_name = name
        match = re.match(r"^(.*?)\s\([A-Z]{2}\)\s(.*)$", name)
        if match:
            clean_name = match.group(1)
            # Extracted category might differ from row['category'], but we trust DB category more for logic

        # Discover
        try:
            new_url = discover_agency_url(clean_name, state_abbr, state_name=state_name, jurisdiction_type=category)

            if new_url:
                if is_better_url(new_url, current_url):
                    print(f"🔄 Updating {name}: {current_url} -> {new_url}")
                    db.update_agency_url(agency_id, new_url)
                    updated_count += 1
        except Exception as e:
            print(f"Error processing {name}: {e}")

    print(f"Sync Complete. Updated {updated_count} agencies.")

if __name__ == "__main__":
    print("Starting Platform Update...")
    update_config()
    generate_scrapers()

    # Run CISA Sync first to seed/repair DB
    run_cisa_sync()

    print("\n--- Starting Agency Sync & Repair ---")
    sync_and_repair_agencies()
    print("Platform Update Complete.")
