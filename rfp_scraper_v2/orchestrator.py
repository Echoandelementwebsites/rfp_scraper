import asyncio
import json
import os
import re
from typing import List, Dict, Any, Optional
import pandas as pd
from pydantic import BaseModel
from crawl4ai import AsyncWebCrawler

from rfp_scraper_v2.core.models import Agency
from rfp_scraper_v2.core.database import DatabaseHandler, ABBR_TO_STATE
from rfp_scraper_v2.crawlers.pipeline import process_agency, discover_portal
import rfp_scraper_v2.crawlers.pipeline as pipeline
from openai import AsyncOpenAI
from rfp_scraper.utils import get_state_abbreviation
from rfp_scraper.cisa_manager import CisaManager

# Concurrency Limit for Agencies
# SEM_AGENCIES removed to prevent event loop binding issues

# Invert ABBR_TO_STATE for lookup
STATE_TO_ABBR = {v: k for k, v in ABBR_TO_STATE.items()}

def load_json(filename: str) -> Dict[str, Any]:
    # Robust path handling: Look in project root relative to this file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))

    # Try multiple locations
    paths = [
        os.path.join(project_root, filename),
        os.path.join(current_dir, filename), # if local
        filename # if cwd
    ]

    for path in paths:
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error reading {path}: {e}")
                return {}

    print(f"Warning: {filename} not found in {paths}")
    return {}

def normalize_name(name: str) -> str:
    return name.lower().replace("city of ", "").replace("town of ", "").replace(" county", "").replace(" ", "")

def generate_homepage_url(name: str, state_abbr: str, atype: str, patterns: List[Dict], specific_domain: Optional[str] = None) -> str:
    """
    Generates a homepage URL.
    Prioritizes specific domain from JSON if available.
    Otherwise uses patterns.
    """
    if specific_domain:
        if not specific_domain.startswith("http"):
            return f"https://{specific_domain}"
        return specific_domain

    name_clean = normalize_name(name)
    state_clean = state_abbr.lower()

    candidates = []
    golden = f"https://www.{name_clean}{state_clean}.gov"

    for p in patterns:
        if atype in p.get("institution_type", []):
            pat = p["pattern"]
            url = pat.replace("[cityname]", name_clean)\
                     .replace("[townname]", name_clean)\
                     .replace("[countyname]", name_clean)\
                     .replace("[parishname]", name_clean)\
                     .replace("[state_abbrev]", state_clean)\
                     .replace("[state]", state_clean)

            if not url.startswith("http"):
                url = f"https://{url}"

            candidates.append(url)

    return candidates[0] if candidates else golden

def get_agencies_for_scraping(db, target_states: List[str]) -> List[Agency]:
    """Pulls verified agencies from the DB for the Scraping pipeline."""
    df = db.get_all_agencies()
    if df.empty: return []

    if target_states:
        df = df[df['state_name'].isin(target_states)]

    agencies = []
    for _, row in df.iterrows():
        if not row.get('url'): continue

        # Handle procurement_url: Ensure it is None if missing/NaN so discovery runs
        p_url = row.get('procurement_url')
        if pd.isna(p_url) or p_url == "":
            p_url = None

        agencies.append(Agency(
            # The 'or' guarantees that if the DB returns None, it falls back to a string
            name=row.get('organization_name') or 'Unknown',
            state=row.get('state_name') or 'Unknown',
            type=row.get('jurisdiction_type') or 'state_agency',
            homepage_url=row.get('url') or '',
            procurement_url=p_url
        ))
    return agencies

def get_jurisdictions_for_discovery(db, target_states: List[str], domain_patterns: List[Dict]) -> List[Agency]:
    """Pulls local jurisdictions from the DB and guesses their URLs for the Discovery pipeline."""
    states_df = db.get_all_states()
    juris_df = db.get_local_jurisdictions()
    if states_df.empty or juris_df.empty: return []

    agencies = []
    for target in target_states:
        state_row = states_df[states_df['name'] == target]
        if state_row.empty: continue

        state_id = state_row.iloc[0]['id']
        state_abbr = get_state_abbreviation(target)
        if not state_abbr: continue

        state_juris = juris_df[juris_df['state_id'] == state_id]
        for _, row in state_juris.iterrows():
            name = row.get('name') or 'Unknown'
            j_type = row.get('type') or 'unknown'

            guessed_url = generate_homepage_url(name, state_abbr, j_type, domain_patterns)

            agencies.append(Agency(
                name=name,
                state=target,
                type=j_type,
                homepage_url=guessed_url,
                procurement_url=None
            ))
    return agencies

def parse_state_agencies(data: Dict[str, Any], target_states: List[str] = None) -> List[Agency]:
    agencies = []
    examples = data.get("state_examples", {})

    state_map = {
        "california": "CA", "texas": "TX", "new_york": "NY", "florida": "FL",
        "louisiana": "LA", "connecticut": "CT", "massachusetts": "MA",
        "illinois": "IL", "arizona": "AZ"
    }

    # Normalize target states to abbreviations if possible, or support full names
    # Assuming target_states contains Full Names (e.g. "California")
    target_abbrs = []
    if target_states:
        # Create reverse map
        name_to_abbr = {k.title(): v for k, v in state_map.items()}
        # Add basic logic
        for t in target_states:
            if len(t) == 2: target_abbrs.append(t.upper())
            else: target_abbrs.append(name_to_abbr.get(t.title(), t[:2].upper()))

    for state_key, state_data in examples.items():
        state_abbr = state_map.get(state_key, state_key[:2].upper())

        # Filter
        if target_states and state_abbr not in target_abbrs:
            continue

        for item in state_data.get("example_agencies", []):
            url = item.get("domain")
            if url and not url.startswith("http"):
                url = f"https://{url}"

            agencies.append(Agency(
                name=item["name"],
                state=state_abbr,
                type="state_agency",
                homepage_url=url
            ))

    return agencies

def parse_local_agencies(data: Dict[str, Any], target_states: List[str] = None) -> List[Agency]:
    agencies = []
    examples = data.get("state_examples", {})
    domain_patterns = data.get("domain_patterns", [])

    state_map = {
        "california": "CA", "texas": "TX", "new_york": "NY", "florida": "FL",
        "louisiana": "LA", "connecticut": "CT", "massachusetts": "MA",
        "illinois": "IL", "arizona": "AZ"
    }

    target_abbrs = []
    if target_states:
        name_to_abbr = {k.title(): v for k, v in state_map.items()}
        for t in target_states:
            if len(t) == 2: target_abbrs.append(t.upper())
            else: target_abbrs.append(name_to_abbr.get(t.title(), t[:2].upper()))

    for state_key, state_data in examples.items():
        state_abbr = state_map.get(state_key, state_key[:2].upper())

        if target_states and state_abbr not in target_abbrs:
            continue

        for item in state_data.get("example_governments", []):
            atype = item.get("type", "city")
            name = item["name"]
            specific_domain = item.get("domain")

            generated_url = generate_homepage_url(name, state_abbr, atype, domain_patterns, specific_domain)

            agencies.append(Agency(
                name=name,
                state=state_abbr,
                type=atype,
                homepage_url=generated_url
            ))

    return agencies

async def discover_agency_only(agency: Agency, db, manager=None, job_id=None, api_key: str = None):
    """
    Step 1 Only: Finds the portal URL and updates the DB without extraction.
    """
    async with AsyncWebCrawler() as crawler:
        procurement_url = agency.procurement_url

        if procurement_url == "NOT_FOUND":
            if manager: manager.add_log(job_id, f"ℹ️ Skipping (Previously flagged as NO PORTAL for {agency.name})")
            else: print(f"ℹ️ Skipping (Previously flagged as NO PORTAL for {agency.name})")
            return

        if not procurement_url:
            if agency.homepage_url:
                if manager: manager.add_log(job_id, f"🔍 Discovering portal for {agency.name}...")
                else: print(f"🔍 Discovering portal for {agency.name}...")

                procurement_url = await discover_portal(crawler, agency.homepage_url, api_key)

                if procurement_url:
                    if manager: manager.add_log(job_id, f"✅ Found: {procurement_url}")
                    else: print(f"✅ Found: {procurement_url}")
                    db.update_agency_procurement_url(agency.name, agency.state, procurement_url)
                else:
                    if manager: manager.add_log(job_id, f"⚠️ No portal found for {agency.name}. Flagging as NOT_FOUND.")
                    else: print(f"⚠️ No portal found for {agency.name}. Flagging as NOT_FOUND.")
                    db.update_agency_procurement_url(agency.name, agency.state, "NOT_FOUND")
            else:
                 if manager: manager.add_log(job_id, f"⚠️ No homepage URL for {agency.name}")
                 else: print(f"⚠️ No homepage URL for {agency.name}")
        else:
             if manager: manager.add_log(job_id, f"ℹ️ Already has portal: {procurement_url}")
             else: print(f"ℹ️ Already has portal: {procurement_url}")
             db.update_agency_procurement_url(agency.name, agency.state, procurement_url)

async def run_discovery_orchestrator(target_states: List[str], manager=None, job_id: str = None, api_key: str = None):
    """
    Discovery-Only Logic for Orchestrator V2.
    """
    if manager:
        manager.add_log(job_id, "🚀 Starting V2 Discovery Orchestrator...")

    db = DatabaseHandler()

    # --- NEW: Implicit CISA Synchronization ---
    if manager: manager.add_log(job_id, f"Synchronizing {len(target_states)} states with CISA Registry...")
    cisa_manager = CisaManager()
    states_df = db.get_all_states()

    for target in target_states:
        state_row = states_df[states_df['name'] == target]
        if not state_row.empty:
            state_id = int(state_row.iloc[0]['id'])
            state_abbr = get_state_abbreviation(target)
            if state_abbr:
                cisa_manager.sync_state_database(db, state_id, state_abbr)
    # ------------------------------------------

    local_data = load_json("cities_towns_dictionary.json")
    domain_patterns = local_data.get("domain_patterns", [])

    if manager: manager.add_log(job_id, f"Fetching targets from DB for {len(target_states)} states...")

    # 1. Pull local jurisdictions (Cities/Counties)
    local_agencies = get_jurisdictions_for_discovery(db, target_states, domain_patterns)

    # 2. Pull state agencies (CISA Registry) and filter for those needing discovery
    state_agencies = get_agencies_for_scraping(db, target_states)
    state_agencies_needing_discovery = [a for a in state_agencies if not a.procurement_url]

    # 3. Combine both lists
    all_agencies = local_agencies + state_agencies_needing_discovery

    msg = f"Loaded {len(all_agencies)} total agencies/jurisdictions for discovery."
    if manager: manager.add_log(job_id, msg)

    if not all_agencies:
        if manager: manager.add_log(job_id, "⚠️ No jurisdictions found in DB. Go to Tab 1A and identify them first.")
        return

    # Process Loop
    sem_agencies = asyncio.Semaphore(5)

    async def bounded_process(a):
        async with sem_agencies:
            try:
                await discover_agency_only(a, db, manager, job_id, api_key)
            except Exception as e:
                err_msg = f"❌ Failed {a.name}: {e}"
                if manager: manager.add_log(job_id, err_msg)
                else: print(err_msg)

    tasks = [bounded_process(a) for a in all_agencies]

    if tasks:
        await asyncio.gather(*tasks)

    if manager: manager.add_log(job_id, "✅ Discovery Orchestration Complete.")

def run_v2_discovery_task(job_id: str, manager, target_states: list, api_key: str):
    """
    Sync-to-Async Bridge for Tab 2: Agency Discovery.
    Reads the local JSON dictionaries, discovers procurement portals
    using the v2 pipeline, and saves them to the database.
    """
    manager.add_log(job_id, f"🔍 Starting V2 JSON Dictionary Discovery for {len(target_states)} states...")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(run_discovery_orchestrator(target_states, manager, job_id, api_key))

        manager.add_log(job_id, "✅ V2 Discovery completed successfully.")
    except Exception as e:
        manager.add_log(job_id, f"❌ Discovery Crash: {str(e)}")
        raise e
    finally:
        pending = asyncio.all_tasks(loop)
        for task in pending: task.cancel()
        if pending: loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()

async def run_orchestrator(target_states: List[str], manager=None, job_id: str = None, api_key: str = None):
    """
    Main Logic Function for Orchestrator V2.
    """
    if manager:
        manager.add_log(job_id, "🚀 Starting Orchestrator V2...")
    else:
        print("🚀 Starting Orchestrator V2...")

    db = DatabaseHandler()

    if manager: manager.add_log(job_id, f"Fetching verified agencies from DB for {len(target_states)} states...")
    all_agencies = get_agencies_for_scraping(db, target_states)

    msg = f"Loaded {len(all_agencies)} target agencies for scraping."
    if manager: manager.add_log(job_id, msg)
    else: print(msg)

    if not all_agencies:
        if manager: manager.add_log(job_id, "⚠️ No agencies found in DB. Go to Tab 2 and run Discovery or CISA Repair first.")
        return

    # Process Loop
    sem_agencies = asyncio.Semaphore(5)

    async def bounded_process(a):
        async with sem_agencies:
            try:
                if manager: manager.add_log(job_id, f"🚀 Starting async extraction for {a.name}...")
                await process_agency(a, db, api_key)
                if manager: manager.add_log(job_id, f"✅ Finished {a.name}")
            except Exception as e:
                err_msg = f"❌ Failed {a.name}: {e}"
                if manager: manager.add_log(job_id, err_msg)
                else: print(err_msg)

    tasks = [bounded_process(a) for a in all_agencies]

    if tasks:
        await asyncio.gather(*tasks)

    # --- Graceful Termination Sequence ---
    termination_msg = (
        f"✅ SESSION TERMINATED GRACEFULLY.\n"
        f"All {len(all_agencies)} agency targets across {len(target_states)} states have been processed. "
        f"Background task shutting down cleanly."
    )

    if manager:
        manager.add_log(job_id, "━" * 40)
        manager.add_log(job_id, termination_msg)
        manager.add_log(job_id, "━" * 40)
    else:
        print("\n" + "━" * 40)
        print(termination_msg)
        print("━" * 40 + "\n")

def run_v2_scraping_task(job_id: str, manager, target_states: list, api_key: str):
    """
    Sync-to-Async Bridge:
    This function is called by the Streamlit threading JobManager. It spins up an
    isolated asyncio event loop inside the background thread to run the v2 engine.
    """
    manager.add_log(job_id, f"🚀 Initializing v2 Async Engine for {len(target_states)} states...")

    # Create a fresh event loop for this background thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # Run the asynchronous orchestrator
        loop.run_until_complete(run_orchestrator(target_states=target_states, manager=manager, job_id=job_id, api_key=api_key))
        manager.add_log(job_id, "✅ V2 Async Engine completed successfully.")
    except Exception as e:
        manager.add_log(job_id, f"❌ Engine Crash: {str(e)}")
        raise e
    finally:
        # Clean up pending tasks to avoid LiteLLM/Playwright zombie warnings
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()

if __name__ == "__main__":
    # Standalone test
    asyncio.run(run_orchestrator(target_states=["Connecticut"]))
