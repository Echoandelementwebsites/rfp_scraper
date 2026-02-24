import asyncio
import json
import os
import re
from typing import List, Dict, Any, Optional
from pydantic import BaseModel

from rfp_scraper_v2.core.models import Agency
from rfp_scraper_v2.core.database import DatabaseHandler
from rfp_scraper_v2.crawlers.pipeline import process_agency
import rfp_scraper_v2.crawlers.pipeline as pipeline
from openai import AsyncOpenAI

# Concurrency Limit for Agencies
SEM_AGENCIES = asyncio.Semaphore(5)

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

async def run_orchestrator(target_states: List[str], manager=None, job_id: str = None, api_key: str = None):
    """
    Main Logic Function for Orchestrator V2.
    """
    if manager:
        manager.add_log(job_id, "🚀 Starting Orchestrator V2...")
    else:
        print("🚀 Starting Orchestrator V2...")

    # Inject API Key if provided
    if api_key:
        os.environ["DEEPSEEK_API_KEY"] = api_key
        # Reload client to pick up new key
        pipeline.client = AsyncOpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com"
        )

    # Database Setup
    db = DatabaseHandler()

    # Load Data
    state_data = load_json("state_agency_dictionary.json")
    local_data = load_json("cities_towns_dictionary.json")

    # Parse with filtering
    if manager: manager.add_log(job_id, f"Parsing agencies for {len(target_states)} states...")

    state_agencies = parse_state_agencies(state_data, target_states)
    local_agencies = parse_local_agencies(local_data, target_states)

    all_agencies = state_agencies + local_agencies

    msg = f"Loaded {len(all_agencies)} total agencies."
    if manager: manager.add_log(job_id, msg)
    else: print(msg)

    if not all_agencies:
        if manager: manager.add_log(job_id, "⚠️ No agencies found for selected states.")
        return

    # Process Loop
    async def bounded_process(a):
        async with SEM_AGENCIES:
            try:
                if manager: manager.add_log(job_id, f"🚀 Starting async extraction for {a.name}...")
                await process_agency(a, db)
                if manager: manager.add_log(job_id, f"✅ Finished {a.name}")
            except Exception as e:
                err_msg = f"❌ Failed {a.name}: {e}"
                if manager: manager.add_log(job_id, err_msg)
                else: print(err_msg)

    tasks = [bounded_process(a) for a in all_agencies]

    if tasks:
        await asyncio.gather(*tasks)

    if manager: manager.add_log(job_id, "✅ Orchestration Complete.")
    else: print("✅ Orchestration Complete.")

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
