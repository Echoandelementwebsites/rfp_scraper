import asyncio
import json
import os
import re
from typing import List, Dict, Any, Optional
from pydantic import BaseModel

from rfp_scraper_v2.core.models import Agency
from rfp_scraper_v2.core.database import DatabaseHandler
from rfp_scraper_v2.crawlers.pipeline import process_agency

# Concurrency Limit for Agencies
SEM_AGENCIES = asyncio.Semaphore(5)

def load_json(filename: str) -> Dict[str, Any]:
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Warning: {filename} not found.")
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

    # Find relevant patterns for this type
    candidates = []

    # Golden Pattern check (always valid fallback)
    golden = f"https://www.{name_clean}{state_clean}.gov"

    for p in patterns:
        if atype in p.get("institution_type", []):
            pat = p["pattern"]
            # Replace placeholders
            url = pat.replace("[cityname]", name_clean)\
                     .replace("[townname]", name_clean)\
                     .replace("[countyname]", name_clean)\
                     .replace("[parishname]", name_clean)\
                     .replace("[state_abbrev]", state_clean)\
                     .replace("[state]", state_clean)

            if not url.startswith("http"):
                # Handle subdomain format vs direct
                if p.get("format") == "subdomain" and not url.startswith("http"):
                     # e.g. ci.phoenix.az.us -> https://ci.phoenix.az.us
                     # usually patterns in JSON don't have protocol
                     pass

                url = f"https://{url}" # Default to https

            candidates.append(url)

    # Return the first candidate or golden
    # Ideally we'd probe, but here we return one for the pipeline to start discovery
    return candidates[0] if candidates else golden

def parse_state_agencies(data: Dict[str, Any]) -> List[Agency]:
    agencies = []
    examples = data.get("state_examples", {})

    # We also have global domain patterns in data['domain_patterns'] but state examples usually have domains.

    state_map = {
        "california": "CA", "texas": "TX", "new_york": "NY", "florida": "FL",
        "louisiana": "LA", "connecticut": "CT", "massachusetts": "MA",
        "illinois": "IL", "arizona": "AZ"
        # Add more if needed or derive from data if possible
    }

    for state_key, state_data in examples.items():
        state_abbr = state_map.get(state_key, state_key[:2].upper()) # Fallback

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

def parse_local_agencies(data: Dict[str, Any]) -> List[Agency]:
    agencies = []
    examples = data.get("state_examples", {})
    domain_patterns = data.get("domain_patterns", [])

    state_map = {
        "california": "CA", "texas": "TX", "new_york": "NY", "florida": "FL",
        "louisiana": "LA", "connecticut": "CT", "massachusetts": "MA",
        "illinois": "IL", "arizona": "AZ"
    }

    for state_key, state_data in examples.items():
        state_abbr = state_map.get(state_key, state_key[:2].upper())

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

async def main():
    print("🚀 Starting Orchestrator V2...")

    # Database Setup
    # "Ensure the local government database table drops the url, category, and verified columns."
    # The DatabaseHandler init handles table creation.
    # We rely on core/database.py schema which is correct (agencies table: id, name, state, type, homepage_url, procurement_url, last_checked).
    db = DatabaseHandler()

    # Load Data
    state_data = load_json("state_agency_dictionary.json")
    local_data = load_json("cities_towns_dictionary.json")

    # Parse in strict order
    print("Parsing State Agencies...")
    state_agencies = parse_state_agencies(state_data)

    print("Parsing Local Agencies...")
    local_agencies = parse_local_agencies(local_data)

    all_agencies = state_agencies + local_agencies
    print(f"Loaded {len(all_agencies)} total agencies.")

    # Process Loop
    async def bounded_process(a):
        async with SEM_AGENCIES:
            try:
                await process_agency(a, db)
            except Exception as e:
                print(f"❌ Error processing {a.name}: {e}")

    tasks = [bounded_process(a) for a in all_agencies]

    if tasks:
        await asyncio.gather(*tasks)
    else:
        print("No agencies to process.")

    print("✅ Orchestration Complete.")

if __name__ == "__main__":
    asyncio.run(main())
