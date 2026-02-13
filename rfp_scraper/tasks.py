import pandas as pd
import random
import logging
from playwright.sync_api import sync_playwright
from typing import List

from rfp_scraper.factory import ScraperFactory
from rfp_scraper.scrapers.hierarchical import HierarchicalScraper
from rfp_scraper.discovery import DiscoveryEngine, discover_agency_url, is_better_url, find_special_district_domain
from rfp_scraper.config_loader import load_agency_template, extract_search_scope, get_local_search_scope, SPECIAL_CATEGORIES
from rfp_scraper.db import DatabaseHandler
from rfp_scraper.ai_parser import DeepSeekClient
from rfp_scraper.utils import validate_url, check_url_reachability, get_state_abbreviation

BROWSER_PROFILES = [
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "platform": "Win32",
        "vendor": "Google Inc.",
        "screen": {"width": 1920, "height": 1080},
        "sec_ch_ua_platform": '"Windows"',
        "sec_ch_ua": '"Google Chrome";v="120", "Chromium";v="120", "Not?A_Brand";v="24"'
    },
    {
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "platform": "MacIntel",
        "vendor": "Google Inc.",
        "screen": {"width": 1440, "height": 900},
        "sec_ch_ua_platform": '"macOS"',
        "sec_ch_ua": '"Google Chrome";v="120", "Chromium";v="120", "Not?A_Brand";v="24"'
    }
]

def run_scraping_task(job_id, manager, states_to_scrape, api_key):
    """
    Background task for scraping RFPs.
    """
    factory = ScraperFactory()
    all_results = pd.DataFrame()
    total_states = len(states_to_scrape)

    manager.update_progress(job_id, 0.0, "Initializing scraper...")

    try:
        with sync_playwright() as p:
            # --- STRICT VISIBLE LAUNCH ---
            # No try/except fallback. If this fails, we WANT it to fail
            # (so we know Xvfb isn't working), rather than falling back to blocked headless mode.
            browser = p.chromium.launch(
                channel="chrome",  # Uses local Google Chrome
                headless=False,    # STRICTLY FALSE
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-infobars",
                    "--start-maximized",
                    "--disable-extensions",
                    "--ignore-certificate-errors"
                ],
                ignore_default_args=["--enable-automation"]
            )

            manager.add_log(job_id, "🌍 Browser launched in Visible Stealth Mode.")

            for i, state in enumerate(states_to_scrape):
                # Update progress
                progress = (i) / total_states
                manager.update_progress(job_id, progress, f"Scraping {state}...")

                # Pick a random profile for this state run
                profile = random.choice(BROWSER_PROFILES)

                # Context with Stealth Headers & Consistent Profile
                context_args = {
                    "user_agent": profile["ua"],
                    "locale": "en-US",
                    "timezone_id": "America/New_York",
                    "viewport": profile["screen"],
                    "extra_http_headers": {
                        "Accept-Language": "en-US,en;q=0.9",
                        "Sec-Ch-Ua": profile["sec_ch_ua"],
                        "Sec-Ch-Ua-Mobile": "?0",
                        "Sec-Ch-Ua-Platform": profile["sec_ch_ua_platform"],
                        "Upgrade-Insecure-Requests": "1",
                    }
                }

                # Proxy Injection
                if factory.config.get("proxy"):
                    context_args["proxy"] = factory.config["proxy"]

                context = browser.new_context(**context_args)

                # Override JS properties to match the profile
                context.add_init_script(f"""
                    Object.defineProperty(navigator, 'platform', {{get: () => '{profile["platform"]}'}});
                    Object.defineProperty(navigator, 'vendor', {{get: () => '{profile["vendor"]}'}});
                    Object.defineProperty(navigator, 'webdriver', {{get: () => undefined}});
                """)

                try:
                    base_scraper = factory.get_scraper(state)
                    # Always use HierarchicalScraper (Deep Scan)
                    scraper = HierarchicalScraper(state, base_scraper=base_scraper, api_key=api_key)

                    # Create a new page for each state to keep it clean
                    page = context.new_page()

                    try:
                        df = scraper.scrape(page)

                        if not df.empty:
                            df["SourceState"] = state
                            all_results = pd.concat([all_results, df], ignore_index=True)
                            manager.add_log(job_id, f"✅ {state}: Found {len(df)} items.")
                        else:
                            manager.add_log(job_id, f"ℹ️ {state}: No items found.")

                    finally:
                        page.close()

                except Exception as e:
                    manager.add_log(job_id, f"❌ Error scraping {state}: {str(e)}")

                finally:
                    # Close context after each state
                    context.close()

            browser.close()

    except Exception as e:
        manager.add_log(job_id, f"🔥 Critical Error: {str(e)}")
        raise e

    manager.update_progress(job_id, 1.0, "Scraping Complete!")
    return all_results

def run_discovery_task(job_id, manager, target_states: List[str], api_key: str):
    """
    Background task for Agency Discovery.
    """
    db = DatabaseHandler()
    discovery_engine = DiscoveryEngine()
    ai_client = DeepSeekClient(api_key=api_key)

    # Load Template (Standard Agencies)
    template = load_agency_template()
    search_scope = extract_search_scope(template)

    # Refresh states from DB to get IDs
    df_current_states = db.get_all_states()

    # Prepare task list
    tasks = [] # (state_id, state_name, type, name, category, phase)

    manager.update_progress(job_id, 0.0, "Preparing discovery tasks...")

    for state_name in target_states:
        state_row = df_current_states[df_current_states['name'] == state_name]
        if state_row.empty:
            manager.add_log(job_id, f"⚠️ State {state_name} not found in DB. Skipping.")
            continue

        state_id = int(state_row.iloc[0]['id'])

        # Phase 1: Standard Agencies (State Level)
        for agency_type in search_scope:
            tasks.append({
                "state_id": state_id, "state_name": state_name,
                "name": agency_type, "category": "state_agency",
                "phase": "standard", "jurisdiction_id": None
            })

        # Phase 2: Local Governments (AI-Native)
        local_jurisdictions = db.get_local_jurisdictions(state_id=state_id)

        for _, juris_row in local_jurisdictions.iterrows():
            juris_id = int(juris_row['id'])
            juris_name = juris_row['name']
            juris_type = juris_row['type']

            # Get simple list of categories (e.g. ['Public Works', 'Police', 'Housing Authority'])
            categories = get_local_search_scope(juris_type)

            for category in categories:
                tasks.append({
                    "state_id": state_id,
                    "state_name": state_name,
                    "name": juris_name,
                    "category": category,
                    "phase": "ai_native_local",  # New Phase Name
                    "jurisdiction_id": juris_id,
                    "juris_type": juris_type
                })

    total_items = len(tasks)
    items_completed = 0
    new_verified_count = 0

    manager.add_log(job_id, f"Starting discovery for {total_items} items across {len(target_states)} states.")

    for task in tasks:
        items_completed += 1
        progress = items_completed / total_items

        state_name = task["state_name"]
        name = task["name"]
        category = task["category"]

        # Log periodically or for important events to avoid spamming logs
        if items_completed % 10 == 0:
            manager.update_progress(job_id, progress, f"Processing {state_name}: {name} ({items_completed}/{total_items})...")
        else:
            manager.update_progress(job_id, progress)

        found_url = None

        if task["phase"] == "standard":
            # Standard Discovery (AI + Browser)
            found_url, method = discovery_engine.find_agency_url(state_name, name, ai_client)

            if found_url:
                # Deduplicate using standard logic (checks URL)
                if not db.agency_exists(task["state_id"], found_url):
                    db.add_agency(task["state_id"], name, found_url, verified=True, category=category)
                    new_verified_count += 1
                    manager.add_log(job_id, f"✅ Found Standard: {name} -> {found_url}")

        elif task["phase"] == "ai_native_local":
            # DIRECT DOMAIN DISCOVERY LOGIC with TIERED SUPPORT
            juris_name = task["name"]
            juris_type = task["juris_type"]
            state_abbr = get_state_abbreviation(task["state_name"])

            # Step 1: Use Smart Discovery to find verified City/Town URL (ideally Bids page)
            main_domain_result_url = discover_agency_url(juris_name, state_abbr, state_name=task["state_name"], jurisdiction_type=juris_type)

            final_url = None

            # Step 2: Special District Logic
            if category in SPECIAL_CATEGORIES:
                # Always probe independent for Special Categories
                independent_url = find_special_district_domain(juris_name, state_abbr, category)

                if independent_url:
                        final_url = independent_url
                        # Independent URL takes precedence
                else:
                        final_url = main_domain_result_url
            else:
                # Standard Department -> Use the discovered City/Town Bids Page
                final_url = main_domain_result_url

            # Naming Convention: Jurisdiction (State) Category
            display_name = f"{juris_name} ({state_abbr}) {category}"

            # 4. Check Database for Existing Record
            # We check by jurisdiction slot (state + category + local_id) to see if we already have an entry
            existing_agency = db.get_agency_by_jurisdiction(task["state_id"], category, task["jurisdiction_id"])

            if existing_agency is None:
                # Case A: New Record
                if final_url:
                    # Standard deduplication check (in case URL is used by another agency, though less likely here)
                    if not db.agency_exists(task["state_id"], url=final_url, category=category, local_jurisdiction_id=task["jurisdiction_id"]):
                        db.add_agency(
                            state_id=task["state_id"],
                            name=display_name,
                            url=final_url,
                            verified=True,
                            category=category,
                            local_jurisdiction_id=task["jurisdiction_id"]
                        )
                        new_verified_count += 1
                        manager.add_log(job_id, f"✅ Found (Direct): {display_name} -> {final_url}")
            else:
                # Existing Record Logic (Remediation)
                existing_url = existing_agency['url']
                existing_id = existing_agency['id']
                current_name = existing_agency.get('organization_name', '')

                # Check for Name Update (Identity Collision Fix)
                if current_name != display_name:
                    db.update_agency_name(existing_id, display_name)

                if final_url:
                    # Case B: Upgrade
                    if is_better_url(final_url, existing_url):
                        db.update_agency_url(existing_id, final_url)
                        new_verified_count += 1
                        manager.add_log(job_id, f"🔄 Upgraded: {display_name} ({existing_url} -> {final_url})")
                elif existing_url:
                    # Case C: Remove Invalid (Discovery failed, check if existing is dead)
                    # Discovery failed (main_url is None), so we verify if the old one is truly dead
                    if not check_url_reachability(existing_url):
                        db.delete_agency(existing_id)
                        manager.add_log(job_id, f"🗑️ Removed: {display_name} (Dead link: {existing_url})")

    manager.update_progress(job_id, 1.0, f"Discovery Complete! Verified {new_verified_count} URLs.")
    return new_verified_count
