import streamlit as st
import pandas as pd
import os
import sys
import time
import io
import sqlite3
import xlsxwriter
from playwright.sync_api import sync_playwright
import datetime
from dotenv import load_dotenv

# Load env vars at startup
load_dotenv()

# Ensure project root is in sys.path so 'rfp_scraper' package can be imported
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

from rfp_scraper.factory import ScraperFactory
from rfp_scraper.scrapers.hierarchical import HierarchicalScraper
from rfp_scraper.db import DatabaseHandler
from rfp_scraper.ai_parser import DeepSeekClient
from rfp_scraper.utils import validate_url, check_url_reachability
from rfp_scraper.discovery import DiscoveryEngine
from rfp_scraper.config_loader import load_agency_template, extract_search_scope, get_local_search_scope, get_domain_patterns

st.set_page_config(page_title="National Construction RFP Dashboard", layout="wide")

st.title("🏗️ National Construction RFP Scraper")

# Initialize Helpers
factory = ScraperFactory()
db = DatabaseHandler()
available_states = factory.get_available_states()

# --- Global Configuration (Sidebar) ---
st.sidebar.header("Global Configuration")

# API Key Status
api_key = os.getenv("DEEPSEEK_API_KEY")
if api_key:
    st.sidebar.success("✅ API Key loaded from .env")
else:
    api_key = st.sidebar.text_input("DeepSeek API Key", type="password")

# Initialize AI Client
ai_client = DeepSeekClient(api_key=api_key)


# --- Tabs ---
tab_states, tab_local_gov, tab_agencies, tab_scraper = st.tabs(["States", "Local Governments", "State Agencies", "RFP Scraper"])

# ==========================================
# TAB 1: STATES
# ==========================================
with tab_states:
    st.header("🇺🇸 State Discovery")
    st.markdown("Generate and manage the master list of US States.")

    col1, col2 = st.columns([1, 3])

    with col1:
        if st.button("Generate States"):
            if not api_key:
                st.error("DeepSeek API Key is required.")
            else:
                with st.spinner("Asking AI for state list..."):
                    states_list = ai_client.generate_us_states()

                    if states_list:
                        count = 0
                        for state_name in states_list:
                            # Simple validation to avoid garbage
                            if isinstance(state_name, str) and len(state_name) > 2:
                                db.add_state(state_name)
                                count += 1
                        st.success(f"Processed {count} states.")
                        if count == 0:
                            st.warning("No valid states found in response.")
                    else:
                        st.error("Failed to generate states (empty response).")

    # Display States Table
    df_states = db.get_all_states()
    st.dataframe(df_states, use_container_width=True)

    # Export
    if not df_states.empty:
        csv = df_states.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Download States CSV",
            csv,
            "us_states.csv",
            "text/csv",
            key='download-states'
        )

# ==========================================
# TAB 1A: LOCAL GOVERNMENTS
# ==========================================
with tab_local_gov:
    st.header("🏘️ Local Government Identification")
    st.markdown("Identify Counties, Cities, and Towns using AI.")

    # Refresh states from DB
    df_current_states_lg = db.get_all_states()
    state_names_list_lg = df_current_states_lg['name'].tolist() if not df_current_states_lg.empty else []

    col_lg1, col_lg2 = st.columns(2)

    with col_lg1:
        lg_mode = st.radio("Identification Mode", ["Single State", "All States"], key="lg_mode")

    target_lg_states = []
    if lg_mode == "Single State":
        with col_lg2:
            selected_lg_state = st.selectbox("Select State", state_names_list_lg, key="lg_state_select")
            if selected_lg_state:
                target_lg_states = [selected_lg_state]
    else:
        target_lg_states = state_names_list_lg
        with col_lg2:
             st.info(f"Will process {len(target_lg_states)} states from DB.")

    if st.button("Identify Jurisdictions", key="identify_jurisdictions_btn"):
        if not api_key:
            st.error("DeepSeek API Key is required.")
        elif not target_lg_states:
             st.error("No states found. Please generate states first.")
        else:
            progress_bar_lg = st.progress(0)
            status_text_lg = st.empty()
            total_lg_states = len(target_lg_states)

            for i, state_name in enumerate(target_lg_states):
                status_text_lg.text(f"Processing {state_name} ({i+1}/{total_lg_states})...")

                # Get State ID
                state_row = df_current_states_lg[df_current_states_lg['name'] == state_name]
                if state_row.empty:
                    continue
                state_id = int(state_row.iloc[0]['id'])

                # Call AI
                jurisdictions = ai_client.generate_local_jurisdictions(state_name)

                # Save to DB (Persistence to local_jurisdictions table)
                # Counties
                for county in jurisdictions.get("counties", []):
                    db.append_local_jurisdiction(state_id, county, "county")

                # Cities
                for city in jurisdictions.get("cities", []):
                    db.append_local_jurisdiction(state_id, city, "city")

                # Towns
                for town in jurisdictions.get("towns", []):
                    db.append_local_jurisdiction(state_id, town, "town")

                progress_bar_lg.progress((i + 1) / total_lg_states)

            status_text_lg.success("Identification Complete!")

    # Display Table
    st.subheader("Identified Local Governments")

    df_local_govs = db.get_local_jurisdictions()

    # Join with states to get state name for better display
    if not df_local_govs.empty and not df_current_states_lg.empty:
        df_local_govs = df_local_govs.merge(
            df_current_states_lg[['id', 'name']],
            left_on='state_id',
            right_on='id',
            suffixes=('', '_state')
        ).rename(columns={'name_state': 'state_name'}).drop(columns=['id_state'])

        # Reorder columns
        cols = ['state_name', 'name', 'type', 'created_at']
        df_local_govs = df_local_govs[cols]

    # Filter by state if single mode
    if lg_mode == "Single State" and selected_lg_state and not df_local_govs.empty:
        df_local_govs = df_local_govs[df_local_govs['state_name'] == selected_lg_state]

    st.dataframe(df_local_govs, use_container_width=True)

    # Export
    if not df_local_govs.empty:
        today_str_lg = datetime.datetime.now().strftime("%Y%m%d")
        if lg_mode == "Single State" and selected_lg_state:
            state_slug = selected_lg_state.replace(' ', '_')
            filename_lg = f"{state_slug}_local_jurisdictions_{today_str_lg}.csv"
        else:
            filename_lg = f"all_local_jurisdictions_{today_str_lg}.csv"

        csv_lg = df_local_govs.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Download CSV",
            csv_lg,
            filename_lg,
            "text/csv",
            key='download-lg'
        )

# ==========================================
# TAB 2: STATE AGENCIES
# ==========================================
with tab_agencies:
    st.header("🏛️ Agency & Local Gov URL Discovery")
    st.markdown("Unified URL discovery for State Agencies and identified Local Governments (AI-Native).")

    # Initialize Discovery Engine
    discovery_engine = DiscoveryEngine()

    # Load Template (Standard Agencies)
    template = load_agency_template()
    search_scope = extract_search_scope(template)

    # Input Options
    col_ag1, col_ag2 = st.columns(2)
    with col_ag1:
        agency_mode = st.radio("Discovery Mode", ["Single State", "All States"], key="agency_mode")

    # Refresh states from DB
    df_current_states = db.get_all_states()
    state_names_list = df_current_states['name'].tolist() if not df_current_states.empty else []

    target_agency_states = []
    if agency_mode == "Single State":
        with col_ag2:
            selected_agency_state = st.selectbox("Select State", state_names_list, key="agency_state_select")
            if selected_agency_state:
                target_agency_states = [selected_agency_state]
    else:
        target_agency_states = state_names_list
        with col_ag2:
             st.info(f"Will process {len(target_agency_states)} states from DB.")

    if st.button("Start URL Discovery", key="start_url_discovery_btn"):
        if not api_key:
            st.error("DeepSeek API Key is required.")
        elif not target_agency_states:
             st.error("No states found in database. Please go to 'States' tab and generate states first.")
        else:
            progress_bar = st.progress(0)
            status_container = st.container()
            log_area = st.empty()

            # Prepare task list
            tasks = [] # (state_id, state_name, type, name, category, phase)

            for state_name in target_agency_states:
                state_row = df_current_states[df_current_states['name'] == state_name]
                if state_row.empty: continue
                state_id = int(state_row.iloc[0]['id'])

                # Phase 1: Standard Agencies (State Level)
                for agency_type in search_scope:
                    tasks.append({
                        "state_id": state_id, "state_name": state_name,
                        "name": agency_type, "category": "state_agency",
                        "phase": "standard", "jurisdiction_id": None
                    })

                # Phase 2: Local Govs (Iterate Jurisdictions)
                local_jurisdictions = db.get_local_jurisdictions(state_id=state_id)

                for _, juris_row in local_jurisdictions.iterrows():
                    juris_id = int(juris_row['id'])
                    juris_name = juris_row['name']
                    juris_type = juris_row['type']

                    # Get categories for this jurisdiction type (List[str])
                    categories = get_local_search_scope(juris_type)

                    for category in categories:
                        # We create a task for each category (Main Office, Public Works, etc.)
                        tasks.append({
                            "state_id": state_id,
                            "state_name": state_name,
                            "name": juris_name, # The jurisdiction name (e.g. "Chicago")
                            "category": category, # e.g. 'Main Office', 'Public Works'
                            "phase": "local",
                            "jurisdiction_id": juris_id,
                            "jurisdiction_type": juris_type
                        })

            total_items = len(tasks)
            items_completed = 0
            new_verified_count = 0

            status_container.write(f"Found {total_items} discovery tasks.")

            for task in tasks:
                items_completed += 1
                progress_bar.progress(items_completed / total_items)

                state_name = task["state_name"]
                name = task["name"]
                category = task["category"]

                if task["phase"] == "standard":
                     log_area.text(f"Processing {state_name}: {name} ({task['phase']})...")
                else:
                     log_area.text(f"Processing {state_name}: {name} - {category} ({task['phase']})...")

                found_url = None
                method = ""

                if task["phase"] == "standard":
                    # Standard Discovery (AI + Browser)
                    found_url, method = discovery_engine.find_agency_url(state_name, name, ai_client)

                    if found_url:
                        # Deduplicate using standard logic (checks URL)
                        if not db.agency_exists(task["state_id"], found_url):
                            db.add_agency(task["state_id"], name, found_url, verified=True, category=category)
                            new_verified_count += 1
                            status_container.write(f"✅ Found Standard: **{name}** -> {found_url}")
                        else:
                            # Already exists
                            pass

                elif task["phase"] == "local":
                    # AI-Native Local Gov Discovery
                    juris_name = task["name"]

                    # Construct Query
                    query = f"Official website for {juris_name} {category}"
                    if category == "Main Office":
                        query = f"Official website for {juris_name}"

                    # 1. Fetch Raw Results (No filtering)
                    raw_results = discovery_engine.fetch_search_context(query)

                    # 2. AI Analysis
                    if raw_results:
                         found_url = ai_client.analyze_serp_results(juris_name, category, raw_results)

                    if found_url:
                        # Construct Display Name
                        display_name = f"{juris_name} {category}"
                        if category == 'Main Office':
                             display_name = juris_name

                        # Check deduplication
                        if not db.agency_exists(task["state_id"], url=found_url, name=display_name, category=category, local_jurisdiction_id=task["jurisdiction_id"]):
                             db.add_agency(
                                 state_id=task["state_id"],
                                 name=display_name,
                                 url=found_url,
                                 verified=True,
                                 category=category,
                                 local_jurisdiction_id=task["jurisdiction_id"]
                             )
                             new_verified_count += 1
                             status_container.write(f"🤖 Found (AI Native): **{display_name}** -> {found_url}")

            log_area.empty()
            st.success(f"Discovery Process Complete! Verified {new_verified_count} URLs.")

    # Display Agencies Table
    st.divider()
    st.subheader("Discovered Agencies")
    df_agencies = db.get_all_agencies()

    # Apply Filter
    if agency_mode == "Single State" and selected_agency_state:
        df_agencies = df_agencies[df_agencies['state_name'] == selected_agency_state]

    # Hide internal IDs and use jurisdiction label
    if not df_agencies.empty:
        # Columns to display
        display_cols = ['state_name', 'jurisdiction_label', 'organization_name', 'url', 'category', 'verified', 'created_at']
        # Filter only existing columns just in case
        display_cols = [c for c in display_cols if c in df_agencies.columns]
        st.dataframe(df_agencies[display_cols], use_container_width=True)
    else:
        st.dataframe(df_agencies, use_container_width=True)

    # Export
    if not df_agencies.empty:
        today_str = datetime.datetime.now().strftime("%Y%m%d")
        if agency_mode == "Single State" and selected_agency_state:
            state_slug = selected_agency_state.replace(' ', '_')
            filename = f"{state_slug}_agencies_{today_str}.csv"
        else:
            filename = f"all_agencies_{today_str}.csv"

        csv_ag = df_agencies.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Download Agencies CSV",
            csv_ag,
            filename,
            "text/csv",
            key='download-agencies'
        )

# ==========================================
# TAB 3: RFP SCRAPER (Existing Logic)
# ==========================================
with tab_scraper:
    st.header("🚜 Construction RFP Scraper")
    st.markdown(
        """
        **Objective**: Fetch and filter construction RFPs from state procurement portals.
        \n**Filter Logic**: `Deadline >= (Today + 4 Days)`
        """
    )

    # --- Local Configuration for Scraper ---
    col_conf1, col_conf2 = st.columns(2)

    with col_conf1:
        scraper_mode = st.radio("Operation Mode", ["Single State", "Scrape All States"], key="scraper_mode")

        if scraper_mode == "Single State":
            # Filter available states to those in factory
            selected_scraper_state = st.selectbox("Select State", available_states, key="scraper_state_select")
            target_states = [selected_scraper_state]
        else:
            st.info(f"Will scrape {len(available_states)} states: {', '.join(available_states)}")
            target_states = available_states

    with col_conf2:
        st.info("ℹ️ Deep Scan is now active by default for comprehensive coverage.")

    # --- Scraping Logic ---
    def run_scraping(states_to_scrape, api_key_val):
        progress_bar = st.progress(0)
        status_text = st.empty()
        all_results = pd.DataFrame()

        total_states = len(states_to_scrape)

        with sync_playwright() as p:
            status_text.text("Launching Browser...")
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                 user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )

            for i, state in enumerate(states_to_scrape):
                status_text.text(f"Scraping {state} ({i+1}/{total_states})...")

                try:
                    base_scraper = factory.get_scraper(state)
                    # Always use HierarchicalScraper (Deep Scan)
                    scraper = HierarchicalScraper(state, base_scraper=base_scraper, api_key=api_key_val)

                    page = context.new_page()
                    df = scraper.scrape(page)
                    page.close()

                    if not df.empty:
                        df["SourceState"] = state
                        all_results = pd.concat([all_results, df], ignore_index=True)

                except Exception as e:
                    st.error(f"Error scraping {state}: {e}")

                progress_bar.progress((i + 1) / total_states)

            browser.close()

        status_text.success("Scraping Complete!")
        return all_results

    # --- Button Section (Moved Up) ---
    if st.button("🚀 Start Scraping", key="start_scraping_btn"):
        if not api_key:
            st.error("Deep Scan requires a DeepSeek API Key. Please provide it in the sidebar.")
        else:
            run_scraping(target_states, api_key)
            # After scraping finishes, script continues...
            # and next lines will fetch updated DB data.

    # --- Persistent Data Display ---
    st.subheader("Active Opportunities")

    # 1. Load Data
    state_filter = selected_scraper_state if scraper_mode == "Single State" else None
    persistent_df = db.get_bids(state=state_filter)

    # 2. Filter Logic (Deadline >= Today)
    if not persistent_df.empty and 'deadline' in persistent_df.columns:
        # Convert deadline to datetime, coerce errors to NaT
        persistent_df['deadline_dt'] = pd.to_datetime(persistent_df['deadline'], errors='coerce')

        # Determine today's date (normalized to midnight)
        today = pd.Timestamp.now().normalize()

        # Filter: Keep if valid date >= today.
        mask = (persistent_df['deadline_dt'] >= today) | (persistent_df['deadline_dt'].isna())
        persistent_df = persistent_df[mask].drop(columns=['deadline_dt'])

    # Display Data (Exclude Description)
    display_df = persistent_df.copy()
    if 'rfp_description' in display_df.columns:
        display_df = display_df.drop(columns=['rfp_description'])
    st.dataframe(display_df, use_container_width=True)

    # Export Section
    if not persistent_df.empty:
        today_str = datetime.datetime.now().strftime("%Y%m%d")
        if scraper_mode == "Single State" and selected_scraper_state:
            state_slug = selected_scraper_state.replace(' ', '_')
            base_filename = f"{state_slug}_rfps_{today_str}"
        else:
            base_filename = f"all_rfps_{today_str}"

        # 1. CSV Download (Clean - No Description)
        csv_df = persistent_df.copy()
        if 'rfp_description' in csv_df.columns:
            csv_df = csv_df.drop(columns=['rfp_description'])

        csv_rfps = csv_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Download RFPs CSV (Summary)",
            csv_rfps,
            f"{base_filename}.csv",
            "text/csv",
            key='download-rfps-csv'
        )

        # 2. JSON Download (Full Data - With Description)
        json_rfps = persistent_df.to_json(orient='records', indent=2).encode('utf-8')
        st.download_button(
            "📥 Download RFP .json (Full Data)",
            json_rfps,
            f"{base_filename}.json",
            "application/json",
            key='download-rfps-json'
        )
