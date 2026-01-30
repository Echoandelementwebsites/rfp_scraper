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
from rfp_scraper.utils import validate_url
from rfp_scraper.discovery import DiscoveryEngine
from rfp_scraper.config_loader import load_agency_template, extract_search_scope

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
tab_states, tab_agencies, tab_scraper = st.tabs(["States", "State Agencies", "RFP Scraper"])

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
# TAB 2: STATE AGENCIES
# ==========================================
with tab_agencies:
    st.header("🏛️ State Agency Discovery")
    st.markdown("Discover and validate agencies and universities for specific states using the constrained discovery workflow.")

    # Initialize Discovery Engine
    discovery_engine = DiscoveryEngine()

    # Load Template
    template = load_agency_template()
    search_scope = extract_search_scope(template)

    st.write(f"Loaded {len(search_scope)} agency types to search for (e.g. {search_scope[:3]}...).")

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

    if st.button("🔍 Scrape Agencies", key="scrape_agencies_btn"):
        if not api_key:
            st.error("DeepSeek API Key is required.")
        elif not target_agency_states:
             st.error("No states found in database. Please go to 'States' tab and generate states first.")
        else:
            progress_bar = st.progress(0)
            status_container = st.container()

            total_tasks = len(target_agency_states) * len(search_scope)
            tasks_completed = 0
            new_agencies_count = 0

            # Create a log area
            log_area = st.empty()

            for state_name in target_agency_states:
                # Get State ID
                state_row = df_current_states[df_current_states['name'] == state_name]
                if state_row.empty:
                    continue
                state_id = int(state_row.iloc[0]['id'])

                status_container.markdown(f"### Processing {state_name}...")

                for agency_type in search_scope:
                    tasks_completed += 1
                    progress_bar.progress(tasks_completed / total_tasks)

                    log_area.text(f"Scanning {state_name}: {agency_type}...")

                    # Discovery
                    url, method = discovery_engine.find_agency_url(state_name, agency_type, ai_client)

                    if url:
                        # Deduplicate
                        if not db.agency_exists(state_id, url):
                            db.add_agency(state_id, agency_type, url, verified=True)
                            new_agencies_count += 1
                            status_container.write(f"✅ Found: **{agency_type}** ({method}) -> {url}")
                        else:
                            status_container.write(f"⚠️ Duplicate: {agency_type} ({url})")
                    else:
                        status_container.write(f"❌ Not Found: {agency_type}")

            log_area.empty()
            st.success(f"Discovery Complete! Added {new_agencies_count} new agencies.")

    # Display Agencies Table
    st.divider()
    st.subheader("Discovered Agencies")
    df_agencies = db.get_all_agencies()

    # Apply Filter
    if agency_mode == "Single State" and selected_agency_state:
        df_agencies = df_agencies[df_agencies['state_name'] == selected_agency_state]

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

    # Define variable for filtered data
    state_filter = selected_scraper_state if scraper_mode == "Single State" else None

    # Start Scraping Button
    if st.button("🚀 Start Scraping", key="start_scraping_btn"):
        if not api_key:
            st.error("Deep Scan requires a DeepSeek API Key. Please provide it in the sidebar.")
        else:
            results_df = run_scraping(target_states, api_key)

            # Session Results
            if not results_df.empty:
                st.subheader("Session Results (New & Updated)")
                st.dataframe(results_df, use_container_width=True)
                st.divider()
            else:
                st.warning("No opportunities found in this session.")

    # --- Persistent Data Display ---
    st.subheader("Active Opportunities (Persistent View)")

    # 1. Load Data
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

    # Display Container
    bids_container = st.empty()
    bids_container.dataframe(persistent_df, use_container_width=True)

    # Export CSV (Persistent Data)
    if not persistent_df.empty:
        today_str = datetime.datetime.now().strftime("%Y%m%d")
        if scraper_mode == "Single State" and selected_scraper_state:
            state_slug = selected_scraper_state.replace(' ', '_')
            filename = f"{state_slug}_rfps_{today_str}.csv"
        else:
            filename = f"all_rfps_{today_str}.csv"

        csv_rfps = persistent_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Download RFPs CSV",
            csv_rfps,
            filename,
            "text/csv",
            key='download-rfps-csv'
        )
