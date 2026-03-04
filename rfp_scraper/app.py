import streamlit as st
import pandas as pd
import os
import sys
import time
import io
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

# Use the new v2 database and the new v2 bridge task
from rfp_scraper_v2.core.database import DatabaseHandler
from rfp_scraper_v2.orchestrator import run_v2_scraping_task, run_v2_discovery_task
from rfp_scraper.ai_parser import DeepSeekClient
from rfp_scraper.utils import validate_url, check_url_reachability, get_state_abbreviation
from rfp_scraper.cisa_manager import CisaManager
from rfp_scraper.job_manager import JobManager

st.set_page_config(page_title="National Construction RFP Dashboard", layout="wide")

st.title("🏗️ National Construction RFP Scraper")

# Initialize Helpers
db = DatabaseHandler()

# --- Cached Data Loaders for UI Performance ---
@st.cache_data(ttl=300) # Cache for 5 minutes
def get_cached_states():
    return db.get_all_states()

@st.cache_data(ttl=300)
def get_cached_local_govs():
    return db.get_local_jurisdictions()

@st.cache_data(ttl=300)
def get_cached_agencies():
    return db.get_all_agencies()

@st.cache_data(ttl=60) # Cache bids for 1 minute (needs more frequent updates)
def get_cached_bids(state_filter=None):
    return db.get_bids(state=state_filter)

# Initialize Job Manager (Global Resource)
@st.cache_resource
def get_job_manager():
    return JobManager()

job_manager = get_job_manager()

# Get available states from DB for scraper
available_states_df = get_cached_states()
available_states = available_states_df['name'].tolist() if not available_states_df.empty else []

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

# --- Sidebar Job Monitor ---
st.sidebar.divider()
st.sidebar.subheader("🏗️ Background Tasks")

active_jobs = job_manager.get_active_jobs()
if not active_jobs:
    st.sidebar.info("No active background tasks.")
else:
    for job in active_jobs:
        job_id = job["id"]
        # In case job is just a dict from list
        progress = job.get("progress", 0.0)
        logs = job.get("logs", [])
        last_log = logs[-1] if logs else "Starting..."

        st.sidebar.text(f"Task: {job_id[:8]}...")
        st.sidebar.progress(progress)
        st.sidebar.caption(last_log)

    if st.sidebar.button("Refresh Status"):
        # Clear caches before refreshing
        get_cached_states.clear()
        get_cached_local_govs.clear()
        get_cached_agencies.clear()
        get_cached_bids.clear()
        st.rerun()


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
                            # Invalidate cache to show new states
                            get_cached_states.clear()
                            st.rerun()
                    else:
                        st.error("Failed to generate states (empty response).")

    # Display States Table
    df_states = get_cached_states()
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
    df_current_states_lg = get_cached_states()
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

            # Pre-load CISA Data to prevent lag during the loop
            cisa_manager = CisaManager()
            cisa_manager._load_data()

            for i, state_name in enumerate(target_lg_states):
                status_text_lg.text(f"Mapping Ecosystem for {state_name} ({i+1}/{total_lg_states}). This may take a minute...")

                state_row = df_current_states_lg[df_current_states_lg['name'] == state_name]
                if state_row.empty:
                    continue
                state_id = int(state_row.iloc[0]['id'])

                state_abbr = get_state_abbreviation(state_name)

                # Call AI Ecosystem Mapper
                ecosystem = ai_client.generate_state_ecosystem(state_name)

                # UI Warning if AI fails completely
                if not any(ecosystem.values()):
                    st.warning(f"⚠️ AI returned no data for {state_name}. You may need to retry.")
                    continue

                # 1. Save State Agencies
                for agency_name in ecosystem.get("state_agencies", []):
                    cisa_url = cisa_manager.get_agency_url(agency_name, state_abbr)
                    db.add_agency(state_id=state_id, name=f"{state_name} - {agency_name}", url=cisa_url, category="state_agency", local_jurisdiction_id=None)

                # 2. Save Counties & Departments
                for county_obj in ecosystem.get("counties", []):
                    name = county_obj.get("name")
                    if not name: continue
                    jur_id = db.append_local_jurisdiction(state_id, name, "county")

                    # CISA often lists counties as "X County" or just "X"
                    cisa_url = cisa_manager.get_agency_url(f"{name} County", state_abbr) or cisa_manager.get_agency_url(name, state_abbr)

                    for dept in county_obj.get("departments", []):
                        db.add_agency(state_id=state_id, name=f"{name} County - {dept}", url=cisa_url, category="county_agency", local_jurisdiction_id=jur_id)

                # 3. Save Cities & Departments
                for city_obj in ecosystem.get("cities", []):
                    name = city_obj.get("name")
                    if not name: continue
                    jur_id = db.append_local_jurisdiction(state_id, name, "city")
                    cisa_url = cisa_manager.get_agency_url(name, state_abbr)

                    for dept in city_obj.get("departments", []):
                        db.add_agency(state_id=state_id, name=f"City of {name} - {dept}", url=cisa_url, category="city_agency", local_jurisdiction_id=jur_id)

                # 4. Save Towns & Departments
                for town_obj in ecosystem.get("towns", []):
                    name = town_obj.get("name")
                    if not name: continue
                    jur_id = db.append_local_jurisdiction(state_id, name, "town")
                    cisa_url = cisa_manager.get_agency_url(name, state_abbr)

                    for dept in town_obj.get("departments", []):
                        db.add_agency(state_id=state_id, name=f"Town of {name} - {dept}", url=cisa_url, category="town_agency", local_jurisdiction_id=jur_id)

                progress_bar_lg.progress((i + 1) / total_lg_states)

            # Invalidate Cache
            get_cached_local_govs.clear()
            get_cached_agencies.clear()

            status_text_lg.success("Ecosystem Mapping Complete!")
            time.sleep(1)
            st.rerun()

    # Display Table
    st.subheader("Identified Local Governments")

    df_local_govs = get_cached_local_govs()

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

    # Input Options
    col_ag1, col_ag2 = st.columns(2)
    with col_ag1:
        agency_mode = st.radio("Discovery Mode", ["Single State", "All States"], key="agency_mode")

    # Refresh states from DB
    df_current_states = get_cached_states()
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

    if st.button("Start URL Discovery (Background)", key="start_url_discovery_btn"):
        if not api_key:
            st.error("DeepSeek API Key is required.")
        elif not target_agency_states:
             st.error("No states found in database. Please go to 'States' tab and generate states first.")
        else:
            # Start Background Job
            # Use V2 Discovery Bridge
            job_id = job_manager.start_job(run_v2_discovery_task, args=(target_agency_states, api_key), name="V2 Discovery Task")
            st.success(f"Discovery started! Job ID: {job_id}")
            st.info("Monitor progress in the sidebar.")
            time.sleep(1)
            st.rerun()


    # Display Agencies Table
    st.divider()
    st.subheader("Discovered Agencies")
    df_agencies = get_cached_agencies()

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


    # --- Button Section (Moved Up) ---
    if st.button("🚀 Start Scraping (Background)", key="start_scraping_btn"):
        if not api_key:
            st.error("Deep Scan requires a DeepSeek API Key. Please provide it in the sidebar.")
        else:
            # Start Background Job
            # Trigger the v2 Async Bridge instead of the legacy synchronous scraper
            job_id = job_manager.start_job(run_v2_scraping_task, args=(target_states, api_key), name="V2 Async Scraping Task")
            st.success(f"Scraping started! Job ID: {job_id}")
            st.info("You can monitor progress in the sidebar. The results will appear in the table below automatically as they are saved to the database.")
            time.sleep(1)
            st.rerun()

    # --- Persistent Data Display ---
    st.subheader("Active Opportunities")

    # 1. Load Data
    state_filter = selected_scraper_state if scraper_mode == "Single State" else None
    persistent_df = get_cached_bids(state_filter=state_filter)

    # 2. Filter Logic (Deadline >= Today)
    if not persistent_df.empty and 'deadline' in persistent_df.columns:
        # Convert deadline to datetime, coerce errors to NaT
        persistent_df['deadline_dt'] = pd.to_datetime(persistent_df['deadline'], errors='coerce')

        # Determine today's date (normalized to midnight)
        today = pd.Timestamp.now().normalize()

        # Filter: Keep if valid date >= today.
        mask = (persistent_df['deadline_dt'] >= today) | (persistent_df['deadline_dt'].isna())
        persistent_df = persistent_df[mask].drop(columns=['deadline_dt'])

    # Define exact columns to display and export
    desired_columns = [
        'client_name', 'title', 'deadline', 'description',
        'link', 'csi_divisions', 'full_text'
    ]

    # Safely filter dataframe to only include desired columns that exist
    if not persistent_df.empty:
        available_columns = [col for col in desired_columns if col in persistent_df.columns]
        display_df = persistent_df[available_columns].copy()
    else:
        display_df = persistent_df.copy()

    st.dataframe(display_df, use_container_width=True)

    # Export Section
    if not display_df.empty:
        today_str = datetime.datetime.now().strftime("%Y%m%d")
        if scraper_mode == "Single State" and selected_scraper_state:
            state_slug = selected_scraper_state.replace(' ', '_')
            base_filename = f"{state_slug}_rfps_{today_str}"
        else:
            base_filename = f"all_rfps_{today_str}"

        # CSV Download (Clean - Only desired columns)
        csv_rfps = display_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Download RFPs CSV",
            csv_rfps,
            f"{base_filename}.csv",
            "text/csv",
            key='download-rfps-csv'
        )
