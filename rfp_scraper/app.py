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
    st.markdown("Discover and validate agencies and universities for specific states.")

    # Input Options
    col_ag1, col_ag2 = st.columns(2)
    with col_ag1:
        agency_mode = st.radio("Discovery Mode", ["Single State", "All States"], key="agency_mode")

    # Refresh states from DB (in case they were just added in Tab 1)
    # Since streamlit runs script top to bottom, df_states is available from Tab 1 block,
    # but strictly speaking scopes in `with` don't leak well if we consider re-runs.
    # Better to fetch again or use st.session_state, but simple fetch is safe.
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
            status_text = st.empty()

            total_states = len(target_agency_states)
            total_agencies_found = 0

            for i, state_name in enumerate(target_agency_states):
                status_text.text(f"Scanning {state_name} ({i+1}/{total_states})...")

                # AI Discovery
                try:
                    discovered = ai_client.discover_state_agencies(state_name)

                    # Validate and Save
                    # Need state_id
                    state_row = df_current_states[df_current_states['name'] == state_name]
                    if not state_row.empty:
                        state_id = int(state_row.iloc[0]['id'])

                        for item in discovered:
                            org_name = item.get('organization_name')
                            url = item.get('url')

                            if org_name and url:
                                # Validation
                                is_valid = validate_url(url)
                                if is_valid:
                                    db.add_agency(state_id, org_name, url, verified=True)
                                    total_agencies_found += 1
                except Exception as e:
                    st.error(f"Error processing {state_name}: {e}")

                progress_bar.progress((i + 1) / total_states)

                # Rate Limiting
                if len(target_agency_states) > 1:
                    time.sleep(2) # Sleep 2 seconds between states

            status_text.success(f"Discovery Complete! Added {total_agencies_found} new agencies.")

    # Display Agencies Table
    st.divider()
    st.subheader("Discovered Agencies")
    df_agencies = db.get_all_agencies()
    st.dataframe(df_agencies, use_container_width=True)

    # Export
    if not df_agencies.empty:
        csv_ag = df_agencies.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Download Agencies CSV",
            csv_ag,
            "state_agencies.csv",
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
        deep_scan_mode = st.checkbox("Deep Scan Mode (Slower)", help="Enables hierarchical discovery of local agency bids using AI.", key="scraper_deep_scan")


    def run_scraping(states_to_scrape, use_deep_scan, api_key_val):
        progress_bar = st.progress(0)
        status_text = st.empty()
        all_results = pd.DataFrame()

        total_states = len(states_to_scrape)

        # Re-init DB just in case? No, global `db` is fine, but existing code used local.
        # We can use global `db` or `DatabaseHandler()`. existing used `db = DatabaseHandler()`

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

                    if use_deep_scan:
                        scraper = HierarchicalScraper(state, base_scraper=base_scraper, api_key=api_key_val)
                    else:
                        scraper = base_scraper

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

    def export_to_excel(df):
        """
        Export DataFrame to Excel with formatting and deduplication check.
        """
        output = io.BytesIO()
        # db is already initialized globally

        fresh_rows = []
        current_time = datetime.datetime.now()

        for index, row in df.iterrows():
            title = row.get('title', '')
            client = row.get('client', row.get('agency', ''))
            link = row.get('link', '')

            slug = db.generate_slug(title, client, link)

            # Check DB
            # Accessing db_path from global db instance
            conn = sqlite3.connect(db.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT scraped_at FROM scraped_bids WHERE slug = ?", (slug,))
            result = cursor.fetchone()
            conn.close()

            is_fresh = False
            if result:
                scraped_at_str = result[0]
                try:
                    scraped_at = datetime.datetime.fromisoformat(scraped_at_str)
                    if (current_time - scraped_at).total_seconds() < 600:
                        is_fresh = True
                except:
                    pass
            else:
                db.insert_bid(slug, client, title, row.get('deadline', ''), link)
                is_fresh = True

            if is_fresh:
                fresh_rows.append(row)

        fresh_df = pd.DataFrame(fresh_rows)

        if fresh_df.empty:
            return None

        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet()

        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#00008B',
            'font_color': '#FFFFFF'
        })

        headers = fresh_df.columns.tolist()
        for col_num, header in enumerate(headers):
            worksheet.write(0, col_num, header, header_format)

        for row_num, row_data in enumerate(fresh_df.values, 1):
            for col_num, cell_data in enumerate(row_data):
                header_name = headers[col_num].lower()
                if 'link' in header_name and isinstance(cell_data, str) and cell_data.startswith('http'):
                    worksheet.write_url(row_num, col_num, cell_data, string=cell_data)
                else:
                    worksheet.write(row_num, col_num, cell_data)

        for i, col in enumerate(headers):
            worksheet.set_column(i, i, 20)

        worksheet.freeze_panes(1, 0)
        workbook.close()
        output.seek(0)
        return output

    if st.button("🚀 Start Scraping", key="start_scraping_btn"):
        if deep_scan_mode and not api_key:
            st.error("Deep Scan Mode requires a DeepSeek API Key.")
        else:
            results_df = run_scraping(target_states, deep_scan_mode, api_key)

            if not results_df.empty:
                st.divider()

                earliest_deadline_str = "-"
                if 'deadline' in results_df.columns:
                    temp_dates = pd.to_datetime(results_df['deadline'], errors='coerce')
                    valid_dates = temp_dates.dropna()

                    if not valid_dates.empty:
                        earliest_deadline_str = valid_dates.min().strftime('%Y-%m-%d')

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Found", len(results_df))
                with col2:
                    st.metric("States Scraped", len(target_states))
                with col3:
                    st.metric("Earliest Deadline", earliest_deadline_str)

                st.subheader("All Opportunities Found (Session)")
                st.dataframe(results_df, use_container_width=True)

                excel_file = export_to_excel(results_df)

                if excel_file:
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"national_rfps_{timestamp}.xlsx"

                    st.download_button(
                        label="📥 Download Excel Report (Fresh Leads Only)",
                        data=excel_file,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_excel_scraper"
                    )
                else:
                    st.info("No fresh leads to export (all found bids are already in database).")
            else:
                st.warning("No qualified RFPs found.")
