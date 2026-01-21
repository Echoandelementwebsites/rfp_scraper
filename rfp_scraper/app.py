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

st.set_page_config(page_title="National Construction RFP Dashboard", layout="wide")

st.title("🏗️ National Construction RFP Scraper")

# Initialize Factory
factory = ScraperFactory()
available_states = factory.get_available_states()

# Sidebar
st.sidebar.header("Configuration")

# API Key Status
api_key = os.getenv("DEEPSEEK_API_KEY")
if api_key:
    st.sidebar.success("✅ API Key loaded from .env")
else:
    api_key = st.sidebar.text_input("DeepSeek API Key", type="password")

# Mode Selection
deep_scan_mode = st.sidebar.checkbox("Deep Scan Mode (Slower)", help="Enables hierarchical discovery of local agency bids using AI.")

mode = st.sidebar.radio("Operation Mode", ["Single State", "Scrape All States"])

if mode == "Single State":
    selected_state = st.sidebar.selectbox("Select State", available_states)
else:
    selected_state = None
    st.sidebar.info(f"Will scrape {len(available_states)} states: {', '.join(available_states)}")

st.markdown(
    """
    **Objective**: Fetch and filter construction RFPs from state procurement portals.
    \n**Filter Logic**: `Deadline >= (Today + 4 Days)`
    """
)

def run_scraping(states_to_scrape, use_deep_scan, api_key_val):
    progress_bar = st.progress(0)
    status_text = st.empty()
    all_results = pd.DataFrame()

    total_states = len(states_to_scrape)

    db = DatabaseHandler()

    with sync_playwright() as p:
        # Launch Browser once
        status_text.text("Launching Browser...")
        browser = p.chromium.launch(headless=True)
        # Use a single context
        context = browser.new_context(
             user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )

        for i, state in enumerate(states_to_scrape):
            status_text.text(f"Scraping {state} ({i+1}/{total_states})...")

            try:
                # Get base scraper
                base_scraper = factory.get_scraper(state)

                if use_deep_scan:
                    # Use Hierarchical wrapper
                    scraper = HierarchicalScraper(state, base_scraper=base_scraper, api_key=api_key_val)
                else:
                    scraper = base_scraper

                page = context.new_page()

                # Run Scrape
                df = scraper.scrape(page)
                page.close()

                if not df.empty:
                    df["SourceState"] = state # Tag the source
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
    db = DatabaseHandler()

    fresh_rows = []

    current_time = datetime.datetime.now()

    for index, row in df.iterrows():
        title = row.get('title', '')
        client = row.get('client', row.get('agency', '')) # Normalize
        link = row.get('link', '')

        slug = db.generate_slug(title, client, link)

        # Check DB
        # We need to query the `scraped_at` for this slug.
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
                # If scraped within last 10 minutes, consider it new/fresh from this run
                if (current_time - scraped_at).total_seconds() < 600:
                    is_fresh = True
            except:
                pass
        else:
            # If from Standard Scraper, it's "fresh" if not in DB.
            # We insert it now to mark it as seen.
            db.insert_bid(slug, client, title, row.get('deadline', ''), link)
            is_fresh = True

        if is_fresh:
            fresh_rows.append(row)

    fresh_df = pd.DataFrame(fresh_rows)

    if fresh_df.empty:
        return None

    # Write to Excel
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet()

    # Formats
    header_format = workbook.add_format({
        'bold': True,
        'bg_color': '#00008B', # Dark Blue
        'font_color': '#FFFFFF' # White
    })

    # Write Header
    headers = fresh_df.columns.tolist()
    for col_num, header in enumerate(headers):
        worksheet.write(0, col_num, header, header_format)

    # Write Data
    for row_num, row_data in enumerate(fresh_df.values, 1):
        for col_num, cell_data in enumerate(row_data):
            # Check for link column to make hyperlink
            header_name = headers[col_num].lower()
            if 'link' in header_name and isinstance(cell_data, str) and cell_data.startswith('http'):
                worksheet.write_url(row_num, col_num, cell_data, string=cell_data)
            else:
                worksheet.write(row_num, col_num, cell_data)

    # Auto-width (approximate)
    for i, col in enumerate(headers):
        worksheet.set_column(i, i, 20)

    # Freeze Top Row
    worksheet.freeze_panes(1, 0)

    workbook.close()
    output.seek(0)
    return output

if st.button("🚀 Start Scraping"):
    if deep_scan_mode and not api_key:
        st.error("Deep Scan Mode requires a DeepSeek API Key.")
    else:
        if mode == "Single State":
            target_states = [selected_state]
        else:
            target_states = available_states

        results_df = run_scraping(target_states, deep_scan_mode, api_key)

        if not results_df.empty:
            # Metrics
            st.divider()

            # --- FIX: Safe Date Calculation ---
            earliest_deadline_str = "-"
            if 'deadline' in results_df.columns:
                # Convert to datetime, forcing errors (and non-dates) to NaT
                temp_dates = pd.to_datetime(results_df['deadline'], errors='coerce')
                # Drop NaT values before calculating min
                valid_dates = temp_dates.dropna()

                if not valid_dates.empty:
                    earliest_deadline_str = valid_dates.min().strftime('%Y-%m-%d')
            # ----------------------------------

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Found", len(results_df))
            with col2:
                st.metric("States Scraped", len(target_states))
            with col3:
                st.metric("Earliest Deadline", earliest_deadline_str)

            # Display Data
            st.subheader("All Opportunities Found (Session)")
            st.dataframe(results_df, use_container_width=True)

            # Excel Download
            excel_file = export_to_excel(results_df)

            if excel_file:
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"national_rfps_{timestamp}.xlsx"

                st.download_button(
                    label="📥 Download Excel Report (Fresh Leads Only)",
                    data=excel_file,
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.info("No fresh leads to export (all found bids are already in database).")
        else:
            st.warning("No qualified RFPs found.")
