import streamlit as st
import pandas as pd
import os
import time
from playwright.sync_api import sync_playwright
from rfp_scraper.factory import ScraperFactory
import datetime

st.set_page_config(page_title="National Construction RFP Dashboard", layout="wide")

st.title("🏗️ National Construction RFP Scraper")

# Initialize Factory
factory = ScraperFactory()
available_states = factory.get_available_states()

# Sidebar
st.sidebar.header("Configuration")
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

def run_scraping(states_to_scrape):
    progress_bar = st.progress(0)
    status_text = st.empty()
    all_results = pd.DataFrame()

    total_states = len(states_to_scrape)

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
                scraper = factory.get_scraper(state)
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

if st.button("🚀 Start Scraping"):
    if mode == "Single State":
        target_states = [selected_state]
    else:
        target_states = available_states

    results_df = run_scraping(target_states)

    if not results_df.empty:
        # Metrics
        st.divider()
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Found", len(results_df))
        with col2:
            st.metric("States Scraped", len(target_states))
        with col3:
            st.metric("Earliest Deadline", results_df['deadline'].min() if 'deadline' in results_df else "-")

        # Display Data
        st.subheader("Filtered Opportunities")
        st.dataframe(results_df, use_container_width=True)

        # CSV Download
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"national_rfps_{timestamp}.csv"
        csv = results_df.to_csv(index=False).encode('utf-8')

        st.download_button(
            label="📥 Download Master CSV",
            data=csv,
            file_name=filename,
            mime="text/csv"
        )
    else:
        st.warning("No qualified RFPs found.")
