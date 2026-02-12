import pandas as pd
from playwright.sync_api import sync_playwright
from rfp_scraper.factory import ScraperFactory
from rfp_scraper.scrapers.hierarchical import HierarchicalScraper
import logging

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
            # Stealth Launch Args
            launch_args = [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-infobars",
                "--window-position=0,0",
                "--ignore-certificate-errors",
                "--ignore-certificate-errors-spki-list",
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            ]

            browser = p.chromium.launch(
                headless=True,
                args=launch_args
            )

            # Context with Stealth Headers
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                locale="en-US",
                timezone_id="America/New_York",
                viewport={"width": 1920, "height": 1080},
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Sec-Ch-Ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
                    "Sec-Ch-Ua-Mobile": "?0",
                    "Sec-Ch-Ua-Platform": '"Windows"',
                    "Upgrade-Insecure-Requests": "1",
                }
            )

            # Mask WebDriver property
            context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            for i, state in enumerate(states_to_scrape):
                # Update progress
                progress = (i) / total_states
                manager.update_progress(job_id, progress, f"Scraping {state}...")

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

            browser.close()

    except Exception as e:
        manager.add_log(job_id, f"🔥 Critical Error: {str(e)}")
        raise e

    manager.update_progress(job_id, 1.0, "Scraping Complete!")
    return all_results
