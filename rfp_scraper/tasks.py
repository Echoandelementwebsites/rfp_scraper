import pandas as pd
import random
from playwright.sync_api import sync_playwright
from rfp_scraper.factory import ScraperFactory
from rfp_scraper.scrapers.hierarchical import HierarchicalScraper
import logging

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

            for i, state in enumerate(states_to_scrape):
                # Update progress
                progress = (i) / total_states
                manager.update_progress(job_id, progress, f"Scraping {state}...")

                # Pick a random profile for this state run
                profile = random.choice(BROWSER_PROFILES)

                # Context with Stealth Headers & Consistent Profile
                context = browser.new_context(
                    user_agent=profile["ua"],
                    locale="en-US",
                    timezone_id="America/New_York",
                    viewport=profile["screen"],
                    extra_http_headers={
                        "Accept-Language": "en-US,en;q=0.9",
                        "Sec-Ch-Ua": profile["sec_ch_ua"],
                        "Sec-Ch-Ua-Mobile": "?0",
                        "Sec-Ch-Ua-Platform": profile["sec_ch_ua_platform"],
                        "Upgrade-Insecure-Requests": "1",
                    }
                )

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
