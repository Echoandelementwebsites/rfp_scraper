import pandas as pd
from typing import Optional, List
from playwright.sync_api import Page
from bs4 import BeautifulSoup

from rfp_scraper.scrapers.base import BaseScraper
from rfp_scraper.compliance import ComplianceManager
from rfp_scraper.ai_parser import DeepSeekClient
from rfp_scraper.db import DatabaseHandler
from rfp_scraper.utils import (
    clean_text, normalize_date, is_valid_rfp, BLOCKED_URL_PATTERNS, is_future_deadline, is_file_url
)

# JavaScript to extract clean HTML and remove noise (scripts, navs)
EXTRACT_MAIN_CONTENT_JS = """
() => {
    if (!document.body) return "";
    const clone = document.body.cloneNode(true);
    const selectors = ['script', 'style', 'noscript', 'iframe', 'svg', 'nav', 'header', 'footer', '.ad', '#cookie-banner'];
    clone.querySelectorAll(selectors.join(',')).forEach(el => el.remove());

    const main = clone.querySelector('main, article, #content, .content, #main, .main-body');
    if (main) return main.innerHTML.trim();
    return clone.innerHTML.trim();
}
"""

class HierarchicalScraper(BaseScraper):
    def __init__(self, state_name: str, base_scraper: Optional[BaseScraper] = None, api_key: Optional[str] = None):
        self.state_name = state_name
        self.base_scraper = base_scraper
        self.compliance = ComplianceManager()
        self.ai_parser = DeepSeekClient(api_key=api_key)
        self.db = DatabaseHandler()

    def _find_better_url(self, page: Page) -> Optional[str]:
        """
        Scans the page for links matching specific procurement keywords.
        Returns the best match URL or None.
        """
        keywords = ["bids", "rfp", "solicitations", "procurement"]
        try:
            links = page.locator("a").all()
            for link in links:
                try:
                    text = (link.inner_text() or "").lower()
                    href = link.get_attribute("href")
                    if not href:
                        continue

                    if any(k in text for k in keywords):
                        # Resolve absolute URL
                        if not href.startswith("http"):
                             href = page.evaluate(f"new URL('{href}', document.baseURI).href")
                        return href
                except:
                    continue
        except:
            pass
        return None

    def should_visit_url(self, url: str, text: str) -> bool:
        """
        Check if a URL should be visited based on blocked patterns.
        Checks both the URL string and the associated text (e.g. link text or title).
        """
        if not url:
            return False

        url_lower = url.lower()
        text_lower = (text or "").lower()

        for pattern in BLOCKED_URL_PATTERNS:
            if pattern in url_lower or pattern in text_lower:
                return False
        return True

    def scrape(self, page: Page) -> pd.DataFrame:
        results = []

        # 1. Run Standard Scraper (Level 1)
        if self.base_scraper:
            print(f"Running Standard Scraper for {self.state_name}...")
            try:
                base_df = self.base_scraper.scrape(page)
                if not base_df.empty:
                    # Sanitize
                    base_df = base_df.where(pd.notnull(base_df), None)
                    base_records = base_df.to_dict('records')

                    print(f"Standard Scraper found {len(base_records)} items. Starting 3-Stage QA Pipeline...")

                    for row in base_records:
                        # Raw Data
                        raw_title = row.get('title') or "Untitled"
                        raw_link = row.get('portfolioLink') or row.get('link') or ""
                        raw_client = row.get('clientName') or row.get('client') or "Unknown Client"
                        raw_deadline = row.get('deadline')

                        # --- Pre-Fetch Block ---
                        if not self.should_visit_url(raw_link, raw_title):
                            print(f"Blocked (Pattern): {raw_title} -> {raw_link}")
                            continue

                        # --- Stage 2: Cleaning (Part A) ---
                        title = clean_text(raw_title)
                        # We don't have description yet, so we pass empty string for now or wait until extraction?
                        # The plan says "Stage 2 (Cleaning): ... if not is_valid_rfp(title, desc, client): continue"
                        # But description comes from visiting the page (Stage 1 extraction/visit).
                        # Let's clean what we have.
                        client = clean_text(raw_client)
                        deadline = normalize_date(raw_deadline)

                        # 1. State Check
                        if not self.state_name or self.state_name == "Unknown":
                            print(f"Skipping {title}: Invalid state ({self.state_name})")
                            continue

                        # 2. Deadline Check
                        if not is_future_deadline(deadline, buffer_days=2):
                            # print(f"Skipping {title}: Deadline too soon or invalid ({deadline})")
                            continue

                        # Check Validity on Title/Client before visiting (save time)
                        if not is_valid_rfp(title, "", client):
                            print(f"Invalid RFP (Title/Client): {title}")
                            continue

                        # --- Stage 1: Extraction / Content Verification ---
                        rfp_description = ""
                        if not raw_link:
                            print(f"Skipping {title}: No link provided.")
                            continue

                        try:
                            print(f"Verifying: {title} -> {raw_link}")
                            # Navigate
                            try:
                                response = page.goto(raw_link, wait_until="domcontentloaded", timeout=20000)
                            except Exception:
                                response = None

                            if not response or response.status >= 400:
                                print(f"Broken link ({response.status if response else 'Error'}): {raw_link}")
                                continue

                            # Extract Text
                            rfp_description = clean_text(page.evaluate("document.body.innerText"), title_case=False)
                            if not rfp_description:
                                rfp_description = ""

                        except Exception as e:
                            print(f"Link verification failed for {raw_link}: {e}")
                            continue

                        # --- Stage 2: Cleaning (Part B - Full Context) ---
                        # Now check with description
                        if not is_valid_rfp(title, rfp_description, client):
                            print(f"Invalid RFP (Content): {title}")
                            continue

                        # --- Stage 3: Classification ---
                        trades = []
                        if self.ai_parser:
                            trades = self.ai_parser.classify_csi_divisions(title, rfp_description)

                        if not trades:
                            print(f"Discarded (Non-Construction): {title}")
                            continue

                        matching_trades_str = ", ".join(trades)
                        print(f"Accepted: {title} [{matching_trades_str}]")

                        # Save to DB
                        slug = self.db.generate_slug(title, client, raw_link)
                        self.db.insert_bid(
                            slug, client, title, deadline, raw_link,
                            state=self.state_name,
                            rfp_description=rfp_description,
                            matching_trades=matching_trades_str
                        )

                        # Add to results list
                        row['title'] = title
                        row['client'] = client
                        row['deadline'] = deadline
                        row['rfp_description'] = rfp_description
                        row['matching_trades'] = matching_trades_str
                        results.append(row)

            except Exception as e:
                print(f"Standard scraper failed for {self.state_name}: {e}")

        # 2. Run Deep Scan (Level 2)
        print(f"Running Deep Scan for {self.state_name}...")

        # Fetch agencies from DB
        df_states = self.db.get_all_states()
        state_row = df_states[df_states['name'] == self.state_name]

        if state_row.empty:
            print(f"State {self.state_name} not found in DB. Skipping deep scan.")
            return pd.DataFrame(results)

        state_id = int(state_row.iloc[0]['id'])
        df_agencies = self.db.get_agencies_by_state(state_id)

        if df_agencies.empty:
            print(f"No agencies found in DB for {self.state_name}. Skipping deep scan.")
            return pd.DataFrame(results)

        for _, row in df_agencies.iterrows():
            agency_name = row['organization_name']
            url = row['url']

            print(f"Deep Scan: Scanning {agency_name} at {url}")

            # Pre-Flight: Check for File URL
            if is_file_url(url):
                print(f"Skipping {url}: Detected File Download.")
                continue

            # Compliance Check
            if not self.compliance.can_fetch(url):
                print(f"Skipping {url} due to robots.txt or rate limit.")
                continue

            try:
                # Navigate (Fail Fast)
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=10000)
                except Exception as e:
                    print(f"Blocked/Timeout/Error: {url} -> {e}")
                    continue

                # Smart Navigation
                better_url = self._find_better_url(page)
                if better_url:
                    if is_file_url(better_url):
                        print(f"Skipping better URL (File): {better_url}")
                    else:
                        print(f"Navigating to better URL: {better_url}")
                        try:
                            page.goto(better_url, wait_until="domcontentloaded", timeout=10000)
                        except Exception:
                            pass # Fallback to original page if better URL fails

                # Extract Text (Cleaned) using JS
                content = page.evaluate(EXTRACT_MAIN_CONTENT_JS)

                # AI Parsing (Stage 1 Extraction)
                extracted_bids = self.ai_parser.parse_rfp_content(content)

                for bid in extracted_bids:
                    # --- Stage 2: Cleaning ---
                    raw_title = bid.get('title', 'Untitled')
                    raw_client = bid.get('clientName') or agency_name
                    raw_deadline = bid.get('deadline')

                    # Clean description without title casing
                    description = clean_text(bid.get('description', ''), title_case=False)

                    title = clean_text(raw_title)
                    client = clean_text(raw_client)
                    deadline = normalize_date(raw_deadline)

                    # 1. State Check
                    if not self.state_name or self.state_name == "Unknown":
                        continue

                    # 2. Deadline Check
                    if not is_future_deadline(deadline, buffer_days=2):
                        continue

                    if not is_valid_rfp(title, description, client):
                        # print(f"Invalid RFP (Deep Scan): {title}") # Optional verbosity
                        continue

                    # --- Stage 3: Classification ---
                    trades = self.ai_parser.classify_csi_divisions(title, description)
                    if not trades:
                        continue

                    matching_trades_str = ", ".join(trades)

                    # Deduplication Slug
                    slug = self.db.generate_slug(title, client, url)

                    # Save to DB (Persistence)
                    self.db.insert_bid(
                        slug, client, title, deadline, url,
                        state=self.state_name,
                        rfp_description=description,
                        matching_trades=matching_trades_str
                    )

                    print(f"Accepted (Deep Scan): {title} [{matching_trades_str}]")

                    # Append to results for current run
                    results.append({
                        "title": title,
                        "client": client,
                        "deadline": deadline,
                        "description": description,
                        "link": url,
                        "source_type": "Deep Scan",
                        "rfp_description": description,
                        "matching_trades": matching_trades_str
                    })

            except Exception as e:
                print(f"Error processing {url}: {e}")

        return pd.DataFrame(results)
