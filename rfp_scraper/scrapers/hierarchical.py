import pandas as pd
from typing import Optional
from playwright.sync_api import Page
from bs4 import BeautifulSoup

from rfp_scraper.scrapers.base import BaseScraper
from rfp_scraper.compliance import ComplianceManager
from rfp_scraper.ai_parser import DeepSeekClient
from rfp_scraper.db import DatabaseHandler

class HierarchicalScraper(BaseScraper):
    def __init__(self, state_name: str, base_scraper: Optional[BaseScraper] = None, api_key: Optional[str] = None):
        self.state_name = state_name
        self.base_scraper = base_scraper
        self.compliance = ComplianceManager()
        self.ai_parser = DeepSeekClient(api_key=api_key)
        self.db = DatabaseHandler()

    def scrape(self, page: Page) -> pd.DataFrame:
        results = []

        # 1. Run Standard Scraper (Level 1)
        if self.base_scraper:
            print(f"Running Standard Scraper for {self.state_name}...")
            try:
                base_df = self.base_scraper.scrape(page)
                if not base_df.empty:
                    # Sanitize: Convert NaN to None to ensure boolean checks work
                    base_df = base_df.where(pd.notnull(base_df), None)
                    base_records = base_df.to_dict('records')
                    results.extend(base_records)

                    # Persistence: Save Level 1 results to DB
                    for row in base_records:
                        # Extract fields safely
                        client = row.get('clientName') or row.get('client') or "Unknown Client"
                        title = row.get('title') or "Untitled"
                        deadline = row.get('deadline')

                        # Determine link
                        link = row.get('portfolioLink') or row.get('link') or ""

                        # Determine slug
                        if 'slug' in row and row['slug']:
                            slug = row['slug']
                        else:
                            slug = self.db.generate_slug(title, client, link)

                        self.db.insert_bid(slug, client, title, deadline, link, state=self.state_name)

            except Exception as e:
                print(f"Standard scraper failed for {self.state_name}: {e}")

        # 2. Run Deep Scan (Level 2)
        print(f"Running Deep Scan for {self.state_name}...")

        # Fetch agencies from DB
        # Look up state_id by name
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

            # Compliance Check
            if not self.compliance.can_fetch(url):
                print(f"Skipping {url} due to robots.txt or rate limit.")
                continue

            try:
                # Navigate
                page.goto(url, wait_until="domcontentloaded", timeout=30000)

                # Extract Text (Cleaned)
                # We want visible text mostly
                content = page.evaluate("document.body.innerText")

                # AI Parsing
                extracted_bids = self.ai_parser.parse_rfp_content(content)

                for bid in extracted_bids:
                    # Normalize keys
                    title = bid.get('title', 'Untitled')
                    client = bid.get('clientName') or agency_name
                    deadline = bid.get('deadline')
                    description = bid.get('description', '')

                    # Deduplication Slug
                    slug = self.db.generate_slug(title, client, url)

                    # Save to DB (Persistence)
                    self.db.insert_bid(slug, client, title, deadline, url, state=self.state_name)

                    # Append to results for current run
                    results.append({
                        "title": title,
                        "client": client,
                        "deadline": deadline,
                        "description": description,
                        "link": url, # Using source URL as link since we don't have deep links from AI yet
                        "source_type": "Deep Scan"
                    })

            except Exception as e:
                print(f"Error processing {url}: {e}")

        return pd.DataFrame(results)
