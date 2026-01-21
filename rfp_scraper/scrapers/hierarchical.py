import pandas as pd
from typing import Optional
from playwright.sync_api import Page
from bs4 import BeautifulSoup

from rfp_scraper.scrapers.base import BaseScraper
from rfp_scraper.discovery import DiscoveryEngine
from rfp_scraper.compliance import ComplianceManager
from rfp_scraper.ai_parser import DeepSeekClient
from rfp_scraper.db import DatabaseHandler

class HierarchicalScraper(BaseScraper):
    def __init__(self, state_name: str, base_scraper: Optional[BaseScraper] = None, api_key: Optional[str] = None):
        self.state_name = state_name
        self.base_scraper = base_scraper
        self.discovery = DiscoveryEngine()
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
                    # Convert to list of dicts to merge later
                    results.extend(base_df.to_dict('records'))
            except Exception as e:
                print(f"Standard scraper failed for {self.state_name}: {e}")

        # 2. Run Deep Scan (Level 2)
        print(f"Running Deep Scan for {self.state_name}...")

        # Discovery
        agencies = self.discovery.search_agencies(self.state_name)

        for agency_name, url in agencies:
            print(f"Deep Scan: Found agency {agency_name} at {url}")

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
                    self.db.insert_bid(slug, client, title, deadline, url)

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
