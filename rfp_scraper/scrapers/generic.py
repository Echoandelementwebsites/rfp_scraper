import re
import pandas as pd
from dateutil import parser
from rfp_scraper.scrapers.base import BaseScraper
import datetime

class GenericScraper(BaseScraper):
    def scrape(self, page) -> pd.DataFrame:
        # Convert "NorthDakotaScraper" -> "North Dakota"
        class_name = self.__class__.__name__.replace("Scraper", "")
        # Insert space before capitals
        state_name = re.sub(r'(?<!^)(?=[A-Z])', ' ', class_name).strip()

        url = self.config.get(state_name, "")
        if not url:
             # Fallback 1: Try looking for the key without spaces just in case
             url = self.config.get(class_name, "")
        if not url:
             # Fallback 2: Case-insensitive lookup (e.g. "District Of Columbia" vs "District of Columbia")
             # Create a mapping of lower-cased keys to real keys
             config_lower = {k.lower(): v for k, v in self.config.items() if isinstance(v, str)}
             url = config_lower.get(state_name.lower(), "")

        if not url:
            print(f"No URL found for {state_name} in config.")
            return pd.DataFrame()

        print(f"Scraping {state_name} at {url}")

        try:
            page.goto(url, timeout=15000, wait_until="domcontentloaded")
        except Exception as e:
            print(f"Error navigating to {url}: {e}")
            return pd.DataFrame()

        # Heuristic 1: Search Interaction
        try:
            # Attempt to find an input field (placeholder "search", "keyword"), type "Construction", and press Enter.
            inputs = page.locator("input[type='text'], input[type='search']").all()
            # Try first 3
            for inp in inputs[:3]:
                if inp.is_visible():
                    try:
                        inp.fill("Construction")
                        inp.press("Enter")
                        page.wait_for_timeout(3000)
                        break
                    except:
                        continue
        except Exception as e:
            print(f"Search interaction failed (non-fatal): {e}")

        # Heuristic 2: Table Extraction
        # Find all <tr> elements. If few exist, look for div[role='row'].
        rows = page.locator("table tr").all()
        if len(rows) < 5:
            rows = page.locator("div[role='row']").all()

        data = []
        for row in rows:
            try:
                row_text = row.inner_text().strip()
                if not row_text:
                    continue

                # Filter out obvious header rows or short rows
                if len(row_text) < 10:
                    continue

                # Heuristic 3: Data Parsing

                # Date Detection: Use dateutil.parser to find the first valid future date in the row text.
                # Treat this as the deadline.
                # We split text by common delimiters to try and isolate date strings
                parts = re.split(r'[\n\t\r]', row_text)

                deadline = None
                potential_dates = []

                for part in parts:
                    part = part.strip()
                    if len(part) < 6: # too short for a date
                        continue
                    try:
                        dt = parser.parse(part, fuzzy=True)
                        # Sanity check: year must be reasonable (e.g. current year or next few)
                        if dt.year < 2020 or dt.year > 2030:
                            continue
                        potential_dates.append(dt)
                    except:
                        pass

                # Select the latest date that is in the future
                future_dates = [d for d in potential_dates if d > self.now]
                if future_dates:
                    deadline = max(future_dates)

                if not deadline:
                    continue

                # Filtering: Apply the strict is_qualified() date check (Deadline >= Today + 4 days).
                if not self.is_qualified(deadline):
                    continue

                # Title Detection: Treat the longest text segment in the row as the title.
                # parts is already split by newlines/tabs
                title = ""
                if parts:
                    title = max(parts, key=len)

                if not title:
                    title = row_text[:100] # Fallback

                # Link extraction (heuristic: find first link in row)
                link_el = row.locator("a").first
                portfolio_link = url
                if link_el.count() > 0:
                    href = link_el.get_attribute("href")
                    if href:
                        if href.startswith("http"):
                            portfolio_link = href
                        elif href.startswith("/"):
                            # Try to construct absolute URL
                            # This is tricky without the base URL easily accessible from page object in some contexts,
                            # but page.url gives current URL.
                            from urllib.parse import urljoin
                            portfolio_link = urljoin(page.url, href)

                data.append({
                    "clientName": state_name, # Default to state name as client
                    "title": title,
                    "slug": re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-'),
                    "description": row_text.replace("\n", " ").strip(),
                    "walkthroughDate": "",
                    "rfiDate": "",
                    "deadline": deadline.isoformat(),
                    "budgetMin": 0,
                    "jobStreet": "",
                    "jobCity": "Statewide", # Default
                    "jobState": "", # Could infer from state_name but schema says jobState
                    "jobZip": "",
                    "portfolioLink": portfolio_link,
                    "status": "open"
                })

            except Exception as e:
                # print(f"Error parsing row: {e}")
                continue

        return self.normalize_data(data)
