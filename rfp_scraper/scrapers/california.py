from rfp_scraper.scrapers.base import BaseScraper
import pandas as pd
from dateutil import parser
import re

class CaliforniaScraper(BaseScraper):
    def scrape(self, page) -> pd.DataFrame:
        url = self.config.get("California", "https://caleprocure.ca.gov/pages/Events-BS3/event-search.aspx")
        try:
            page.goto(url, timeout=60000)
            page.wait_for_load_state("networkidle")

            # Search for "Construction" or Service Code "99999"
            # Assuming there is a search box.
            # Note: Playwright selectors need to be robust.
            # Look for input with id or placeholder.

            # Usually strict inputs have IDs.
            # If we can't find specific ID, try generic placeholders.

            # Based on standard PeopleSoft/Oracle sites (often used by gov),
            # inputs are messy.

            # Let's try to find an input that looks like a keyword search.
            # Or assume we just grab the list if it auto-loads.

            # Instructions: Search for Event Name "Construction"
            try:
                # Try to find input for Event Name.
                # Often labeled "Event Name" or "Description"
                # Using get_by_label if possible, or placeholder
                search_input = page.get_by_label("Event Name", exact=False).first
                if not search_input.count():
                    search_input = page.locator("input[type='text']").first

                if search_input.count():
                    search_input.fill("Construction")
                    search_input.press("Enter")
                    # Wait for results to reload
                    page.wait_for_timeout(5000)
            except Exception as e:
                print(f"CA Search failed: {e}")

            # Extract Table Rows
            # Look for a main results table.
            rows = page.locator("table.ps_grid-body tr, table tr").all()

            data = []
            for row in rows:
                text = row.inner_text()
                if not text.strip(): continue

                # Check headers
                if "Event ID" in text or "End Date" in text:
                    continue

                # Parse columns.
                cells = row.locator("td").all()
                if len(cells) < 3: continue

                # Heuristic Extraction
                # Usually: Event ID, Event Name, End Date, Status

                # Let's try to identify date column
                deadline = None
                title = ""
                slug = ""

                for cell in cells:
                    c_text = cell.inner_text().strip()
                    # Check date
                    try:
                        dt = parser.parse(c_text)
                        # Assume future dates are deadlines
                        if dt.year > 2000: # Sanity check
                             if deadline is None or dt > deadline:
                                 deadline = dt
                    except:
                        pass

                    # Check title (longest text usually)
                    if len(c_text) > 10 and not title:
                         # Ensure it's not a date
                         try:
                             parser.parse(c_text)
                         except:
                             title = c_text

                if not deadline:
                     continue

                if not self.is_qualified(deadline):
                    continue

                slug = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')

                data.append({
                    "clientName": "Cal eProcure",
                    "title": title,
                    "slug": slug,
                    "description": title, # No deep dive for now
                    "walkthroughDate": "",
                    "rfiDate": "",
                    "deadline": deadline.isoformat(),
                    "budgetMin": 0,
                    "jobStreet": "",
                    "jobCity": "Statewide", # Default as per spec if missing
                    "jobState": "CA",
                    "jobZip": "",
                    "portfolioLink": url,
                    "status": "open"
                })

            return self.normalize_data(data)

        except Exception as e:
            print(f"Error scraping CA: {e}")
            return self.normalize_data([])
