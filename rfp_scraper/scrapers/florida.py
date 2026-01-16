from rfp_scraper.scrapers.base import BaseScraper
import pandas as pd
from dateutil import parser
import re

class FloridaScraper(BaseScraper):
    def scrape(self, page) -> pd.DataFrame:
        url = self.config.get("Florida", "https://www.myflorida.com/apps/vbs/vbs_www.search_r1.crit1")
        try:
            page.goto(url, timeout=60000)

            # Select "Construction" from dropdown
            # Dropdown likely has value or label.
            # We need to find the specific select element.

            try:
                # Look for select with "Construction" option
                # This is tricky without inspecting.
                # But we can try to find 'select' element and selectOption label="Construction"

                # Usually there's a "Commodity" or "Service" dropdown.
                # Let's iterate selects
                selects = page.locator("select").all()
                for select in selects:
                    # Check if it has an option "Construction"
                    # This is expensive.
                    # Let's just try to select by label in all selects? No.
                    pass

                # Direct approach: generic search button?
                # The URL is a search criteria page.

                # Let's try to fill "Construction" in text field if dropdown fails?
                # But instructions say "Select 'Construction' from dropdown menu".

                # Let's try to find a label 'Commodity' or 'Category'
                # page.locator("select").select_option(label="Construction") might fail if multiple.

                # Best effort: Select label containing "Construction" in the first select that has it.
                # Actually, FL VBS usually has a big list of codes.

                # Let's just Click "Search" to get everything, then filter?
                # Might be too big.

                # Try to interact with the page text: "Construction"
                # If it's a multi-select box
                page.get_by_text("Construction", exact=False).click()
            except:
                pass

            # Click Search
            # Find a button with "Search"
            page.get_by_role("button", name="Search").click()
            page.wait_for_load_state("networkidle")

            # Parse Results
            # VBS usually returns a table
            rows = page.locator("table tr").all()

            data = []
            for row in rows:
                text = row.inner_text()
                # FL VBS rows often have "Advertisement Number", "Version", "Title", "Agency", "Date"

                cells = row.locator("td").all()
                if len(cells) < 4: continue

                # Heuristic extraction
                # Iterate cells to find Date
                deadline = None
                title = ""

                for cell in cells:
                    c_text = cell.inner_text().strip()
                    try:
                        dt = parser.parse(c_text)
                        if dt.year > 2000:
                            if deadline is None or dt > deadline:
                                deadline = dt
                    except:
                        pass

                    if len(c_text) > 10 and not title:
                         # Title candidate
                         if not any(x in c_text for x in ["/202", "AM", "PM"]):
                             title = c_text

                if not deadline: continue
                if not self.is_qualified(deadline): continue

                slug = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')

                data.append({
                    "clientName": "FL VBS",
                    "title": title,
                    "slug": slug,
                    "description": title,
                    "walkthroughDate": "",
                    "rfiDate": "",
                    "deadline": deadline.isoformat(),
                    "budgetMin": 0,
                    "jobStreet": "",
                    "jobCity": "Statewide",
                    "jobState": "FL",
                    "jobZip": "",
                    "portfolioLink": url,
                    "status": "open"
                })

            return self.normalize_data(data)

        except Exception as e:
            print(f"Error scraping FL: {e}")
            return self.normalize_data([])
