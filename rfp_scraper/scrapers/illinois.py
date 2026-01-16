from rfp_scraper.scrapers.base import BaseScraper
import pandas as pd
from dateutil import parser
import re

class IllinoisScraper(BaseScraper):
    def scrape(self, page) -> pd.DataFrame:
        url = self.config.get("Illinois", "https://www.bidbuy.illinois.gov/bso/")
        try:
            page.goto(url, timeout=60000)

            # Look for "Open Bids" -> "Construction".
            # This is likely a navigation step.

            try:
                # Click "Open Bids" link if available
                page.get_by_text("Open Bids", exact=False).click()
                page.wait_for_load_state("networkidle")
            except:
                pass

            # Search "Construction"
            try:
                # There might be a search input on the home page or open bids page
                search_input = page.locator("input[type='text'], input[type='search']").first
                if search_input.count():
                    search_input.fill("Construction")
                    search_input.press("Enter")
                    page.wait_for_timeout(5000)
            except:
                pass

            # Parse table
            rows = page.locator("table tr").all()
            data = []

            for row in rows:
                text = row.inner_text()
                if not text.strip(): continue

                cells = row.locator("td").all()
                if not cells: continue

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
                         try:
                             parser.parse(c_text)
                         except:
                             title = c_text

                if not deadline: continue
                if not self.is_qualified(deadline): continue

                slug = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')

                data.append({
                    "clientName": "IL BidBuy",
                    "title": title,
                    "slug": slug,
                    "description": title,
                    "walkthroughDate": "",
                    "rfiDate": "",
                    "deadline": deadline.isoformat(),
                    "budgetMin": 0,
                    "jobStreet": "",
                    "jobCity": "Statewide",
                    "jobState": "IL",
                    "jobZip": "",
                    "portfolioLink": url,
                    "status": "open"
                })

            return self.normalize_data(data)
        except Exception as e:
            print(f"Error scraping IL: {e}")
            return self.normalize_data([])
