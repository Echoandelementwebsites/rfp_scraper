from rfp_scraper.scrapers.base import BaseScraper
import pandas as pd
from dateutil import parser
import re

class NewYorkScraper(BaseScraper):
    def scrape(self, page) -> pd.DataFrame:
        url = self.config.get("New York", "https://ogs.ny.gov/design-construction-bid-openings")
        try:
            page.goto(url, timeout=60000)

            # This site usually lists links to PDFs or a table.
            # Focus on "Design & Construction" opportunities.

            # Look for table rows
            rows = page.locator("table tr").all()
            data = []

            for row in rows:
                text = row.inner_text()
                if not text.strip(): continue

                cells = row.locator("td").all()
                if not cells: continue

                deadline = None
                title = ""

                # Iterate cells
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
                         # Ensure not date
                         try:
                             parser.parse(c_text)
                         except:
                             title = c_text

                if not deadline: continue
                if not self.is_qualified(deadline): continue

                slug = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')

                data.append({
                    "clientName": "NYS OGS",
                    "title": title,
                    "slug": slug,
                    "description": title,
                    "walkthroughDate": "",
                    "rfiDate": "",
                    "deadline": deadline.isoformat(),
                    "budgetMin": 0,
                    "jobStreet": "",
                    "jobCity": "Statewide",
                    "jobState": "NY",
                    "jobZip": "",
                    "portfolioLink": url,
                    "status": "open"
                })

            return self.normalize_data(data)
        except Exception as e:
            print(f"Error scraping NY: {e}")
            return self.normalize_data([])
