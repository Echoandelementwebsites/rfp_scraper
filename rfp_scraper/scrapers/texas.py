from rfp_scraper.scrapers.base import BaseScraper
import pandas as pd
from dateutil import parser
import re

class TexasScraper(BaseScraper):
    def scrape(self, page) -> pd.DataFrame:
        url = self.config.get("Texas", "https://www.txsmartbuy.gov/esbd")
        try:
            page.goto(url, timeout=60000)
            page.wait_for_load_state("networkidle")

            # Filter by NIGP Class "909" (Building Construction).
            # Look for input or filter.

            try:
                # Assuming there is an input for NIGP Code
                nigp_input = page.get_by_placeholder("NIGP", exact=False).first
                if not nigp_input.count():
                     nigp_input = page.locator("input[name*='nigp'], input[id*='nigp']").first

                if nigp_input.count():
                    nigp_input.fill("909")
                    # Press enter or click search
                    page.get_by_role("button", name="Search", exact=False).click()
                    page.wait_for_timeout(5000)
            except:
                pass

            rows = page.locator("table tr, .row").all()
            data = []

            for row in rows:
                text = row.inner_text()
                if "Deadline" in text or "Due Date" in text:
                     # This might be header or row
                     pass

                # Heuristic extraction
                # TX ESBD usually has nicely formatted cards or table

                deadline = None
                title = ""

                # Check for dates in text
                # RegEx for date
                date_matches = re.findall(r'(\d{1,2}/\d{1,2}/\d{4})', text)
                for d_str in date_matches:
                    try:
                        dt = parser.parse(d_str)
                        if dt.year > 2000:
                            if deadline is None or dt > deadline:
                                deadline = dt
                    except:
                        pass

                # Extract Title (first line or bold)
                lines = text.split('\n')
                if lines:
                    title = lines[0]

                if not deadline: continue
                if not self.is_qualified(deadline): continue

                slug = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')

                data.append({
                    "clientName": "TX SmartBuy",
                    "title": title,
                    "slug": slug,
                    "description": text.replace("\n", " "),
                    "walkthroughDate": "",
                    "rfiDate": "",
                    "deadline": deadline.isoformat(),
                    "budgetMin": 0,
                    "jobStreet": "",
                    "jobCity": "Statewide",
                    "jobState": "TX",
                    "jobZip": "",
                    "portfolioLink": url,
                    "status": "open"
                })

            return self.normalize_data(data)
        except Exception as e:
            print(f"Error scraping TX: {e}")
            return self.normalize_data([])
