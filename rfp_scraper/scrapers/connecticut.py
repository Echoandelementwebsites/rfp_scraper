from rfp_scraper.scrapers.base import BaseScraper
import pandas as pd
from dateutil import parser
import re

# List of Connecticut towns/cities for location inference (reused)
CT_CITIES = [
    "Andover", "Ansonia", "Ashford", "Avon", "Barkhamsted", "Beacon Falls", "Berlin", "Bethany", "Bethel", "Bethlehem",
    "Bloomfield", "Bolton", "Bozrah", "Branford", "Bridgeport", "Bridgewater", "Bristol", "Brookfield", "Brooklyn",
    "Burlington", "Canaan", "Canterbury", "Canton", "Chaplin", "Cheshire", "Chester", "Clinton", "Colchester",
    "Colebrook", "Columbia", "Cornwall", "Coventry", "Cromwell", "Danbury", "Darien", "Deep River", "Derby", "Durham",
    "East Granby", "East Haddam", "East Hampton", "East Hartford", "East Haven", "East Lyme", "East Windsor", "Eastford",
    "Easton", "Ellington", "Enfield", "Essex", "Fairfield", "Farmington", "Franklin", "Glastonbury", "Goshen", "Granby",
    "Greenwich", "Griswold", "Groton", "Guilford", "Haddam", "Hamden", "Hampton", "Hartford", "Hartland", "Harwinton",
    "Hebron", "Kent", "Killingly", "Killingworth", "Lebanon", "Ledyard", "Lisbon", "Litchfield", "Lyme", "Madison",
    "Manchester", "Mansfield", "Marlborough", "Meriden", "Middlebury", "Middlefield", "Middletown", "Milford", "Monroe",
    "Montville", "Morris", "Naugatuck", "New Britain", "New Canaan", "New Fairfield", "New Hartford", "New Haven",
    "New London", "New Milford", "Newington", "Newtown", "Norfolk", "North Branford", "North Canaan", "North Haven",
    "North Stonington", "Norwalk", "Norwich", "Old Lyme", "Old Saybrook", "Orange", "Oxford", "Plainfield", "Plainville",
    "Plymouth", "Pomfret", "Portland", "Preston", "Prospect", "Putnam", "Redding", "Ridgefield", "Rocky Hill", "Roxbury",
    "Salem", "Salisbury", "Scotland", "Seymour", "Sharon", "Shelton", "Sherman", "Simsbury", "Somers", "South Windsor",
    "Southbury", "Southington", "Sprague", "Stafford", "Stamford", "Sterling", "Stonington", "Stratford", "Suffield",
    "Thomaston", "Thompson", "Tolland", "Torrington", "Trumbull", "Union", "Vernon", "Voluntown", "Wallingford", "Warren",
    "Washington", "Waterbury", "Waterford", "Watertown", "West Hartford", "West Haven", "Westbrook", "Weston", "Westport",
    "Wethersfield", "Willington", "Wilton", "Winchester", "Windham", "Windsor", "Windsor Locks", "Wolcott", "Woodbridge",
    "Woodbury", "Woodstock"
]

class ConnecticutScraper(BaseScraper):
    def scrape(self, page) -> pd.DataFrame:
        data = []
        try:
            # UConn
            try:
                 uconn_data = self.scrape_uconn(page)
                 data.extend(uconn_data)
            except Exception as e:
                 print(f"Error scraping UConn: {e}")

            # CTSource
            try:
                 ctsource_data = self.scrape_ct_source(page)
                 data.extend(ctsource_data)
            except Exception as e:
                 print(f"Error scraping CTSource: {e}")
        except Exception as e:
            print(f"Error in Connecticut scrape: {e}")

        return self.normalize_data(data)

    def infer_city(self, title, description):
        """Infers city from title or description."""
        text = (str(title) + " " + str(description)).lower()
        for city in CT_CITIES:
            if city.lower() in text:
                return city
        return "Connecticut"

    def scrape_uconn(self, page):
        url = self.config.get("Connecticut", {}).get("UConn", "https://cpfp.procurement.uconn.edu/construction-current-opportunities-2020-2/")
        page.goto(url)
        page.wait_for_timeout(3000)

        results = []
        titles = page.locator("h3, h4").all()

        for title_el in titles:
            title_text = title_el.inner_text().strip()
            if not title_text or "Search" in title_text or "Navigation" in title_text:
                continue

            card = title_el.locator("..")
            card_text = card.inner_text()

            if "Close" not in card_text:
                continue

            match = re.search(r"Close\s+(\d{1,2}/\d{1,2}/\d{4}.*?)(?:\n|$|\r)", card_text)
            if not match:
                continue

            date_str = match.group(1).strip()
            try:
                deadline = parser.parse(date_str, fuzzy=True)
            except:
                continue

            if not self.is_qualified(deadline):
                continue

            # Number
            number_match = re.search(r"Number\s+([^\n]+)", card_text)
            slug_base = number_match.group(1).strip() if number_match else title_text
            slug = re.sub(r'[^a-z0-9]+', '-', (slug_base + "-" + title_text).lower()).strip('-')

            link_el = card.locator("a").first
            link = link_el.get_attribute("href") if link_el.count() else url

            city = self.infer_city(title_text, card_text)

            results.append({
                "clientName": "UConn",
                "title": title_text,
                "slug": slug,
                "description": card_text.replace("\n", " ").strip()[:500],
                "walkthroughDate": "",
                "rfiDate": "",
                "deadline": deadline.isoformat(),
                "budgetMin": 0,
                "jobStreet": "",
                "jobCity": city,
                "jobState": "CT",
                "jobZip": "",
                "portfolioLink": link,
                "status": "open"
            })
        return results

    def scrape_ct_source(self, page):
        url = self.config.get("Connecticut", {}).get("CTSource", "https://portal.ct.gov/das/ctsource/bidboard")
        page.goto(url)
        page.wait_for_timeout(5000)

        try:
            search_box = page.get_by_placeholder("Search", exact=False).first
            if not search_box.count():
                search_box = page.locator("input[type='search']").first

            if search_box.count():
                search_box.fill("Construction")
                search_box.press("Enter")
                page.wait_for_timeout(3000)
        except:
            pass

        rows = page.locator("table tr").all()
        results = []

        for row in rows:
            text = row.inner_text()
            cells = row.locator("td").all()
            if not cells: continue

            deadline = None
            title = ""
            details_link = None

            for cell in cells:
                cell_text = cell.inner_text().strip()
                try:
                    parsed = parser.parse(cell_text)
                    if parsed.year > 2000:
                        if deadline is None or parsed > deadline:
                            deadline = parsed
                except:
                    pass

                if len(cell_text) > 10 and not title:
                     try:
                         parser.parse(cell_text)
                     except:
                         title = cell_text
                         link = cell.locator("a").first
                         if link.count():
                             details_link = link.get_attribute("href")

            if not deadline: continue
            if not self.is_qualified(deadline): continue

            keywords = ["Construction", "Paving", "Roofing", "HVAC", "Renovation", "Demolition"]
            if not any(k.lower() in text.lower() for k in keywords):
                continue

            if details_link:
                if not details_link.startswith("http"):
                    details_link = "https://portal.ct.gov" + details_link
            else:
                details_link = url

            city = self.infer_city(title, text)

            results.append({
                "clientName": "CT DAS",
                "title": title,
                "slug": re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-'),
                "description": title,
                "walkthroughDate": "",
                "rfiDate": "",
                "deadline": deadline.isoformat(),
                "budgetMin": 0,
                "jobStreet": "",
                "jobCity": city,
                "jobState": "CT",
                "jobZip": "",
                "portfolioLink": details_link,
                "status": "open"
            })

        return results
