import pandas as pd
import re
import os
import asyncio
import json
import threading
import requests
import PyPDF2
import io
from typing import Optional, List
from playwright.sync_api import Page
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from crawl4ai.async_configs import LLMConfig

from rfp_scraper.scrapers.base import BaseScraper
from rfp_scraper.compliance import ComplianceManager
from rfp_scraper.ai_parser import DeepSeekClient
from rfp_scraper.db import DatabaseHandler
from rfp_scraper.utils import (
    clean_text, normalize_date, is_valid_rfp, BLOCKED_URL_PATTERNS, is_future_deadline, is_file_url
)
from rfp_scraper.behavior import smooth_scroll, human_delay, mimic_human_arrival

class ExtractedBidSchema(BaseModel):
    title: str = Field(description="The title or name of the project/RFP.")
    deadline: str = Field(description="The due date of the bid in YYYY-MM-DD format. Return empty string if none.")
    description: str = Field(description="A brief summary of the work required.")
    clientName: str = Field(description="The name of the agency, city, or department issuing the bid.")
    link: str = Field(description="The specific absolute URL pointing to the full details OR the PDF document of this specific bid. Return empty string if none.")

# JavaScript to extract clean HTML and remove noise (scripts, navs)
EXTRACT_MAIN_CONTENT_JS = """
() => {
    if (!document.body) return "";
    const clone = document.body.cloneNode(true);
    const selectors = ['script', 'style', 'noscript', 'iframe', 'svg', 'nav', 'header', 'footer', '.ad', '#cookie-banner'];
    clone.querySelectorAll(selectors.join(',')).forEach(el => el.remove());

    const main = clone.querySelector('main, article, #content, .content, #main, .main-body');
    if (main) return main.innerText.trim();
    return clone.innerText.trim();
}
"""

class HierarchicalScraper(BaseScraper):
    def __init__(self, state_name: str, base_scraper: Optional[BaseScraper] = None, api_key: Optional[str] = None, manager=None, job_id=None):
        self.state_name = state_name
        self.base_scraper = base_scraper
        self.compliance = ComplianceManager()
        self.ai_parser = DeepSeekClient(api_key=api_key)
        self.db = DatabaseHandler()
        self.manager = manager
        self.job_id = job_id

    def _handle_captcha(self, page, agency_name):
        """
        Checks for CAPTCHA/bot protection and returns False if blocked so the scraper can skip it.
        """
        # Common block indicators
        block_markers = ["verify you are human", "just a moment", "challenge", "cf-turnstile", "security check", "access denied"]

        # Fast check using title (cheapest) and partial content
        is_blocked = False
        try:
            if any(m in page.title().lower() for m in block_markers):
                is_blocked = True
            else:
                # Only check content if title was clean (avoids expensive DOM read if not needed)
                content_snippet = page.evaluate("document.body.innerText.slice(0, 1000)").lower()
                if any(m in content_snippet for m in block_markers):
                    is_blocked = True
        except:
            pass # Page might be closed or empty

        if is_blocked:
            print(f"🛑 CAPTCHA BLOCK DETECTED on {agency_name}! Skipping site immediately.")
            return False # Returning False tells the main loop to skip this URL

        return True # Not blocked, safe to proceed

    def _find_better_url(self, page: Page) -> Optional[str]:
        """
        INSTANT SCAN: Runs JavaScript inside the browser to find procurement links.
        Replaces the slow Python loop that caused hangs on large sites.
        """
        return page.evaluate("""
            () => {
                const keywords = ["bids", "rfp", "solicitations", "procurement", "contracting", "tenders"];
                // Get all links at once
                const links = Array.from(document.querySelectorAll('a'));

                for (const link of links) {
                    const text = (link.innerText || "").toLowerCase();
                    const href = link.href; // .href gets the absolute URL

                    if (!href || href.startsWith('javascript') || href.startsWith('mailto') || href.startsWith('tel')) continue;

                    // Strict Keyword Matching
                    if (keywords.some(k => text.includes(k))) {
                        // Avoid generic "home" links if possible
                        if (text.length < 30) return href;
                    }
                }
                return null;
            }
        """)

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

    async def _extract_with_crawl4ai(self, url: str) -> List[dict]:
        """Uses Crawl4AI to bypass Cloudflare, flatten iframes, and extract JSON using DeepSeek."""
        print(f"   -> 🕷️ Launching Crawl4AI for: {url}")

        extraction_strategy = LLMExtractionStrategy(
            llm_config=LLMConfig(
                provider="openai/deepseek-chat",
                api_token=self.ai_parser.api_key,
                base_url="https://api.deepseek.com"
            ),
            schema=ExtractedBidSchema.model_json_schema(),
            extraction_type="schema",
            instruction=(
                "Extract all construction, infrastructure, and maintenance RFPs. "
                "Ignore Janitorial, Software, or Admin work. "
                "ALWAYS try to find the specific detail link or PDF link for the bid."
            )
        )

        run_config = CrawlerRunConfig(
            extraction_strategy=extraction_strategy,
            cache_mode=CacheMode.BYPASS,
            word_count_threshold=10,
            process_iframes=True,
            remove_overlay_elements=True,
            magic=True
        )

        browser_config = BrowserConfig(headless=True, verbose=False)

        try:
            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(url=url, config=run_config)
                if not result.success: return []
                if result.extracted_content:
                    parsed_data = json.loads(result.extracted_content)
                    if isinstance(parsed_data, list): return parsed_data
                    elif isinstance(parsed_data, dict) and "items" in parsed_data: return parsed_data["items"]
                    elif isinstance(parsed_data, dict): return [parsed_data]
                return []
        except:
            return []

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

                        if is_file_url(raw_link):
                            print(f"Skipping (File): {raw_title} -> {raw_link}")
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
                        if not is_future_deadline(deadline, buffer_days=4):
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

        # Ensure logs directory exists
        os.makedirs("logs/errors", exist_ok=True)

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

            # --- DEDUPLICATION CHECK ---
            if self.db.url_already_scraped(url):
                print(f"Skipping {agency_name}: URL already scraped.")
                continue  # Skip to next agency

            # Pre-Flight: Check for File URL
            if is_file_url(url):
                print(f"Skipping {url}: Detected File Download.")
                continue

            # Compliance Check (Soft Bypass during testing)
            if not self.compliance.can_fetch(url):
                print(f"   ⚠️ WARNING: {url} flagged by compliance/robots.txt, but proceeding anyway.")
                # continue  <-- Commented out to prevent hard skipping

            try:
                # 1. NAVIGATION (With Timeout)
                try:
                    # 20s hard limit on loading
                    mimic_human_arrival(page, url, referrer_url="https://www.google.com/", timeout=20000)

                    # --- FIX: Wait for Human instead of Skipping ---
                    if not self._handle_captcha(page, agency_name):
                        continue # Timed out or failed
                    # -----------------------------------------------

                    # 10s hard limit on scrolling
                    smooth_scroll(page, max_seconds=10)
                except Exception as e:
                    print(f"   ⚠️ Navigation incomplete: {str(e)[:50]}")
                    # We continue anyway; maybe the page loaded partially.

                # 2. SMART NAVIGATION (Instant JS version)
                # This call now takes 50ms instead of 5 minutes
                better_url = self._find_better_url(page)

                if better_url and better_url != page.url and better_url != url:
                    print(f"   -> Better URL found: {better_url}")
                    if is_file_url(better_url):
                         print(f"   -> Skipping better URL (File): {better_url}")
                    else:
                        try:
                            # Navigate to the better section
                            mimic_human_arrival(page, better_url, referrer_url=page.url, timeout=15000)
                            smooth_scroll(page, max_seconds=5)
                        except:
                            print("   -> Could not load better URL, staying on main page.")

                target_url = better_url if (better_url and better_url != page.url and better_url != url) else page.url

                # 3. CRAWL4AI LIST EXTRACTION (Thread-Safe with Guillotine)
                print(f"   -> 🤖 Analyzing List Page via Crawl4AI...")
                try:
                    extracted_bids = []
                    ai_error = None

                    def run_async_crawl():
                        nonlocal extracted_bids, ai_error
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            # INTERNAL GUILLOTINE: 120 seconds
                            extracted_bids = loop.run_until_complete(
                                asyncio.wait_for(self._extract_with_crawl4ai(target_url), timeout=120.0)
                            )
                        except asyncio.TimeoutError:
                            ai_error = Exception("Crawl4AI hit the 120s internal timeout.")
                        except Exception as e:
                            ai_error = e
                        finally:
                            try: loop.close()
                            except: pass

                    t = threading.Thread(target=run_async_crawl)
                    t.start()
                    t.join(timeout=130.0) # EXTERNAL GUILLOTINE

                    if t.is_alive():
                        raise Exception("CRITICAL HANG: Crawl4AI thread zombified.")
                    if ai_error:
                        raise ai_error

                    if extracted_bids:
                        print(f"   ✅ Crawl4AI found {len(extracted_bids)} bids.")

                        # GOVERNOR: Cap maximum detail extractions to 25 to prevent infinite loops
                        for bid in extracted_bids[:25]:
                            raw_title = bid.get('title', 'Untitled')
                            raw_client = bid.get('clientName') or agency_name
                            raw_deadline = bid.get('deadline')

                            bid_link = bid.get('link') or bid.get('url') or target_url
                            ai_description = clean_text(bid.get('description', ''), title_case=False)
                            description = ai_description

                            # 4. FETCH FULL DESCRIPTION (Handles HTML or PDF)
                            if bid_link and bid_link != url and bid_link != better_url:
                                if is_file_url(bid_link) and ".pdf" in bid_link.lower():
                                    # --- PDF EXTRACTION (Hardened) ---
                                    try:
                                        print(f"      -> 📑 Extracting PDF text: {bid_link}")
                                        # Use tuple timeout: (10s connect, 20s read) to defeat slow-streaming servers
                                        resp = requests.get(bid_link, timeout=(10, 20), headers={"User-Agent": "Mozilla/5.0"})
                                        if resp.status_code == 200:
                                            # Truncate content length to prevent PyPDF2 memory bombs (Max 10MB)
                                            pdf_bytes = resp.content[:10485760]
                                            pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
                                            # Read max 10 pages
                                            pdf_text = " ".join([p.extract_text() for p in pdf_reader.pages[:10] if p.extract_text()])
                                            if len(pdf_text) > 50:
                                                description = clean_text(pdf_text, title_case=False)
                                    except Exception as e:
                                        print(f"      -> ⚠️ PDF extraction failed: {str(e)[:50]}")
                                elif not is_file_url(bid_link):
                                    # --- HTML EXTRACTION (Background Tab) ---
                                    detail_page = None
                                    try:
                                        print(f"      -> 📄 Opening detail page: {bid_link}")
                                        detail_page = page.context.new_page()
                                        detail_page.set_default_timeout(30000) # Hard limit 30s per tab

                                        # Use 'commit' instead of 'domcontentloaded'. It returns as soon as the
                                        # network response is received, ignoring broken third-party trackers.
                                        detail_page.goto(bid_link, wait_until="commit")

                                        # Force a fast 1-second pause to let core DOM paint
                                        detail_page.wait_for_timeout(1000)

                                        full_text = detail_page.evaluate(EXTRACT_MAIN_CONTENT_JS)
                                        if full_text and len(full_text) > len(ai_description):
                                            description = clean_text(full_text, title_case=False)
                                    except Exception as e:
                                        print(f"      -> ⚠️ Detail extraction failed: {str(e)[:50]}")
                                    finally:
                                        if detail_page: detail_page.close()

                            title = clean_text(raw_title)
                            client = clean_text(raw_client)
                            deadline = normalize_date(raw_deadline)

                            # Validation Checks
                            if not self.state_name or self.state_name == "Unknown": continue
                            if not is_future_deadline(deadline, buffer_days=4): continue
                            if not is_valid_rfp(title, description, client): continue

                            # --- Stage 3: Classification ---
                            trades = self.ai_parser.classify_csi_divisions(title, description)
                            if not trades: continue

                            matching_trades_str = ", ".join(trades)
                            slug = self.db.generate_slug(title, client, bid_link)

                            # Save to DB
                            self.db.insert_bid(
                                slug, client, title, deadline, bid_link,
                                state=self.state_name,
                                rfp_description=description,
                                matching_trades=matching_trades_str
                            )

                            print(f"      + Saved: {title} [{matching_trades_str}]")
                            results.append({
                                "title": title, "client": client, "deadline": deadline,
                                "description": description, "link": bid_link,
                                "source_type": "Deep Scan", "rfp_description": description,
                                "matching_trades": matching_trades_str
                            })
                    else:
                        print("   -> No bids found by Crawl4AI.")

                except Exception as ai_err:
                    msg = f"   ❌ AI/Crawl4AI Error: {ai_err}"
                    print(msg)
                    if self.manager: self.manager.add_log(self.job_id, msg)

            except Exception as e:
                msg = f"   ❌ Critical Error on {agency_name}: {e}"
                print(msg)
                if self.manager: self.manager.add_log(self.job_id, msg)
                # Save a screenshot so we know what killed it
                try: page.screenshot(path=f"logs/errors/crash_{agency_name[:10]}.png")
                except: pass

        return pd.DataFrame(results)
