import json
import asyncio
import requests
import io
import PyPDF2
from typing import List, Optional
from pydantic import BaseModel, Field

from crawl4ai import AsyncWebCrawler
from crawl4ai.extraction_strategy import LLMExtractionStrategy

from rfp_scraper_v2.core.models import Agency, Bid
from rfp_scraper_v2.core.database import DatabaseHandler
from rfp_scraper_v2.crawlers.engine import engine

# Semaphore to control concurrent browser instances for detail fetching
SEM_BIDS = asyncio.Semaphore(3)

# --- Step 1 Schema ---
class ProcurementLink(BaseModel):
    url: str = Field(..., description="The single absolute URL pointing to the agency's procurement/bids portal.")

# --- Step 2 Schema ---
class BidItem(BaseModel):
    title: str
    clientName: str
    deadline: str
    description: str
    link: str

# --- Pipeline Functions ---

async def discover_procurement_link(agency: Agency) -> Optional[str]:
    """
    Step 1: AI-Driven Discovery (The Pathfinder)
    Fetches homepage, uses DeepSeek to find the procurement URL.
    """
    if not agency.homepage_url:
        print(f"Skipping Discovery: No homepage for {agency.name}")
        return None

    print(f"[{agency.name}] Step 1: Discovery on {agency.homepage_url}")

    # Configure LLM Strategy for Discovery
    strategy = LLMExtractionStrategy(
        llm_config=engine.get_llm_config(),
        schema=ProcurementLink.model_json_schema(),
        extraction_type="schema",
        instruction=(
            "Analyze the homepage content and return the single absolute URL that points to the "
            "agency's procurement, bids, or purchasing portal. Look for 'Bids', 'RFP', 'Purchasing', 'Business'. "
            "If not found, return null."
        )
    )

    config = engine.get_run_config(strategy=strategy)

    try:
        async with AsyncWebCrawler(config=engine.get_browser_config()) as crawler:
            result = await crawler.arun(url=agency.homepage_url, config=config)

            if result.success and result.extracted_content:
                data = json.loads(result.extracted_content)
                # Handle list or dict return
                if isinstance(data, list) and data:
                    return data[0].get("url")
                elif isinstance(data, dict):
                    return data.get("url")

            print(f"[{agency.name}] Discovery Failed (No URL found).")
            return None
    except Exception as e:
        print(f"[{agency.name}] Discovery Error: {e}")
        return None

async def extract_bids(procurement_url: str) -> List[Bid]:
    """
    Step 2: List Extraction (The Harvester)
    Uses Crawl4AI + DeepSeek to extract a list of bids.
    """
    print(f"Step 2: Harvesting from {procurement_url}")

    strategy = LLMExtractionStrategy(
        llm_config=engine.get_llm_config(),
        schema=BidItem.model_json_schema(),
        extraction_type="schema",
        instruction=(
            "Extract a list of all active Bids/RFPs. "
            "Filter for Construction, Infrastructure, Engineering, and Maintenance projects. "
            "Ignore Janitorial, Security, or Software. "
            "Return a JSON array of objects with title, clientName, deadline (YYYY-MM-DD), "
            "description, and link (absolute URL)."
        )
    )

    # Enable magic and iframe processing
    config = engine.get_run_config(strategy=strategy, process_iframes=True, wait_until="networkidle")

    extracted_bids = []

    try:
        async with AsyncWebCrawler(config=engine.get_browser_config()) as crawler:
            result = await crawler.arun(url=procurement_url, config=config)

            if result.success and result.extracted_content:
                data = json.loads(result.extracted_content)
                items = []
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict) and "items" in data:
                    items = data["items"]
                elif isinstance(data, dict):
                    items = [data] # Single item

                for item in items:
                    # Validate minimum fields
                    if item.get("title") and item.get("link"):
                        extracted_bids.append(Bid(
                            title=item.get("title"),
                            clientName=item.get("clientName") or "Unknown",
                            deadline=item.get("deadline"),
                            description=item.get("description"),
                            link=item.get("link")
                        ))
            else:
                print(f"Step 2: No content extracted from {procurement_url}")

    except Exception as e:
        print(f"Step 2 Error: {e}")
        return []

    return extracted_bids

async def fetch_details(bid: Bid) -> Bid:
    """
    Step 3: Detail Fetching (The Deep Dive)
    Downloads PDF or scrapes HTML detail page.
    """
    # print(f"Step 3: Deep Dive for '{bid.title}'")

    if not bid.link:
        return bid

    # Check for PDF
    if bid.link.lower().endswith(".pdf"):
        try:
            # print(f"Downloading PDF: {bid.link}")
            response = requests.get(bid.link, timeout=(10, 20))
            if response.status_code == 200:
                # Extract text with PyPDF2 (First 10 pages)
                with io.BytesIO(response.content) as f:
                    try:
                        reader = PyPDF2.PdfReader(f)
                        text = ""
                        for i, page in enumerate(reader.pages):
                            if i >= 10: break
                            text += page.extract_text() + "\n"
                        bid.full_text = text[:100000] # Limit size
                    except Exception:
                        print(f"PDF Read Error: {bid.link}")
            else:
                print(f"Failed to download PDF: {response.status_code}")
        except Exception as e:
            print(f"PDF Error: {e}")
    else:
        # HTML Extraction (Markdown)
        # Use Crawl4AI without LLM strategy, just pure markdown
        config = engine.get_run_config(strategy=None, wait_until="commit") # Fast commit

        try:
            async with AsyncWebCrawler(config=engine.get_browser_config()) as crawler:
                result = await crawler.arun(url=bid.link, config=config)
                if result.success:
                    bid.full_text = result.markdown
                else:
                    print(f"HTML Detail Error: {result.error_message}")
        except Exception as e:
            print(f"HTML Detail Exception: {e}")

    return bid

async def classify_and_save(bid: Bid, db: DatabaseHandler, state: str):
    """
    Step 4: Classification & Storage (The Brain)
    """
    if not bid.full_text:
        # print(f"Skipping Classification: No text for '{bid.title}'")
        return

    # print(f"Step 4: Classifying '{bid.title}'")

    # Combine title, description, and full_text for context
    context = f"Title: {bid.title}\nDescription: {bid.description}\n\nFull Text:\n{bid.full_text or ''}"

    # Classify
    divisions = await engine.classify_text(context)
    bid.csi_divisions = divisions

    if not divisions:
        # print(f"Discarding '{bid.title}' - No construction divisions found.")
        return

    # Generate Slug (Simple deterministic hash)
    import hashlib
    slug_str = f"{bid.title}|{bid.client_name}|{bid.link}".lower()
    bid.slug = hashlib.md5(slug_str.encode()).hexdigest()

    # Save
    print(f" + SAVED: {bid.title} [{', '.join(divisions)}]")
    db.save_bid(bid, state)

async def process_agency(agency: Agency, db: DatabaseHandler):
    """
    Orchestrates the pipeline for a single agency.
    """
    # Step 1
    procurement_url = await discover_procurement_link(agency)

    if not procurement_url:
        print(f"[{agency.name}] No procurement URL found. Stopping.")
        return

    print(f"[{agency.name}] Found Procurement URL: {procurement_url}")
    # Update Agency Record
    # Note: DatabaseHandler.update_agency_procurement_url expects (name, state, url)
    # We might need to ensure 'name' matches what's in DB or just use what we have.
    # The Orchestrator will insert the agency first?
    # Actually, let's just update.
    # db.update_agency_procurement_url(agency.name, agency.state, procurement_url)

    # Step 2
    bids = await extract_bids(procurement_url)
    print(f"[{agency.name}] Found {len(bids)} candidate bids.")

    if not bids:
        return

    # Step 3 & 4 (Concurrent for speed, limited by semaphore)
    async def process_single_bid(b: Bid):
        async with SEM_BIDS:
            b = await fetch_details(b)
            await classify_and_save(b, db, agency.state)

    tasks = [process_single_bid(bid) for bid in bids]
    await asyncio.gather(*tasks)
