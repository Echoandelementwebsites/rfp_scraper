import asyncio
import json
import os
import requests
import io
import PyPDF2
from typing import List, Optional
from crawl4ai import AsyncWebCrawler, LLMConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from openai import AsyncOpenAI
from pydantic import ValidationError

from rfp_scraper_v2.core.models import (
    DiscoverySchema,
    BidExtractionSchema,
    ClassificationSchema,
    Bid,
    Agency,
    AgencySchema
)
from rfp_scraper_v2.crawlers.prompts import (
    DISCOVERY_SYSTEM_PROMPT,
    EXTRACTION_INSTRUCTION,
    CLASSIFICATION_SYSTEM_PROMPT
)

# Initialize DeepSeek Client (for direct calls)
# Assuming DEEPSEEK_API_KEY is in environment
api_key = os.getenv("DEEPSEEK_API_KEY")
if not api_key:
    api_key = "dummy"

client = AsyncOpenAI(
    api_key=api_key,
    base_url="https://api.deepseek.com"
)

async def discover_portal(crawler: AsyncWebCrawler, agency_url: str) -> Optional[str]:
    """
    Step 1: Discover the procurement portal URL.
    """
    try:
        print(f"  [Discovery] Crawling {agency_url}...")
        result = await crawler.arun(url=agency_url)
        if not result.success:
            print(f"  [Discovery] Failed to crawl {agency_url}: {result.error_message}")
            return None

        markdown = result.markdown
        # Truncate markdown to fit context window if necessary (e.g. 50k chars)
        truncated_markdown = markdown[:20000]

        response = await client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": DISCOVERY_SYSTEM_PROMPT},
                {"role": "user", "content": f"Analyze this homepage markdown:\n\n{truncated_markdown}"}
            ],
            temperature=0.0
        )

        content = response.choices[0].message.content
        # Heuristic clean up if DeepSeek is chatty (though prompt says ONLY absolute URL)
        # We try to extract URL.
        # DeepSeek might return just the URL or a sentence.
        # We can try to parse it or just take the content.
        # Prompt says "Return ONLY the absolute URL".

        url = content.strip()
        if "null" in url.lower() or not url.startswith("http"):
             # Fallback: sometimes LLM wraps in quotes or code blocks
             clean_url = url.replace('"', '').replace("'", "").replace("`", "")
             if clean_url.startswith("http"):
                 return clean_url
             return None

        return url

    except Exception as e:
        print(f"  [Discovery] Error: {e}")
        return None

async def extract_bids(crawler: AsyncWebCrawler, portal_url: str) -> List[BidExtractionSchema]:
    """
    Step 2: Extract bids using LLMExtractionStrategy.
    """
    try:
        print(f"  [Extraction] Extracting from {portal_url}...")

        # Configure Strategy: Chunking OFF, Standard Markdown ON
        strategy = LLMExtractionStrategy(
            llm_config=LLMConfig(
                provider="openai/deepseek-chat",
                api_token=os.getenv("DEEPSEEK_API_KEY"),
                base_url="https://api.deepseek.com",
                temperature=0.0
            ),
            instruction=EXTRACTION_INSTRUCTION,
            schema=BidExtractionSchema.model_json_schema(),
            extraction_type="schema",
            apply_chunking=False,
            input_format="markdown"
        )

        result = await crawler.arun(
            url=portal_url,
            process_iframes=True,
            extraction_strategy=strategy,
            magic=True # Enhance extraction
        )

        if not result.success:
            print(f"  [Extraction] Failed to crawl {portal_url}: {result.error_message}")
            return []

        # SAFETY GUARD: Ensure the AI actually returned content before parsing
        if not result.extracted_content:
            print(f"  [Extraction] AI returned no content for {portal_url}.")
            return []

        try:
            extracted_data = json.loads(result.extracted_content)
        except json.JSONDecodeError as e:
            print(f"  [Extraction] JSON Parse Error: {e}. Raw content: {result.extracted_content[:200]}...")
            return []

        bids = []
        # Ensure extracted_data is actually a list before looping
        if not isinstance(extracted_data, list):
            print(f"  [Extraction] Warning: Expected a list of bids, got {type(extracted_data)}.")
            return []

        for item in extracted_data:
            try:
                bid = BidExtractionSchema(**item)
                bids.append(bid)
            except ValidationError as ve:
                print(f"  [Extraction] Validation Error: {ve}")

        return bids

    except Exception as e:
        print(f"  [Extraction] Error: {e}")
        return []

async def fetch_bid_detail(crawler: AsyncWebCrawler, bid_link: str) -> str:
    """
    Step 3: Fetch full text from HTML or PDF.
    """
    try:
        # Check if PDF
        if bid_link.lower().endswith(".pdf"):
            print(f"  [Detail] Fetching PDF: {bid_link}")
            try:
                response = requests.get(bid_link, timeout=(10, 20))
                response.raise_for_status()

                with io.BytesIO(response.content) as f:
                    pdf = PyPDF2.PdfReader(f)
                    text = ""
                    # Extract up to 10 pages
                    for i in range(min(len(pdf.pages), 10)):
                        text += pdf.pages[i].extract_text() + "\n"
                    return text
            except Exception as e:
                print(f"  [Detail] PDF Error: {e}")
                return ""
        else:
            # HTML
            print(f"  [Detail] Fetching HTML: {bid_link}")
            result = await crawler.arun(url=bid_link)
            if result.success:
                return result.markdown
            else:
                print(f"  [Detail] Failed: {result.error_message}")
                return ""

    except Exception as e:
        print(f"  [Detail] Error: {e}")
        return ""

async def classify_and_save(db, bid_obj: BidExtractionSchema, full_text: str, state: str):
    """
    Step 4: Classify and Save if construction related.
    """
    try:
        # Truncate full_text for classification context
        truncated_text = full_text[:20000]

        response = await client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": CLASSIFICATION_SYSTEM_PROMPT},
                {"role": "user", "content": f"Classify this bid scope:\nTitle: {bid_obj.title}\nDescription: {bid_obj.description}\n\nFull Text Snippet:\n{truncated_text}"}
            ],
            temperature=0.0,
            response_format={ "type": "json_object" } # DeepSeek supports JSON mode
        )

        content = response.choices[0].message.content
        data = json.loads(content)

        classification = ClassificationSchema(**data)

        if classification.is_construction_related:
            print(f"  [Classification] SAVING BID: {bid_obj.title}")

            # Map to DB Bid model
            db_bid = Bid(
                title=bid_obj.title,
                clientName=bid_obj.clientName,
                deadline=bid_obj.deadline,
                description=bid_obj.description,
                link=bid_obj.link,
                full_text=full_text,
                csi_divisions=classification.csi_divisions,
                slug=f"{bid_obj.clientName}-{bid_obj.title}"[:100].replace(" ", "-").lower() # Simple slug gen
            )

            db.save_bid(db_bid, state)
        else:
            print(f"  [Classification] Skipped (Not Construction): {bid_obj.title}")

    except Exception as e:
        print(f"  [Classification] Error: {e}")

async def process_agency(agency: Agency, db):
    """
    Orchestrates the pipeline for a single agency.
    """
    print(f"Processing Agency: {agency.name} ({agency.state})")

    async with AsyncWebCrawler() as crawler:
        # Step 1: Discovery
        procurement_url = agency.procurement_url

        if procurement_url == "NOT_FOUND":
            print(f"  [Skip] Previously flagged as NO PORTAL for {agency.name}")
            return

        if not procurement_url:
            if agency.homepage_url:
                procurement_url = await discover_portal(crawler, agency.homepage_url)
                if procurement_url:
                    print(f"  Found Portal: {procurement_url}")
                    try: db.update_agency_procurement_url(agency.name, agency.state, procurement_url)
                    except: pass
                else:
                    print(f"  No procurement portal found. Flagging as NOT_FOUND.")
                    try: db.update_agency_procurement_url(agency.name, agency.state, "NOT_FOUND")
                    except: pass
                    return
            else:
                print(f"  No homepage URL for {agency.name}")
                return

        if not procurement_url or procurement_url == "NOT_FOUND":
            return

        # Step 2: Extraction
        bids = await extract_bids(crawler, procurement_url)
        print(f"  Found {len(bids)} potential bids.")

        # Step 3 & 4: Detail & Classification
        for bid in bids:
            # CRITICAL GUARD: Do not re-download PDFs or re-run AI classification on existing bids
            if db.url_already_scraped(bid.link):
                print(f"  [Skip] Already scraped: {bid.link}")
                continue

            full_text = await fetch_bid_detail(crawler, bid.link)
            if full_text:
                await classify_and_save(db, bid, full_text, agency.state)
