import asyncio
import json
import os
import re
import requests
import tempfile
import PyPDF2
from typing import List, Optional
from urllib.parse import urljoin
from crawl4ai import AsyncWebCrawler, LLMConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy, JsonCssExtractionStrategy
from openai import AsyncOpenAI
from pydantic import ValidationError

from rfp_scraper_v2.core.logger import logger
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
from rfp_scraper_v2.crawlers.schemas import (
    BONFIRE_SCHEMA,
    IONWAVE_SCHEMA,
    PLANETBIDS_SCHEMA,
    OPENGOV_SCHEMA,
    BIDNET_SCHEMA
)

async def discover_portal(crawler: AsyncWebCrawler, agency_url: str, api_key: str) -> Optional[str]:
    """
    Step 1: Discover the procurement portal URL.
    """
    client = AsyncOpenAI(api_key=api_key, base_url="https://api.deepseek.com")
    try:
        logger.info(f"  [Discovery] Crawling {agency_url}...")
        result = await crawler.arun(url=agency_url)
        if not result.success:
            logger.warning(f"  [Discovery] Failed to crawl {agency_url}: {result.error_message}")
            return None

        markdown = result.markdown
        # Truncate markdown to fit context window if necessary (e.g. 50k chars)
        truncated_markdown = markdown[:20000]

        logger.debug(f"[Discovery AI Input] Sending {len(truncated_markdown)} chars to LLM for {agency_url}")

        response = await client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": DISCOVERY_SYSTEM_PROMPT},
                {"role": "user", "content": f"Analyze this homepage markdown:\n\n{truncated_markdown}"}
            ],
            temperature=0.0
        )

        content = response.choices[0].message.content
        logger.debug(f"[Discovery AI RAW Output] for {agency_url}: '{content}'")

        url = content.strip()
        if "null" in url.lower() or not url.startswith("http"):
             clean_url = url.replace('"', '').replace("'", "").replace("`", "")
             if clean_url.startswith("http"):
                 return clean_url
             return None

        return url

    except Exception as e:
        logger.error(f"  [Discovery] Error: {e}", exc_info=True)
        return None

async def extract_bids_ai(crawler: AsyncWebCrawler, portal_url: str, api_key: str) -> List[BidExtractionSchema]:
    """
    Step 2: Extract bids using direct AsyncOpenAI call to bypass Crawl4AI's strict schema enforcement.
    """
    try:
        logger.info(f"  [Extraction] Extracting from {portal_url} (AI Mode)...")

        # 1. Run basic crawl to get the Markdown
        result = await crawler.arun(
            url=portal_url,
            process_iframes=True,
            magic=True,
            wait_until="domcontentloaded",
            delay_before_return_html=5.0
        )

        if not result.success:
            logger.warning(f"  [Extraction] Failed to crawl {portal_url}: {result.error_message}")
            return []

        markdown = result.markdown
        if not markdown:
            logger.warning(f"  [Extraction] Crawler returned no markdown for {portal_url}.")
            return []

        # 2. Prepare the LLM Call directly
        client = AsyncOpenAI(api_key=api_key, base_url="https://api.deepseek.com")
        truncated_markdown = markdown[:40000]

        logger.debug(f"[Extraction AI Input] Sending {len(truncated_markdown)} chars to LLM for {portal_url}")

        instruction = EXTRACTION_INSTRUCTION + f"\n\nBASE URL FOR RELATIVE LINKS: {portal_url}\n\nIMPORTANT: You must return ONLY a raw JSON array of objects. Do not wrap it in a parent JSON object."

        response = await client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": instruction},
                {"role": "user", "content": f"Extract active bids from this markdown:\n\n{truncated_markdown}"}
            ],
            temperature=0.0
        )

        content = response.choices[0].message.content
        logger.debug(f"[Extraction AI RAW Output] from {portal_url}:\n{content}")

        if not content:
            logger.warning(f"  [Extraction] AI returned empty string.")
            return []

        # 3. Robust Parsing Logic (Keep Existing)
        content = content.strip()
        if content.startswith("```"):
            content = re.sub(r"^```[a-z]*\s*", "", content)
            content = re.sub(r"\s*```$", "", content)

        extracted_data = []
        try:
            extracted_data = json.loads(content)
        except json.JSONDecodeError:
            logger.warning(f"  [Extraction] JSON Decode Error. Attempting Regex Fallback.")
            match = re.search(r'(\[.*?\])', content, re.DOTALL)
            if match:
                try:
                    extracted_data = json.loads(match.group(1))
                except json.JSONDecodeError as e2:
                    logger.warning(f"  [Extraction] Regex Fallback Failed: {e2}")
                    return []
            else:
                 logger.warning(f"  [Extraction] No JSON array found in content.")
                 return []

        bids = []
        if not isinstance(extracted_data, list):
            return []

        for item in extracted_data:
            try:
                if not item.get("title") and not item.get("link"):
                    continue
                bid = BidExtractionSchema(**item)
                bids.append(bid)
            except ValidationError as ve:
                logger.warning(f"  [Extraction] Validation Error for item: {ve}")

        return bids

    except Exception as e:
        logger.error(f"  [Extraction] Error: {e}", exc_info=True)
        return []

async def extract_deterministic(crawler: AsyncWebCrawler, portal_url: str, schema: dict) -> List[BidExtractionSchema]:
    """
    Executes standard CSS extraction for known platforms using Crawl4AI's JsonCssExtractionStrategy.
    """
    logger.info(f"  [Extraction] Running fast CSS extractor...")

    try:
        strategy = JsonCssExtractionStrategy(schema)
        result = await crawler.arun(
            url=portal_url,
            extraction_strategy=strategy,
            wait_until="domcontentloaded",
            delay_before_return_html=5.0
        )

        if not result.success:
            logger.warning(f"  [Extraction] CSS Extraction failed: {result.error_message}")
            return []

        content = result.extracted_content
        if not content:
            return []

        data = json.loads(content)
        bids = []

        if isinstance(data, list):
            for item in data:
                try:
                    # Clean/Normalize Link
                    link = item.get("link", "")
                    if link and not link.startswith("http") and not link.startswith("javascript"):
                         link = urljoin(portal_url, link)
                    item["link"] = link

                    # Ensure minimal fields
                    if not item.get("title"):
                        continue

                    bid = BidExtractionSchema(**item)
                    bids.append(bid)
                except ValidationError:
                    continue

        return bids

    except Exception as e:
        logger.error(f"  [Extraction] Deterministic Error: {e}", exc_info=True)
        return []

async def extract_bids(crawler: AsyncWebCrawler, portal_url: str, api_key: str) -> List[BidExtractionSchema]:
    """
    The Hybrid Router. Routes known domains to fast CSS extractors.
    Automatically falls back to DeepSeek AI if the domain is unknown or CSS yields 0 bids.
    """
    url_lower = portal_url.lower()
    bids = []

    # 1. Deterministic Fast-Path
    if "bonfirehub.com" in url_lower:
        bids = await extract_deterministic(crawler, portal_url, BONFIRE_SCHEMA)
    elif "ionwave.net" in url_lower:
        bids = await extract_deterministic(crawler, portal_url, IONWAVE_SCHEMA)
    elif "planetbids.com" in url_lower:
        bids = await extract_deterministic(crawler, portal_url, PLANETBIDS_SCHEMA)
    elif "opengov.com" in url_lower or "procurenow.com" in url_lower:
        bids = await extract_deterministic(crawler, portal_url, OPENGOV_SCHEMA)
    elif "bidnetdirect.com" in url_lower:
        bids = await extract_deterministic(crawler, portal_url, BIDNET_SCHEMA)

    # 2. The AI Safety Net (Fallback)
    if not bids:
        if any(x in url_lower for x in ["bonfirehub", "ionwave", "planetbids", "opengov", "procurenow", "bidnetdirect"]):
            logger.info(f"  [Router] Fast CSS extractor yielded 0 bids. Falling back to DeepSeek AI.")
        else:
            logger.info(f"  [Router] Custom domain detected. Routing directly to DeepSeek AI.")

        # Route to the renamed AI function
        bids = await extract_bids_ai(crawler, portal_url, api_key)

    return bids

async def fetch_bid_detail(crawler: AsyncWebCrawler, bid_link: str) -> str:
    """
    Step 3: Fetch full text from HTML or PDF.
    """
    if not bid_link: return ""
    try:
        # Check if PDF
        if bid_link.lower().endswith(".pdf"):
            logger.info(f"  [Detail] Fetching PDF: {bid_link}")
            try:
                # Use Stream to avoid loading large files into RAM
                with requests.get(bid_link, stream=True, timeout=(10, 20)) as response:
                    response.raise_for_status()

                    # Create a temporary file to store the PDF
                    with tempfile.NamedTemporaryFile(delete=True) as temp_pdf:
                        for chunk in response.iter_content(chunk_size=8192):
                            temp_pdf.write(chunk)
                        temp_pdf.flush()

                        # Read from temp file
                        temp_pdf.seek(0)
                        pdf = PyPDF2.PdfReader(temp_pdf)
                        text = ""
                        # Extract up to 10 pages
                        for i in range(min(len(pdf.pages), 10)):
                            text += pdf.pages[i].extract_text() + "\n"
                        return text

            except Exception as e:
                logger.error(f"  [Detail] PDF Error: {e}", exc_info=True)
                return ""
        else:
            # HTML
            # Skip javascript: links
            if bid_link.startswith("javascript:"):
                return ""

            logger.info(f"  [Detail] Fetching HTML: {bid_link}")
            result = await crawler.arun(url=bid_link)
            if result.success:
                return result.markdown
            else:
                logger.warning(f"  [Detail] Failed: {result.error_message}")
                return ""

    except Exception as e:
        logger.error(f"  [Detail] Error: {e}", exc_info=True)
        return ""

async def classify_and_save(db, bid_obj: BidExtractionSchema, full_text: str, state: str, api_key: str):
    """
    Step 4: Classify and Save if construction related.
    """
    client = AsyncOpenAI(api_key=api_key, base_url="https://api.deepseek.com")
    try:
        # Truncate full_text for classification context
        truncated_text = full_text[:20000]

        logger.debug(f"[Classification AI Input] Analyzing Bid: '{bid_obj.title}'. Scope snippet length: {len(truncated_text)}")

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
        logger.debug(f"[Classification AI RAW Output] for '{bid_obj.title}':\n{content}")

        data = json.loads(content)

        classification = ClassificationSchema(**data)

        if classification.is_construction_related:
            logger.info(f"  [Classification] SAVING BID: {bid_obj.title}")

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

            # Use Async DB Save
            await db.async_save_bid(db_bid, state)
        else:
            logger.info(f"  [Classification] Skipped (Not Construction): {bid_obj.title}")

    except Exception as e:
        logger.error(f"  [Classification] Error: {e}", exc_info=True)

async def process_agency(agency: Agency, db, api_key: str):
    """
    Orchestrates the pipeline for a single agency.
    """
    logger.info(f"Processing Agency: {agency.name} ({agency.state})")

    try:
        async with AsyncWebCrawler() as crawler:
            # Step 1: Discovery
            procurement_url = agency.procurement_url

            if procurement_url == "NOT_FOUND":
                logger.info(f"  [Skip] Previously flagged as NO PORTAL for {agency.name}")
                return

            if not procurement_url:
                if agency.homepage_url:
                    procurement_url = await discover_portal(crawler, agency.homepage_url, api_key)
                    if procurement_url:
                        logger.info(f"  Found Portal: {procurement_url}")
                        try:
                            await db.async_update_agency_procurement_url(agency.name, agency.state, procurement_url)
                        except: pass
                    else:
                        logger.info(f"  No procurement portal found. Flagging as NOT_FOUND.")
                        try:
                            await db.async_update_agency_procurement_url(agency.name, agency.state, "NOT_FOUND")
                        except: pass
                        return
                else:
                    logger.info(f"  No homepage URL for {agency.name}")
                    return

            if not procurement_url or procurement_url == "NOT_FOUND":
                return

            # Step 2: Extraction
            bids = await extract_bids(crawler, procurement_url, api_key)
            logger.info(f"  Found {len(bids)} potential bids.")

            # Step 3 & 4: Detail & Classification
            for bid in bids:
                # Check duplication using Async DB Method
                if await db.async_url_already_scraped(bid.link):
                    logger.info(f"  [Skip] Already scraped: {bid.link}")
                    continue

                full_text = await fetch_bid_detail(crawler, bid.link)
                if full_text:
                    await classify_and_save(db, bid, full_text, agency.state, api_key)
    except asyncio.CancelledError:
        logger.warning(f"  [Cancelled] Processing for {agency.name} was cancelled. Crawler cleaning up.")
        raise
    except Exception as e:
        logger.error(f"  [Error] Processing {agency.name} failed: {e}", exc_info=True)
