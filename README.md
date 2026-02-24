# ConstructionBidHub RFP Scraper (v2 Async Engine)

A high-performance, asynchronous web scraping pipeline designed to extract, classify, and store government construction and infrastructure RFPs (Requests for Proposals).

Version 2 completely removes legacy imperative web automation (Playwright manual loops) in favor of a declarative, LLM-driven extraction pipeline powered by **Crawl4AI** and **DeepSeek v3.2**.

## 🏗️ Architecture: The 4-Step "Golden Path"

The `orchestrator.py` script manages a strict, highly concurrent pipeline for every government agency:

1. **Discovery (The Pathfinder):** Uses Crawl4AI to read an agency's homepage and DeepSeek to identify the exact URL of their purchasing/bids portal.
2. **Extraction (The Harvester):** Uses Crawl4AI (`magic=True`, `process_iframes=True`) to flatten complex government portals (like Bonfire/OpenGov) and extracts a structured JSON list of active bids using an LLM Extraction Strategy.
3. **Detail Fetching (The Deep Dive):** Asynchronously fetches the unabridged Scope of Work.
   * **HTML:** Pure DOM-to-Markdown conversion via Crawl4AI.
   * **PDF:** Native background extraction via `requests` and `PyPDF2`.
4. **Classification (The Brain):** DeepSeek evaluates the full text, verifies it is a construction/infrastructure project, and tags it with the appropriate **CSI MasterFormat Divisions**.

## ⚙️ Prerequisites & Setup

Ensure you have Python 3.12+ installed.

1. **Create and activate your virtual environment:**
   `python3 -m venv venv`
   `source venv/bin/activate`

2. **Install dependencies:**
   `pip install --upgrade pip`
   `pip install -r requirements.txt`

3. **Install Playwright Browsers:**
   *(Required by Crawl4AI's underlying asynchronous engine)*
   `playwright install`

4. **Environment Variables:**
   Create a `.env` file in the root directory:
   ```env
   DEEPSEEK_API_KEY="your_deepseek_api_key"

   # Optional: Include this to use Neon Postgres.
   # If omitted, the system falls back to a local SQLite database (rfp_scraper.db).
   DATABASE_URL="postgresql://user:password@ep-cool-db.us-east-2.aws.neon.tech/dbname"
   ```

## 🚀 Usage

The Streamlit UI is now bridged to the asynchronous engine.

1.  Run the dashboard:
    `streamlit run rfp_scraper/app.py`

2.  Navigate to the **RFP Scraper** tab.

3.  Click **Start Scraping (Background)** to spin up the asyncio event loop and execute the v2 engine across your selected states.

## Concurrency

The orchestrator is governed by an `asyncio.Semaphore` (default limit: 5 concurrent agencies) to prevent memory exhaustion and API rate-limiting while maintaining high throughput.

## 📂 Project Structure

```
├── rfp_scraper_v2/
│   ├── core/
│   │   ├── database.py       # Dual SQLite/Neon Postgres handler
│   │   └── models.py         # Strict Pydantic schemas for data validation
│   ├── crawlers/
│   │   ├── pipeline.py       # Core execution logic for the 4-step pipeline
│   │   └── prompts.py        # Highly tuned DeepSeek negative-constraint prompts
│   └── orchestrator.py       # Main async entry point and Streamlit Bridge
├── rfp_scraper/
│   └── app.py                # Main Streamlit Dashboard UI
├── state_agency_dictionary.json  # Target dictionary for state-level portals
└── cities_towns_dictionary.json  # Target dictionary for local/municipal portals
```
