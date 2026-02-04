# National Construction RFP Scraper (DeepSeek AI Powered)

A specialized tool to scrape, filter, and aggregate "Request for Proposal" (RFP) data for construction projects from state and local government sources across the US.

## Features

*   **National Coverage**: Modular architecture supports scraping across all 50 states.
*   **AI-Driven Discovery**:
    *   **State Agencies**: Automatically finds official procurement sites for agencies and universities.
    *   **Local Governments**: Identifies counties, cities, and towns and discovers their specific department pages (e.g., "City of Hartford Public Works").
*   **Strict Filtering**:
    *   **Relevance Check**: Uses DeepSeek AI to analyze bid titles and content, ensuring only Construction/Architecture/Engineering bids are saved.
    *   **Link Verification**: Physically visits every deep link to ensure accessibility (no 404s/403s) before listing.
    *   **Deadlines**: Automatically filters out opportunities due within **4 days** to ensure actionability.
*   **Data Persistence**: Uses SQLite to store discovery data and scraped bids.
*   **Deep Scan**: Recursively scans agency websites to find opportunities not listed on main portals.
*   **Interactive Dashboard**: A Streamlit interface for managing discovery, viewing data, and running scrapers.

## Prerequisites

*   Python 3.9+
*   `DEEPSEEK_API_KEY` (Required for AI features and strict filtering)

## Installation

1.  Clone the repository:
    ```bash
    git clone <repo_url>
    cd rfp_scraper
    ```

2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3.  Install Playwright browsers:
    ```bash
    playwright install chromium
    ```

4.  Set up Environment:
    Create a `.env` file in the root directory:
    ```
    DEEPSEEK_API_KEY=your_key_here
    ```

## Usage

Start the dashboard:

```bash
streamlit run rfp_scraper/app.py
```

### Workflow

1.  **States Tab**: Generate the master list of US States.
2.  **Local Governments Tab**: Use AI to identify counties, cities, and towns for your target state.
3.  **State Agencies Tab**: Run "URL Discovery" to find official websites for state agencies and local departments (e.g., "Public Works").
4.  **RFP Scraper Tab**:
    *   Select a state or "Scrape All".
    *   Click **Start Scraping**.
    *   The scraper will visit portals, verify links, and use AI to filter results.
    *   **Download**:
        *   **CSV**: Summary view (clean table).
        *   **JSON**: Full data dump including full text descriptions (`rfp_description`).

## Export Formats

*   **CSV**: Contains `slug`, `client_name`, `title`, `deadline`, `scraped_at`, `source_url`, `state`.
*   **JSON**: Contains all CSV fields plus `rfp_description` (full page text extracted from the bid link).

## Deployment

### Streamlit Cloud

1.  Push code to GitHub.
2.  Connect repository to Streamlit Cloud.
3.  Add `DEEPSEEK_API_KEY` to Streamlit Secrets.
4.  The `requirements.txt` and `packages.txt` (if applicable) handle dependencies.
