from playwright.sync_api import sync_playwright, expect
import time

def verify_rfp_scraper_tab(page):
    # Navigate to the app
    page.goto("http://localhost:8501")

    # Wait for the app to load
    page.wait_for_timeout(5000)

    # Click on the "RFP Scraper" tab
    # Use specific role locator
    tab_locator = page.get_by_role("tab", name="RFP Scraper")
    expect(tab_locator).to_be_visible()
    tab_locator.click()

    # Wait for tab content
    page.wait_for_timeout(2000)

    # Check for "Start Scraping" button
    start_button = page.get_by_role("button", name="Start Scraping") # Regex matching partial text if needed, or exact name
    # The button text is "🚀 Start Scraping", so matching "Start Scraping" might work if exact=False (default)
    # But let's try get_by_role with the exact string first, or use a partial match.
    # To be safe, I'll search for the text and then ensure it's a button, or just use regex.

    # Actually, let's look for the button containing "Start Scraping"
    # Streamlit buttons are usually button elements.
    start_button = page.get_by_role("button", name="🚀 Start Scraping")
    expect(start_button).to_be_visible()

    # Check for "Active Opportunities" header
    header = page.get_by_role("heading", name="Active Opportunities")
    expect(header).to_be_visible()

    # Determine positions to verify order
    button_box = start_button.bounding_box()
    header_box = header.bounding_box()

    print(f"Button Y: {button_box['y']}")
    print(f"Header Y: {header_box['y']}")

    # Verification Logic: Button should be ABOVE Header (smaller Y)
    if button_box['y'] < header_box['y']:
        print("PASS: Button is above the header.")
    else:
        print("FAIL: Button is NOT above the header.")

    # Take Screenshot
    page.screenshot(path="verification/verification.png")

if __name__ == "__main__":
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            verify_rfp_scraper_tab(page)
        except Exception as e:
            print(f"Error: {e}")
            page.screenshot(path="verification/error.png")
        finally:
            browser.close()
