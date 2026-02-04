from playwright.sync_api import sync_playwright, expect
import os

def test_app(page):
    print("Navigating to app...")
    page.goto("http://localhost:8501")

    # Wait for app to load (Streamlit usually has a 'running' indicator or just wait for title)
    expect(page).to_have_title("National Construction RFP Dashboard", timeout=10000)
    print("App loaded.")

    # 1. Verify 'State Agencies' tab
    # Streamlit tabs are buttons with role 'tab'
    # Click "State Agencies"
    page.get_by_role("tab", name="State Agencies").click()
    print("Clicked State Agencies tab.")
    page.wait_for_timeout(2000) # Wait for render
    page.screenshot(path="verification/state_agencies.png")

    # 2. Verify 'RFP Scraper' tab and JSON button
    page.get_by_role("tab", name="RFP Scraper").click()
    print("Clicked RFP Scraper tab.")

    # Wait for the button to appear.
    # Streamlit buttons are usually buttons.
    # We look for "Download RFP .json (Full Data)"
    # Note: Streamlit download buttons sometimes render as anchor tags or buttons.
    # Using get_by_text is safer for Streamlit widgets.

    # First, let's take a screenshot of the scraper tab
    page.wait_for_timeout(2000)
    page.screenshot(path="verification/rfp_scraper.png")

    # Check for the button text
    expect(page.get_by_text("Download RFP .json (Full Data)")).to_be_visible()
    print("Found JSON Download button.")

if __name__ == "__main__":
    if not os.path.exists("verification"):
        os.makedirs("verification")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            test_app(page)
        except Exception as e:
            print(f"Test failed: {e}")
            page.screenshot(path="verification/failure.png")
        finally:
            browser.close()
