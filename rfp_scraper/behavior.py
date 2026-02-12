import time
import random

def human_delay(min_s=1.0, max_s=3.0):
    """
    Sleeps for a random float duration to mimic human pauses.
    """
    time.sleep(random.uniform(min_s, max_s))

def smooth_scroll(page):
    """
    Scrolls the page down to mimic reading, triggering lazy loads.
    """
    try:
        total_height = page.evaluate("document.body.scrollHeight")
        viewport_height = page.viewport_size['height']
        current_scroll = 0

        while current_scroll < total_height:
            # Random scroll amount
            scroll_step = random.randint(300, 700)
            current_scroll += scroll_step

            # Perform scroll
            page.mouse.wheel(0, scroll_step)

            # Brief pause to "read"
            if random.random() < 0.3: # 30% chance to pause longer
                time.sleep(random.uniform(0.5, 1.2))
            else:
                time.sleep(random.uniform(0.1, 0.4))

            # Break if we hit bottom or close enough
            if current_scroll >= total_height:
                break
    except Exception as e:
        # Log or ignore scroll errors (e.g. if page closed or element not found)
        # In a helper like this, it's safer to just return if something goes wrong
        # so we don't crash the main scraper.
        print(f"Error during smooth scroll: {e}")

def mimic_human_arrival(page, target_url, referrer_url=None, **kwargs):
    """
    Navigates to a URL with a fake referrer and simulates mouse movement.
    Accepts additional kwargs for page.goto (e.g. timeout).
    """
    if referrer_url:
        # Set the Referer header for this navigation
        # Note: This persists on the page object until changed.
        page.set_extra_http_headers({"Referer": referrer_url})

    # Navigate
    # We let exceptions propagate so the caller can handle timeouts/errors
    # Ensure wait_until is domcontentloaded by default if not provided,
    # but kwargs can override it.
    if "wait_until" not in kwargs:
        kwargs["wait_until"] = "domcontentloaded"

    page.goto(target_url, **kwargs)

    # Randomly move mouse after load to simulate presence
    try:
        x = random.randint(100, 800)
        y = random.randint(100, 600)
        page.mouse.move(x, y)
    except Exception:
        # Ignore mouse move errors if page is not interactive
        pass
