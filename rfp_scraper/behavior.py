import time
import random
import math

# Global state to track mouse position across calls
# Initialize with a plausible starting point (e.g., center-ish of a 1920x1080 screen)
_last_mouse_pos = (random.randint(500, 1500), random.randint(300, 800))

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

def natural_mouse_move(page, target_x, target_y):
    """
    Moves mouse in a Bezier curve to the target.
    Updates the global _last_mouse_pos to avoid teleportation.
    """
    global _last_mouse_pos
    start_x, start_y = _last_mouse_pos

    # Random control point for the curve (offset from the midpoint)
    control_x = (start_x + target_x) / 2 + random.randint(-200, 200)
    control_y = (start_y + target_y) / 2 + random.randint(-200, 200)

    steps = random.randint(10, 25)
    for i in range(steps):
        t = i / steps
        # Bezier calculation
        x = (1-t)**2 * start_x + 2*(1-t)*t * control_x + t**2 * target_x
        y = (1-t)**2 * start_y + 2*(1-t)*t * control_y + t**2 * target_y

        page.mouse.move(x, y)
        time.sleep(random.uniform(0.005, 0.02)) # Fast micro-movements

    # Final move to exact target
    page.mouse.move(target_x, target_y)
    _last_mouse_pos = (target_x, target_y)

def human_type(page, locator, text):
    """
    Types text like a human with variable speed.
    Accepts a Playwright Locator object.
    """
    try:
        locator.focus()
        for char in text:
            page.keyboard.type(char)
            # Delay: Average 100ms, mostly between 50ms and 150ms
            delay = random.gauss(0.1, 0.03)
            time.sleep(max(0.02, abs(delay)))
    except Exception as e:
        print(f"Error during human_type: {e}")

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
        natural_mouse_move(page, x, y)
    except Exception:
        # Ignore mouse move errors if page is not interactive
        pass
