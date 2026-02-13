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

def smooth_scroll(page, max_seconds=15):
    """
    Scrolls page with a hard time limit to prevent infinite loops on broken sites.
    """
    start_time = time.time()
    try:
        # Get total height
        total_height = page.evaluate("document.body.scrollHeight")
        viewport_height = page.viewport_size['height']
        current_scroll = 0

        while current_scroll < total_height:
            # 1. SAFETY BRAKE: Stop after max_seconds
            if (time.time() - start_time) > max_seconds:
                # print("Stopped scrolling: Time limit reached.")
                break

            scroll_step = random.randint(400, 800)
            current_scroll += scroll_step
            page.mouse.wheel(0, scroll_step)

            # 2. STUCK CHECK: Did we actually move?
            real_scroll = page.evaluate("window.scrollY")
            if (real_scroll + viewport_height) >= (total_height - 50):
                break # Reached bottom

            # Brief pause
            time.sleep(random.uniform(0.1, 0.4))

            # Update height (for lazy loading)
            total_height = page.evaluate("document.body.scrollHeight")

    except Exception as e:
        print(f"Scroll warning: {e}")

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

def mimic_human_arrival(page, url, referrer_url=None, timeout=30000):
    """Navigates with optional referrer and timeout."""
    if referrer_url:
        page.set_extra_http_headers({"Referer": referrer_url})

    page.goto(url, wait_until="domcontentloaded", timeout=timeout)

    # Initialize mouse
    try:
        safe_width = page.viewport_size['width'] - 100
        safe_height = page.viewport_size['height'] - 100
        # Ensure positive range for randint
        safe_width = max(100, safe_width)
        safe_height = max(100, safe_height)

        natural_mouse_move(page, random.randint(100, safe_width), random.randint(100, safe_height))
    except Exception:
        # Ignore mouse move errors if page context is invalid or closed
        pass
