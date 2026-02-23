import urllib.robotparser
import time
import random
from urllib.parse import urlparse
import requests

class ComplianceManager:
    def __init__(self):
        self._robot_parsers = {}
        self._last_request_time = {}

    def can_fetch(self, url: str, user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) (ConstructionBidHub Bot)") -> bool:
        """
        Check if the URL is allowed by robots.txt and enforce rate limiting.
        """
        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

        # 1. Rate Limiting
        if base_url in self._last_request_time:
            elapsed = time.time() - self._last_request_time[base_url]
            delay = random.uniform(2, 5)
            if elapsed < delay:
                time.sleep(delay - elapsed)

        self._last_request_time[base_url] = time.time()

        # 2. Robots.txt Compliance
        if base_url not in self._robot_parsers:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(f"{base_url}/robots.txt")
            try:
                # Use requests with a strict 5-second timeout
                resp = requests.get(f"{base_url}/robots.txt", timeout=5, headers={"User-Agent": user_agent})

                if resp.status_code == 200:
                    rp.parse(resp.text.splitlines())
                elif resp.status_code in (401, 403):
                    # Standard protocol: 401/403 means strictly forbidden
                    rp.disallow_all = True
                else:
                    # 404 Not Found (or 500 errors): No valid robots.txt, assume allowed
                    rp.allow_all = True

            except Exception:
                # Timeout or DNS error: Server is unresponsive.
                # Fail-open so our scraper doesn't hang or skip valid agencies.
                rp.allow_all = True

            self._robot_parsers[base_url] = rp

        return self._robot_parsers[base_url].can_fetch(user_agent, url)
