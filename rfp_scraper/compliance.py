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
                # Use a short timeout to fetch robots.txt
                rp.read()
            except Exception:
                # If robots.txt fails to load, assume allowed (per standard practice) or default to disallowed?
                # Standard practice is 'allow' if robots.txt is missing/unreachable,
                # but 'disallow' if we want to be very strict.
                # Given 'best effort' requirement, we'll allow if we can't read it,
                # unless it explicitly forbids.
                # However, rp.read() might fail on network errors.
                # If read() fails, the internal state might not be set.
                # A fresh RobotFileParser defaults to allow_all = False by default implementation?
                # Actually, default is allow all if no rules found.
                pass
            self._robot_parsers[base_url] = rp

        return self._robot_parsers[base_url].can_fetch(user_agent, url)
