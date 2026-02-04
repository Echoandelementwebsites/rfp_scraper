import requests
from urllib.parse import urlparse

def validate_url(url: str) -> bool:
    """
    Validates a URL based on:
    1. Format checks.
    2. Domain restriction (.gov or .edu).
    3. Live connectivity check (Status 200).
    """
    if not url or not isinstance(url, str):
        return False

    try:
        # 1. Format Check
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return False

        # Ensure scheme is http or https
        if parsed.scheme not in ('http', 'https'):
            return False

        # 2. Domain Restriction (.gov or .edu)
        # We need to handle cases like 'www.dot.ca.gov' or 'university.edu'
        domain = parsed.netloc.lower()
        # Remove port if present
        if ':' in domain:
            domain = domain.split(':')[0]

        if not (domain.endswith('.gov') or domain.endswith('.edu')):
            return False

        # 3. Connectivity Check
        # User-Agent to avoid immediate blocking
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=5)
        return response.status_code == 200

    except Exception:
        # Any error (timeout, dns, invalid url) makes it invalid
        return False

def check_url_reachability(url: str) -> bool:
    """
    Checks if a URL is reachable (Status 200) without strict domain filtering.
    Used for AI-discovered URLs which might be .org or .com.
    """
    if not url or not isinstance(url, str):
        return False

    try:
        # 1. Format Check
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return False

        if parsed.scheme not in ('http', 'https'):
            return False

        # 2. Connectivity Check
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=5)
        return response.status_code == 200

    except Exception:
        return False
