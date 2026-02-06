import requests
import re
from urllib.parse import urlparse
from typing import Optional
from dateutil import parser
import datetime

# --- Constants for Filtering ---

# Prevent visiting these links
BLOCKED_URL_PATTERNS = [
    "calendar", "event", "newsletter", "storytime", "meeting", "minutes",
    "pay-bill", "tax", "portal", "login", "job", "career", "employment",
    "faq", "policy", "contact"
]

# Reject these titles/descriptions immediately
INVALID_CONTENT_TERMS = [
    "tax return", "reading challenge", "vaccination", "support group",
    "substitute teacher", "internship", "janitorial", "cleaning",
    "software licensing", "catering", "security guard",
    "highway bridge rehabilitation" # Specific hallucination blocker
]

GENERIC_TITLES = ["untitled", "home", "page not found", "bids", "rfp", "procurement"]


# --- Validation Helpers ---

def clean_text(text: str) -> str:
    """
    Cleans text by stripping whitespace and converting to Title Case.
    """
    if not text:
        return ""
    # Remove extra whitespace
    cleaned = " ".join(text.split())
    # Convert to Title Case
    return cleaned.title()

def normalize_date(date_str: str) -> Optional[str]:
    """
    Standardize date string to YYYY-MM-DD.
    Returns None if parsing fails.
    """
    if not date_str:
        return None

    try:
        dt = parser.parse(date_str)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None

def is_future_deadline(date_str: str, buffer_days: int = 2) -> bool:
    """
    Checks if the deadline is at least buffer_days into the future.
    Returns False if date_str is None or invalid.
    """
    if not date_str:
        return False

    try:
        dt = parser.parse(date_str).date()
        cutoff_date = datetime.datetime.now().date() + datetime.timedelta(days=buffer_days)
        return dt >= cutoff_date
    except Exception:
        return False

def is_valid_rfp(title: str, description: str, client_name: str) -> bool:
    """
    Validates if the RFP candidate is worth processing based on text content.

    Logic:
    1. Return False if Title is in GENERIC_TITLES or length < 5.
    2. Return False if content contains INVALID_CONTENT_TERMS.
    3. Return False if client_name contains "Library" AND Title contains "Bridge"/"Highway".
    4. Return False if Title looks like a date/time (heuristic).
    """
    if not title:
        return False

    title_lower = title.lower().strip()
    desc_lower = (description or "").lower()
    client_lower = (client_name or "").lower()

    # 1. Title Check
    if title_lower in GENERIC_TITLES:
        return False
    if len(title_lower) < 5:
        return False

    # 2. Invalid Content Terms
    # Check both title and description for invalid terms
    combined_text = f"{title_lower} {desc_lower}"
    for term in INVALID_CONTENT_TERMS:
        if term in combined_text:
            return False

    # 3. Specific Logic: Library + Bridge/Highway (Hallucination check)
    if "library" in client_lower:
        if "bridge" in title_lower or "highway" in title_lower:
            return False

    # 4. Date/Time Title Check
    # If title is just a date like "October 12, 2023" or "10/12/2023", it's likely a meeting agenda or noise.
    # We attempt to parse the title as a date. If it succeeds and covers most of the string, reject it.
    try:
        # strict=False allows fuzzy, but if the whole string parses easily, it might be a date.
        # However, parser.parse("Project 123") might fail or pass depending on context.
        # A safer check is if the title is very short and contains digits/slashes
        # or if we explicitly try to parse it.
        # Let's try a strict parse logic: if the title can be parsed as a date and has no other words
        # (ignoring common day names/months), reject.

        # Simple heuristic: if it parses as a date and length is short (< 20 chars)
        if len(title_lower) < 20:
            dt = parser.parse(title_lower)
            # If we got here, it's a date-like string.
            return False
    except:
        pass

    return True


# --- Existing Helpers ---

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

def get_state_abbreviation(state_name: str) -> str:
    """
    Returns the 2-letter abbreviation for a given state name.
    """
    if not state_name:
        return ""

    clean_name = state_name.strip()

    # If already an abbreviation, return it upper-cased
    if len(clean_name) == 2:
        return clean_name.upper()

    states = {
        "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
        "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
        "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
        "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
        "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO",
        "montana": "MT", "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
        "new mexico": "NM", "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH",
        "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
        "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
        "virginia": "VA", "washington": "WA", "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
        "district of columbia": "DC"
    }

    return states.get(clean_name.lower(), "")
