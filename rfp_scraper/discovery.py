from duckduckgo_search import DDGS
from typing import List, Tuple, Optional, Any, Dict
import time
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import re
from rfp_scraper.utils import validate_url, check_url_reachability, normalize_for_domain
import rfp_scraper.config_loader as config_loader
from rfp_scraper.cisa_manager import CisaManager

def is_root_domain(url: str) -> bool:
    """Returns True if URL is just a domain with no path (or common index pages)."""
    if not url:
        return False
    parsed = urlparse(url)
    path = parsed.path.strip("/").lower()
    return path in ["", "index.html", "index.php", "default.aspx", "home", "main"]

def verify_agency_identity(url: str, city: str, state_abbr: str, state_name: str = None, unwanted_terms: List[str] = None) -> bool:
    """
    Verifies that the URL content matches the target city and state.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        # 5 second timeout
        response = requests.get(url, timeout=5, headers=headers, allow_redirects=True)
        if response.status_code != 200:
            return False

        soup = BeautifulSoup(response.text, 'html.parser')

        # Check Title and Meta Description
        title = soup.title.string if soup.title and soup.title.string else ""
        meta_desc = ""
        meta_tag = soup.find('meta', attrs={'name': 'description'})
        if meta_tag:
            content_attr = meta_tag.get('content', '')
            if isinstance(content_attr, str):
                meta_desc = content_attr

        # Combine for analysis
        page_content = (title + " " + meta_desc).lower()

        # 1. Anti-Pattern Check
        bad_indicators = ["domain for sale", "parked", "godaddy", "hugedomains", "buy this domain", "under construction"]
        if any(b in page_content for b in bad_indicators):
            return False

        # 2. Strict Rule: Must contain city name (Word Boundary)
        # Escape city name to handle special chars
        city_regex = r"\b" + re.escape(city.lower()) + r"\b"
        if not re.search(city_regex, page_content):
            return False

        # 3. Soft Rule: Should contain state_abbr or state_name
        state_match = False
        if state_abbr.lower() in page_content:
            state_match = True
        elif state_name and state_name.lower() in page_content:
            state_match = True

        if not state_match:
             return False

        # 4. Specificity Protection (Unwanted Terms)
        if unwanted_terms:
            for term in unwanted_terms:
                if term.lower() in title.lower(): # Check Title specifically
                    return False

        return True
    except Exception:
        return False

def find_procurement_page(base_url: str) -> Optional[str]:
    """
    Crawls the base URL to find a 'Bids' or 'Procurement' related page.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(base_url, timeout=5, headers=headers)
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', href=True)

        # Keywords (Priority Order)
        # Tier 1: Explicit Bid Pages
        tier1 = ["bids", "rfp", "solicitations", "bid opportunities", "current bids", "open bids"]
        # Tier 2: Departments
        tier2 = ["purchasing", "procurement"]

        # Check Tier 1
        for link in links:
            text = link.get_text(separator=" ").strip().lower()
            href = link['href']
            if any(k in text for k in tier1):
                return requests.compat.urljoin(base_url, href)

        # Check Tier 2
        for link in links:
            text = link.get_text(separator=" ").strip().lower()
            href = link['href']
            if any(k in text for k in tier2):
                return requests.compat.urljoin(base_url, href)

        return None
    except Exception:
        return None

def _generate_candidates(name: str, state_abbr: str, patterns: List[str]) -> List[str]:
    name_clean = name.lower().replace(" ", "")
    state_clean = state_abbr.lower().strip()

    candidates = []

    for pattern in patterns:
        domain = pattern
        # Replace specific placeholders
        domain = domain.replace("[cityname]", name_clean)
        domain = domain.replace("[townname]", name_clean)
        domain = domain.replace("[countyname]", name_clean)
        domain = domain.replace("[parishname]", name_clean)
        domain = domain.replace("[state_abbrev]", state_clean)

        # Generic fallback if patterns use [name] or [state]
        domain = domain.replace("[name]", name_clean)
        domain = domain.replace("[state]", state_clean)

        # Construct full URL (try https)
        if not domain.startswith("http"):
             candidates.append(f"https://{domain}")
             # Also try www. prefix if the pattern doesn't have it
             if not domain.startswith("www."):
                 candidates.append(f"https://www.{domain}")

    # Deduplicate while preserving order
    seen = set()
    unique_candidates = []
    for c in candidates:
        if c not in seen:
            unique_candidates.append(c)
            seen.add(c)

    return unique_candidates

def _filter_verified_candidates(candidates: List[str], name: str, state_abbr: str, state_name: str = None, unwanted_terms: List[str] = None) -> List[str]:
    """Helper to verify a list of candidates and return only valid ones."""
    valid_urls = []
    for url in candidates:
        if verify_agency_identity(url, name, state_abbr, state_name, unwanted_terms):
            valid_urls.append(url)
    return valid_urls

def generate_and_validate_domains(name: str, state_abbr: str, specific_patterns: List[str], generic_patterns: List[str], state_name: str = None, unwanted_terms: List[str] = None) -> Optional[str]:
    """
    Generates candidate URLs from patterns and validates them via direct connection AND Identity Verification.
    Implements 3-Phase Strategy:
      Phase 0: Golden Check (Strict [name][state].gov check).
      Phase 1: Specific Patterns (Prioritize shortest valid URL).
      Phase 2: Generic Patterns (Fallback if Phase 1 fails).
    """
    if not name or not state_abbr:
        return None

    # Phase 0: Golden Check (Strict name+state.gov)
    clean_name = normalize_for_domain(name)
    clean_state = state_abbr.lower().strip()

    golden_candidates = [
        f"https://www.{clean_name}{clean_state}.gov",
        f"https://{clean_name}{clean_state}.gov"
    ]

    valid_golden = _filter_verified_candidates(golden_candidates, name, state_abbr, state_name, unwanted_terms)
    if valid_golden:
        return valid_golden[0]

    # Phase 1: Probe Specific (State-Suffixed) Patterns
    if specific_patterns:
        phase1_candidates = _generate_candidates(name, state_abbr, specific_patterns)
        valid_phase1 = _filter_verified_candidates(phase1_candidates, name, state_abbr, state_name, unwanted_terms)

        if valid_phase1:
             return min(valid_phase1, key=len)

    # Phase 2: Probe Generic Patterns (Only if Phase 1 failed)
    if generic_patterns:
        phase2_candidates = _generate_candidates(name, state_abbr, generic_patterns)
        valid_phase2 = _filter_verified_candidates(phase2_candidates, name, state_abbr, state_name, unwanted_terms)

        if valid_phase2:
            return min(valid_phase2, key=len)

    return None

def find_special_district_domain(city_name: str, state_abbr: str, district_type: str) -> Optional[str]:
    """
    Generates and probes patterns specifically for special districts.
    """
    specific, generic = config_loader.get_special_district_patterns(district_type)
    # Special districts don't use the same "unwanted terms" as cities.
    return generate_and_validate_domains(city_name, state_abbr, specific, generic)

def is_better_url(new_url: str, old_url: str) -> bool:
    """
    Determines if a new URL is a valid upgrade over an existing one.
    """
    if not new_url:
        return False

    if not old_url:
        return True # Any new url is better than None

    # Check 0: Specificity Guard
    # If new_url is a Root Domain AND old_url is a Deep Link or Different Domain...
    # Return False (Protect the specific data).
    # Logic: If old URL has path (deep link), and new one is root -> Downgrade.
    # Logic: If old URL domain != new URL domain... (Wait, if new is .gov upgrade, we might want it).
    # Requirement: "If new_url is a Root Domain AND old_url is a Deep Link (path exists) or Different Domain... Return False"
    # Exception: "unless the old one is dead".

    # So first, check if old is dead.
    old_is_dead = False
    try:
        # Check reachability with a short timeout
        if not check_url_reachability(old_url):
            old_is_dead = True
    except:
        old_is_dead = True

    if old_is_dead:
        return True

    # If old is alive, apply Specificity Guard
    is_new_root = is_root_domain(new_url)
    is_old_deep = not is_root_domain(old_url)

    # Check if domains are different
    new_domain = urlparse(new_url).netloc.replace("www.", "")
    old_domain = urlparse(old_url).netloc.replace("www.", "")
    different_domain = new_domain != old_domain

    if is_new_root and (is_old_deep or different_domain):
        # But wait, what if it's a Gov Upgrade?
        # Requirement: "If new_url is .gov and old_url is NOT .gov (and Guard didn't trigger), Return True."
        # This implies Guard takes precedence over Gov Upgrade?
        # "Specificity Guard: ... Return False (Protect the specific data)."
        # So yes, Guard blocks Gov Upgrade if Guard triggers.
        # Example: Old=milfordhousing.org (Specific), New=milford.gov (Root).
        # We want to KEEP milfordhousing.org. So Guard triggers.
        return False

    # Check 1: Gov Upgrade
    new_is_gov = ".gov" in new_url.lower()
    old_is_gov = ".gov" in old_url.lower()

    if new_is_gov and not old_is_gov:
        return True

    return False

def discover_agency_url(name: str, state_abbr: str, state_name: str = None, jurisdiction_type: str = "city") -> Optional[str]:
    """
    Orchestrator for Smart Discovery.
    1. Checks CISA Registry.
    2. Generates and verifies a candidate URL.
    3. Navigates to find a specific procurement page.
    """
    # 0. Check CISA Registry
    try:
        cisa = CisaManager()
        cisa_url = cisa.get_agency_url(name, state_abbr)
        if cisa_url:
            return cisa_url
    except Exception as e:
        print(f"CISA Check Failed: {e}")

    # Define unwanted terms for Cities/Towns to avoid sub-agencies
    unwanted_terms = []
    if jurisdiction_type in ["city", "town", "county"]:
        unwanted_terms = ["School", "Police", "Fire", "Library", "Housing Authority"]

    # Get patterns
    specific_patterns, generic_patterns = config_loader.get_domain_patterns(jurisdiction_type)

    # 1. Generate & Verify
    candidate = generate_and_validate_domains(name, state_abbr, specific_patterns, generic_patterns, state_name=state_name, unwanted_terms=unwanted_terms)

    if not candidate:
        return None

    # 2. Smart Navigation (Find Bids Page)
    # Only run this if we found a verified candidate
    procurement_url = find_procurement_page(candidate)

    # Return the procurement URL if found, else the verified homepage
    return procurement_url if procurement_url else candidate


def find_department_on_domain(main_domain: str, department: str) -> str:
    """
    Attempts to find a specific department page on the main domain.
    If not found, returns the main domain.
    """
    if not main_domain:
        return ""

    if department == "Main Office":
        return main_domain

    # Construct sub-path: e.g. /public-works
    slug = department.replace(" ", "-").lower()

    # Handle main_domain trailing slash
    base = main_domain.rstrip("/")
    candidate = f"{base}/{slug}"

    try:
        response = requests.get(candidate, timeout=5, allow_redirects=True)
        if response.status_code == 200:
            return response.url
    except Exception:
        pass

    # Fallback to main domain
    return main_domain

class DiscoveryEngine:
    def __init__(self):
        pass

    def fetch_search_context(self, query: str, num_results: int = 10) -> List[Dict[str, str]]:
        """
        Executes a search and returns raw results (Title, URL, Snippet)
        without validation. Used for AI Analysis.
        """
        raw_results = []
        try:
            with DDGS() as ddgs:
                # Add a small delay to avoid rate limits
                time.sleep(1.5)
                results = ddgs.text(query, max_results=num_results)
                for res in results:
                    raw_results.append({
                        "title": res.get("title", ""),
                        "url": res.get("href", ""),
                        "snippet": res.get("body", "")
                    })
        except Exception as e:
            print(f"Discovery Error ({query}): {e}")

        return raw_results

    def find_agency_url(self, state_name: str, agency_type: str, ai_client: Any) -> Tuple[Optional[str], str]:
        """Legacy Hybrid Finder (Keep for State Agencies)"""
        if ai_client:
            url = ai_client.find_specific_agency(state_name, agency_type)
            if url and validate_url(url):
                return url, "AI"

        url = self._search_ddg(state_name, agency_type)
        if url: return url, "Browser"
        return None, "Failed"

    def _search_ddg(self, state_name: str, agency_type: str) -> Optional[str]:
        query = f"{state_name} {agency_type} official site"
        return self.find_url_by_query(query)

    def find_url_by_query(self, query: str) -> Optional[str]:
        try:
            with DDGS() as ddgs:
                time.sleep(1)
                results = ddgs.text(query, max_results=5)
                for res in results:
                    url = res.get('href', '')
                    if validate_url(url):
                        return url
        except Exception:
            pass
        return None

    def get_raw_candidates(self, query: str, limit: int = 5) -> List[Dict[str, str]]:
        """
        Retrieves raw search candidates without strict filtering.
        Wrapper around fetch_search_context to match the legacy API.
        """
        return self.fetch_search_context(query, num_results=limit)

    def search_and_rank_candidates(self, query: str, num_results: int = 5) -> List[Dict]:
        """
        Legacy method alias.
        """
        return self.fetch_search_context(query, num_results)
