from duckduckgo_search import DDGS
from typing import List, Tuple, Optional, Any, Dict
import time
import requests
from rfp_scraper.utils import validate_url, check_url_reachability
import rfp_scraper.config_loader as config_loader

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

def _probe_candidates(candidates: List[str]) -> List[str]:
    valid_urls = []
    for url in candidates:
        try:
            # 3-second timeout, follow redirects
            response = requests.get(url, timeout=3, allow_redirects=True)
            if response.status_code == 200:
                # Basic check to ensure we didn't land on a parked page or generic 404 handler
                # (This is hard to do perfectly without content analysis, but 200 is the requirement)
                valid_urls.append(response.url)
        except Exception:
            continue
    return valid_urls

def generate_and_validate_domains(name: str, state_abbr: str, specific_patterns: List[str], generic_patterns: List[str]) -> Optional[str]:
    """
    Generates candidate URLs from patterns and validates them via direct connection.
    Implements 2-Phase Strategy:
      Phase 1: Specific Patterns (Prioritize shortest valid URL).
      Phase 2: Generic Patterns (Fallback if Phase 1 fails).
    """
    if not name or not state_abbr:
        return None

    # Phase 1: Probe Specific (State-Suffixed) Patterns
    if specific_patterns:
        phase1_candidates = _generate_candidates(name, state_abbr, specific_patterns)
        valid_phase1 = _probe_candidates(phase1_candidates)

        if valid_phase1:
             # Found match in specific patterns. Return immediately.
             return min(valid_phase1, key=len)

    # Phase 2: Probe Generic Patterns (Only if Phase 1 failed)
    if generic_patterns:
        phase2_candidates = _generate_candidates(name, state_abbr, generic_patterns)
        valid_phase2 = _probe_candidates(phase2_candidates)

        if valid_phase2:
            return min(valid_phase2, key=len)

    return None

def find_special_district_domain(city_name: str, state_abbr: str, district_type: str) -> Optional[str]:
    """
    Generates and probes patterns specifically for special districts.
    """
    specific, generic = config_loader.get_special_district_patterns(district_type)
    return generate_and_validate_domains(city_name, state_abbr, specific, generic)

def is_better_url(new_url: str, old_url: str) -> bool:
    """
    Determines if a new URL is a valid upgrade over an existing one.
    Upgrade Criteria:
      1. New URL is .gov AND Old URL is NOT .gov.
      2. Old URL is dead (unreachable) AND New URL is live.
    """
    if not new_url:
        return False

    # Check 1: Gov Upgrade
    new_is_gov = ".gov" in new_url.lower()
    old_is_gov = ".gov" in old_url.lower() if old_url else False

    if new_is_gov and not old_is_gov:
        return True

    # Check 2: Liveness Upgrade
    # If old URL is dead, any valid new URL is better
    if old_url:
        # We only check reachability of old_url if the domain extension check didn't trigger an upgrade.
        # This assumes new_url is already validated/reachable (which it is, coming from discovery).
        if not check_url_reachability(old_url):
            return True

    return False

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
