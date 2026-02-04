from duckduckgo_search import DDGS
from typing import List, Tuple, Optional, Any, Dict
import time
from rfp_scraper.utils import validate_url

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
