from duckduckgo_search import DDGS
from typing import List, Tuple, Optional, Any, Dict
import time
from rfp_scraper.utils import validate_url

class DiscoveryEngine:
    def __init__(self):
        pass

    def search_agencies(self, state_name: str, max_results: int = 5) -> List[Tuple[str, str]]:
        """
        Search for agency URLs using DuckDuckGo.
        Returns a list of (Agency Name, URL) tuples.
        Legacy method kept for reference.
        """
        query = f"{state_name} city purchasing construction bids"
        results = []

        try:
            with DDGS() as ddgs:
                # Use text search
                search_results = ddgs.text(query, max_results=max_results)

                for res in search_results:
                    title = res.get('title', 'Unknown Agency')
                    url = res.get('href', '')
                    if url:
                        results.append((title, url))

        except Exception as e:
            print(f"Error during discovery for {state_name}: {e}")

        return results

    def find_agency_url(self, state_name: str, agency_type: str, ai_client: Any) -> Tuple[Optional[str], str]:
        """
        Finds a validated URL for a specific agency type in a state.
        Returns (url, method_used) or (None, "Failed").
        Method used: "AI" or "Browser".
        """
        # 1. AI Attempt
        if ai_client:
            url = ai_client.find_specific_agency(state_name, agency_type)
            if url and validate_url(url):
                return url, "AI"

        # 2. Browser Fallback
        # We search specifically for the agency type
        url = self._search_ddg(state_name, agency_type)
        if url:
            return url, "Browser"

        return None, "Failed"

    def _search_ddg(self, state_name: str, agency_type: str) -> Optional[str]:
        """
        Helper to search DDG for a specific agency.
        Returns the first validated URL found.
        """
        query = f"{state_name} {agency_type} official site"
        return self.find_url_by_query(query)

    def find_url_by_query(self, query: str) -> Optional[str]:
        """
        Search DDG using a specific query string.
        Returns the first validated URL found.
        """
        try:
            with DDGS() as ddgs:
                # Add a small delay to be polite
                time.sleep(1)
                results = ddgs.text(query, max_results=5)
                for res in results:
                    url = res.get('href', '')
                    if validate_url(url):
                        return url
        except Exception as e:
            print(f"DDG Search error for query '{query}': {e}")

        return None

    def fetch_search_context(self, query: str, num_results: int = 10) -> List[Dict]:
        """
        Execute the search (DuckDuckGo) for the given query.
        Returns a list of the top results containing {'title', 'url', 'snippet'}.
        Does not perform strict URL validation or filtering.
        """
        candidates = []
        try:
            with DDGS() as ddgs:
                # Add a small delay to be polite
                time.sleep(1)
                results = ddgs.text(query, max_results=num_results)
                for res in results:
                    candidates.append({
                        "title": res.get('title', ''),
                        "url": res.get('href', ''),
                        "snippet": res.get('body', '')
                    })
        except Exception as e:
            print(f"DDG Search error for Context query '{query}': {e}")

        return candidates

    def search_and_rank_candidates(self, query: str, num_results: int = 5) -> List[Dict]:
        """
        Legacy method alias.
        """
        return self.fetch_search_context(query, num_results)

    def get_raw_candidates(self, query: str, limit: int = 5) -> List[Dict]:
        """
        Retrieves raw search candidates without strict filtering.
        Wrapper around fetch_search_context to match the requested API.
        """
        return self.fetch_search_context(query, num_results=limit)
