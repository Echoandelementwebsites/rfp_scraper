from duckduckgo_search import DDGS
from typing import List, Tuple
import time

class DiscoveryEngine:
    def __init__(self):
        pass

    def search_agencies(self, state_name: str, max_results: int = 5) -> List[Tuple[str, str]]:
        """
        Search for agency URLs using DuckDuckGo.
        Returns a list of (Agency Name, URL) tuples.
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
