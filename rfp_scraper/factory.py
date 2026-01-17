import importlib
import pkgutil
from typing import List
from rfp_scraper.scrapers.base import BaseScraper
import rfp_scraper.scrapers

class ScraperFactory:
    def __init__(self):
        self._scrapers = {}
        self._load_scrapers()

    def _load_scrapers(self):
        """
        Dynamically loads all scraper modules in the scrapers package.
        Assumes module name matches state name (snake_case) and contains a class ending in 'Scraper'.
        """
        package = rfp_scraper.scrapers
        path = package.__path__
        prefix = package.__name__ + "."

        for _, name, _ in pkgutil.iter_modules(path, prefix):
            if name.endswith("base") or name.endswith("generic"):
                continue

            try:
                module = importlib.import_module(name)
                # Find the class that inherits from BaseScraper
                for attribute_name in dir(module):
                    attribute = getattr(module, attribute_name)
                    if (isinstance(attribute, type) and
                        issubclass(attribute, BaseScraper) and
                        attribute is not BaseScraper):

                        # Register scraper using module name (cleaned)
                        # e.g. rfp_scraper.scrapers.california -> California
                        state_key = name.split(".")[-1].replace("_", " ").title()
                        self._scrapers[state_key] = attribute
            except Exception as e:
                print(f"Failed to load scraper module {name}: {e}")

    def get_scraper(self, state_name: str) -> BaseScraper:
        """
        Returns an instance of the scraper for the given state.
        """
        scraper_class = self._scrapers.get(state_name)
        if not scraper_class:
            raise ValueError(f"No scraper implementation found for state: {state_name}")
        return scraper_class()

    def get_available_states(self) -> List[str]:
        """
        Returns a list of available state names.
        """
        return sorted(list(self._scrapers.keys()))
