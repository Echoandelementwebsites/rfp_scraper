from abc import ABC, abstractmethod
import pandas as pd
import datetime
import os
import json
from typing import List, Optional

class BaseScraper(ABC):
    def __init__(self):
        self.results = []
        self.now = datetime.datetime.now()
        self.config = self._load_config()

    def _load_config(self) -> dict:
        """
        Loads the config.json file.
        """
        # Determine path relative to this file
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # config.json is in rfp_scraper/ (parent of scrapers/)
        config_path = os.path.join(current_dir, "..", "config.json")
        try:
            with open(config_path, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Could not load config.json: {e}")
            return {}

    @abstractmethod
    def scrape(self, page) -> pd.DataFrame:
        """
        Main scraping method.
        :param page: Playwright Page object (shared context)
        :return: DataFrame with scraped data
        """
        pass

    def is_qualified(self, deadline: Optional[datetime.datetime]) -> bool:
        """
        Checks if the deadline is qualified (Deadline >= Today + 4 days).
        :param deadline: The deadline datetime object.
        :return: True if qualified, False otherwise.
        """
        if not deadline:
            return False

        # Calculate difference in days.
        # Requirement: "Deadline >= Today + 4 days"
        # If Today is Jan 1. Deadline Jan 5 is 4 days away.

        # We use .date() to compare calendar days, ignoring time execution.
        if isinstance(deadline, datetime.datetime):
            deadline_date = deadline.date()
        else:
            deadline_date = deadline # Assume date object

        current_date = self.now.date()
        days_diff = (deadline_date - current_date).days

        return days_diff >= 4

    def normalize_data(self, data: List[dict]) -> pd.DataFrame:
        """
        Normalizes list of dicts to a standard DataFrame schema.
        """
        df = pd.DataFrame(data)

        required_columns = [
            "clientName", "title", "slug", "description", "walkthroughDate",
            "rfiDate", "deadline", "budgetMin", "jobStreet", "jobCity",
            "jobState", "jobZip", "portfolioLink", "status"
        ]

        # Ensure all columns exist
        for col in required_columns:
            if col not in df.columns:
                df[col] = "" # or None, or specific default

        # Fill specific defaults
        if "budgetMin" in df.columns:
            df["budgetMin"] = df["budgetMin"].fillna(0)

        if "jobCity" in df.columns:
            df["jobCity"] = df["jobCity"].replace("", "Statewide").fillna("Statewide")

        # Reorder and select
        df = df[required_columns]
        return df

    def save_csv(self, df: pd.DataFrame, filename: str) -> str:
        """
        Saves DataFrame to CSV.
        """
        if not filename.endswith(".csv"):
            filename += ".csv"

        df.to_csv(filename, index=False)
        return filename
