import sqlite3
import os
import datetime
from typing import Optional

class DatabaseHandler:
    def __init__(self, db_path: str = "rfp_scraper/rfp_scraper.db"):
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize the database and tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Create table for scraped bids
        # slug is a unique identifier (hash of title + client + deadline) or URL
        # For simplicity and robustness, we'll use URL as slug if available, or a hash.
        # But user requested 'slug' as PK. We'll use URL as the main dedupe key, or a hash if URL is generic.
        # Actually, let's use a composite hash as slug to be safe.

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scraped_bids (
                slug TEXT PRIMARY KEY,
                client_name TEXT,
                title TEXT,
                deadline TEXT,
                scraped_at TEXT,
                source_url TEXT
            )
        """)

        conn.commit()
        conn.close()

    def bid_exists(self, slug: str) -> bool:
        """Check if a bid with the given slug already exists."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM scraped_bids WHERE slug = ?", (slug,))
        exists = cursor.fetchone() is not None
        conn.close()
        return exists

    def insert_bid(self, slug: str, client_name: str, title: str, deadline: str, source_url: str):
        """Insert a new bid into the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        scraped_at = datetime.datetime.now().isoformat()
        try:
            cursor.execute("""
                INSERT INTO scraped_bids (slug, client_name, title, deadline, scraped_at, source_url)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (slug, client_name, title, deadline, scraped_at, source_url))
            conn.commit()
        except sqlite3.IntegrityError:
            # Already exists, ignore
            pass
        finally:
            conn.close()

    @staticmethod
    def generate_slug(title: str, client_name: str, source_url: str) -> str:
        """Generate a deterministic slug based on bid properties."""
        import hashlib
        # Normalize inputs
        raw_string = f"{str(title).lower()}|{str(client_name).lower()}|{str(source_url).lower()}"
        return hashlib.md5(raw_string.encode('utf-8')).hexdigest()
