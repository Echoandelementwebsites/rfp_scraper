import sqlite3
import os
import datetime
from typing import Optional, List, Tuple

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

        # Table for scraped bids (Successes)
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

        # Table for discovery log (Attempts/Queue)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS discovery_log (
                url TEXT PRIMARY KEY,
                state TEXT,
                status TEXT, -- 'pending', 'processed', 'error'
                last_attempted_at TEXT
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

    def add_discovered_url(self, url: str, state: str):
        """Add a discovered URL to the log if it doesn't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            # We use INSERT OR IGNORE to avoid overwriting existing status
            cursor.execute("""
                INSERT OR IGNORE INTO discovery_log (url, state, status, last_attempted_at)
                VALUES (?, ?, 'pending', NULL)
            """, (url, state))
            conn.commit()
        finally:
            conn.close()

    def get_pending_urls(self, state: str) -> List[str]:
        """Get all pending URLs for a given state."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT url FROM discovery_log
            WHERE state = ? AND status = 'pending'
        """, (state,))
        rows = cursor.fetchall()
        conn.close()
        return [r[0] for r in rows]

    def mark_url_processed(self, url: str, status: str = 'processed'):
        """Update the status of a URL."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now = datetime.datetime.now().isoformat()
        cursor.execute("""
            UPDATE discovery_log
            SET status = ?, last_attempted_at = ?
            WHERE url = ?
        """, (status, now, url))
        conn.commit()
        conn.close()

    @staticmethod
    def generate_slug(title: str, client_name: str, source_url: str) -> str:
        """Generate a deterministic slug based on bid properties."""
        import hashlib
        # Normalize inputs
        raw_string = f"{str(title).lower()}|{str(client_name).lower()}|{str(source_url).lower()}"
        return hashlib.md5(raw_string.encode('utf-8')).hexdigest()
