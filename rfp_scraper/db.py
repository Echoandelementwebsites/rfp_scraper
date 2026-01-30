import sqlite3
import os
import datetime
from typing import Optional, List, Tuple
import pandas as pd

class DatabaseHandler:
    def __init__(self, db_path: str = "rfp_scraper/rfp_scraper.db"):
        # Ensure directory exists
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
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
                source_url TEXT,
                state TEXT
            )
        """)

        # Migration: Ensure state column exists for existing tables
        try:
            cursor.execute("SELECT state FROM scraped_bids LIMIT 1")
        except sqlite3.OperationalError:
            # Column missing, add it
            cursor.execute("ALTER TABLE scraped_bids ADD COLUMN state TEXT DEFAULT 'Unknown'")

        # Table for discovery log (Attempts/Queue)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS discovery_log (
                url TEXT PRIMARY KEY,
                state TEXT,
                status TEXT, -- 'pending', 'processed', 'error'
                last_attempted_at TEXT
            )
        """)

        # Table for States
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                created_at TEXT
            )
        """)

        # Table for Agencies
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agencies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state_id INTEGER,
                organization_name TEXT,
                url TEXT,
                verified INTEGER DEFAULT 0,
                created_at TEXT,
                FOREIGN KEY(state_id) REFERENCES states(id)
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

    def insert_bid(self, slug: str, client_name: str, title: str, deadline: str, source_url: str, state: str = "Unknown"):
        """Insert a new bid into the database or update if exists."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        scraped_at = datetime.datetime.now().isoformat()
        try:
            cursor.execute("""
                INSERT INTO scraped_bids (slug, client_name, title, deadline, scraped_at, source_url, state)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(slug) DO UPDATE SET
                    state=excluded.state,
                    scraped_at=excluded.scraped_at,
                    deadline=excluded.deadline
            """, (slug, client_name, title, deadline, scraped_at, source_url, state))
            conn.commit()
        except sqlite3.Error as e:
            print(f"Error inserting bid: {e}")
        finally:
            conn.close()

    def get_bids(self, state: Optional[str] = None) -> pd.DataFrame:
        """Retrieve bids, optionally filtered by state."""
        conn = sqlite3.connect(self.db_path)
        try:
            if state:
                query = "SELECT * FROM scraped_bids WHERE state = ?"
                df = pd.read_sql_query(query, conn, params=(state,))
            else:
                query = "SELECT * FROM scraped_bids"
                df = pd.read_sql_query(query, conn)
        except Exception:
            df = pd.DataFrame(columns=['slug', 'client_name', 'title', 'deadline', 'scraped_at', 'source_url', 'state'])
        finally:
            conn.close()
        return df

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

    # --- New Methods for States & Agencies ---

    def add_state(self, name: str):
        """Insert a state if it doesn't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        created_at = datetime.datetime.now().isoformat()
        try:
            cursor.execute("""
                INSERT INTO states (name, created_at)
                VALUES (?, ?)
            """, (name, created_at))
            conn.commit()
        except sqlite3.IntegrityError:
            # Already exists
            pass
        finally:
            conn.close()

    def get_all_states(self) -> pd.DataFrame:
        """Return all states as a DataFrame."""
        conn = sqlite3.connect(self.db_path)
        try:
            df = pd.read_sql_query("SELECT * FROM states ORDER BY name", conn)
        except Exception:
             # If table doesn't exist or other error, return empty DF
             df = pd.DataFrame(columns=['id', 'name', 'created_at'])
        finally:
            conn.close()
        return df

    @staticmethod
    def _normalize_url(url: str) -> str:
        """Normalize URL for deduplication checks."""
        if not url:
            return ""
        url = url.strip().lower()

        # Remove scheme
        if url.startswith("https://"):
            url = url[8:]
        elif url.startswith("http://"):
            url = url[7:]

        # Remove www.
        if url.startswith("www."):
            url = url[4:]

        if url.endswith('/'):
            url = url[:-1]
        return url

    def agency_exists(self, state_id: int, url: str) -> bool:
        """Check if an agency exists for a specific state using normalized URL."""
        normalized_url = self._normalize_url(url)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # We need to check all agencies for this state and compare normalized URLs
        # SQLite doesn't have a great normalize function, so we might need to do some of this in python
        # or rely on the stored url being normalized?
        # For safety, let's select all URLs for the state and check in python.
        # Ideally, we should store normalized URLs, but for now we check against existing.

        cursor.execute("SELECT url FROM agencies WHERE state_id = ?", (state_id,))
        rows = cursor.fetchall()
        conn.close()

        for row in rows:
            existing_url = self._normalize_url(row[0])
            if existing_url == normalized_url:
                return True
        return False

    def get_agency_by_url(self, url: str) -> Optional[dict]:
        """Retrieve an agency by its URL."""
        normalized_url = self._normalize_url(url)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id, state_id, organization_name, url, verified FROM agencies")
        rows = cursor.fetchall()
        conn.close()

        for row in rows:
            if self._normalize_url(row[3]) == normalized_url:
                return {
                    "id": row[0],
                    "state_id": row[1],
                    "organization_name": row[2],
                    "url": row[3],
                    "verified": bool(row[4])
                }
        return None

    def add_agency(self, state_id: int, name: str, url: str, verified: bool = False):
        """Insert an agency linked to a state, ensuring no duplicates."""
        # Clean inputs
        url = url.strip()

        if self.agency_exists(state_id, url):
            print(f"Skipping duplicate agency: {url} for state_id {state_id}")
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        created_at = datetime.datetime.now().isoformat()
        verified_int = 1 if verified else 0
        try:
            cursor.execute("""
                INSERT INTO agencies (state_id, organization_name, url, verified, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (state_id, name, url, verified_int, created_at))
            conn.commit()
        except sqlite3.Error as e:
            print(f"Error adding agency: {e}")
        finally:
            conn.close()

    def get_agencies_by_state(self, state_id: int) -> pd.DataFrame:
        """Return agencies for a specific state as a DataFrame."""
        conn = sqlite3.connect(self.db_path)
        try:
            query = "SELECT * FROM agencies WHERE state_id = ?"
            df = pd.read_sql_query(query, conn, params=(state_id,))
        except Exception:
            df = pd.DataFrame(columns=['id', 'state_id', 'organization_name', 'url', 'verified', 'created_at'])
        finally:
            conn.close()
        return df

    def get_all_agencies(self) -> pd.DataFrame:
        """Return all agencies with their associated state names."""
        conn = sqlite3.connect(self.db_path)
        try:
            query = """
                SELECT a.*, s.name as state_name
                FROM agencies a
                JOIN states s ON a.state_id = s.id
                ORDER BY s.name, a.organization_name
            """
            df = pd.read_sql_query(query, conn)
        except Exception:
             df = pd.DataFrame(columns=['id', 'state_id', 'organization_name', 'url', 'verified', 'created_at', 'state_name'])
        finally:
            conn.close()
        return df
