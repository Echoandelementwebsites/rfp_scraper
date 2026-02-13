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
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.commit()

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
                state TEXT,
                rfp_description TEXT,
                matching_trades TEXT
            )
        """)

        # Migration: Ensure state column exists for existing tables
        try:
            cursor.execute("SELECT state FROM scraped_bids LIMIT 1")
        except sqlite3.OperationalError:
            # Column missing, add it
            cursor.execute("ALTER TABLE scraped_bids ADD COLUMN state TEXT DEFAULT 'Unknown'")

        # Migration: Ensure rfp_description column exists
        try:
            cursor.execute("SELECT rfp_description FROM scraped_bids LIMIT 1")
        except sqlite3.OperationalError:
            cursor.execute("ALTER TABLE scraped_bids ADD COLUMN rfp_description TEXT")

        # Migration: Ensure matching_trades column exists
        try:
            cursor.execute("SELECT matching_trades FROM scraped_bids LIMIT 1")
        except sqlite3.OperationalError:
            cursor.execute("ALTER TABLE scraped_bids ADD COLUMN matching_trades TEXT")

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

        # Table: local_jurisdictions (The Container)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS local_jurisdictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state_id INTEGER,
                name TEXT,
                type TEXT, -- 'county', 'city', 'town'
                created_at TEXT,
                FOREIGN KEY(state_id) REFERENCES states(id),
                UNIQUE(state_id, name, type)
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
                category TEXT DEFAULT 'state_agency',
                local_jurisdiction_id INTEGER,
                FOREIGN KEY(state_id) REFERENCES states(id),
                FOREIGN KEY(local_jurisdiction_id) REFERENCES local_jurisdictions(id)
            )
        """)

        # Migration: Ensure category column exists for existing tables
        try:
            cursor.execute("SELECT category FROM agencies LIMIT 1")
        except sqlite3.OperationalError:
            # Column missing, add it
            cursor.execute("ALTER TABLE agencies ADD COLUMN category TEXT DEFAULT 'state_agency'")

        # Migration: Ensure local_jurisdiction_id column exists
        try:
            cursor.execute("SELECT local_jurisdiction_id FROM agencies LIMIT 1")
        except sqlite3.OperationalError:
            cursor.execute("ALTER TABLE agencies ADD COLUMN local_jurisdiction_id INTEGER")

        # --- Data Migration ---
        # Move "pending" agencies (no URL) to local_jurisdictions

        # 1. Select candidates
        cursor.execute("""
            SELECT id, state_id, organization_name, category, created_at
            FROM agencies
            WHERE (url IS NULL OR url = '')
              AND category IN ('county', 'city', 'town')
        """)
        pending_rows = cursor.fetchall()

        ids_to_delete = []
        for row in pending_rows:
            agency_id, state_id, name, category, created_at = row
            # Insert into local_jurisdictions
            try:
                cursor.execute("""
                    INSERT INTO local_jurisdictions (state_id, name, type, created_at)
                    VALUES (?, ?, ?, ?)
                """, (state_id, name, category, created_at))
            except sqlite3.IntegrityError:
                # Duplicate, skip
                pass
            ids_to_delete.append(agency_id)

        # 2. Delete migrated rows from agencies
        if ids_to_delete:
            cursor.execute(f"DELETE FROM agencies WHERE id IN ({','.join(['?']*len(ids_to_delete))})", ids_to_delete)
            print(f"Migrated {len(ids_to_delete)} pending agencies to local_jurisdictions.")

        conn.commit()
        conn.close()

    def url_already_scraped(self, url: str) -> bool:
        """Checks if a URL source has already been processed."""
        if not url: return False

        # Normalize: Remove protocol/www to catch variations
        clean_url = url.replace("https://", "").replace("http://", "").replace("www.", "").rstrip("/")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        # Check source_url column in scraped_bids
        cursor.execute("SELECT 1 FROM scraped_bids WHERE source_url LIKE ?", (f"%{clean_url}%",))
        exists = cursor.fetchone() is not None
        conn.close()
        return exists

    def bid_exists(self, slug: str) -> bool:
        """Check if a bid with the given slug already exists."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM scraped_bids WHERE slug = ?", (slug,))
        exists = cursor.fetchone() is not None
        conn.close()
        return exists

    def insert_bid(self, slug: str, client_name: str, title: str, deadline: str, source_url: str, state: str = "Unknown", rfp_description: Optional[str] = None, matching_trades: Optional[str] = None):
        """Insert a new bid into the database or update if exists."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        scraped_at = datetime.datetime.now().isoformat()
        try:
            cursor.execute("""
                INSERT INTO scraped_bids (slug, client_name, title, deadline, scraped_at, source_url, state, rfp_description, matching_trades)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(slug) DO UPDATE SET
                    state=excluded.state,
                    scraped_at=excluded.scraped_at,
                    deadline=excluded.deadline,
                    rfp_description=COALESCE(excluded.rfp_description, scraped_bids.rfp_description),
                    matching_trades=excluded.matching_trades
            """, (slug, client_name, title, deadline, scraped_at, source_url, state, rfp_description, matching_trades))
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
            df = pd.DataFrame(columns=['slug', 'client_name', 'title', 'deadline', 'scraped_at', 'source_url', 'state', 'rfp_description', 'matching_trades'])
        finally:
            conn.close()
        return df

    def get_agency_by_jurisdiction(self, state_id: int, category: str, local_jurisdiction_id: Optional[int]) -> Optional[dict]:
        """
        Retrieves a single agency record matching the jurisdiction and category.
        Returns a dictionary representation of the row.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            if local_jurisdiction_id is None:
                return None
            else:
                cursor.execute("""
                    SELECT * FROM agencies
                    WHERE state_id = ? AND category = ? AND local_jurisdiction_id = ?
                """, (state_id, category, local_jurisdiction_id))

            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
        finally:
            conn.close()

    def get_agency_by_name(self, state_id: int, name: str, category: Optional[str] = None) -> Optional[dict]:
        """
        Retrieves a single agency record matching the name.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            if category:
                cursor.execute("""
                    SELECT * FROM agencies
                    WHERE state_id = ? AND organization_name = ? AND category = ?
                """, (state_id, name, category))
            else:
                 cursor.execute("""
                    SELECT * FROM agencies
                    WHERE state_id = ? AND organization_name = ?
                """, (state_id, name))

            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
        finally:
            conn.close()

    def update_agency_url(self, agency_id: int, new_url: str):
        """Updates the URL for a specific agency and sets verified=True."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE agencies
                SET url = ?, verified = 1
                WHERE id = ?
            """, (new_url, agency_id))
            conn.commit()
        except sqlite3.Error as e:
            print(f"Error updating agency URL: {e}")
        finally:
            conn.close()

    def update_agency_name(self, agency_id: int, new_name: str):
        """Updates the name for a specific agency."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE agencies
                SET organization_name = ?
                WHERE id = ?
            """, (new_name, agency_id))
            conn.commit()
        except sqlite3.Error as e:
            print(f"Error updating agency name: {e}")
        finally:
            conn.close()

    def delete_agency(self, agency_id: int):
        """Deletes an agency record."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM agencies WHERE id = ?", (agency_id,))
            conn.commit()
        except sqlite3.Error as e:
            print(f"Error deleting agency: {e}")
        finally:
            conn.close()

    def get_local_jurisdictions(self, state_id: Optional[int] = None) -> pd.DataFrame:
        """Retrieve local jurisdictions, optionally filtered by state."""
        conn = sqlite3.connect(self.db_path)
        try:
            if state_id:
                query = "SELECT * FROM local_jurisdictions WHERE state_id = ?"
                df = pd.read_sql_query(query, conn, params=(state_id,))
            else:
                query = "SELECT * FROM local_jurisdictions"
                df = pd.read_sql_query(query, conn)
        except Exception:
             df = pd.DataFrame(columns=['id', 'state_id', 'name', 'type', 'created_at'])
        finally:
            conn.close()
        return df

    def append_local_jurisdiction(self, state_id: int, name: str, jurisdiction_type: str) -> int:
        """
        Add a local jurisdiction if it doesn't exist.
        Returns the ID of the jurisdiction.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Check existence
        cursor.execute("SELECT id FROM local_jurisdictions WHERE state_id = ? AND name = ? AND type = ?", (state_id, name, jurisdiction_type))
        row = cursor.fetchone()

        if row:
            conn.close()
            return row[0]

        # Insert
        created_at = datetime.datetime.now().isoformat()
        cursor.execute("INSERT INTO local_jurisdictions (state_id, name, type, created_at) VALUES (?, ?, ?, ?)",
                       (state_id, name, jurisdiction_type, created_at))
        new_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return new_id

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

    def agency_exists(self, state_id: int, url: Optional[str] = None, name: Optional[str] = None, category: Optional[str] = None, local_jurisdiction_id: Optional[int] = None) -> bool:
        """
        Check if an agency exists for a specific state.
        If URL is provided, check by normalized URL.
        If URL is None, check by (state_id, organization_name, category, local_jurisdiction_id).
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if url:
            normalized_url = self._normalize_url(url)
            cursor.execute("SELECT url FROM agencies WHERE state_id = ?", (state_id,))
            rows = cursor.fetchall()
            conn.close()

            for row in rows:
                existing_url = self._normalize_url(row[0])
                if existing_url == normalized_url:
                    return True
            return False

        # If no URL, check by name, category, and local_jurisdiction_id
        if name and category:
            # Handle NULL explicitly for local_jurisdiction_id in SQL
            if local_jurisdiction_id is None:
                cursor.execute("""
                    SELECT 1 FROM agencies
                    WHERE state_id = ? AND organization_name = ? AND category = ? AND local_jurisdiction_id IS NULL
                """, (state_id, name, category))
            else:
                cursor.execute("""
                    SELECT 1 FROM agencies
                    WHERE state_id = ? AND organization_name = ? AND category = ? AND local_jurisdiction_id = ?
                """, (state_id, name, category, local_jurisdiction_id))

            exists = cursor.fetchone() is not None
            conn.close()
            return exists

        conn.close()
        return False

    def add_agency(self, state_id: int, name: str, url: Optional[str] = None, verified: bool = False, category: str = 'state_agency', local_jurisdiction_id: Optional[int] = None):
        """Insert an agency linked to a state, ensuring no duplicates."""
        # Clean inputs
        if url:
            url = url.strip()

        if self.agency_exists(state_id, url=url, name=name, category=category, local_jurisdiction_id=local_jurisdiction_id):
            print(f"Skipping duplicate agency: {name} (URL: {url}) for state_id {state_id}")
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        created_at = datetime.datetime.now().isoformat()
        verified_int = 1 if verified else 0
        try:
            cursor.execute("""
                INSERT INTO agencies (state_id, organization_name, url, verified, created_at, category, local_jurisdiction_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (state_id, name, url, verified_int, created_at, category, local_jurisdiction_id))
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
            df = pd.DataFrame(columns=['id', 'state_id', 'organization_name', 'url', 'verified', 'created_at', 'category', 'local_jurisdiction_id'])
        finally:
            conn.close()
        return df

    def get_all_agencies(self) -> pd.DataFrame:
        """Return all agencies with their associated state names and jurisdiction labels."""
        conn = sqlite3.connect(self.db_path)
        try:
            query = """
                SELECT
                    a.*,
                    s.name as state_name,
                    COALESCE(lj.name || ' (' || lj.type || ')', s.name) as jurisdiction_label
                FROM agencies a
                JOIN states s ON a.state_id = s.id
                LEFT JOIN local_jurisdictions lj ON a.local_jurisdiction_id = lj.id
                ORDER BY s.name, a.organization_name
            """
            df = pd.read_sql_query(query, conn)
        except Exception:
             df = pd.DataFrame(columns=['id', 'state_id', 'organization_name', 'url', 'verified', 'created_at', 'category', 'local_jurisdiction_id', 'state_name', 'jurisdiction_label'])
        finally:
            conn.close()
        return df
