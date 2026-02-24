import os
import sqlite3
import datetime
import json
import pandas as pd
import urllib.parse
import hashlib
from typing import Optional, List, Dict, Any, Tuple
from .models import Bid

# Try importing Postgres drivers (psycopg2 or asyncpg)
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PG = True
except ImportError:
    HAS_PG = False

# Static mapping for state abbreviation to full name
ABBR_TO_STATE = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
    "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "DC": "District of Columbia"
}

class DatabaseHandler:
    def __init__(self, db_url: Optional[str] = None):
        """
        Initializes database connection.
        Prioritizes DATABASE_URL from environment or passed explicitly.
        Falls back to local SQLite.
        """
        self.db_url = db_url or os.getenv("DATABASE_URL")
        self.is_postgres = bool(self.db_url and "postgres" in self.db_url)

        if self.is_postgres and not HAS_PG:
            print("Warning: Postgres URL detected but psycopg2 not installed. Falling back to SQLite.")
            self.is_postgres = False
            self.db_url = None

        if not self.is_postgres:
            # Ensure SQLite file path
            self.db_path = "rfp_scraper_v2/rfp_scraper_v2.db"
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self._init_sqlite()
        else:
            self._init_postgres()

    @property
    def _param_placeholder(self) -> str:
        return "%s" if self.is_postgres else "?"

    def _get_connection(self):
        if self.is_postgres:
            return psycopg2.connect(self.db_url)
        else:
            return sqlite3.connect(self.db_path, check_same_thread=False, timeout=15.0)

    def _init_sqlite(self):
        conn = self._get_connection()
        conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()

        # --- Legacy Schema Tables ---

        # States Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                created_at TEXT
            )
        """)

        # Local Jurisdictions Table
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

        # Agencies Table (Legacy Schema + V2 fields)
        # Using state_id instead of state string for relational integrity
        # Added procurement_url, last_checked for V2
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agencies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state_id INTEGER,
                organization_name TEXT,
                url TEXT,
                procurement_url TEXT, -- NEW: Store the AI-discovered portal
                verified INTEGER DEFAULT 0,
                created_at TEXT,
                category TEXT DEFAULT 'state_agency',
                local_jurisdiction_id INTEGER,
                last_checked TEXT,
                FOREIGN KEY(state_id) REFERENCES states(id),
                FOREIGN KEY(local_jurisdiction_id) REFERENCES local_jurisdictions(id),
                UNIQUE(state_id, organization_name)
            )
        """)

        # Discovery Log Table (Legacy)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS discovery_log (
                url TEXT PRIMARY KEY,
                state TEXT,
                status TEXT, -- 'pending', 'processed', 'error'
                last_attempted_at TEXT
            )
        """)

        # --- V2 Schema Tables ---

        # Bids Table (V2 Schema)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bids (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slug TEXT UNIQUE,
                client_name TEXT,
                title TEXT,
                deadline TEXT,
                description TEXT,
                link TEXT,
                full_text TEXT,
                csi_divisions TEXT, -- JSON Array
                scraped_at TEXT,
                state TEXT
            )
        """)

        # Performance Indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_bids_link ON bids(link)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_agencies_state ON agencies(state_id)")

        conn.commit()
        conn.close()

    def _init_postgres(self):
        conn = self._get_connection()
        cursor = conn.cursor()

        # States Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS states (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE,
                created_at TIMESTAMP
            )
        """)

        # Local Jurisdictions Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS local_jurisdictions (
                id SERIAL PRIMARY KEY,
                state_id INTEGER REFERENCES states(id),
                name TEXT,
                type TEXT,
                created_at TIMESTAMP,
                UNIQUE(state_id, name, type)
            )
        """)

        # Agencies Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agencies (
                id SERIAL PRIMARY KEY,
                state_id INTEGER REFERENCES states(id),
                organization_name TEXT,
                url TEXT,
                procurement_url TEXT, -- NEW: Store the AI-discovered portal
                verified INTEGER DEFAULT 0,
                created_at TIMESTAMP,
                category TEXT DEFAULT 'state_agency',
                local_jurisdiction_id INTEGER REFERENCES local_jurisdictions(id),
                last_checked TIMESTAMP,
                UNIQUE(state_id, organization_name)
            )
        """)

        # Discovery Log Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS discovery_log (
                url TEXT PRIMARY KEY,
                state TEXT,
                status TEXT,
                last_attempted_at TIMESTAMP
            )
        """)

        # Bids Table (V2)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bids (
                id SERIAL PRIMARY KEY,
                slug TEXT UNIQUE,
                client_name TEXT,
                title TEXT,
                deadline DATE,
                description TEXT,
                link TEXT,
                full_text TEXT,
                csi_divisions JSONB,
                scraped_at TIMESTAMP DEFAULT NOW(),
                state TEXT
            )
        """)

        # Indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_bids_link ON bids(link)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_agencies_state ON agencies(state_id)")

        conn.commit()
        conn.close()

    # --- Helper Methods ---

    def _get_state_id(self, state_abbr_or_name: str) -> Optional[int]:
        """Resolves state ID from abbreviation or full name."""
        if not state_abbr_or_name:
            return None

        # Determine full name
        full_name = state_abbr_or_name
        if len(state_abbr_or_name) == 2:
             full_name = ABBR_TO_STATE.get(state_abbr_or_name.upper(), state_abbr_or_name)

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            query = f"SELECT id FROM states WHERE name = {self._param_placeholder}"
            cursor.execute(query, (full_name,))
            row = cursor.fetchone()
            if row:
                return row[0]

            # If not found, try inserting strictly if it's a known US state?
            # Or just return None.
            # To be safe for V2 orchestrator, we might want to auto-create known states.
            if full_name in ABBR_TO_STATE.values():
                self.add_state(full_name)
                # Retry fetch
                cursor.execute(query, (full_name,))
                row = cursor.fetchone()
                if row:
                    return row[0]

            return None
        finally:
            conn.close()

    @staticmethod
    def _normalize_url(url: str) -> str:
        """Normalize URL for deduplication checks."""
        if not url:
            return ""
        parsed = urllib.parse.urlparse(url)
        clean_url = f"{parsed.netloc}{parsed.path}"
        clean_url = clean_url.strip().lower()

        if not parsed.netloc and not parsed.scheme:
             clean_url = url.strip().lower()
             if '?' in clean_url: clean_url = clean_url.split('?')[0]
             if '#' in clean_url: clean_url = clean_url.split('#')[0]

        if clean_url.startswith("https://"): clean_url = clean_url[8:]
        elif clean_url.startswith("http://"): clean_url = clean_url[7:]
        if clean_url.startswith("www."): clean_url = clean_url[4:]

        for sub in ['/en/', '/portal/']:
            clean_url = clean_url.replace(sub, '/')

        if clean_url.endswith('/en'): clean_url = clean_url[:-3]
        if clean_url.endswith('/portal'): clean_url = clean_url[:-7]
        if clean_url.endswith('/'): clean_url = clean_url[:-1]

        return clean_url

    @staticmethod
    def generate_slug(title: str, client_name: str, source_url: str) -> str:
        """Generate a deterministic slug based on bid properties."""
        raw_string = f"{str(title).lower()}|{str(client_name).lower()}|{str(source_url).lower()}"
        return hashlib.md5(raw_string.encode('utf-8')).hexdigest()

    # --- Legacy Methods (States) ---

    def add_state(self, name: str):
        conn = self._get_connection()
        cursor = conn.cursor()
        created_at = datetime.datetime.now().isoformat()
        try:
            if self.is_postgres:
                cursor.execute("INSERT INTO states (name, created_at) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING", (name, created_at))
            else:
                cursor.execute("INSERT OR IGNORE INTO states (name, created_at) VALUES (?, ?)", (name, created_at))
            conn.commit()
        finally:
            conn.close()

    def get_all_states(self) -> pd.DataFrame:
        conn = self._get_connection()
        try:
            df = pd.read_sql_query("SELECT * FROM states ORDER BY name", conn)
        except Exception:
            df = pd.DataFrame(columns=['id', 'name', 'created_at'])
        finally:
            conn.close()
        return df

    # --- Legacy Methods (Local Jurisdictions) ---

    def append_local_jurisdiction(self, state_id: int, name: str, jurisdiction_type: str) -> int:
        conn = self._get_connection()
        cursor = conn.cursor()
        created_at = datetime.datetime.now().isoformat()

        # Check existence
        query = f"SELECT id FROM local_jurisdictions WHERE state_id = {self._param_placeholder} AND name = {self._param_placeholder} AND type = {self._param_placeholder}"
        cursor.execute(query, (state_id, name, jurisdiction_type))
        row = cursor.fetchone()
        if row:
            conn.close()
            return row[0]

        # Insert
        try:
            if self.is_postgres:
                cursor.execute("INSERT INTO local_jurisdictions (state_id, name, type, created_at) VALUES (%s, %s, %s, %s) RETURNING id", (state_id, name, jurisdiction_type, created_at))
                new_id = cursor.fetchone()[0]
            else:
                cursor.execute("INSERT INTO local_jurisdictions (state_id, name, type, created_at) VALUES (?, ?, ?, ?)", (state_id, name, jurisdiction_type, created_at))
                new_id = cursor.lastrowid
            conn.commit()
            return new_id
        finally:
            conn.close()

    def get_local_jurisdictions(self, state_id: Optional[int] = None) -> pd.DataFrame:
        conn = self._get_connection()
        try:
            if state_id:
                query = f"SELECT * FROM local_jurisdictions WHERE state_id = {self._param_placeholder}"
                df = pd.read_sql_query(query, conn, params=(state_id,))
            else:
                df = pd.read_sql_query("SELECT * FROM local_jurisdictions", conn)
        except Exception:
            df = pd.DataFrame(columns=['id', 'state_id', 'name', 'type', 'created_at'])
        finally:
            conn.close()
        return df

    # --- Legacy Methods (Agencies) ---

    def agency_exists(self, state_id: int, url: Optional[str] = None, name: Optional[str] = None, category: Optional[str] = None, local_jurisdiction_id: Optional[int] = None) -> bool:
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            if url:
                normalized_url = self._normalize_url(url)
                query = f"SELECT url FROM agencies WHERE state_id = {self._param_placeholder}"
                cursor.execute(query, (state_id,))
                rows = cursor.fetchall()
                for row in rows:
                    if self._normalize_url(row[0]) == normalized_url:
                        return True
                return False

            if name and category:
                if local_jurisdiction_id is None:
                     query = f"SELECT 1 FROM agencies WHERE state_id = {self._param_placeholder} AND organization_name = {self._param_placeholder} AND category = {self._param_placeholder} AND local_jurisdiction_id IS NULL"
                     cursor.execute(query, (state_id, name, category))
                else:
                     query = f"SELECT 1 FROM agencies WHERE state_id = {self._param_placeholder} AND organization_name = {self._param_placeholder} AND category = {self._param_placeholder} AND local_jurisdiction_id = {self._param_placeholder}"
                     cursor.execute(query, (state_id, name, category, local_jurisdiction_id))
                return cursor.fetchone() is not None

            return False
        finally:
            conn.close()

    def add_agency(self, state_id: int, name: str, url: Optional[str] = None, verified: bool = False, category: str = 'state_agency', local_jurisdiction_id: Optional[int] = None):
        if url: url = url.strip()

        if self.agency_exists(state_id, url=url, name=name, category=category, local_jurisdiction_id=local_jurisdiction_id):
            return

        conn = self._get_connection()
        cursor = conn.cursor()
        created_at = datetime.datetime.now().isoformat()
        verified_int = 1 if verified else 0

        try:
            if self.is_postgres:
                 cursor.execute("""
                    INSERT INTO agencies (state_id, organization_name, url, verified, created_at, category, local_jurisdiction_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (state_id, organization_name) DO NOTHING
                """, (state_id, name, url, verified_int, created_at, category, local_jurisdiction_id))
            else:
                 cursor.execute("""
                    INSERT OR IGNORE INTO agencies (state_id, organization_name, url, verified, created_at, category, local_jurisdiction_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (state_id, name, url, verified_int, created_at, category, local_jurisdiction_id))
            conn.commit()
        except Exception as e:
            print(f"Error adding agency: {e}")
        finally:
            conn.close()

    def get_all_agencies(self) -> pd.DataFrame:
        conn = self._get_connection()
        try:
            # Join with states and local_jurisdictions to get labels
            query = """
                SELECT
                    a.*,
                    s.name as state_name,
                    lj.type as jurisdiction_type,
                    COALESCE(lj.name || ' (' || lj.type || ')', s.name) as jurisdiction_label
                FROM agencies a
                JOIN states s ON a.state_id = s.id
                LEFT JOIN local_jurisdictions lj ON a.local_jurisdiction_id = lj.id
                ORDER BY s.name, a.organization_name
            """
            df = pd.read_sql_query(query, conn)
        except Exception as e:
             print(f"CRITICAL DB ERROR in get_all_agencies: {e}")
             df = pd.DataFrame(columns=['id', 'state_id', 'organization_name', 'url', 'verified', 'created_at', 'category', 'local_jurisdiction_id', 'state_name', 'jurisdiction_label'])
        finally:
            conn.close()
        return df

    def get_agencies_by_state(self, state_id: int) -> pd.DataFrame:
        conn = self._get_connection()
        try:
            query = f"SELECT * FROM agencies WHERE state_id = {self._param_placeholder}"
            df = pd.read_sql_query(query, conn, params=(state_id,))
        except Exception:
            df = pd.DataFrame(columns=['id', 'state_id', 'organization_name', 'url', 'verified', 'created_at', 'category', 'local_jurisdiction_id'])
        finally:
            conn.close()
        return df

    def get_agency_by_jurisdiction(self, state_id: int, category: str, local_jurisdiction_id: Optional[int]) -> Optional[dict]:
        conn = self._get_connection()
        if not self.is_postgres:
            conn.row_factory = sqlite3.Row
        cursor = conn.cursor(cursor_factory=RealDictCursor) if self.is_postgres else conn.cursor()

        try:
            if local_jurisdiction_id is None:
                return None

            query = f"SELECT * FROM agencies WHERE state_id = {self._param_placeholder} AND category = {self._param_placeholder} AND local_jurisdiction_id = {self._param_placeholder}"
            cursor.execute(query, (state_id, category, local_jurisdiction_id))

            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
        finally:
            conn.close()

    def get_agency_by_name(self, state_id: int, name: str, category: Optional[str] = None) -> Optional[dict]:
        conn = self._get_connection()
        if not self.is_postgres:
            conn.row_factory = sqlite3.Row
        cursor = conn.cursor(cursor_factory=RealDictCursor) if self.is_postgres else conn.cursor()

        try:
            if category:
                query = f"SELECT * FROM agencies WHERE state_id = {self._param_placeholder} AND organization_name = {self._param_placeholder} AND category = {self._param_placeholder}"
                cursor.execute(query, (state_id, name, category))
            else:
                 query = f"SELECT * FROM agencies WHERE state_id = {self._param_placeholder} AND organization_name = {self._param_placeholder}"
                 cursor.execute(query, (state_id, name))

            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
        finally:
            conn.close()

    def update_agency_url(self, agency_id: int, new_url: str):
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            query = f"UPDATE agencies SET url = {self._param_placeholder}, verified = 1 WHERE id = {self._param_placeholder}"
            cursor.execute(query, (new_url, agency_id))
            conn.commit()
        finally:
            conn.close()

    def update_agency_name(self, agency_id: int, new_name: str):
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            query = f"UPDATE agencies SET organization_name = {self._param_placeholder} WHERE id = {self._param_placeholder}"
            cursor.execute(query, (new_name, agency_id))
            conn.commit()
        finally:
            conn.close()

    def delete_agency(self, agency_id: int):
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            query = f"DELETE FROM agencies WHERE id = {self._param_placeholder}"
            cursor.execute(query, (agency_id,))
            conn.commit()
        finally:
            conn.close()

    # --- Legacy Methods (Discovery Log) ---

    def add_discovered_url(self, url: str, state: str):
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            if self.is_postgres:
                cursor.execute("INSERT INTO discovery_log (url, state, status, last_attempted_at) VALUES (%s, %s, 'pending', NULL) ON CONFLICT (url) DO NOTHING", (url, state))
            else:
                cursor.execute("INSERT OR IGNORE INTO discovery_log (url, state, status, last_attempted_at) VALUES (?, ?, 'pending', NULL)", (url, state))
            conn.commit()
        finally:
            conn.close()

    def get_pending_urls(self, state: str) -> List[str]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            query = f"SELECT url FROM discovery_log WHERE state = {self._param_placeholder} AND status = 'pending'"
            cursor.execute(query, (state,))
            rows = cursor.fetchall()
            return [r[0] for r in rows]
        finally:
            conn.close()

    def mark_url_processed(self, url: str, status: str = 'processed'):
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.datetime.now().isoformat()
        try:
            query = f"UPDATE discovery_log SET status = {self._param_placeholder}, last_attempted_at = {self._param_placeholder} WHERE url = {self._param_placeholder}"
            cursor.execute(query, (status, now, url))
            conn.commit()
        finally:
            conn.close()

    # --- V2 Methods (Bids) ---

    def save_bid(self, bid: Bid, state: str):
        """Save a processed bid to the database (V2)."""
        conn = self._get_connection()
        cursor = conn.cursor()

        scraped_at = datetime.datetime.now().isoformat()
        csi_json = json.dumps(bid.csi_divisions) if bid.csi_divisions else None

        try:
            if self.is_postgres:
                cursor.execute("""
                    INSERT INTO bids (slug, client_name, title, deadline, description, link, full_text, csi_divisions, state, scraped_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (slug) DO UPDATE SET
                        deadline = EXCLUDED.deadline,
                        description = EXCLUDED.description,
                        full_text = EXCLUDED.full_text,
                        csi_divisions = EXCLUDED.csi_divisions,
                        scraped_at = EXCLUDED.scraped_at
                """, (bid.slug, bid.clientName, bid.title, bid.deadline, bid.description, bid.link, bid.full_text, csi_json, state, scraped_at))
            else:
                cursor.execute("""
                    INSERT INTO bids (slug, client_name, title, deadline, description, link, full_text, csi_divisions, state, scraped_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(slug) DO UPDATE SET
                        deadline=excluded.deadline,
                        description=excluded.description,
                        full_text=excluded.full_text,
                        csi_divisions=excluded.csi_divisions,
                        scraped_at=excluded.scraped_at
                """, (bid.slug, bid.clientName, bid.title, bid.deadline, bid.description, bid.link, bid.full_text, csi_json, state, scraped_at))
            conn.commit()
        except Exception as e:
            print(f"Error saving bid {bid.slug}: {e}")
        finally:
            conn.close()

    def insert_bid(self, slug: str, client_name: str, title: str, deadline: str, source_url: str, state: str = "Unknown", rfp_description: Optional[str] = None, matching_trades: Optional[str] = None):
        """
        Compatibility method for Legacy tasks (unused in UI but kept for integrity).
        Adapts legacy args to V2 Bid object and calls save_bid.
        """
        # Convert legacy matching_trades (string) to csi_divisions (List[str])
        csi = [matching_trades] if matching_trades else []

        bid = Bid(
            title=title,
            clientName=client_name,
            deadline=deadline,
            description=rfp_description or "",
            link=source_url,
            full_text="",
            csi_divisions=csi,
            slug=slug
        )
        self.save_bid(bid, state)

    def get_bids(self, state: Optional[str] = None) -> pd.DataFrame:
        """
        Retrieve bids from V2 table, mapping columns to Legacy expectations.
        - description -> rfp_description
        - link -> source_url
        - csi_divisions (JSON) -> matching_trades (CSV String)
        """
        conn = self._get_connection()
        try:
            if state:
                query = f"SELECT * FROM bids WHERE state = {self._param_placeholder}"
                df = pd.read_sql_query(query, conn, params=(state,))
            else:
                df = pd.read_sql_query("SELECT * FROM bids", conn)

            # Column Mapping
            rename_map = {
                'description': 'rfp_description',
                'link': 'source_url'
            }
            df = df.rename(columns=rename_map)

            # CSI Divisions Transformation
            if 'csi_divisions' in df.columns:
                def transform_csi(val):
                    if not val: return ""
                    try:
                        # Handle JSON string vs List
                        data = val if isinstance(val, list) else json.loads(val)
                        if isinstance(data, list):
                            return ", ".join([str(x) for x in data])
                        return str(data)
                    except:
                        return str(val)

                df['matching_trades'] = df['csi_divisions'].apply(transform_csi)
            else:
                df['matching_trades'] = ""

            # Ensure minimal required columns exist
            required = ['slug', 'client_name', 'title', 'deadline', 'scraped_at', 'source_url', 'state', 'rfp_description', 'matching_trades']
            for col in required:
                if col not in df.columns:
                    df[col] = None

            return df[required]

        except Exception:
            return pd.DataFrame(columns=['slug', 'client_name', 'title', 'deadline', 'scraped_at', 'source_url', 'state', 'rfp_description', 'matching_trades'])
        finally:
            conn.close()

    def update_agency_procurement_url(self, name: str, state: str, procurement_url: str):
        """Saves the AI-discovered procurement portal URL to the agency record."""
        query = """
            UPDATE agencies
            SET procurement_url = ?
            WHERE organization_name = ?
        """
        if self.is_postgres:
            query = query.replace("?", "%s")

        params = (procurement_url, name)
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query, params)
                conn.commit()
            except Exception as e:
                print(f"Database Error updating procurement URL for {name}: {e}")

    def url_already_scraped(self, url: str) -> bool:
        """Checks if a URL source (link in v2) has already been processed."""
        if not url: return False
        clean_url = self._normalize_url(url)

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # Check link column in bids
            query = f"SELECT 1 FROM bids WHERE link LIKE {self._param_placeholder}"
            cursor.execute(query, (f"%{clean_url}%",))
            return cursor.fetchone() is not None
        finally:
            conn.close()

    def bid_exists(self, slug: str) -> bool:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            query = f"SELECT 1 FROM bids WHERE slug = {self._param_placeholder}"
            cursor.execute(query, (slug,))
            return cursor.fetchone() is not None
        finally:
            conn.close()
