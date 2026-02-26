import os
import datetime
import json
import pandas as pd
import urllib.parse
import hashlib
import time
from typing import Optional, List, Dict, Any, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor
import asyncpg
from .models import Bid

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
        Strictly requires DATABASE_URL from environment or passed explicitly.
        """
        self.db_url = db_url or os.getenv("DATABASE_URL")
        if not self.db_url:
             raise ValueError("CRITICAL: DATABASE_URL environment variable is required. SQLite is not supported.")

        self.async_pool = None
        self._init_postgres()

    def _get_connection(self):
        """Returns a synchronous psycopg2 connection."""
        return psycopg2.connect(self.db_url)

    def _init_postgres(self):
        """Ensure tables exist using synchronous connection."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
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
                    procurement_url TEXT,
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
        finally:
            conn.close()

    # --- Async Methods (Using asyncpg) ---

    async def connect_async(self):
        """Initializes the asyncpg connection pool."""
        if not self.async_pool:
            print("Initializing asyncpg pool...")
            self.async_pool = await asyncpg.create_pool(self.db_url)

    async def close_async(self):
        """Closes the asyncpg connection pool."""
        if self.async_pool:
            print("Closing asyncpg pool...")
            await self.async_pool.close()
            self.async_pool = None

    async def async_save_bid(self, bid: Bid, state: str):
        """Async version of save_bid."""
        if not self.async_pool:
            await self.connect_async()

        scraped_at = datetime.datetime.now().isoformat()
        csi_json = json.dumps(bid.csi_divisions) if bid.csi_divisions else None

        # asyncpg uses $1, $2 placeholders
        query = """
            INSERT INTO bids (slug, client_name, title, deadline, description, link, full_text, csi_divisions, state, scraped_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (slug) DO UPDATE SET
                deadline = EXCLUDED.deadline,
                description = EXCLUDED.description,
                full_text = EXCLUDED.full_text,
                csi_divisions = EXCLUDED.csi_divisions,
                scraped_at = EXCLUDED.scraped_at
        """

        deadline_val = bid.deadline
        # Validate deadline format for Postgres DATE
        if deadline_val:
            try:
                # Basic check YYYY-MM-DD
                datetime.datetime.strptime(deadline_val, "%Y-%m-%d")
            except ValueError:
                deadline_val = None

        try:
             async with self.async_pool.acquire() as conn:
                await conn.execute(query, bid.slug, bid.clientName, bid.title, deadline_val, bid.description, bid.link, bid.full_text, csi_json, state, scraped_at)
        except Exception as e:
            print(f"Error saving bid {bid.slug} (Async): {e}")

    async def async_url_already_scraped(self, url: str) -> bool:
        if not url: return False
        clean_url = self._normalize_url(url)

        if not self.async_pool:
            await self.connect_async()

        query = "SELECT 1 FROM bids WHERE link LIKE $1"
        try:
            async with self.async_pool.acquire() as conn:
                row = await conn.fetchrow(query, f"%{clean_url}%")
                return row is not None
        except Exception as e:
            print(f"Error checking url {url} (Async): {e}")
            return False

    async def async_update_agency_procurement_url(self, name: str, state: str, procurement_url: str):
        if not self.async_pool:
            await self.connect_async()

        query = """
            UPDATE agencies
            SET procurement_url = $1
            WHERE organization_name = $2
        """
        try:
            async with self.async_pool.acquire() as conn:
                await conn.execute(query, procurement_url, name)
        except Exception as e:
            print(f"Error updating procurement url for {name} (Async): {e}")

    # --- Sync Methods (psycopg2) ---

    def _get_state_id(self, state_abbr_or_name: str) -> Optional[int]:
        """Resolves state ID from abbreviation or full name."""
        if not state_abbr_or_name:
            return None

        full_name = state_abbr_or_name
        if len(state_abbr_or_name) == 2:
             full_name = ABBR_TO_STATE.get(state_abbr_or_name.upper(), state_abbr_or_name)

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            query = "SELECT id FROM states WHERE name = %s"
            cursor.execute(query, (full_name,))
            row = cursor.fetchone()
            if row:
                return row[0]

            # Auto-create known states
            if full_name in ABBR_TO_STATE.values():
                self.add_state(full_name)
                cursor.execute(query, (full_name,))
                row = cursor.fetchone()
                if row:
                    return row[0]
            return None
        finally:
            conn.close()

    @staticmethod
    def _normalize_url(url: str) -> str:
        if not url: return ""
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
        raw_string = f"{str(title).lower()}|{str(client_name).lower()}|{str(source_url).lower()}"
        return hashlib.md5(raw_string.encode('utf-8')).hexdigest()

    def add_state(self, name: str):
        conn = self._get_connection()
        cursor = conn.cursor()
        created_at = datetime.datetime.now().isoformat()
        try:
            cursor.execute("INSERT INTO states (name, created_at) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING", (name, created_at))
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

    def append_local_jurisdiction(self, state_id: int, name: str, jurisdiction_type: str) -> int:
        conn = self._get_connection()
        cursor = conn.cursor()
        created_at = datetime.datetime.now().isoformat()

        # Check existence
        query = "SELECT id FROM local_jurisdictions WHERE state_id = %s AND name = %s AND type = %s"
        cursor.execute(query, (state_id, name, jurisdiction_type))
        row = cursor.fetchone()
        if row:
            conn.close()
            return row[0]

        try:
            cursor.execute("INSERT INTO local_jurisdictions (state_id, name, type, created_at) VALUES (%s, %s, %s, %s) RETURNING id", (state_id, name, jurisdiction_type, created_at))
            new_id = cursor.fetchone()[0]
            conn.commit()
            return new_id
        finally:
            conn.close()

    def get_local_jurisdictions(self, state_id: Optional[int] = None) -> pd.DataFrame:
        conn = self._get_connection()
        try:
            if state_id:
                query = "SELECT * FROM local_jurisdictions WHERE state_id = %s"
                df = pd.read_sql_query(query, conn, params=(state_id,))
            else:
                df = pd.read_sql_query("SELECT * FROM local_jurisdictions", conn)
        except Exception:
            df = pd.DataFrame(columns=['id', 'state_id', 'name', 'type', 'created_at'])
        finally:
            conn.close()
        return df

    def agency_exists(self, state_id: int, url: Optional[str] = None, name: Optional[str] = None, category: Optional[str] = None, local_jurisdiction_id: Optional[int] = None) -> bool:
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            if url:
                normalized_url = self._normalize_url(url)
                query = "SELECT url FROM agencies WHERE state_id = %s"
                cursor.execute(query, (state_id,))
                rows = cursor.fetchall()
                for row in rows:
                    if self._normalize_url(row[0]) == normalized_url:
                        return True
                return False

            if name and category:
                if local_jurisdiction_id is None:
                     query = "SELECT 1 FROM agencies WHERE state_id = %s AND organization_name = %s AND category = %s AND local_jurisdiction_id IS NULL"
                     cursor.execute(query, (state_id, name, category))
                else:
                     query = "SELECT 1 FROM agencies WHERE state_id = %s AND organization_name = %s AND category = %s AND local_jurisdiction_id = %s"
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
            cursor.execute("""
                INSERT INTO agencies (state_id, organization_name, url, verified, created_at, category, local_jurisdiction_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (state_id, organization_name) DO NOTHING
            """, (state_id, name, url, verified_int, created_at, category, local_jurisdiction_id))
            conn.commit()
        except Exception as e:
            print(f"Error adding agency: {e}")
        finally:
            conn.close()

    def get_all_agencies(self) -> pd.DataFrame:
        conn = self._get_connection()
        try:
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
            query = "SELECT * FROM agencies WHERE state_id = %s"
            df = pd.read_sql_query(query, conn, params=(state_id,))
        except Exception:
            df = pd.DataFrame(columns=['id', 'state_id', 'organization_name', 'url', 'verified', 'created_at', 'category', 'local_jurisdiction_id'])
        finally:
            conn.close()
        return df

    def get_agency_by_jurisdiction(self, state_id: int, category: str, local_jurisdiction_id: Optional[int]) -> Optional[dict]:
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        try:
            if local_jurisdiction_id is None:
                return None

            query = "SELECT * FROM agencies WHERE state_id = %s AND category = %s AND local_jurisdiction_id = %s"
            cursor.execute(query, (state_id, category, local_jurisdiction_id))

            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
        finally:
            conn.close()

    def get_agency_by_name(self, state_id: int, name: str, category: Optional[str] = None) -> Optional[dict]:
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        try:
            if category:
                query = "SELECT * FROM agencies WHERE state_id = %s AND organization_name = %s AND category = %s"
                cursor.execute(query, (state_id, name, category))
            else:
                 query = "SELECT * FROM agencies WHERE state_id = %s AND organization_name = %s"
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
            query = "UPDATE agencies SET url = %s, verified = 1 WHERE id = %s"
            cursor.execute(query, (new_url, agency_id))
            conn.commit()
        finally:
            conn.close()

    def update_agency_name(self, agency_id: int, new_name: str):
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            query = "UPDATE agencies SET organization_name = %s WHERE id = %s"
            cursor.execute(query, (new_name, agency_id))
            conn.commit()
        finally:
            conn.close()

    def delete_agency(self, agency_id: int):
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            query = "DELETE FROM agencies WHERE id = %s"
            cursor.execute(query, (agency_id,))
            conn.commit()
        finally:
            conn.close()

    def add_discovered_url(self, url: str, state: str):
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("INSERT INTO discovery_log (url, state, status, last_attempted_at) VALUES (%s, %s, 'pending', NULL) ON CONFLICT (url) DO NOTHING", (url, state))
            conn.commit()
        finally:
            conn.close()

    def get_pending_urls(self, state: str) -> List[str]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            query = "SELECT url FROM discovery_log WHERE state = %s AND status = 'pending'"
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
            query = "UPDATE discovery_log SET status = %s, last_attempted_at = %s WHERE url = %s"
            cursor.execute(query, (status, now, url))
            conn.commit()
        finally:
            conn.close()

    def save_bid(self, bid: Bid, state: str):
        """Sync save_bid for compatibility (though largely unused in v2 pipeline)."""
        conn = self._get_connection()
        cursor = conn.cursor()

        scraped_at = datetime.datetime.now().isoformat()
        csi_json = json.dumps(bid.csi_divisions) if bid.csi_divisions else None

        # Determine deadline logic for sync
        deadline_val = bid.deadline
        if deadline_val:
            try:
                datetime.datetime.strptime(deadline_val, "%Y-%m-%d")
            except ValueError:
                deadline_val = None

        try:
            cursor.execute("""
                INSERT INTO bids (slug, client_name, title, deadline, description, link, full_text, csi_divisions, state, scraped_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (slug) DO UPDATE SET
                    deadline = EXCLUDED.deadline,
                    description = EXCLUDED.description,
                    full_text = EXCLUDED.full_text,
                    csi_divisions = EXCLUDED.csi_divisions,
                    scraped_at = EXCLUDED.scraped_at
            """, (bid.slug, bid.clientName, bid.title, deadline_val, bid.description, bid.link, bid.full_text, csi_json, state, scraped_at))
            conn.commit()
        except Exception as e:
            print(f"Error saving bid {bid.slug}: {e}")
        finally:
            conn.close()

    def get_bids(self, state: Optional[str] = None) -> pd.DataFrame:
        conn = self._get_connection()
        try:
            if state:
                query = "SELECT * FROM bids WHERE state = %s"
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
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
             query = """
                UPDATE agencies
                SET procurement_url = %s
                WHERE organization_name = %s
            """
             cursor.execute(query, (procurement_url, name))
             conn.commit()
        finally:
            conn.close()

    def url_already_scraped(self, url: str) -> bool:
        if not url: return False
        clean_url = self._normalize_url(url)
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            query = "SELECT 1 FROM bids WHERE link LIKE %s"
            cursor.execute(query, (f"%{clean_url}%",))
            return cursor.fetchone() is not None
        finally:
            conn.close()

    def bid_exists(self, slug: str) -> bool:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            query = "SELECT 1 FROM bids WHERE slug = %s"
            cursor.execute(query, (slug,))
            return cursor.fetchone() is not None
        finally:
            conn.close()

    def insert_bid(self, slug: str, client_name: str, title: str, deadline: str, source_url: str, state: str = "Unknown", rfp_description: Optional[str] = None, matching_trades: Optional[str] = None):
        """Compatibility wrapper."""
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
