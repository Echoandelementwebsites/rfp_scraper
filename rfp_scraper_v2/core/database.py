import os
import sqlite3
import datetime
import json
from typing import Optional, List, Dict, Any
from .models import Bid

# Try importing Postgres drivers (psycopg2 or asyncpg)
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PG = True
except ImportError:
    HAS_PG = False

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

    def _get_connection(self):
        if self.is_postgres:
            return psycopg2.connect(self.db_url)
        else:
            return sqlite3.connect(self.db_path, check_same_thread=False)

    def _init_sqlite(self):
        conn = self._get_connection()
        cursor = conn.cursor()

        # Bids Table
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

        # Agencies Table (Optional, for tracking discovery)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agencies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                state TEXT,
                type TEXT,
                homepage_url TEXT,
                procurement_url TEXT,
                last_checked TEXT,
                UNIQUE(name, state)
            )
        """)
        conn.commit()
        conn.close()

    def _init_postgres(self):
        conn = self._get_connection()
        cursor = conn.cursor()

        # Bids Table
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

        # Agencies Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agencies (
                id SERIAL PRIMARY KEY,
                name TEXT,
                state TEXT,
                type TEXT,
                homepage_url TEXT,
                procurement_url TEXT,
                last_checked TIMESTAMP,
                UNIQUE(name, state)
            )
        """)
        conn.commit()
        conn.close()

    def save_bid(self, bid: Bid, state: str):
        """Save a processed bid to the database."""
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
                """, (bid.slug, bid.client_name, bid.title, bid.deadline, bid.description, bid.link, bid.full_text, csi_json, state, scraped_at))
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
                """, (bid.slug, bid.client_name, bid.title, bid.deadline, bid.description, bid.link, bid.full_text, csi_json, state, scraped_at))
            conn.commit()
        except Exception as e:
            print(f"Error saving bid {bid.slug}: {e}")
        finally:
            conn.close()

    def update_agency_procurement_url(self, name: str, state: str, url: str):
        """Update the discovered procurement URL for an agency."""
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.datetime.now().isoformat()

        try:
            if self.is_postgres:
                cursor.execute("""
                    INSERT INTO agencies (name, state, procurement_url, last_checked)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (name, state) DO UPDATE SET
                        procurement_url = EXCLUDED.procurement_url,
                        last_checked = EXCLUDED.last_checked
                """, (name, state, url, now))
            else:
                cursor.execute("""
                    INSERT INTO agencies (name, state, procurement_url, last_checked)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(name, state) DO UPDATE SET
                        procurement_url=excluded.procurement_url,
                        last_checked=excluded.last_checked
                """, (name, state, url, now))
            conn.commit()
        except Exception as e:
            print(f"Error updating agency {name}: {e}")
        finally:
            conn.close()
