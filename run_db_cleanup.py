import sys
import os
import sqlite3

# 1. Setup Path to find your app modules
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# 2. Import your existing DatabaseHandler and utilities
from rfp_scraper.db import DatabaseHandler
from rfp_scraper import utils, ai_parser


def clean_database():
    print("🚀 Starting Database Cleanup & Remediation (SQLite Mode)...")

    # Initialize your existing handler
    db_handler = DatabaseHandler()
    db_path = db_handler.db_path

    print(f"📂 Connecting to database at: {db_path}")

    # Connect directly using sqlite3
    try:
        conn = sqlite3.connect(db_path)
        # Use Row factory to access columns by name
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
    except Exception as e:
        print(f"❌ Could not connect to database: {e}")
        return

    try:
        # Fetch existing bids
        # Note: We use the table name 'scraped_bids' found in your db.py (not 'bids')
        print("📊 Fetching existing records from 'scraped_bids'...")
        cursor.execute("SELECT slug, title, rfp_description, client_name, matching_trades, deadline, state FROM scraped_bids")
        rows = cursor.fetchall()

        print(f"🔎 Analyzing {len(rows)} records for noise and hallucinations...")

        deleted_count = 0
        updated_count = 0

        for row in rows:
            slug = row['slug']
            title = row['title'] or ""
            desc = row['rfp_description'] or ""
            client = row['client_name'] or ""
            deadline = row['deadline']
            state = row['state']

            # --- STAGE 1: Protocol Checks (Fastest) ---
            # 1. State Check
            if not state or state == "Unknown":
                print(f"❌ Deleting [Invalid State]: {title} (State: {state})")
                cursor.execute("DELETE FROM scraped_bids WHERE slug = ?", (slug,))
                deleted_count += 1
                continue

            # 2. Deadline Check
            if not utils.is_future_deadline(deadline, buffer_days=2):
                print(f"❌ Deleting [Expired]: {title} (Deadline: {deadline})")
                cursor.execute("DELETE FROM scraped_bids WHERE slug = ?", (slug,))
                deleted_count += 1
                continue

            # --- STAGE 2: Hard Filtering (Fast) ---
            clean_title = utils.clean_text(title)

            # Check validity using your new utils logic
            if not utils.is_valid_rfp(clean_title, desc, client):
                print(f"❌ Deleting [Noise]: {clean_title}")
                cursor.execute("DELETE FROM scraped_bids WHERE slug = ?", (slug,))
                deleted_count += 1
                continue

            # --- STAGE 3: AI Classification (Deep) ---
            # Classify using the new CSI logic
            trades = ai_parser.classify_csi_divisions(clean_title, desc)

            if not trades:
                print(f"❌ Deleting [Non-Construction]: {clean_title}")
                cursor.execute("DELETE FROM scraped_bids WHERE slug = ?", (slug,))
                deleted_count += 1
                continue

            # --- SAVE: Update Valid Rows ---
            trade_str = ", ".join(trades)
            print(f"✅ Keeping & Tagging: {clean_title} -> [{trade_str}]")

            cursor.execute(
                "UPDATE scraped_bids SET matching_trades = ? WHERE slug = ?",
                (trade_str, slug)
            )
            updated_count += 1

            # Commit periodically
            if (deleted_count + updated_count) % 10 == 0:
                conn.commit()

        conn.commit()
        print(f"\n✨ Cleanup Complete!")
        print(f"🗑️  Deleted: {deleted_count} junk records")
        print(f"🏷️  Tagged: {updated_count} valid records")

    except Exception as e:
        print(f"Error during cleanup: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    clean_database()