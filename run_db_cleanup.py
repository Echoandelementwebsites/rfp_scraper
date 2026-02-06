import os
import sqlite3
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 1. Setup Path to find your app modules
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# Load environment variables (for API Key)
load_dotenv()

from rfp_scraper import utils
from rfp_scraper.ai_parser import DeepSeekClient


def find_databases(start_dir):
    """Walks the directory to find all .db files."""
    db_files = []
    print(f"🔎 Scanning for database files in: {start_dir}...")

    for root, dirs, files in os.walk(start_dir):
        for file in files:
            if file.endswith(".db"):
                full_path = os.path.join(root, file)
                db_files.append(full_path)
    return db_files


def fix_database_schema(db_path):
    """Ensures the matching_trades column exists."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("PRAGMA table_info(scraped_bids)")
        columns = [row[1] for row in cursor.fetchall()]

        if 'matching_trades' not in columns:
            print(f"🛠️  Fixing schema: Adding 'matching_trades' column to {os.path.basename(db_path)}...")
            cursor.execute("ALTER TABLE scraped_bids ADD COLUMN matching_trades TEXT")
            conn.commit()
            print("✅ Schema updated successfully.")

    except Exception as e:
        print(f"⚠️  Schema check failed: {e}")
    finally:
        conn.close()


def check_database_content(db_path):
    """Checks if a DB file has data."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check if table exists (your db.py uses 'scraped_bids')
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='scraped_bids'")
        if not cursor.fetchone():
            conn.close()
            return 0, False

            # Check row count
        cursor.execute("SELECT count(*) FROM scraped_bids")
        count = cursor.fetchone()[0]
        conn.close()
        return count, True
    except Exception:
        return 0, False


def is_future_deadline(date_str, buffer_days=2):
    """
    Checks if the deadline is at least buffer_days in the future.
    """
    if not date_str:
        return False  # Missing deadline = Reject

    try:
        # Normalize first via utils if possible
        norm_date = utils.normalize_date(date_str)
        if not norm_date:
            return False

        dt = datetime.strptime(norm_date, "%Y-%m-%d").date()
        cutoff = datetime.now().date() + timedelta(days=buffer_days)

        return dt >= cutoff
    except:
        return False


def clean_specific_database(db_path):
    """Runs the full cleanup pipeline on the verified database."""

    # --- SAFETY CHECK: API KEY ---
    ai_client = DeepSeekClient()
    if not ai_client.api_key:
        print("\n⛔ CRITICAL ERROR: NO DEEPSEEK API KEY FOUND")
        print("Aborting to prevent data loss (AI would return empty tags).")
        print("Please set DEEPSEEK_API_KEY in your .env file.")
        return

    # 1. FIX SCHEMA
    fix_database_schema(db_path)

    print(f"\n🚀 STARTING CLEANUP ON: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    write_cursor = conn.cursor()  # Separate cursor for writes

    try:
        # Fetch records
        cursor.execute(
            "SELECT slug, title, rfp_description, client_name, matching_trades, state, deadline FROM scraped_bids")
        # Use fetchall to get list so we don't hold a read lock while writing
        rows = cursor.fetchall()

        print(f"📊 Analyzing {len(rows)} records...")

        deleted_count = 0
        updated_count = 0
        skipped_count = 0

        slugs_to_delete = []
        updates_to_perform = []

        for row in rows:
            slug = row['slug']
            title = row['title'] or ""
            desc = row['rfp_description'] or ""
            client = row['client_name'] or ""
            state = row['state'] or ""
            deadline = row['deadline']

            clean_title = utils.clean_text(title)

            # --- STAGE 1: PROTOCOL CHECKS (Fast) ---

            # Filter A: Attribution Check (State)
            if not state or state.lower() in ["unknown", "none", ""]:
                print(f"❌ Deleting [No State]: {clean_title}")
                slugs_to_delete.append(slug)
                continue

            # Filter B: Freshness Check (N+2 Days)
            if not is_future_deadline(deadline, buffer_days=2):
                print(f"❌ Deleting [Expired]: {clean_title} (Due: {deadline})")
                slugs_to_delete.append(slug)
                continue

            # Filter C: Noise Check (Content)
            if not utils.is_valid_rfp(clean_title, desc, client):
                print(f"❌ Deleting [Noise]: {clean_title}")
                slugs_to_delete.append(slug)
                continue

            # --- ECONOMY CHECK: Skip if already tagged ---
            current_trades = row['matching_trades']
            trades_str_val = str(current_trades).strip().lower()

            if (current_trades and
                    len(str(current_trades)) > 3 and
                    trades_str_val not in ['none', 'null', '[]']):
                print(f"⏩ Skipped [Already Tagged]: {clean_title}")
                skipped_count += 1
                continue

            # --- STAGE 3: AI CLASSIFICATION (Only for Untagged) ---

            print(f"🤖 AI Classifying: {clean_title}...")
            trades = ai_client.classify_csi_divisions(clean_title, desc)

            if not trades:
                print(f"❌ Deleting [Non-Construction]: {clean_title}")
                slugs_to_delete.append(slug)
                continue

            # --- SAVE ---
            trade_str = ", ".join(trades)
            print(f"✅ Tagging: {clean_title} -> [{trade_str}]")
            updates_to_perform.append((trade_str, slug))
            updated_count += 1

        # --- BATCH EXECUTE ---
        if slugs_to_delete:
            print(f"\n🗑️  Committing {len(slugs_to_delete)} deletions...")
            # SQLite limit is usually 999 variables, so we batch
            batch_size = 500
            for i in range(0, len(slugs_to_delete), batch_size):
                batch = slugs_to_delete[i:i + batch_size]
                placeholders = ','.join(['?'] * len(batch))
                write_cursor.execute(f"DELETE FROM scraped_bids WHERE slug IN ({placeholders})", batch)
                deleted_count += len(batch)

        if updates_to_perform:
            print(f"📝 Committing {len(updates_to_perform)} updates...")
            write_cursor.executemany(
                "UPDATE scraped_bids SET matching_trades = ? WHERE slug = ?",
                updates_to_perform
            )

        conn.commit()
        print(f"\n✨ Cleanup Complete for {os.path.basename(db_path)}!")
        print(f"🗑️  Deleted: {deleted_count} (Noise/Expired/Stateless)")
        print(f"⏩ Skipped: {skipped_count} (Already Tagged)")
        print(f"🏷️  Tagged:  {updated_count} (New/Updated)")

    except Exception as e:
        print(f"Error during cleanup: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    dbs = find_databases(current_dir)

    if not dbs:
        print("❌ No .db files found in this directory.")
        exit()

    valid_dbs = []
    print("\nFound the following databases:")
    for i, db in enumerate(dbs):
        count, valid = check_database_content(db)
        status = f"✅ Contains {count} bids" if valid and count > 0 else "⚠️ Empty or Invalid"
        print(f"[{i + 1}] {db} ({status})")
        if valid and count > 0:
            valid_dbs.append(db)

    if not valid_dbs:
        print("\n❌ No databases found with actual data in 'scraped_bids'.")
        print("Tip: Your scraper might be saving to a different folder, or hasn't saved anything yet.")
    elif len(valid_dbs) == 1:
        # Auto-select the only good one
        clean_specific_database(valid_dbs[0])
    else:
        # Ask user if multiple have data
        selection = input("\nEnter the number of the database to clean: ")
        try:
            idx = int(selection) - 1
            clean_specific_database(dbs[idx])
        except (ValueError, IndexError):
            print("Invalid selection.")