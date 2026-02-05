import sys
import os
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

# 1. Setup Path: Allow importing from the 'rfp_scraper' package
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# 2. Imports from your app
# NOTE: Ensure rfp_scraper/db.py has a get_db_engine() or equivalent.
# If not, import your specific engine/session creator here.
from rfp_scraper.db import get_db_engine
from rfp_scraper import utils, ai_parser


def clean_database():
    print("🚀 Starting Database Cleanup & Remediation...")

    # Connect to DB
    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Fetch all bids
        print("📊 Fetching existing records...")
        result = session.execute(text("SELECT id, title, rfp_description, client_name, deadline FROM bids"))
        rows = result.fetchall()

        print(f"🔎 Analyzing {len(rows)} records for noise and hallucinations...")

        deleted_count = 0
        updated_count = 0

        for row in rows:
            rfp_id = row.id
            title = row.title or ""
            desc = row.rfp_description or ""
            client = row.client_name or ""

            # --- STAGE 2: Hard Filtering (Fast) ---
            clean_title = utils.clean_text(title)

            # Check validity using your new utils logic
            if not utils.is_valid_rfp(clean_title, desc, client):
                print(f"❌ Deleting [Noise]: {clean_title}")
                session.execute(text("DELETE FROM bids WHERE id = :id"), {"id": rfp_id})
                deleted_count += 1
                continue

            # --- STAGE 3: AI Classification (Deep) ---
            # Classify using the new CSI logic
            trades = ai_parser.classify_csi_divisions(clean_title, desc)

            if not trades:
                print(f"❌ Deleting [Non-Construction]: {clean_title}")
                session.execute(text("DELETE FROM bids WHERE id = :id"), {"id": rfp_id})
                deleted_count += 1
                continue

            # --- SAVE: Update Valid Rows ---
            trade_str = ", ".join(trades)
            print(f"✅ Keeping & Tagging: {clean_title} -> [{trade_str}]")

            session.execute(
                text("UPDATE bids SET matching_trades = :trades WHERE id = :id"),
                {"trades": trade_str, "id": rfp_id}
            )
            updated_count += 1

            # Commit periodically
            if (deleted_count + updated_count) % 10 == 0:
                session.commit()

        session.commit()
        print(f"\n✨ Cleanup Complete!")
        print(f"🗑️  Deleted: {deleted_count} junk records")
        print(f"🏷️  Tagged: {updated_count} valid records")

    except Exception as e:
        print(f"Error during cleanup: {e}")
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
    clean_database()