import os
import sys
from dotenv import load_dotenv


def load_environment():
    """Aggressively hunts for the .env file in multiple possible locations."""
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Possible locations for the .env file
    possible_paths = [
        os.path.join(current_dir, ".env"),  # Project Root
        os.path.join(current_dir, "rfp_scraper", ".env")  # Inside the rfp_scraper subfolder
    ]

    loaded = False
    for path in possible_paths:
        if os.path.exists(path):
            load_dotenv(dotenv_path=path)
            print(f"✅ Loaded environment variables from: {path}")
            loaded = True
            break

    if not loaded:
        print("❌ WARNING: No .env file found in expected locations!")


# 1. LOAD THE ENVIRONMENT FIRST (Before importing DatabaseHandler)
load_environment()

# 2. VERIFY THE ENVS
db_url = os.environ.get("DATABASE_URL")
api_key = os.environ.get("DEEPSEEK_API_KEY")

print("-" * 40)
print(f"DATABASE_URL Check: {'✅ Found' if db_url else '❌ MISSING'}")
print(f"DEEPSEEK_API_KEY Check: {'✅ Found' if api_key else '❌ MISSING'}")
print("-" * 40)

if not db_url:
    print("CRITICAL: Cannot proceed without DATABASE_URL.")
    sys.exit(1)

# 3. NOW IMPORT AND RUN THE DB LOGIC
try:
    from rfp_scraper_v2.core.database import DatabaseHandler

    print("Connecting to Neon PostgreSQL...")
    db = DatabaseHandler()
    conn = db._get_connection()
    cursor = conn.cursor()

    # Reset the graveyard
    print("Executing reset query...")
    cursor.execute("UPDATE agencies SET procurement_url = NULL WHERE procurement_url = 'NOT_FOUND'")

    # Get the number of rows that were actually changed
    rows_updated = cursor.rowcount

    conn.commit()
    cursor.close()
    conn.close()

    print(f"🎉 Success! Reset {rows_updated} agencies from the 'NO PORTAL' graveyard.")

except Exception as e:
    print(f"❌ Error resetting portals: {e}")