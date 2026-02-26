import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from rfp_scraper_v2.core.database import DatabaseHandler

def reset_portals():
    try:
        # Initialize DatabaseHandler
        db = DatabaseHandler()

        # Establish connection
        conn = db._get_connection()
        cursor = conn.cursor()

        # Execute reset query
        query = "UPDATE agencies SET procurement_url = NULL WHERE procurement_url = 'NOT_FOUND'"
        cursor.execute(query)
        rows_updated = cursor.rowcount

        # Commit and close
        conn.commit()
        conn.close()

        print(f"Successfully reset {rows_updated} agencies flagged as 'NOT_FOUND'.")

    except Exception as e:
        print(f"Error resetting portals: {e}")

if __name__ == "__main__":
    reset_portals()
