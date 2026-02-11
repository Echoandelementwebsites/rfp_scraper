import pandas as pd
import requests
import io
import re
from typing import Optional, Dict, Tuple
from rfp_scraper.db import DatabaseHandler

class CisaManager:
    CISA_CSV_URL = "https://raw.githubusercontent.com/cisagov/dotgov-data/main/current-full.csv"

    def __init__(self):
        self._df: Optional[pd.DataFrame] = None

    def _load_data(self):
        """Downloads and caches the CISA registry CSV."""
        if self._df is not None:
            return

        try:
            print("Downloading CISA Registry...")
            response = requests.get(self.CISA_CSV_URL, timeout=10)
            response.raise_for_status()

            # Read CSV
            df = pd.read_csv(io.StringIO(response.text))

            # Normalize columns
            # Standardize State to uppercase
            if 'State' in df.columns:
                df['State'] = df['State'].str.upper().str.strip()

            # Fill NaNs with empty strings for easier processing
            df = df.fillna("")

            self._df = df
            print(f"Loaded {len(df)} records from CISA Registry.")

        except Exception as e:
            print(f"Error loading CISA data: {e}")
            # Initialize empty DF to avoid crashes
            self._df = pd.DataFrame(columns=[
                "Domain name", "Domain type", "Organization name",
                "Suborganization name", "City", "State"
            ])

    def get_agency_url(self, name: str, state_abbr: str) -> Optional[str]:
        """
        Look up agency by name in 'Organization name' or 'City' columns.
        Returns the verified .gov URL if found.
        """
        self._load_data()

        if self._df is None or self._df.empty:
            return None

        state_abbr = state_abbr.upper().strip()
        name_lower = name.lower().strip()

        # Filter by State
        state_df = self._df[self._df['State'] == state_abbr]

        if state_df.empty:
            return None

        # Search logic: Check Organization, Suborganization, City
        # We look for exact matches or "contains" matches if appropriate
        # Prioritize exact matches

        # 1. Exact match on Organization Name
        match = state_df[state_df['Organization name'].str.lower().str.strip() == name_lower]
        if not match.empty:
            return f"https://{match.iloc[0]['Domain name']}"

        # 2. Exact match on City (for local agencies)
        match = state_df[state_df['City'].str.lower().str.strip() == name_lower]
        if not match.empty:
            # If multiple domains for a city, prefer "City of" or basic domain
            # Heuristic: Shortest domain usually best for main city page?
            # Or just take the first one.
            return f"https://{match.iloc[0]['Domain name']}"

        # 3. "City of [Name]" check
        city_of_name = f"city of {name_lower}"
        match = state_df[state_df['Organization name'].str.lower().str.strip() == city_of_name]
        if not match.empty:
            return f"https://{match.iloc[0]['Domain name']}"

        # 4. Fallback: Check if name is in Organization name (partial)
        # Be careful not to match "Milford Housing" when looking for "Milford"
        # So maybe avoid this for now to prevent false positives.

        return None

    def sync_state_database(self, db: DatabaseHandler, state_id: int, state_abbr: str) -> Dict[str, int]:
        """
        Iterate through all CISA entries for that state.
        Insert missing agencies (set verified=1, category='local'/'state').
        Update existing agencies if the CISA URL is different.
        """
        self._load_data()

        stats = {'added': 0, 'updated': 0}

        if self._df is None or self._df.empty:
            return stats

        state_abbr = state_abbr.upper().strip()

        # Filter for state
        state_df = self._df[self._df['State'] == state_abbr]

        if state_df.empty:
            print(f"No CISA records found for {state_abbr}")
            return stats

        # Pre-fetch local jurisdictions for linking
        # Create a lookup: {(name_lower, type): id}
        ljs = db.get_local_jurisdictions(state_id=state_id)
        lj_lookup = {}
        if not ljs.empty:
            for _, row in ljs.iterrows():
                key = (str(row['name']).lower().strip(), str(row['type']).lower().strip())
                lj_lookup[key] = row['id']
                # Also lookup just by name if it's unique?
                # Let's stick to name+type if possible, but CISA only gives us City name.
                # So maybe a secondary lookup {name_lower: id} (first match)

        lj_name_lookup = {}
        if not ljs.empty:
            for _, row in ljs.iterrows():
                n = str(row['name']).lower().strip()
                if n not in lj_name_lookup:
                    lj_name_lookup[n] = row['id']

        # Iterate CISA records
        for _, row in state_df.iterrows():
            domain = row['Domain name'].strip().lower()
            if not domain:
                continue

            url = f"https://{domain}"
            org_name = row['Organization name'].strip()
            if not org_name:
                org_name = row['Suborganization name'].strip()
            if not org_name:
                org_name = domain # Fallback

            city_name = str(row['City']).strip()
            domain_type = str(row['Domain type']).strip().lower()

            # Determine Category
            category = 'state_agency' # Default
            jurisdiction_type = None # 'city', 'county', etc.

            if 'state' in domain_type:
                category = 'state_agency'
            elif any(x in domain_type for x in ['local', 'city', 'county']):
                # Refine local category
                if 'county' in domain_type or 'county' in org_name.lower():
                    category = 'county'
                    jurisdiction_type = 'county'
                elif 'city' in domain_type or 'city' in org_name.lower():
                    category = 'city'
                    jurisdiction_type = 'city'
                else:
                    category = 'local' # Generic local

            # Special case handling for "Town of"
            if 'town of' in org_name.lower():
                category = 'town'
                jurisdiction_type = 'town'

            # Attempt Linking
            local_jurisdiction_id = None
            if jurisdiction_type and city_name:
                # Try to match City name + Type
                key = (city_name.lower(), jurisdiction_type)
                if key in lj_lookup:
                    local_jurisdiction_id = lj_lookup[key]
                # Fallback: Try just name match (e.g. CISA says City="Milford", DB has Town="Milford")
                elif city_name.lower() in lj_name_lookup:
                    local_jurisdiction_id = lj_name_lookup[city_name.lower()]

            # Check if agency exists
            # We check by:
            # 1. URL (exact match) -> already good, maybe update name?
            # 2. Jurisdiction ID + Category (for locals) -> Update URL
            # 3. Name + Category (for states/others) -> Update URL

            # Step 1: Check by URL (Normalization handled in db.agency_exists)
            # If exists by URL, we assume it's the same agency.
            # We might want to update the name if CISA name is "better", but prompt focuses on URL.
            # "Update existing agencies if the CISA URL is different".

            # So, we first try to find the agency record *without* using the URL, to see if we need to update it.

            existing_agency = db.get_agency_by_jurisdiction(state_id, category, local_jurisdiction_id)

            if existing_agency:
                # Found by Jurisdiction Link
                current_url = existing_agency['url']
                agency_id = existing_agency['id']

                # Compare URLs (ignoring scheme/www)
                if not self._urls_match(current_url, url):
                    # Update!
                    print(f"Updating URL for {org_name}: {current_url} -> {url}")
                    db.update_agency_url(agency_id, url)
                    stats['updated'] += 1
                else:
                    # Match, no update needed
                    pass
            else:
                # Not found by Jurisdiction. Try Name match for State Agencies (where lj_id is None)
                if local_jurisdiction_id is None:
                    existing_by_name = db.get_agency_by_name(state_id, org_name, category=category)

                    if existing_by_name:
                         # Found by Name
                        current_url = existing_by_name['url']
                        agency_id = existing_by_name['id']

                        # Compare URLs
                        if not self._urls_match(current_url, url):
                            # Update!
                            print(f"Updating URL for {org_name}: {current_url} -> {url}")
                            db.update_agency_url(agency_id, url)
                            stats['updated'] += 1
                        continue # Done with this agency

                # If we get here, we either didn't find it or it's a new record.
                # db.add_agency handles deduplication by URL.
                # If URL is new, it adds.
                # If name is new, it adds.

                # Check if we should add it
                # Logic: If we found a local_jurisdiction_id, we definitely want to add/link it if not exists.
                # If it's a state agency, we add it.

                # Check for duplication by URL before adding (add_agency does this, but returns void)
                if not db.agency_exists(state_id, url=url):
                     # Insert
                     # print(f"Adding new agency: {org_name} ({url})")
                     db.add_agency(state_id, org_name, url, verified=True, category=category, local_jurisdiction_id=local_jurisdiction_id)
                     stats['added'] += 1

        return stats

    def _urls_match(self, url1: str, url2: str) -> bool:
        """Helper to compare two URLs ignoring scheme and www."""
        if not url1 or not url2:
            return False
        u1 = url1.lower().replace("https://", "").replace("http://", "").replace("www.", "").strip("/")
        u2 = url2.lower().replace("https://", "").replace("http://", "").replace("www.", "").strip("/")
        return u1 == u2
