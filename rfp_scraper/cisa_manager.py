import pandas as pd
import requests
import io
import re
from typing import Optional, Dict, Tuple
from rfp_scraper_v2.core.database import DatabaseHandler

class CisaManager:
    CISA_CSV_URL = "https://raw.githubusercontent.com/cisagov/dotgov-data/main/current-full.csv"
    _shared_df = None

    def __init__(self):
        self._df: Optional[pd.DataFrame] = None

    def _load_data(self):
        """Downloads and caches the CISA registry CSV."""
        if CisaManager._shared_df is not None:
            self._df = CisaManager._shared_df
            return

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
            CisaManager._shared_df = df
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

            # Determine Category & Clean Name
            category = 'state_agency'
            jurisdiction_type = None
            clean_name = org_name

            org_lower = org_name.lower()

            if 'state' in domain_type:
                category = 'state_agency'
            elif any(x in domain_type for x in ['local', 'city', 'county']):
                if 'county' in domain_type or 'county' in org_lower:
                    category = 'county'
                    jurisdiction_type = 'county'
                    clean_name = re.sub(r'(?i)\bcounty\b', '', org_name).strip()
                elif 'city' in domain_type or 'city of' in org_lower:
                    category = 'city'
                    jurisdiction_type = 'city'
                    clean_name = re.sub(r'(?i)^city of\s*', '', org_name).strip()
                elif 'town' in domain_type or 'town of' in org_lower:
                    category = 'town'
                    jurisdiction_type = 'town'
                    clean_name = re.sub(r'(?i)^town of\s*', '', org_name).strip()
                elif 'village' in domain_type or 'village of' in org_lower:
                    category = 'village'
                    jurisdiction_type = 'village'
                    clean_name = re.sub(r'(?i)^village of\s*', '', org_name).strip()
                else:
                    category = 'local'
                    jurisdiction_type = 'city' # Default fallback
                    clean_name = org_name

            # Strip trailing/leading punctuation
            clean_name = clean_name.strip(' ,.-')

            # Attempt Linking / Auto-Create
            local_jurisdiction_id = None
            if jurisdiction_type and clean_name:
                # Automatically append to local_jurisdictions if it doesn't exist, and get the ID
                local_jurisdiction_id = db.append_local_jurisdiction(state_id, clean_name, jurisdiction_type)

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
