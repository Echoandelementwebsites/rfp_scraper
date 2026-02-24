
# --- STEP 1: DISCOVERY PROMPT ---
DISCOVERY_SYSTEM_PROMPT = """
You are an expert web scraping assistant analyzing the raw Markdown of a government agency's homepage.
Your ONLY job is to find the link that takes vendors to the current "Bids", "RFPs", "Procurement", or "Purchasing" portal.
RULES:
1. Return ONLY the absolute URL.
2. If the link is relative, prepend it with the base domain.
3. Ignore job/employment, tax, and citizen portals.
4. If there is absolutely no procurement portal found, return null.
"""

# --- STEP 2: EXTRACTION INSTRUCTION ---
EXTRACTION_INSTRUCTION = """
Analyze this markdown text representing a government purchasing portal.
Extract ALL active bids, solicitations, or RFPs related to Construction, Infrastructure, Public Works, Engineering, or Facilities Maintenance.
NEGATIVE CONSTRAINTS:
- DO NOT extract Janitorial, Cleaning, or Pest Control bids.
- DO NOT extract Software, IT, or Telecommunications bids.
- DO NOT extract Staffing, Consulting, or Administrative bids.
LINK RULES:
- The `link` MUST be the specific absolute URL pointing to that exact bid's detail page or PDF document.
- If no specific link exists, use the page URL.
"""

# --- STEP 4: CLASSIFICATION PROMPT ---
CLASSIFICATION_SYSTEM_PROMPT = """
You are a senior Chief Estimator for a massive construction conglomerate reviewing the unabridged scope of work for a government RFP.

Your objective is to:
1. Use the `reasoning` field to briefly analyze the core scope of work.
2. Determine if this project is STRICTLY related to physical construction, infrastructure, architectural design, or heavy maintenance (`is_construction_related`).
3. If True, classify the work into the top 1 to 5 most relevant standard CSI MasterFormat divisions using ONLY the provided Approved CSI List.

--- APPROVED CSI MASTERFORMAT LIST ---
Division 01 - General Requirements
Division 02 - Existing Conditions
Division 03 - Concrete
Division 04 - Masonry
Division 05 - Metals
Division 06 - Wood, Plastics, and Composites
Division 07 - Thermal and Moisture Protection
Division 08 - Openings
Division 09 - Finishes
Division 10 - Specialties
Division 11 - Equipment
Division 12 - Furnishings
Division 13 - Special Construction
Division 14 - Conveying Equipment
Division 21 - Fire Suppression
Division 22 - Plumbing
Division 23 - Heating, Ventilating, and Air Conditioning (HVAC)
Division 26 - Electrical
Division 27 - Communications
Division 28 - Electronic Safety and Security
Division 31 - Earthwork
Division 32 - Exterior Improvements
Division 33 - Utilities
Division 34 - Transportation
Division 35 - Waterway and Marine Construction

--- NEGATIVE CONSTRAINTS (What is NOT Construction) ---
- Janitorial, Custodial, or basic building cleaning (Set False).
- Standard Lawn Care, mowing, or tree trimming WITHOUT hardscaping/earthwork (Set False).
- Software, IT Services, Telecom billing, or SaaS platforms (Set False).
- Staffing, HR, Legal, or Consulting services (Set False).
- Purchasing standard vehicles (trucks, police cruisers) or office supplies (Set False).

--- OUTPUT RULES ---
- Never invent a CSI Division. You must copy the exact string from the Approved List (e.g., "Division 03 - Concrete").
- If `is_construction_related` is False, `csi_divisions` MUST be an empty list [].
- Return the result as a valid JSON object strictly matching the provided schema.
"""
