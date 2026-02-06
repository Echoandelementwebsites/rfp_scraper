import json
import os
from typing import List, Dict, Any, Tuple

# Define Special Categories globally
SPECIAL_CATEGORIES = ["School District", "Housing Authority", "Public Library", "Transit Authority"]

def get_absolute_path(filename: str) -> str:
    """Helper to resolve file paths relative to the project root."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(base_dir)
    return os.path.join(project_root, filename)

def load_cities_template(filename: str = "cities_towns_dictionary.json") -> Dict[str, Any]:
    filepath = get_absolute_path(filename)
    if not os.path.exists(filepath):
         if os.path.exists(filename): filepath = filename
         else: raise FileNotFoundError(f"Configuration file not found at {filepath}")
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_agency_template(filename: str = "state_agency_dictionary.json") -> Dict[str, Any]:
    filepath = get_absolute_path(filename)
    if not os.path.exists(filepath):
         if os.path.exists(filename): filepath = filename
         else: raise FileNotFoundError(f"Configuration file not found at {filepath}")
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_local_search_scope(jurisdiction_type: str) -> List[str]:
    """
    Returns a simple list of Service Categories to search for via AI.
    Example: ['Main Office', 'Public Works', 'Police', 'School District', 'Housing Authority']
    """
    categories = ["Main Office"] # Always include Main Office

    try:
        template = load_cities_template()

        # 1. Common Services (Extract Keys)
        # e.g. "public_works" -> "Public Works"
        services = template.get("common_local_services", {})
        for key in services.keys():
            categories.append(key.replace('_', ' ').title())

        # 2. Special Districts (Extract Types) & Standardize Names
        special = template.get("special_districts", {})
        types = special.get("types", [])

        # Mapping for standardization
        mapping = {
            "School Districts": "School District",
            "Library Districts": "Public Library",
            "Transit Authorities": "Transit Authority"
        }

        for item in types:
            if isinstance(item, dict) and "type" in item:
                raw_type = item["type"]
                # Apply mapping if exists, otherwise use raw
                std_type = mapping.get(raw_type, raw_type)
                categories.append(std_type)

        # 3. Explicitly Add Housing Authority (missing from JSON)
        categories.append("Housing Authority")

    except Exception as e:
        print(f"Error loading local scope: {e}")
        # Fallback list if JSON fails
        return ["Main Office", "Public Works", "Police", "Fire", "School District"]

    return list(set(categories)) # Dedupe

def extract_search_scope(template: Dict[str, Any]) -> List[str]:
    """Parses the state template to return a flat list of agency types."""
    scope = []
    common_types = template.get("common_agency_types", {})
    for key, info in common_types.items():
        if isinstance(info, dict) and "typical_name" in info:
            scope.append(info["typical_name"])

    return list(set(scope))

def get_domain_patterns(jurisdiction_type: str) -> Tuple[List[str], List[str]]:
    """
    Returns two lists of domain patterns for the given jurisdiction type:
    1. Specific Patterns (contain state abbreviation) - High Priority
    2. Generic Patterns (fallback)
    """
    specific_patterns = []
    generic_patterns = []
    jurisdiction_type = jurisdiction_type.lower()

    # Default Hardcoded Fallbacks (if JSON fails or empty)
    # Note: We prioritize these based on the requirement
    if jurisdiction_type == 'city':
        specific_patterns = [
            "[cityname][state_abbrev].gov",
            "[cityname]-[state_abbrev].gov",
            "cityof[cityname][state_abbrev].gov"
        ]
        generic_patterns = [
            "[cityname].gov",
            "cityof[cityname].gov",
            "cityof[cityname].org",
            "cityof[cityname].com",
            "[cityname][state_abbrev].com",
            "[cityname]-[state_abbrev].com",
            "cityof[cityname][state_abbrev].com",
            "[cityname][state_abbrev].org",
            "[cityname]-[state_abbrev].org",
            "cityof[cityname][state_abbrev].org"

        ]
    elif jurisdiction_type == 'town':
        specific_patterns = [
            "[townname][state_abbrev].gov",
            "townof[townname][state_abbrev].gov",
            "[townname]-[state_abbrev].gov"
        ]
        generic_patterns = [
            "[townname].gov",
            "townof[townname].gov",
            "townof[townname].org",
            "[townname][state_abbrev].org",
            "townof[townname][state_abbrev].org",
            "[townname]-[state_abbrev].org",
            "[townname][state_abbrev].com",
            "townof[townname][state_abbrev].com",
            "[townname]-[state_abbrev].com"
        ]
    elif jurisdiction_type == 'county':
        specific_patterns = [
            "co.[countyname].[state_abbrev].us",
            "[countyname]county[state_abbrev].gov"
        ]
        generic_patterns = [
            "[countyname]county.gov",
            "[countyname].gov"
        ]

    # Try to load from JSON to override/augment
    try:
        template = load_cities_template()
        domain_patterns = template.get("domain_patterns", [])

        # Collect patterns from JSON
        json_patterns = []
        for entry in domain_patterns:
            inst_types = entry.get("institution_type", [])
            if jurisdiction_type in inst_types:
                json_patterns.append(entry.get("pattern", ""))

        if json_patterns:
            # If we found patterns in JSON, we rely on them, but we must split them
            specific_patterns = []
            generic_patterns = []
            for p in json_patterns:
                if not p: continue
                # Heuristic: if it has state placeholder, it is specific
                if "[state_abbrev]" in p or "[state]" in p or ".[state_abbrev]." in p:
                    specific_patterns.append(p)
                else:
                    generic_patterns.append(p)

    except Exception as e:
        print(f"Error loading domain patterns: {e}")
        # Keep the hardcoded defaults set above

    # Enforce Golden Patterns at the top (User Requirement)
    # Ensure [name][state_abbr].gov and [name]-[state_abbr].gov are prioritized
    golden_patterns = []
    if jurisdiction_type == 'city':
        golden_patterns = ["[cityname][state_abbrev].gov", "[cityname]-[state_abbrev].gov"]
    elif jurisdiction_type == 'town':
        golden_patterns = ["[townname][state_abbrev].gov", "[townname]-[state_abbrev].gov"]

    # Prepend golden patterns, removing duplicates if they exist elsewhere
    for p in reversed(golden_patterns):
        if p in specific_patterns:
            specific_patterns.remove(p)
        specific_patterns.insert(0, p)

    return specific_patterns, generic_patterns

def get_special_district_patterns(district_type: str) -> Tuple[List[str], List[str]]:
    """
    Returns a prioritized tuple of patterns (Specific, Generic)
    for the specific special district type.
    """
    specific = []
    generic = []

    if district_type == "Housing Authority":
        # Specific
        specific.append("[name][state_abbrev]housing.gov")
        specific.append("[name][state_abbrev]ha.gov")
        # Generic
        generic.append("[name]housing.gov")
        generic.append("[name]ha.gov")
        generic.append("[name]housing.org")
        generic.append("[name]ha.org")
        generic.append("[name]housingauthority.com")

    elif district_type == "Public Library":
        # Specific
        specific.append("[name][state_abbrev]library.gov")
        specific.append("[name][state_abbrev]pl.gov")
        # Generic
        generic.append("[name]library.gov")
        generic.append("[name]pl.gov")
        generic.append("[name]library.org")
        generic.append("[name]publiclibrary.org")
        generic.append("[name]pl.org")

    elif district_type == "Transit Authority":
        # Specific
        specific.append("[name][state_abbrev]transit.gov")
        # Generic
        generic.append("[name]transit.gov")
        generic.append("[name]transit.org")
        generic.append("[name]metro.org")

    elif district_type == "School District":
        # Specific
        specific.append("[name][state_abbrev]schools.gov")
        specific.append("[name][state_abbrev]sd.gov")
        specific.append("[name][state_abbrev]ps.gov")
        specific.append("[name]schools[state_abbrev].org")
        # Generic
        generic.append("[name]schools.gov")
        generic.append("[name]sd.org")
        generic.append("[name]isd.org")
        generic.append("[name]schools.org")

    return specific, generic
