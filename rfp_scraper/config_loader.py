import json
import os
from typing import List, Dict, Any

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
    Example: ['Main Office', 'Public Works', 'Police', 'School Districts']
    """
    categories = ["Main Office"] # Always include Main Office

    try:
        template = load_cities_template()

        # 1. Common Services (Extract Keys)
        # e.g. "public_works" -> "Public Works"
        services = template.get("common_local_services", {})
        for key in services.keys():
            categories.append(key.replace('_', ' ').title())

        # 2. Special Districts (Extract Types)
        special = template.get("special_districts", {})
        types = special.get("types", [])
        for item in types:
            if isinstance(item, dict) and "type" in item:
                categories.append(item["type"])

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

def get_domain_patterns(jurisdiction_type: str) -> List[str]:
    """
    Returns a list of domain patterns for the given jurisdiction type (e.g., 'city', 'county').
    """
    patterns = []
    jurisdiction_type = jurisdiction_type.lower()

    try:
        template = load_cities_template()
        domain_patterns = template.get("domain_patterns", [])

        for entry in domain_patterns:
            inst_types = entry.get("institution_type", [])
            if jurisdiction_type in inst_types:
                patterns.append(entry.get("pattern", ""))

    except Exception as e:
        print(f"Error loading domain patterns: {e}")

    # Fallback if no patterns found (or file missing)
    if not patterns:
        if jurisdiction_type == 'city':
            # Fallback: [name].gov, cityof[name].gov, [name][state].gov, cityof[name].org
            # Using specific placeholders for consistency with discovery logic
            patterns = [
                "[cityname].gov",
                "cityof[cityname].gov",
                "[cityname][state_abbrev].gov",
                "cityof[cityname].org"
            ]
        elif jurisdiction_type == 'county':
            # Fallback: [name]county.gov, co.[name].[state].us
            patterns = [
                "[countyname]county.gov",
                "co.[countyname].[state_abbrev].us"
            ]
        elif jurisdiction_type == 'town':
             patterns = [
                "[townname].gov",
                "townof[townname].gov",
                "[townname][state_abbrev].gov",
                "townof[townname].org"
             ]

    # Sort patterns: .gov first, then others
    gov_patterns = [p for p in patterns if p and p.endswith('.gov')]
    other_patterns = [p for p in patterns if p and not p.endswith('.gov')]

    return gov_patterns + other_patterns
