import json
import os
from typing import List, Dict, Any

def get_absolute_path(filename: str) -> str:
    """Helper to resolve file paths relative to the project root."""
    # Assuming config_loader.py is in rfp_scraper/ directory,
    # and json files are in the root (one level up).
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(base_dir)
    return os.path.join(project_root, filename)

def load_agency_template(filename: str = "state_agency_dictionary.json") -> Dict[str, Any]:
    """Load the agency schema from JSON file."""
    filepath = get_absolute_path(filename)

    if not os.path.exists(filepath):
         # Try local existence (legacy support if moved)
         if os.path.exists(filename):
             filepath = filename
         else:
             raise FileNotFoundError(f"Configuration file not found at {filepath}")

    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_cities_template(filename: str = "cities_towns_dictionary.json") -> Dict[str, Any]:
    """Load the cities/towns/counties schema from JSON file."""
    filepath = get_absolute_path(filename)

    if not os.path.exists(filepath):
         # Try local existence
         if os.path.exists(filename):
             filepath = filename
         else:
             raise FileNotFoundError(f"Configuration file not found at {filepath}")

    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_local_search_scope(jurisdiction_type: str = "city") -> List[str]:
    """
    Returns a list of Service Categories to search for.
    Extracts Top-Level Keys from common_local_services and special_districts.
    Ignores patterns and sub-agencies.
    """
    scope = ["Main Office"]

    try:
        template = load_cities_template()

        # 1. Common Services (Top-Level Keys)
        services = template.get("common_local_services", {})
        for service_key in services.keys():
            # Convert snake_case (public_works) to Title Case (Public Works)
            category_name = service_key.replace('_', ' ').title()
            scope.append(category_name)

        # 2. Special Districts (Types)
        special_districts = template.get("special_districts", {})
        districts_list = special_districts.get("types", [])
        for d in districts_list:
            if "type" in d:
                scope.append(d["type"])

    except Exception as e:
        print(f"Error loading local search scope: {e}")

    # Remove duplicates if any
    return list(dict.fromkeys(scope))

# Deprecated but kept for compatibility if needed (though we will refactor usage)
def get_local_patterns(jurisdiction_type: str) -> List[str]:
    # Redirecting to the new logic, although the return type changed (List[str] instead of Dict).
    # This might break legacy calls if not updated, but we are updating app.py anyway.
    return get_local_search_scope(jurisdiction_type)

def extract_search_scope(template: Dict[str, Any]) -> List[str]:
    """
    Parses the template to return a flat list of agency types/names to search for.
    Includes common agencies, universities, and hospitals.
    """
    scope = []

    # 1. Common Agency Types
    common_types = template.get("common_agency_types", {})
    for key, info in common_types.items():
        if isinstance(info, dict) and "typical_name" in info:
            scope.append(info["typical_name"])

    # 2. Higher Education
    higher_ed = template.get("higher_education_institutions", {})

    # University Types
    uni_types = higher_ed.get("university_types", [])
    for u_type in uni_types:
        # Add the generic type name, e.g. "Flagship Research University"
        if "type" in u_type:
            scope.append(u_type["type"])

    # Governing Boards
    gov_boards = higher_ed.get("state_governing_boards", {})
    if "typical_name" in gov_boards:
        scope.append(gov_boards["typical_name"])

    # 3. Hospitals
    hospitals = template.get("state_hospitals_and_medical_facilities", {})
    for key, info in hospitals.items():
        if key == "state_psychiatric_hospitals":
            scope.append("State Psychiatric Hospital")
        elif key == "university_medical_centers":
            scope.append("University Medical Center")
        elif key == "public_health_hospitals":
            scope.append("Public Health Hospital")
        elif key == "veterans_homes":
            scope.append("State Veterans Home")

    return list(set(scope)) # De-dupe just in case

def get_domain_patterns(jurisdiction_type: str) -> List[str]:
    """
    Extracts valid domain patterns from the configuration for a given jurisdiction type.
    """
    patterns = []
    try:
        template = load_cities_template()
        domain_patterns = template.get("domain_patterns", [])

        for entry in domain_patterns:
            institution_types = entry.get("institution_type", [])
            if jurisdiction_type in institution_types:
                patterns.append(entry.get("pattern", ""))

    except Exception as e:
        print(f"Error extracting domain patterns for {jurisdiction_type}: {e}")

    return patterns
