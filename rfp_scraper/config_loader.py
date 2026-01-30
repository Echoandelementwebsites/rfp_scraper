import json
import os
from typing import List, Dict, Any

def load_agency_template(filepath: str = "state_agency_dictionary.json") -> Dict[str, Any]:
    """Load the agency schema from JSON file."""
    if not os.path.exists(filepath):
        # Fallback for different execution contexts
        alt_path = os.path.join(os.path.dirname(__file__), "..", filepath)
        if os.path.exists(alt_path):
            filepath = alt_path
        else:
            raise FileNotFoundError(f"Configuration file not found at {filepath}")

    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_cities_template(filepath: str = "cities_towns_dictionary.json") -> Dict[str, Any]:
    """Load the cities/towns/counties schema from JSON file."""
    if not os.path.exists(filepath):
        # Fallback for different execution contexts
        alt_path = os.path.join(os.path.dirname(__file__), "..", filepath)
        if os.path.exists(alt_path):
            filepath = alt_path
        else:
            raise FileNotFoundError(f"Configuration file not found at {filepath}")

    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_local_patterns(jurisdiction_type: str) -> Dict[str, List[str]]:
    """
    Returns a dictionary of categories and their search patterns for a given jurisdiction type.
    Includes 'main_office' and service-specific categories.
    """
    patterns = {}
    try:
        template = load_cities_template()

        # 1. Main Office
        conventions = template.get("general_naming_conventions", {})
        key_map = {
            'county': 'county_level',
            'city': 'city_level',
            'town': 'town_level'
        }

        section_key = key_map.get(jurisdiction_type)
        if section_key:
            main_patterns = conventions.get(section_key, {}).get("patterns", [])
            if main_patterns:
                patterns['main_office'] = main_patterns

        # 2. Common Services
        services = template.get("common_local_services", {})
        for group, subgroups in services.items():
            for service_key, service_info in subgroups.items():
                if isinstance(service_info, dict) and "naming_patterns" in service_info:
                    # Use service_key (e.g. 'police', 'water_sewer') as category
                    patterns[service_key] = service_info["naming_patterns"]

    except Exception as e:
        print(f"Error loading local patterns for {jurisdiction_type}: {e}")

    return patterns

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
