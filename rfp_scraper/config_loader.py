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

def get_local_search_scope(jurisdiction_type: str) -> Dict[str, List[str]]:
    """
    Returns a dictionary of Top-Level Categories and their aggregated search patterns
    for a given jurisdiction type.

    Structure: {'Public Works': ['[Jurisdiction] Water', ...], 'Main Office': [...]}
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
                patterns['Main Office'] = main_patterns

        # 2. Common Services (Aggregated by Top-Level Key)
        services = template.get("common_local_services", {})
        for top_category, subgroups in services.items():
            # Convert snake_case (public_works) to Title Case (Public Works)
            category_name = top_category.replace('_', ' ').title()

            aggregated_patterns = []

            for service_key, service_info in subgroups.items():
                if isinstance(service_info, dict) and "naming_patterns" in service_info:
                    aggregated_patterns.extend(service_info["naming_patterns"])

            if aggregated_patterns:
                patterns[category_name] = list(set(aggregated_patterns)) # Dedupe

    except Exception as e:
        print(f"Error loading local patterns for {jurisdiction_type}: {e}")

    return patterns

# Deprecated but kept for compatibility if needed (though we will refactor usage)
def get_local_patterns(jurisdiction_type: str) -> Dict[str, List[str]]:
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
