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
        naming_patterns = template.get("naming_patterns", [])
        main_office_patterns = []

        # Target placeholders based on jurisdiction type
        target_placeholders = ["[Jurisdiction]"]
        if jurisdiction_type == 'city':
            target_placeholders.append("[City Name]")
        elif jurisdiction_type == 'county':
            target_placeholders.append("[County Name]")
        elif jurisdiction_type == 'town':
            target_placeholders.append("[Town Name]")

        for entry in naming_patterns:
            pattern = entry.get("pattern", "")
            # check if pattern contains any of the target placeholders
            if any(ph in pattern for ph in target_placeholders):
                main_office_patterns.append(pattern)

        if main_office_patterns:
            patterns['Main Office'] = list(set(main_office_patterns))

        # 2. Common Services (Aggregated by Top-Level Key)
        services = template.get("common_local_services", {})

        for service_key, service_info in services.items():
            # Convert snake_case (public_works) to Title Case (Public Works)
            category_name = service_key.replace('_', ' ').title()

            aggregated_patterns = []

            # typical_name: Add this string directly
            if "typical_name" in service_info:
                aggregated_patterns.append(service_info["typical_name"])

            # variations: If present, add all strings in this list
            if "variations" in service_info:
                aggregated_patterns.extend(service_info["variations"])

            # common_subsidiaries: If present (List of Dicts), extract name_pattern
            common_subsidiaries = service_info.get("common_subsidiaries", [])
            for sub in common_subsidiaries:
                if isinstance(sub, dict) and "name_pattern" in sub:
                    aggregated_patterns.append(sub["name_pattern"])

            # sub_agencies: If present (List of Strings), prepend [Jurisdiction]
            sub_agencies = service_info.get("sub_agencies", [])
            for sub in sub_agencies:
                aggregated_patterns.append(f"[Jurisdiction] {sub}")

            # related_services: If present (List of Strings), prepend [Jurisdiction]
            related_services = service_info.get("related_services", [])
            for related in related_services:
                aggregated_patterns.append(f"[Jurisdiction] {related}")

            # acronym: If present, prepend [Jurisdiction]
            if "acronym" in service_info:
                aggregated_patterns.append(f"[Jurisdiction] {service_info['acronym']}")

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
