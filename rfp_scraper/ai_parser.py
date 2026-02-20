import os
import json
import re
from typing import List, Optional, Any, Dict
from openai import OpenAI
from dotenv import load_dotenv

# Load env vars
load_dotenv()

class DeepSeekClient:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        if not self.api_key:
            # We will handle missing key gracefully in the UI or orchestrator,
            # but here we can't do much without it.
            self.client = None
        else:
            self.client = OpenAI(
                api_key=self.api_key,
                base_url="https://api.deepseek.com"
            )

    def _clean_and_parse_json(self, content: str) -> Any:
        """Helper to clean markdown code blocks and parse JSON."""
        # Simple cleanup
        content = content.strip()
        if content.startswith("```json"):
            content = content.replace("```json", "", 1)
            if content.endswith("```"):
                content = content[:-3]
        elif content.startswith("```"):
            content = content.replace("```", "", 1)
            if content.endswith("```"):
                content = content[:-3]

        return json.loads(content)

    def classify_csi_divisions(self, title: str, description: str) -> List[str]:
        """
        Analyzes the project and identifies which CSI MasterFormat Divisions (02-16) apply.
        Returns a list of strings (e.g., ['Division 03 - Concrete']).
        Returns empty list if not relevant.
        """
        if not self.api_key:
            return []

        prompt = (
            "You are a Construction Estimator. Analyze the project and identify which CSI MasterFormat Divisions (02-16) apply.\n\n"
            "Divisions: 02 Site Work, 03 Concrete, 04 Masonry, 05 Metals, 06 Wood/Plastics, 07 Thermal/Moisture, "
            "08 Doors/Windows, 09 Finishes, 10 Specialties, 11 Equipment, 12 Furnishings, 13 Special Construction, "
            "14 Conveying Systems, 15 Mechanical (Plumbing/HVAC), 16 Electrical.\n\n"
            "Rules:\n"
            "    1. Strict Match: Only return a Division if the text explicitly mentions work in that trade.\n"
            "    2. MAINTENANCE & RENOVATION ARE CONSTRUCTION: Painting, Flooring, Roofing, HVAC upgrades, and Renovation projects ARE valid. Do NOT discard them.\n"
            "    3. Exclusions: Ignore 'General Requirements' (Div 01). Ignore Janitorial, Software, or Admin work (return []).\n"
            "    4. No Hallucinations: If the text is vague or unrelated, return [].\n\n"
            "Output: Return a JSON list of strings (e.g., ['Division 03 - Concrete'])."
        )

        user_content = f"Title: {title}\n\nDescription: {description[:3000]}"

        try:
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": user_content}
                ],
                response_format={ "type": "json_object" },
            )

            content = response.choices[0].message.content
            data = self._clean_and_parse_json(content)

            if isinstance(data, dict):
                # Sometimes models return {"divisions": [...]}
                if "divisions" in data:
                    return data["divisions"]
                # Sometimes they obey strictly and return list (but parsed as dict if root is object?)
                # If the prompt asked for a JSON list, response_format json_object might force an object wrapper.
                # DeepSeek might return {"key": [...]}. We should handle both.
                # However, the prompt says "Return a JSON list".
                # If we use json_object, we should ask for an object.
                # Let's check keys.
                for key, val in data.items():
                    if isinstance(val, list):
                        return val
                return []
            elif isinstance(data, list):
                return data

            return []

        except Exception as e:
            print(f"Error classifying CSI divisions: {e}")
            return []

    def parse_rfp_content(self, text_content: str) -> List[dict]:
        """
        Parses raw text content using DeepSeek API to extract RFP opportunities.
        """
        if not self.api_key:
            return []

        prompt = (
            "Analyze this text. Extract construction RFP opportunities. "
            "Return ONLY a JSON list with keys: title, deadline (YYYY-MM-DD), "
            "description, clientName. If no specific deadline, return null.\n"
            "Do NOT extract projects if they are purely 'Citizen Services' (Taxes, Permits) or 'Events'. Return []."
        )

        try:
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": text_content[:10000]}
                ],
                response_format={ "type": "json_object" },
            )

            content = response.choices[0].message.content

            # Regex JSON Extraction
            match = re.search(r'\[.*\]', content, re.DOTALL)
            if match:
                json_str = match.group(0)
                data = json.loads(json_str)
            else:
                print("Failed to locate JSON array in AI response.")
                return []

            # Ensure it's a list
            if isinstance(data, dict):
                 if "rfps" in data:
                     return data["rfps"]
                 return [data]
            elif isinstance(data, list):
                return data

            return []

        except Exception as e:
            print(f"Error parsing with DeepSeek: {e}")
            return []

    def generate_us_states(self) -> List[str]:
        """
        Generates a list of all 50 US states.
        """
        if not self.api_key:
            return []

        prompt = "List all 50 United States with their full names. Return ONLY a JSON list of strings."

        try:
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "user", "content": prompt}
                ],
                response_format={ "type": "json_object" },
            )

            content = response.choices[0].message.content
            data = self._clean_and_parse_json(content)

            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                 if "states" in data:
                     return data["states"]
                 # Fallback if structure is unexpected but contains list
                 for key, value in data.items():
                     if isinstance(value, list):
                         return value

            return []

        except Exception as e:
            print(f"Error generating states: {e}")
            return []

    def discover_state_agencies(self, state_name: str) -> List[dict]:
        """
        Discovers agencies and universities for a given state.
        """
        if not self.api_key:
            return []

        prompt = (
            f"List major state agencies, departments, and public universities in {state_name} "
            "that issue construction RFPs. Return a JSON list of objects with keys: "
            "'organization_name' and 'url'. Filter for .gov or .edu domains only."
        )

        try:
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "user", "content": prompt}
                ],
                response_format={ "type": "json_object" },
            )

            content = response.choices[0].message.content
            data = self._clean_and_parse_json(content)

            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                 if "agencies" in data:
                     return data["agencies"]
                 # Fallback: look for any list
                 for key, value in data.items():
                     if isinstance(value, list):
                         return value

            return []

        except Exception as e:
            print(f"Error discovering agencies for {state_name}: {e}")
            return []

    def find_specific_agency(self, state_name: str, agency_type: str) -> Optional[str]:
        """
        Attempts to find a specific agency URL using AI.
        """
        if not self.api_key:
            return None

        prompt = (
            f"Find the official website URL for the '{agency_type}' in {state_name}. "
            "Return ONLY a JSON object with one key 'url'. "
            "Ensure the domain is .gov or .edu."
        )

        try:
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "user", "content": prompt}
                ],
                response_format={ "type": "json_object" },
            )

            content = response.choices[0].message.content
            data = self._clean_and_parse_json(content)

            if isinstance(data, dict):
                return data.get("url")

            return None

        except Exception as e:
            print(f"Error finding specific agency {agency_type} in {state_name}: {e}")
            return None

    def generate_local_jurisdictions(self, state_name: str) -> dict:
        """
        Generates lists of counties, cities, and towns for a given state.
        Returns a dict with keys: 'counties', 'cities', 'towns'.
        """
        if not self.api_key:
            return {"counties": [], "cities": [], "towns": []}

        prompt = (
            f"List all counties, top 20 major cities, and top 20 major towns for {state_name}. "
            "Return a JSON object with three keys: 'counties', 'cities', and 'towns'. "
            "Each value must be a list of strings containing ONLY the names (e.g., 'Cook', 'Chicago', 'Cicero')."
        )

        try:
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "user", "content": prompt}
                ],
                response_format={ "type": "json_object" },
            )

            content = response.choices[0].message.content
            data = self._clean_and_parse_json(content)

            if isinstance(data, dict):
                # Ensure keys exist
                return {
                    "counties": data.get("counties", []),
                    "cities": data.get("cities", []),
                    "towns": data.get("towns", [])
                }

            return {"counties": [], "cities": [], "towns": []}

        except Exception as e:
            print(f"Error generating local jurisdictions for {state_name}: {e}")
            return {"counties": [], "cities": [], "towns": []}

    def identify_best_agency_url(self, candidates: List[Dict], agency_name: str, domain_rules: List[str]) -> Optional[str]:
        """
        Identifies the best matching URL from a list of search candidates using AI.
        """
        if not self.api_key or not candidates:
            return None

        # Format candidates for prompt
        candidates_formatted = json.dumps(candidates, indent=2)
        domain_rules_str = ", ".join(domain_rules)

        prompt = (
            f"You are a research analyst. Below are search results for '{agency_name}'. "
            "Your goal is to find the Official Government Homepage for this specific department.\n\n"
            f"Validation Rules:\n"
            f"1. Prioritize domains ending in: {domain_rules_str}.\n"
            "2. Reject social media (Facebook, LinkedIn), news articles, and PDF documents.\n"
            "3. The URL must point to the agency's main landing page or the city's department sub-page.\n\n"
            f"Search Results:\n{candidates_formatted}\n\n"
            "Return ONLY a JSON object with one key 'url'. "
            "If none are the official site, return 'url': null."
        )

        try:
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "user", "content": prompt}
                ],
                response_format={ "type": "json_object" },
            )

            content = response.choices[0].message.content
            data = self._clean_and_parse_json(content)

            if isinstance(data, dict):
                return data.get("url")

            return None

        except Exception as e:
            print(f"Error identifying best agency URL for {agency_name}: {e}")
            return None

    def find_agency_in_search_results(self, agency_name: str, jurisdiction: str, candidates: List[Dict], domain_rules: List[str]) -> Optional[str]:
        """
        Analyzes search results and picks the best agency URL.
        """
        if not self.api_key or not candidates:
            return None

        candidates_formatted = json.dumps(candidates, indent=2)
        domain_rules_str = ", ".join(domain_rules) if domain_rules else ".gov, .org, state.us"

        prompt = (
            f"I am looking for the official website for {agency_name} in {jurisdiction}. Here are the top search results: {candidates_formatted}\n\n"
            "Rules:\n\n"
            f"    Identify the official government link (prioritize {domain_rules_str} like .gov, .org, state.us).\n\n"
            "    Ignore social media, news articles, and third-party directories.\n\n"
            "    If the official link is present, return ONLY the URL.\n\n"
            "    If no official link is found, return 'None'."
        )

        try:
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "user", "content": prompt}
                ],
            )

            content = response.choices[0].message.content.strip()

            # Handle potential None string
            if content.lower() == 'none' or not content:
                return None

            # Basic cleanup if the model adds quotes or markdown
            if content.startswith("```"):
                content = content.strip("`").strip()
            if content.startswith("'") and content.endswith("'"):
                content = content[1:-1]
            if content.startswith('"') and content.endswith('"'):
                content = content[1:-1]

            return content

        except Exception as e:
            print(f"Error in find_agency_in_search_results: {e}")
            return None

    def analyze_serp_results(self, jurisdiction: str, service_category: str, search_results: List[dict]) -> Optional[str]:
        """
        Analyzes raw search results to find the official government landing page.
        """
        if not self.api_key or not search_results:
            return None

        # Format results for the prompt
        results_text = ""
        for i, res in enumerate(search_results):
            results_text += f"Result {i+1}:\nTitle: {res.get('title', '')}\nURL: {res.get('url', '')}\nSnippet: {res.get('snippet', '')}\n\n"

        prompt = (
            f"I am finding the official website for **{service_category}** in **{jurisdiction}**.\n"
            "Below are the top search results.\n\n"
            "**Your Task:**\n"
            "1. Identify the **Single Official Government Landing Page** for this specific department.\n"
            "2. **Strict Exclusion:** Reject news articles, social media (Facebook/LinkedIn), PDF files, and third-party directories.\n"
            "3. **Preference:** Prefer .gov, .org, or state-specific domains (e.g., .tx.us).\n"
            "4. **Logic:** If looking for 'Public Works' and you see 'city.gov/public-works', that is the correct link.\n\n"
            f"**Search Results:**\n{results_text}\n\n"
            "Return ONLY a JSON object with one key 'url'. If no official URL is found, set 'url': null."
        )

        try:
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "user", "content": prompt}
                ],
                response_format={ "type": "json_object" },
            )

            content = response.choices[0].message.content
            data = self._clean_and_parse_json(content)

            if isinstance(data, dict):
                return data.get("url")

            return None

        except Exception as e:
            print(f"Error analyzing SERP: {e}")
            return None
