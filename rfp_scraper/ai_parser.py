import os
import json
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

    def validate_construction_relevance(self, title: str, text_content: str) -> bool:
        """
        Determines if the content is related to construction, architecture, or engineering.
        """
        if not self.api_key:
            # If no API key is provided, we cannot validate strictly.
            # Depending on policy, we might fail open or closed.
            # Given "Strict Filtering", failing closed (False) makes sense,
            # but usually users without keys want to see something.
            # However, this feature is dependent on AI.
            return False

        prompt = (
            "You are a construction bid analyst. Determine if the following RFP is related to "
            "Construction, Architecture, Engineering, Roadwork, Infrastructure, or Public Works. "
            "Ignore software, IT, or consulting unless it is explicitly engineering/architectural consulting. "
            "Return ONLY a JSON object with one key 'is_relevant' (boolean)."
        )

        user_content = f"Title: {title}\n\nSnippet: {text_content[:2000]}"

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
                return data.get("is_relevant", False)

            return False

        except Exception as e:
            print(f"Error validating relevance: {e}")
            return False

    def parse_rfp_content(self, text_content: str) -> List[dict]:
        """
        Parses raw text content using DeepSeek API to extract RFP opportunities.
        """
        if not self.api_key:
            return []

        prompt = (
            "Analyze this text. Extract construction RFP opportunities. "
            "Return ONLY a JSON list with keys: title, deadline (YYYY-MM-DD), "
            "description, clientName. If no specific deadline, return null."
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
            data = self._clean_and_parse_json(content)

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
