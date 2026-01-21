import os
import json
from typing import List, Optional
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
            pass

        self.client = OpenAI(
            api_key=self.api_key,
            base_url="https://api.deepseek.com"
        )

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
                model="deepseek-chat", # Assuming 'deepseek-chat' is the model name, or 'deepseek-r1' etc. using standard 'deepseek-chat' for now.
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": text_content[:10000]} # Limit context window just in case
                ],
                response_format={ "type": "json_object" }, # DeepSeek supports json_object? If not, we rely on prompt.
                # Assuming DeepSeek API compatible with OpenAI SDK follows similar conventions.
                # If json_object not strictly supported, we just parse text.
            )

            content = response.choices[0].message.content

            # Clean up markdown code blocks if present
            if content.startswith("```json"):
                content = content.replace("```json", "").replace("```", "")
            elif content.startswith("```"):
                content = content.replace("```", "")

            data = json.loads(content)

            # Ensure it's a list
            if isinstance(data, dict):
                 # Sometimes models return {"rfps": [...]} or just the object if one found
                 if "rfps" in data:
                     return data["rfps"]
                 return [data] # Treat as single item list if it matches schema, unlikely but possible
            elif isinstance(data, list):
                return data

            return []

        except Exception as e:
            print(f"Error parsing with DeepSeek: {e}")
            return []
