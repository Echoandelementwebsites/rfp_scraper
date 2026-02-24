import os
import json
from typing import Optional, List, Dict, Any
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.async_configs import LLMConfig
from openai import AsyncOpenAI

class CrawlerEngine:
    def __init__(self):
        self.api_key = os.getenv("DEEPSEEK_API_KEY")
        self.base_url = "https://api.deepseek.com"
        if not self.api_key:
            print("WARNING: DEEPSEEK_API_KEY not found in environment variables.")
            # Use dummy key to prevent client init failure during setup
            self.api_key = "dummy_key_for_setup"

        self.client = AsyncOpenAI(api_key=self.api_key, base_url=self.base_url)

    def get_llm_config(self) -> LLMConfig:
        return LLMConfig(
            provider="openai/deepseek-chat",
            api_token=self.api_key,
            base_url=self.base_url
        )

    def get_browser_config(self) -> BrowserConfig:
        return BrowserConfig(
            headless=True,
            verbose=False
        )

    def get_run_config(self, strategy=None, process_iframes: bool = True, wait_until: str = "networkidle") -> CrawlerRunConfig:
        return CrawlerRunConfig(
            extraction_strategy=strategy,
            cache_mode=CacheMode.BYPASS,
            process_iframes=process_iframes,
            remove_overlay_elements=True,
            magic=True,
            wait_until=wait_until,
            page_timeout=60000,
            delay_before_return_html=2.0
        )

    async def classify_text(self, text: str) -> List[str]:
        """
        Classifies the text into CSI MasterFormat divisions using DeepSeek.
        """
        if not text:
            return []

        system_prompt = (
            "You are an expert construction estimator. Analyze the following project description/scope of work "
            "and identify the relevant CSI MasterFormat Divisions. "
            "Return ONLY a JSON object with a single key 'divisions' containing a list of strings (e.g., 'Division 03 - Concrete'). "
            "If no construction work is found, return an empty list."
        )

        try:
            response = await self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": text[:50000]} # Truncate to avoid context limits
                ],
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            content = response.choices[0].message.content
            data = json.loads(content)
            return data.get("divisions", [])
        except Exception as e:
            print(f"Classification Error: {e}")
            return []

# Global instance
engine = CrawlerEngine()
