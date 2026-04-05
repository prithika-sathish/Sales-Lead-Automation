from __future__ import annotations

import logging
import os
from typing import Any

import requests


logger = logging.getLogger(__name__)


class SerpApiDiscovery:
    BASE_URL = "https://serpapi.com/search.json"

    def __init__(self, api_key: str | None = None, timeout_seconds: int = 10) -> None:
        self.api_key = api_key or os.getenv("SERPAPI_API_KEY", "")
        self.timeout_seconds = timeout_seconds

    def discover(self, query: str, *, num_results: int = 10) -> list[dict[str, Any]]:
        if not self.api_key:
            logger.warning("serpapi key missing")
            return []

        params = {
            "engine": "google",
            "q": query,
            "num": num_results,
            "api_key": self.api_key,
        }

        try:
            response = requests.get(self.BASE_URL, params=params, timeout=self.timeout_seconds)
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            logger.warning("serpapi request failed | query=%s err=%s", query, exc)
            return []

        organic = payload.get("organic_results") if isinstance(payload, dict) else []
        if not isinstance(organic, list):
            return []

        output: list[dict[str, Any]] = []
        for row in organic:
            if not isinstance(row, dict):
                continue
            title = str(row.get("title") or "").strip()
            link = str(row.get("link") or "").strip()
            snippet = str(row.get("snippet") or "").strip()
            if not title or not link:
                continue
            output.append({"title": title, "link": link, "snippet": snippet})
        return output
