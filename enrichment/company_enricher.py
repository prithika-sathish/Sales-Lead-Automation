from __future__ import annotations

import logging
from typing import Any

import requests
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)


class CompanyEnricher:
    def __init__(self, timeout_seconds: int = 10) -> None:
        self.timeout_seconds = timeout_seconds

    def fetch_website_text(self, url: str) -> str:
        if not url:
            return ""
        try:
            response = requests.get(url, timeout=self.timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("enrichment fetch failed | url=%s err=%s", url, exc)
            return ""

        soup = BeautifulSoup(response.text, "html.parser")
        return soup.get_text(" ", strip=True)[:50000]

    def enrich(self, company: dict[str, Any]) -> dict[str, Any]:
        text = self.fetch_website_text(str(company.get("website") or ""))
        enriched = dict(company)
        enriched["website_text"] = text
        return enriched
