from __future__ import annotations

import os
from typing import Any

from tavily import TavilyClient


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _get_client() -> TavilyClient:
    api_key = _clean_text(os.getenv("TAVILY_API_KEY"))
    if not api_key:
        raise RuntimeError("TAVILY_API_KEY not set")
    return TavilyClient(api_key=api_key)


def search_tavily(query: str, *, max_results: int = 10) -> list[dict[str, Any]]:
    client = _get_client()
    response = client.search(
        query=_clean_text(query),
        search_depth="advanced",
        max_results=max(1, min(int(max_results), 20)),
    )

    results: list[dict[str, Any]] = []
    for row in response.get("results", []) if isinstance(response, dict) else []:
        if not isinstance(row, dict):
            continue
        results.append(
            {
                "name": _clean_text(row.get("title")),
                "website": _clean_text(row.get("url")),
                "snippet": _clean_text(row.get("content")),
            }
        )

    return results


def fallback_search(query: str, *, max_results: int = 10) -> list[dict[str, Any]]:
    return search_tavily(f"{_clean_text(query)} companies", max_results=max_results)
