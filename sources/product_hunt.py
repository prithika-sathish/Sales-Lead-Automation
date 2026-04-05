from __future__ import annotations

from typing import Any

from sources.apify_common import run_apify_actor


PRODUCT_HUNT_ACTOR = "apify/producthunt-scraper"
GOOGLE_SEARCH_ACTOR = "apify/google-search-scraper"


def _expand_search_rows(raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expanded: list[dict[str, Any]] = []
    for row in raw_rows:
        organic = row.get("organicResults")
        if isinstance(organic, list) and organic:
            for item in organic:
                if isinstance(item, dict):
                    expanded.append(item)
        else:
            expanded.append(row)
    return expanded


def _normalize_product_hunt_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": str(row.get("name") or row.get("title") or row.get("productName") or "").strip(),
        "website": str(row.get("website") or row.get("websiteUrl") or row.get("url") or "").strip(),
        "tagline": str(row.get("tagline") or row.get("description") or "").strip(),
        "source": "product_hunt",
    }


def fetch_product_hunt_leads(query: str, limit: int = 100) -> list[dict[str, Any]]:
    payload = {
        "search": query,
        "maxItems": max(10, min(limit, 100)),
        "sort": "recent",
    }
    try:
        raw_rows = run_apify_actor(PRODUCT_HUNT_ACTOR, payload)
    except Exception:
        raw_rows = []

    if not raw_rows:
        fallback_payload = {
            "queries": f"site:producthunt.com {query} startups",
            "resultsPerPage": max(10, min(limit, 40)),
            "maxPagesPerQuery": 1,
        }
        try:
            raw_rows = run_apify_actor(GOOGLE_SEARCH_ACTOR, fallback_payload)
        except Exception:
            raw_rows = []

    raw_rows = _expand_search_rows(raw_rows)

    rows: list[dict[str, Any]] = []
    for raw in raw_rows:
        row = _normalize_product_hunt_row(
            {
                "name": raw.get("name") or raw.get("title") or raw.get("organicTitle") or "",
                "website": raw.get("website") or raw.get("url") or raw.get("link") or "",
                "tagline": raw.get("tagline") or raw.get("description") or raw.get("snippet") or "",
            }
        )
        if row["name"]:
            rows.append(row)
    return rows
