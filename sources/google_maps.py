from __future__ import annotations

from typing import Any

from sources.apify_common import run_apify_actor


GOOGLE_MAPS_ACTOR = "apify/google-maps-scraper"
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


def _normalize_maps_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": str(row.get("title") or row.get("name") or row.get("business_name") or "").strip(),
        "source": "google_maps",
        "website": str(row.get("website") or row.get("domain") or row.get("url") or "").strip(),
        "category": str(row.get("category") or row.get("type") or "").strip(),
        "location": str(row.get("address") or row.get("location") or "").strip(),
    }


def fetch_google_maps_leads(query: str, location: str = "", limit: int = 100) -> list[dict[str, Any]]:
    try:
        payload = {
            "searchStringsArray": [query],
            "maxCrawledPlacesPerSearch": max(10, min(limit, 120)),
            "language": "en",
            "includeWebResults": False,
        }
        if location.strip():
            payload["locationQuery"] = location.strip()

        raw_rows = run_apify_actor(GOOGLE_MAPS_ACTOR, payload)
    except Exception:
        raw_rows = []

    if not raw_rows:
        search_query = f"{query} company directory {location}".strip()
        fallback_payload = {
            "queries": search_query,
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
        normalized = _normalize_maps_row(
            {
                "name": raw.get("name") or raw.get("title") or raw.get("organicTitle") or "",
                "website": raw.get("website") or raw.get("url") or raw.get("link") or "",
                "address": raw.get("address") or raw.get("displayedUrl") or "",
                "category": raw.get("category") or raw.get("type") or "",
            }
        )
        if normalized["name"]:
            rows.append(normalized)
    return rows
