from __future__ import annotations

import os

from sources.apify_common import run_apify_actor


INDEED_ACTOR = os.getenv("APIFY_INDEED_ACTOR", "apify/indeed-scraper")
NAUKRI_ACTOR = os.getenv("APIFY_NAUKRI_ACTOR", "apify/naukri-scraper")
GOOGLE_SEARCH_ACTOR = "apify/google-search-scraper"


def _expand_search_rows(raw_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    expanded: list[dict[str, object]] = []
    for row in raw_rows:
        organic = row.get("organicResults")
        if isinstance(organic, list) and organic:
            for item in organic:
                if isinstance(item, dict):
                    expanded.append(item)
        else:
            expanded.append(row)
    return expanded


def _normalize_job_row(row: dict[str, object], source_name: str) -> dict[str, str]:
    return {
        "name": str(
            row.get("company")
            or row.get("companyName")
            or row.get("company_name")
            or row.get("employer")
            or ""
        ).strip(),
        "website": str(row.get("website") or row.get("companyWebsite") or "").strip(),
        "source": source_name,
        "title": str(row.get("title") or row.get("jobTitle") or "").strip(),
    }


def _fetch_jobs_from_actor(actor: str, source_name: str, query: str, limit: int) -> list[dict[str, str]]:
    payload = {
        "query": query,
        "search": query,
        "maxItems": max(10, min(limit, 120)),
        "maxRows": max(10, min(limit, 120)),
    }
    try:
        raw_rows = run_apify_actor(actor, payload)
    except Exception:
        raw_rows = []

    if not raw_rows:
        site_hint = "indeed.com" if source_name == "indeed" else "naukri.com"
        fallback_payload = {
            "queries": f"site:{site_hint} {query} hiring companies",
            "resultsPerPage": max(10, min(limit, 40)),
            "maxPagesPerQuery": 1,
        }
        try:
            raw_rows = run_apify_actor(GOOGLE_SEARCH_ACTOR, fallback_payload)
        except Exception:
            raw_rows = []

    raw_rows = _expand_search_rows(raw_rows)

    rows: list[dict[str, str]] = []
    for raw in raw_rows:
        row = _normalize_job_row(
            {
                "companyName": raw.get("companyName") or raw.get("company") or raw.get("name") or raw.get("title") or raw.get("organicTitle") or "",
                "website": raw.get("website") or raw.get("url") or raw.get("link") or "",
                "title": raw.get("title") or raw.get("jobTitle") or raw.get("organicTitle") or "",
            },
            source_name,
        )
        if row["name"]:
            rows.append(row)
    return rows


def fetch_jobs_leads(query: str, limit: int = 120) -> list[dict[str, str]]:
    indeed_rows = _fetch_jobs_from_actor(INDEED_ACTOR, "indeed", query, limit)
    naukri_rows = _fetch_jobs_from_actor(NAUKRI_ACTOR, "naukri", query, limit)
    if indeed_rows or naukri_rows:
        return indeed_rows + naukri_rows
    return []
