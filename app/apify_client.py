from __future__ import annotations

import json
import os
from typing import Any
from urllib import error, request

from dotenv import load_dotenv


APIFY_ACTOR_ID = "kenny256~leads-generator"

load_dotenv()
if not os.getenv("APIFY_API_TOKEN"):
    load_dotenv(".env.example")


def _extract_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if isinstance(payload, dict):
        for key in ("items", "data", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

    return []


def _normalize_signal_source(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "company": item.get("company")
        or item.get("companyName")
        or item.get("name")
        or item.get("title")
        or "Unknown company",
        "description": item.get("description")
        or item.get("text")
        or item.get("content")
        or item.get("activityText")
        or "",
        "source": item.get("source")
        or item.get("platform")
        or item.get("origin")
        or "",
        "activity_text": item.get("activityText")
        or item.get("activity")
        or item.get("text")
        or item.get("content")
        or "",
        "url": item.get("url") or item.get("link") or "",
    }


def fetch_apify_query(query: str, limit: int = 10) -> list[dict[str, Any]]:
    api_token = os.getenv("APIFY_API_TOKEN")
    if not api_token:
        raise RuntimeError("APIFY_API_TOKEN is not set")

    actor_url = (
        f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/run-sync-get-dataset-items"
        f"?token={api_token}&timeout=300"
    )
    payload = json.dumps({"query": query, "limit": limit}).encode("utf-8")
    req = request.Request(
        actor_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=300) as response:
            raw = response.read().decode("utf-8")
    except error.HTTPError as exc:
        raise RuntimeError(f"Apify request failed with status {exc.code}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Apify request failed: {exc.reason}") from exc

    payload_json: Any = json.loads(raw)
    items = _extract_items(payload_json)
    normalized = [_normalize_signal_source(item) for item in items[:limit]]
    if not normalized:
        raise RuntimeError("Apify returned no usable results")
    return normalized


def fetch_from_apify(domain: str, limit: int = 4) -> list[dict[str, Any]]:
    query = f"{domain} companies startups"
    return fetch_apify_query(query, limit=limit)


def merge_results(domains: list[str], per_domain_limit: int = 4, total_limit: int = 15) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen_companies: set[str] = set()

    for domain in domains:
        if len(merged) >= total_limit:
            break

        items = fetch_from_apify(domain, limit=per_domain_limit)
        for item in items:
            company_name = str(item.get("company", "")).strip()
            if not company_name:
                continue

            dedupe_key = company_name.lower()
            if dedupe_key in seen_companies:
                continue

            seen_companies.add(dedupe_key)
            merged.append(
                {
                    "company": company_name,
                    "matched_domain": domain,
                    "description": str(item.get("description", "")),
                    "source": str(item.get("source", "")),
                    "activity_text": str(item.get("activity_text", "")),
                    "url": str(item.get("url", "")),
                }
            )

            if len(merged) >= total_limit:
                break

    if not merged:
        raise RuntimeError("Apify returned no usable results")

    return merged


def fetch_apify_signals(query: str, limit: int = 10) -> list[dict[str, Any]]:
    return fetch_apify_query(query, limit=limit)