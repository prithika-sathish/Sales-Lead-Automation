from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from collections.abc import Iterable
from typing import Any
from urllib import error, request

from app.apify_client import APIFY_ACTOR_ID
from core.discovery_playwright import discover_companies_from_jobs
from discovery.search_scraper import scrape_google_results


logger = logging.getLogger(__name__)


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalize_company_key(value: str) -> str:
    normalized = _clean_text(value).lower()
    normalized = re.sub(r"[^a-z0-9&.+\- ]", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def _is_meaningful_context(value: str) -> bool:
    text = _clean_text(value)
    return len(text) >= 8


def _dedupe_records(items: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for row in items:
        company = _clean_text(row.get("company"))
        role = _clean_text(row.get("role"))
        context = _clean_text(row.get("context"))
        source = _clean_text(row.get("source"))
        region = _clean_text(row.get("region"))
        if not company or not source:
            continue

        key = (_normalize_company_key(company), source.lower(), role.lower(), context.lower(), region.lower())
        if key in seen:
            continue
        seen.add(key)

        record = {"company": company, "source": source}
        if role:
            record["role"] = role
        if region:
            record["region"] = region
        if context:
            record["context"] = context
        deduped.append(record)
    return deduped


def _apify_run_actor(actor_id: str, token: str) -> list[dict[str, Any]]:
    endpoint = (
        f"https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items"
        f"?token={token}&timeout=300"
    )
    req = request.Request(endpoint, method="POST", headers={"Content-Type": "application/json"}, data=b"{}")
    with request.urlopen(req, timeout=300) as response:
        raw = response.read().decode("utf-8")

    payload: Any = json.loads(raw)
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("items", "data", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


async def fetch_companies_apify(actor_id: str) -> list[dict[str, str]]:
    token = os.getenv("APIFY_API_TOKEN")
    if not token:
        logger.warning("apify skipped: APIFY_API_TOKEN missing")
        return []

    actor = _clean_text(actor_id) or APIFY_ACTOR_ID
    try:
        rows = await asyncio.to_thread(_apify_run_actor, actor, token)
    except (error.HTTPError, error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        logger.warning("apify request failed: %s", exc)
        return []
    except Exception as exc:  # noqa: BLE001
        logger.warning("apify unexpected failure: %s", exc)
        return []

    records: list[dict[str, str]] = []
    for item in rows:
        company = _clean_text(item.get("company") or item.get("companyName") or item.get("name"))
        role = _clean_text(item.get("role") or item.get("title") or item.get("jobTitle"))
        region = _clean_text(item.get("region") or item.get("country") or item.get("location"))
        if not company:
            continue
        row = {"company": company, "source": "apify"}
        if role:
            row["role"] = role
        if region:
            row["region"] = region
        records.append(row)

    deduped = _dedupe_records(records)
    logger.info("apify companies: %s", len(deduped))
    return [
        {
            "company": row["company"],
            "role": row.get("role", ""),
            "source": "apify",
            "region": row.get("region", ""),
        }
        for row in deduped
    ]


async def fetch_companies_playwright(role: str, max_pages: int = 3) -> list[dict[str, str]]:
    rows = await discover_companies_from_jobs(role=role, max_pages=max_pages)
    normalized: list[dict[str, str]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        company = _clean_text(item.get("company"))
        role_name = _clean_text(item.get("role"))
        location = _clean_text(item.get("location"))
        if not company:
            continue

        row = {"company": company, "source": "playwright"}
        if role_name:
            row["role"] = role_name
        if location:
            row["region"] = location
            row["context"] = location
        normalized.append(row)

    deduped = _dedupe_records(normalized)
    logger.info("playwright companies: %s", len(deduped))
    return [
        {
            "company": row["company"],
            "role": row.get("role", ""),
            "source": "playwright",
            "region": row.get("region", ""),
            "context": row.get("context", ""),
        }
        for row in deduped
    ]


async def fetch_companies_search(query: str, region: str, max_pages: int = 2) -> list[dict[str, str]]:
    rows = await scrape_google_results(query=query, region=region, max_pages=max_pages)
    return [
        {
            "company": _clean_text(row.get("company")),
            "source": "serp",
            "region": _clean_text(row.get("region")),
            "context": _clean_text(row.get("snippet")),
            "role": "",
        }
        for row in rows
        if _clean_text(row.get("company"))
    ]


def merge_and_dedupe(sources: list[list[dict]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}

    for source_rows in sources:
        for row in source_rows:
            if not isinstance(row, dict):
                continue
            company = _clean_text(row.get("company"))
            source = _clean_text(row.get("source"))
            role = _clean_text(row.get("role"))
            context = _clean_text(row.get("context"))
            region = _clean_text(row.get("region"))
            if not company or not source:
                continue

            key = _normalize_company_key(company)
            if not key:
                continue

            bucket = merged.setdefault(
                key,
                {
                    "company": company,
                    "sources": [],
                    "roles": [],
                    "role_counts": {},
                    "regions": [],
                    "contexts": [],
                    "high_confidence": False,
                },
            )

            if source and source not in bucket["sources"]:
                bucket["sources"].append(source)
            if role and role not in bucket["roles"]:
                bucket["roles"].append(role)
            if role:
                role_counts = bucket.get("role_counts") if isinstance(bucket.get("role_counts"), dict) else {}
                role_counts[role] = int(role_counts.get(role, 0)) + 1
                bucket["role_counts"] = role_counts
            if region and region not in bucket["regions"]:
                bucket["regions"].append(region)
            if context and _is_meaningful_context(context) and context not in bucket["contexts"]:
                bucket["contexts"].append(context)

    output: list[dict[str, Any]] = []
    for item in merged.values():
        roles = [r for r in item.get("roles", []) if _clean_text(r)]
        contexts = [c for c in item.get("contexts", []) if _is_meaningful_context(c)]
        if not roles and not contexts:
            continue

        item["roles"] = roles
        item["contexts"] = contexts
        item["sources"] = sorted(item.get("sources", []))
        item["regions"] = sorted(item.get("regions", []))
        item["high_confidence"] = len(item["sources"]) >= 2
        output.append(item)

    output.sort(key=lambda row: str(row.get("company") or "").lower())
    return output
