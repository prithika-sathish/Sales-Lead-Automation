from __future__ import annotations

import asyncio
import logging
from typing import Any

from ingestion.event_sources.gdelt_adapter import fetch_gdelt_events
from ingestion.event_sources.news_adapter import fetch_news_events
from ingestion.job_sources.greenhouse_adapter import fetch_greenhouse_signals
from ingestion.job_sources.jobspy_adapter import fetch_jobspy_signals


logger = logging.getLogger(__name__)


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _company_key(value: str) -> str:
    return _clean_text(value).lower()


def _domain_from_company(value: str) -> str:
    token = _clean_text(value).lower().replace(" ", "")
    if not token:
        return ""
    return f"{token}.com"


def _normalize_unified(
    companies: list[str],
    hiring_rows: list[dict[str, Any]],
    event_rows: list[dict[str, Any]],
    tech_rows: list[dict[str, Any]],
    *,
    max_leads: int,
) -> list[dict[str, Any]]:
    selected_companies = [c for c in (_clean_text(c) for c in companies) if c]
    output: dict[str, dict[str, Any]] = {}

    for company in selected_companies:
        output[_company_key(company)] = {
            "company": company,
            "signals": {
                "hiring": [],
                "events": [],
                "tech": [],
            },
        }

    for row in hiring_rows:
        company = _clean_text(row.get("company"))
        if not company:
            continue
        key = _company_key(company)
        node = output.setdefault(
            key,
            {
                "company": company,
                "signals": {"hiring": [], "events": [], "tech": []},
            },
        )
        node["signals"]["hiring"].append(row)

    for row in event_rows:
        company = _clean_text(row.get("company"))
        if not company:
            continue
        key = _company_key(company)
        node = output.setdefault(
            key,
            {
                "company": company,
                "signals": {"hiring": [], "events": [], "tech": []},
            },
        )
        node["signals"]["events"].append(
            {
                "event_type": _clean_text(row.get("event_type")),
                "confidence": float(row.get("confidence") or 0.0),
                "source": _clean_text(row.get("source")),
                "timestamp": _clean_text(row.get("timestamp")),
                "url": _clean_text(row.get("url")),
                "title": _clean_text(row.get("title")),
            }
        )

    for row in tech_rows:
        company = _clean_text(row.get("company"))
        if not company:
            continue
        key = _company_key(company)
        node = output.setdefault(
            key,
            {
                "company": company,
                "signals": {"hiring": [], "events": [], "tech": []},
            },
        )
        node["signals"]["tech"].append(
            {
                "stack": row.get("tech_stack") if isinstance(row.get("tech_stack"), list) else [],
                "source": _clean_text(row.get("source")),
            }
        )

    rows = list(output.values())
    # Keep companies that have at least one real signal.
    rows = [
        item
        for item in rows
        if item["signals"]["hiring"] or item["signals"]["events"] or item["signals"]["tech"]
    ]

    rows.sort(
        key=lambda item: (
            len(item["signals"]["hiring"]),
            len(item["signals"]["events"]),
            len(item["signals"]["tech"]),
            item["company"].lower(),
        ),
        reverse=True,
    )
    return rows[: max(1, int(max_leads))]


async def run_ingestion(payload: dict[str, Any], *, strict_real_only: bool = False) -> list[dict[str, Any]]:
    companies = payload.get("companies") if isinstance(payload.get("companies"), list) else []
    companies = [_clean_text(c) for c in companies if _clean_text(c)]
    regions = payload.get("regions") if isinstance(payload.get("regions"), list) else []
    regions = [_clean_text(r) for r in regions if _clean_text(r)]
    max_leads = int(payload.get("max_leads") or 10)

    if not companies:
        logger.warning("[EMPTY RESPONSE] ingestion companies")
        return []

    jobspy_task = asyncio.create_task(fetch_jobspy_signals(companies, regions=regions, max_rows=max_leads * 3))
    greenhouse_task = asyncio.create_task(fetch_greenhouse_signals(companies, max_rows=max_leads * 3))
    gdelt_task = asyncio.create_task(fetch_gdelt_events(companies, max_rows=max_leads * 4))

    gathered = await asyncio.gather(jobspy_task, greenhouse_task, gdelt_task, return_exceptions=True)

    job_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    failures = {"jobspy": 0, "greenhouse": 0, "gdelt": 0, "news": 0}

    if not isinstance(gathered[0], Exception):
        data, failed = gathered[0]
        job_rows.extend(data)
        failures["jobspy"] += int(failed)
    else:
        failures["jobspy"] += 1
        logger.warning("[SOURCE FAILED] jobspy reason=exception detail=%s", gathered[0])

    if not isinstance(gathered[1], Exception):
        data, failed = gathered[1]
        job_rows.extend(data)
        failures["greenhouse"] += int(failed)
    else:
        failures["greenhouse"] += 1
        logger.warning("[SOURCE FAILED] greenhouse reason=exception detail=%s", gathered[1])

    gdelt_rows: list[dict[str, Any]] = []
    if not isinstance(gathered[2], Exception):
        gdelt_rows, failed = gathered[2]
        event_rows.extend(gdelt_rows)
        failures["gdelt"] += int(failed)
    else:
        failures["gdelt"] += 1
        logger.warning("[SOURCE FAILED] gdelt reason=exception detail=%s", gathered[2])

    news_seed_urls = [str(row.get("url") or "") for row in gdelt_rows if isinstance(row, dict) and _clean_text(row.get("url"))]
    news_rows, news_failed = await fetch_news_events(companies, seed_urls=news_seed_urls, max_rows=max_leads * 3)
    event_rows.extend(news_rows)
    failures["news"] += int(news_failed)

    # BuiltWith is a paid API and intentionally disabled from this zero-cost pipeline.
    tech_rows: list[dict[str, Any]] = []
    logger.info("[SOURCE DISABLED] builtwith reason=paid_api")

    unified = _normalize_unified(companies, job_rows, event_rows, tech_rows, max_leads=max_leads)

    logger.info("[RESULT COUNT] jobspy: %s", len([r for r in job_rows if _clean_text(r.get('source')) == 'jobspy']))
    logger.info("[RESULT COUNT] greenhouse: %s", len([r for r in job_rows if _clean_text(r.get('source')) == 'greenhouse']))
    logger.info("[RESULT COUNT] events: %s", len(event_rows))
    logger.info("[RESULT COUNT] tech: %s", len(tech_rows))
    logger.info("[FAILURES] %s", failures)

    if not unified:
        logger.warning("[EMPTY RESPONSE] unified_ingestion")

    if strict_real_only and not unified:
        return []

    return unified
