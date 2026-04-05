from __future__ import annotations

import asyncio
import importlib
import logging
import re
from datetime import datetime, timezone
from typing import Any


logger = logging.getLogger(__name__)


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalize_company(value: str) -> str:
    cleaned = _clean_text(value)
    if not cleaned:
        return ""
    return re.sub(r"\s+", " ", cleaned)


def _row_to_signal(row: dict[str, Any]) -> dict[str, str]:
    company = _normalize_company(_clean_text(row.get("company") or row.get("company_name") or row.get("employer_name")))
    role = _clean_text(row.get("title") or row.get("job_title") or row.get("position"))
    location = _clean_text(row.get("location") or row.get("city") or row.get("region"))
    if not company or not role:
        return {}
    return {
        "company": company,
        "role": role,
        "location": location,
        "source": "jobspy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def fetch_jobspy_signals(companies: list[str], *, regions: list[str] | None = None, max_rows: int = 25) -> tuple[list[dict[str, str]], int]:
    regions = [r for r in (regions or []) if _clean_text(r)]
    try:
        module = importlib.import_module("jobspy")
    except Exception as exc:  # noqa: BLE001
        logger.warning("[SOURCE FAILED] jobspy reason=import_error detail=%s", exc)
        return [], 1

    scrape_jobs = getattr(module, "scrape_jobs", None)
    if not callable(scrape_jobs):
        logger.warning("[SOURCE FAILED] jobspy reason=missing_scrape_jobs")
        return [], 1

    keywords = [c for c in (_clean_text(c) for c in companies) if c]
    if not keywords:
        return [], 0

    failures = 0
    signals: list[dict[str, str]] = []

    for keyword in keywords:
        search_term = keyword
        if regions:
            search_term = f"{keyword} {' '.join(regions[:2])}"

        try:
            frame = await asyncio.to_thread(
                scrape_jobs,
                site_name=["linkedin", "indeed", "glassdoor"],
                search_term=search_term,
                results_wanted=max(10, max_rows),
                country_indeed="India",
                hours_old=72,
            )
        except Exception as exc:  # noqa: BLE001
            failures += 1
            logger.warning("[SOURCE FAILED] jobspy reason=runtime_error detail=%s", exc)
            continue

        rows: list[dict[str, Any]] = []
        if hasattr(frame, "to_dict"):
            try:
                rows = frame.to_dict(orient="records")
            except Exception as exc:  # noqa: BLE001
                failures += 1
                logger.warning("[SOURCE FAILED] jobspy reason=schema_parse_error detail=%s", exc)
                continue

        if not isinstance(rows, list) or not rows:
            logger.warning("[EMPTY RESPONSE] jobspy")
            continue

        for row in rows:
            if not isinstance(row, dict):
                continue
            signal = _row_to_signal(row)
            if signal:
                signals.append(signal)

    # Deterministic de-dupe by company-role-location.
    unique: dict[tuple[str, str, str], dict[str, str]] = {}
    for item in signals:
        key = (item["company"].lower(), item["role"].lower(), item.get("location", "").lower())
        unique[key] = item

    final = list(unique.values())[: max(1, max_rows)]
    logger.info("[RESULT COUNT] jobspy: %s", len(final))
    return final, failures
