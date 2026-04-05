from __future__ import annotations

import asyncio
import importlib
import logging
import re
from datetime import datetime, timezone
from typing import Any

httpx = importlib.import_module("httpx")


logger = logging.getLogger(__name__)
_GREENHOUSE_BOARD_URL = "https://boards-api.greenhouse.io/v1/boards/{token}/jobs"


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _tokenize_company(value: str) -> str:
    lowered = _clean_text(value).lower()
    if not lowered:
        return ""
    lowered = lowered.replace(".com", "").replace(".io", "").replace(".in", "")
    lowered = re.sub(r"[^a-z0-9]+", "", lowered)
    return lowered


def _row_to_signal(company: str, row: dict[str, Any]) -> dict[str, str]:
    role = _clean_text(row.get("title"))
    location_obj = row.get("location") if isinstance(row.get("location"), dict) else {}
    location = _clean_text(location_obj.get("name"))
    if not role:
        return {}
    return {
        "company": company,
        "role": role,
        "location": location,
        "source": "greenhouse",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def _fetch_token_jobs(client: Any, token: str) -> tuple[list[dict[str, Any]], str | None]:
    url = _GREENHOUSE_BOARD_URL.format(token=token)
    for attempt in range(3):
        try:
            response = await client.get(url)
        except httpx.TimeoutException:
            if attempt < 2:
                await asyncio.sleep(0.4 * (attempt + 1))
                continue
            return [], "timeout"
        except httpx.TransportError as exc:
            if attempt < 2:
                await asyncio.sleep(0.4 * (attempt + 1))
                continue
            return [], f"transport_error:{exc}"

        if response.status_code in {429, 500, 502, 503, 504} and attempt < 2:
            await asyncio.sleep(0.4 * (attempt + 1))
            continue
        if response.status_code >= 400:
            return [], f"status_{response.status_code}"

        try:
            payload: Any = response.json()
        except Exception as exc:  # noqa: BLE001
            if attempt < 2:
                await asyncio.sleep(0.4 * (attempt + 1))
                continue
            return [], f"invalid_json:{exc}"
        break
    else:
        return [], "retries_exhausted"

    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    if not isinstance(jobs, list):
        return [], "schema_invalid"
    return [row for row in jobs if isinstance(row, dict)], None


async def fetch_greenhouse_signals(companies: list[str], *, max_rows: int = 25) -> tuple[list[dict[str, str]], int]:
    company_list = [c for c in (_clean_text(c) for c in companies) if c]
    if not company_list:
        return [], 0

    failures = 0
    signals: list[dict[str, str]] = []
    timeout = httpx.Timeout(5.0, read=5.0)

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        tasks = []
        mapping: list[tuple[str, str]] = []
        for company in company_list:
            token = _tokenize_company(company)
            if not token:
                continue
            tasks.append(asyncio.create_task(_fetch_token_jobs(client, token)))
            mapping.append((company, token))

        gathered = await asyncio.gather(*tasks, return_exceptions=True)
        for idx, result in enumerate(gathered):
            company, token = mapping[idx]
            if isinstance(result, Exception):
                failures += 1
                logger.warning("[SOURCE FAILED] greenhouse token=%s reason=exception detail=%s", token, result)
                continue

            rows, error = result
            if error:
                failures += 1
                logger.warning("[SOURCE FAILED] greenhouse token=%s reason=%s", token, error)
                continue
            if not rows:
                logger.warning("[EMPTY RESPONSE] greenhouse token=%s", token)
                continue

            for row in rows:
                signal = _row_to_signal(company, row)
                if signal:
                    signals.append(signal)

    unique: dict[tuple[str, str, str], dict[str, str]] = {}
    for item in signals:
        key = (item["company"].lower(), item["role"].lower(), item.get("location", "").lower())
        unique[key] = item

    final = list(unique.values())[: max(1, max_rows)]
    logger.info("[RESULT COUNT] greenhouse: %s", len(final))
    return final, failures
