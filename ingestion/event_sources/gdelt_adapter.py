from __future__ import annotations

import asyncio
import importlib
import logging
from datetime import datetime, timezone
from typing import Any

httpx = importlib.import_module("httpx")


logger = logging.getLogger(__name__)
_GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
_EVENT_TERMS: dict[str, list[str]] = {
    "funding": ["raised", "funding", "series a", "series b", "investment"],
    "expansion": ["expansion", "opens", "launches", "new office", "scale"],
    "partnership": ["partnership", "partners with", "collaboration", "alliance"],
}


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _infer_event_type(text: str) -> str | None:
    lowered = text.lower()
    for event_type, terms in _EVENT_TERMS.items():
        if any(term in lowered for term in terms):
            return event_type
    return None


def _extract_confidence(text: str, matched_event: str | None) -> float:
    if not matched_event:
        return 0.0
    lowered = text.lower()
    if matched_event == "funding" and any(k in lowered for k in ["series a", "series b", "raised"]):
        return 0.85
    if matched_event == "expansion" and any(k in lowered for k in ["expansion", "launches", "new office"]):
        return 0.75
    if matched_event == "partnership" and any(k in lowered for k in ["partnership", "partners", "alliance"]):
        return 0.7
    return 0.6


async def _fetch_company_events(client: Any, company: str) -> tuple[list[dict[str, Any]], str | None]:
    query = f'"{company}" AND (funding OR expansion OR partnership OR raised OR hires OR launch)'
    params = {
        "query": query,
        "mode": "ArtList",
        "maxrecords": "50",
        "format": "json",
    }
    for attempt in range(3):
        try:
            response = await client.get(_GDELT_DOC_API, params=params)
        except httpx.TimeoutException:
            if attempt < 2:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            return [], "timeout"
        except httpx.TransportError as exc:
            if attempt < 2:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            return [], f"transport_error:{exc}"

        if response.status_code in {429, 500, 502, 503, 504} and attempt < 2:
            await asyncio.sleep(0.7 * (attempt + 1))
            continue
        if response.status_code >= 400:
            return [], f"status_{response.status_code}"

        try:
            payload: Any = response.json()
        except Exception as exc:  # noqa: BLE001
            if attempt < 2:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            return [], f"invalid_json:{exc}"
        break
    else:
        return [], "retries_exhausted"

    articles = payload.get("articles") if isinstance(payload, dict) else None
    if not isinstance(articles, list):
        return [], "schema_invalid"

    events: list[dict[str, Any]] = []
    for article in articles:
        if not isinstance(article, dict):
            continue
        title = _clean_text(article.get("title"))
        snippet = _clean_text(article.get("seendate") or article.get("socialimage") or "")
        text = f"{title} {snippet}"
        event_type = _infer_event_type(text)
        if not event_type:
            continue
        events.append(
            {
                "company": company,
                "event_type": event_type,
                "confidence": _extract_confidence(text, event_type),
                "source": "gdelt",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "url": _clean_text(article.get("url")),
                "title": title,
            }
        )

    return events, None


async def fetch_gdelt_events(companies: list[str], *, max_rows: int = 40) -> tuple[list[dict[str, Any]], int]:
    company_list = [c for c in (_clean_text(c) for c in companies) if c]
    if not company_list:
        return [], 0

    failures = 0
    timeout = httpx.Timeout(5.0, read=5.0)
    signals: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        tasks = [asyncio.create_task(_fetch_company_events(client, company)) for company in company_list]
        gathered = await asyncio.gather(*tasks, return_exceptions=True)
        for idx, result in enumerate(gathered):
            company = company_list[idx]
            if isinstance(result, Exception):
                failures += 1
                logger.warning("[SOURCE FAILED] gdelt company=%s reason=exception detail=%s", company, result)
                continue
            rows, error = result
            if error:
                failures += 1
                logger.warning("[SOURCE FAILED] gdelt company=%s reason=%s", company, error)
                continue
            if not rows:
                logger.warning("[EMPTY RESPONSE] gdelt company=%s", company)
                continue
            signals.extend(rows)

    deduped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for item in signals:
        key = (str(item.get("company", "")).lower(), str(item.get("event_type", "")).lower(), str(item.get("title", "")).lower())
        deduped[key] = item

    final = list(deduped.values())[: max(1, max_rows)]
    logger.info("[RESULT COUNT] gdelt: %s", len(final))
    return final, failures
