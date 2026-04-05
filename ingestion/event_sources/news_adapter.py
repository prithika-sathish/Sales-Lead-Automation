from __future__ import annotations

import asyncio
import importlib
import logging
from datetime import datetime, timezone
from typing import Any


logger = logging.getLogger(__name__)
_KEYWORDS = ["raised", "hiring", "expansion", "launch", "growth"]


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _infer_event_type(text: str) -> str | None:
    lowered = text.lower()
    if any(token in lowered for token in ["raised", "funding", "series a", "series b"]):
        return "funding"
    if any(token in lowered for token in ["partnership", "partners", "alliance"]):
        return "partnership"
    if any(token in lowered for token in ["expansion", "launch", "growth", "new office"]):
        return "expansion"
    return None


def _event_confidence(text: str) -> float:
    lowered = text.lower()
    weight = 0.55
    if any(token in lowered for token in ["series a", "series b", "raised"]):
        weight += 0.25
    if any(token in lowered for token in ["official", "announced", "press release"]):
        weight += 0.1
    return round(min(0.95, weight), 2)


async def _extract_article_text(url: str) -> tuple[dict[str, Any], str | None]:
    try:
        module = importlib.import_module("newsplease")
    except Exception as exc:  # noqa: BLE001
        return {}, f"import_error:{exc}"

    news_cls = getattr(module, "NewsPlease", None)
    if news_cls is None or not hasattr(news_cls, "from_url"):
        return {}, "missing_newsplease_api"

    try:
        article = await asyncio.to_thread(news_cls.from_url, url)
    except Exception as exc:  # noqa: BLE001
        return {}, f"runtime_error:{exc}"

    if article is None:
        return {}, "empty_article"

    title = _clean_text(getattr(article, "title", ""))
    maintext = _clean_text(getattr(article, "maintext", ""))
    date_publish = _clean_text(getattr(article, "date_publish", ""))
    if not title and not maintext:
        return {}, "empty_content"

    return {
        "title": title,
        "text": maintext,
        "published": date_publish,
    }, None


async def fetch_news_events(companies: list[str], *, seed_urls: list[str] | None = None, max_rows: int = 40) -> tuple[list[dict[str, Any]], int]:
    companies_norm = [c for c in (_clean_text(c) for c in companies) if c]
    urls = [u for u in (_clean_text(u) for u in (seed_urls or [])) if u]
    if not companies_norm or not urls:
        logger.warning("[EMPTY RESPONSE] news")
        return [], 0

    failures = 0
    tasks = [asyncio.create_task(_extract_article_text(url)) for url in urls]
    gathered = await asyncio.gather(*tasks, return_exceptions=True)

    events: list[dict[str, Any]] = []
    for idx, result in enumerate(gathered):
        url = urls[idx]
        if isinstance(result, Exception):
            failures += 1
            logger.warning("[SOURCE FAILED] news url=%s reason=exception detail=%s", url, result)
            continue

        payload, error = result
        if error:
            failures += 1
            logger.warning("[SOURCE FAILED] news url=%s reason=%s", url, error)
            continue

        text = f"{payload.get('title', '')} {payload.get('text', '')}"
        lowered = text.lower()
        if not any(keyword in lowered for keyword in _KEYWORDS):
            continue

        matched_companies = [company for company in companies_norm if company.lower() in lowered]
        if not matched_companies:
            continue

        event_type = _infer_event_type(text)
        if not event_type:
            continue

        for company in matched_companies:
            events.append(
                {
                    "company": company,
                    "event_type": event_type,
                    "confidence": _event_confidence(text),
                    "source": "news",
                    "timestamp": _clean_text(payload.get("published")) or datetime.now(timezone.utc).isoformat(),
                    "url": url,
                    "title": _clean_text(payload.get("title")),
                }
            )

    deduped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for item in events:
        key = (str(item.get("company", "")).lower(), str(item.get("event_type", "")).lower(), str(item.get("title", "")).lower())
        deduped[key] = item

    final = list(deduped.values())[: max(1, max_rows)]
    logger.info("[RESULT COUNT] news: %s", len(final))
    return final, failures
