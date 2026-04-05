from __future__ import annotations

import logging
from typing import Any

from data_sources.aggregator import aggregate_candidates


logger = logging.getLogger(__name__)


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _to_signal_row(candidate: dict[str, Any]) -> dict[str, Any]:
    company_name = _clean_text(candidate.get("company_name"))
    domain = _clean_text(candidate.get("domain"))
    source = _clean_text(candidate.get("source"))
    signal_type = _clean_text(candidate.get("signal_type"))
    region = _clean_text(candidate.get("region"))
    industry = _clean_text(candidate.get("industry"))
    context = _clean_text(candidate.get("description") or candidate.get("snippet") or candidate.get("title"))

    return {
        "company_name": company_name,
        "domain": domain,
        "source": source,
        "source_type": _clean_text(candidate.get("source_type") or "serp"),
        "signal_type": signal_type,
        "region": region,
        "industry": industry,
        "context": context,
        "confidence_score": float(candidate.get("confidence_score") or 0.0),
        "website": _clean_text(candidate.get("website")),
        "tags": candidate.get("tags") if isinstance(candidate.get("tags"), list) else [],
    }


async def aggregate_companies_with_metrics(
    queries: list[dict[str, str]],
    *,
    per_query_limit: int = 12,
    target_min: int = 20,
    query_timeout_seconds: float = 20.0,
    max_concurrent_queries: int = 4,
) -> dict[str, Any]:
    acquisition = await aggregate_candidates(
        queries,
        target_min=max(1, target_min),
        per_query_limit=max(3, per_query_limit),
        query_timeout_seconds=max(5.0, float(query_timeout_seconds)),
        max_concurrent_queries=max(1, int(max_concurrent_queries)),
    )

    candidates = acquisition.get("candidates") if isinstance(acquisition.get("candidates"), list) else []
    metrics = acquisition.get("metrics") if isinstance(acquisition.get("metrics"), dict) else {}

    rows = [_to_signal_row(item) for item in candidates if isinstance(item, dict)]
    rows = [row for row in rows if _clean_text(row.get("company_name"))]

    logger.info(
        "source_aggregator output | rows=%s queries=%s",
        len(rows),
        int(metrics.get("queries_executed") or 0),
    )

    return {
        "rows": rows,
        "metrics": metrics,
    }


async def aggregate_companies_from_sources(
    queries: list[dict[str, str]],
    *,
    per_query_limit: int = 12,
    target_min: int = 20,
    query_timeout_seconds: float = 20.0,
    max_concurrent_queries: int = 4,
) -> list[dict[str, Any]]:
    payload = await aggregate_companies_with_metrics(
        queries,
        per_query_limit=per_query_limit,
        target_min=target_min,
        query_timeout_seconds=query_timeout_seconds,
        max_concurrent_queries=max_concurrent_queries,
    )
    return payload.get("rows") if isinstance(payload.get("rows"), list) else []
