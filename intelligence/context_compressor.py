from __future__ import annotations

from collections import defaultdict
from typing import Any


MAX_COMPRESSED_ENTRIES = 15


def _safe_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return default


def _extract_key_attributes(signal_type: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    attrs: dict[str, Any] = {}

    if signal_type in {"hiring", "hiring_spike", "sales_expansion"}:
        role_counts: dict[str, int] = defaultdict(int)
        seniority_counts: dict[str, int] = defaultdict(int)
        for item in items:
            metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
            role = str(metadata.get("role") or metadata.get("title") or "").strip().lower()
            seniority = str(metadata.get("seniority") or "").strip().lower()
            if role:
                role_counts[role] += 1
            if seniority:
                seniority_counts[seniority] += 1
        if role_counts:
            attrs["roles"] = dict(sorted(role_counts.items(), key=lambda kv: kv[1], reverse=True)[:4])
        if seniority_counts:
            attrs["seniority"] = dict(sorted(seniority_counts.items(), key=lambda kv: kv[1], reverse=True)[:3])

    if signal_type in {"github_activity", "dev_activity"}:
        repo_count = 0
        for item in items:
            metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
            if metadata.get("repo") or metadata.get("repository"):
                repo_count += 1
        if repo_count:
            attrs["repo_mentions"] = repo_count

    if signal_type == "integration_added":
        integration_keywords: dict[str, int] = defaultdict(int)
        for item in items:
            text = str(item.get("raw_text") or "").lower()
            for keyword in ["slack", "salesforce", "hubspot", "zapier", "shopify", "stripe"]:
                if keyword in text:
                    integration_keywords[keyword] += 1
        if integration_keywords:
            attrs["integrations"] = dict(sorted(integration_keywords.items(), key=lambda kv: kv[1], reverse=True)[:4])

    return attrs


def compress_company_context(company_row: dict[str, Any]) -> dict[str, Any]:
    company = str(company_row.get("company") or "").strip()
    signals = company_row.get("signals") if isinstance(company_row.get("signals"), list) else []
    normalized = [sig for sig in signals if isinstance(sig, dict)]

    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for signal in normalized:
        signal_type = str(signal.get("signal_type") or "").strip()
        if not signal_type:
            continue
        buckets[signal_type].append(signal)

    compressed_signals: list[dict[str, Any]] = []
    for signal_type, items in buckets.items():
        total_score = sum(_safe_int(item.get("final_score"), 0) for item in items)
        max_recency = max((_safe_int(item.get("recency_score"), 1) for item in items), default=1)
        avg_intensity = round(total_score / max(1, len(items)))

        compressed_signals.append(
            {
                "signal_type": signal_type,
                "intensity": max(1, avg_intensity),
                "count": len(items),
                "max_recency": max_recency,
                "attributes": _extract_key_attributes(signal_type, items),
            }
        )

    compressed_signals.sort(
        key=lambda item: (int(item.get("intensity") or 0), int(item.get("count") or 0), int(item.get("max_recency") or 0)),
        reverse=True,
    )

    derived = company_row.get("derived_signals") if isinstance(company_row.get("derived_signals"), list) else []
    trends = company_row.get("trend_signals") if isinstance(company_row.get("trend_signals"), list) else []
    topics = company_row.get("topics") if isinstance(company_row.get("topics"), list) else []

    unique_derived = [str(item).strip() for item in derived if str(item).strip()]
    unique_trends = [str(item).strip() for item in trends if str(item).strip()]
    clean_topics = [str(item).strip() for item in topics if str(item).strip()]

    return {
        "company": company,
        "compressed_signals": compressed_signals[:MAX_COMPRESSED_ENTRIES],
        "derived_signals": sorted(set(unique_derived)),
        "trend_signals": sorted(set(unique_trends)),
        "topics": clean_topics[:8],
    }