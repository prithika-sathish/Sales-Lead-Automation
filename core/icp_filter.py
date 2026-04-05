from __future__ import annotations

from typing import Any


def _icp_text(row: dict[str, Any]) -> str:
    return " ".join(
        [
            str(row.get("name") or ""),
            str(row.get("description") or ""),
            str(row.get("category") or ""),
            " ".join(str(t) for t in (row.get("tags") or [])),
        ]
    ).lower()


def _has_saas_subscription_signal(text: str) -> bool:
    core = ["saas", "subscription", "billing", "recurring", "api"]
    return any(word in text for word in core)


def _has_growth_signal(text: str) -> bool:
    growth = ["hiring", "launch", "beta", "scale", "scaling", "growth"]
    return any(word in text for word in growth)


def filter_companies_by_icp(companies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for row in companies:
        if not isinstance(row, dict):
            continue
        text = _icp_text(row)
        if not _has_saas_subscription_signal(text):
            continue
        if not _has_growth_signal(text):
            continue
        filtered.append(row)
    return filtered
