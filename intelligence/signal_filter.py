from __future__ import annotations

import re
from typing import Any


KEEP_CATEGORIES = {
    "hiring",
    "funding",
    "product",
    "partnership",
    "expansion",
    "leadership_hire",
    "customer_feedback",
    "review",
}

GITHUB_BLOCKLIST = {"github", "repo", "repository", "commit", "pull request", "pr", "release"}

HIRE_PATTERNS = [
    "we're hiring",
    "we are hiring",
    "now hiring",
    "hiring",
    "open role",
    "open positions",
    "multiple roles",
    "head of",
    "vice president",
    "vp of",
    "director of",
    "lead engineer",
    "engineering manager",
    "senior engineer",
    "sales hire",
    "recruiting",
]

FUNDING_PATTERNS = [
    "funding",
    "raised",
    "series a",
    "series b",
    "series c",
    "seed round",
    "venture round",
    "investment",
    "backed by",
]

PRODUCT_PATTERNS = [
    "launch",
    "launched",
    "product update",
    "feature update",
    "new feature",
    "release",
    "beta",
    "product expansion",
    "api update",
    "integration",
    "roadmap",
]

PARTNERSHIP_PATTERNS = [
    "partnership",
    "partnered",
    "collaboration",
    "integration",
    "reseller",
    "channel partner",
    "strategic alliance",
]

EXPANSION_PATTERNS = [
    "expansion",
    "new market",
    "new office",
    "office opening",
    "international",
    "launching in",
    "entering",
    "geo expansion",
    "regional expansion",
]

LEADERSHIP_PATTERNS = [
    "appointed",
    "joins as",
    "hired as",
    "named",
    "promoted to",
    "chief",
    "cto",
    "ceo",
    "cfo",
    "cro",
    "coo",
    "vp sales",
    "head of",
]

CUSTOMER_FEEDBACK_PATTERNS = [
    "complaint",
    "review",
    "ratings",
    "frustrated",
    "disappointed",
    "pain point",
    "issue with",
    "doesn't work",
    "does not work",
    "would like",
    "wish",
    "missing",
    "alternative",
    "switching",
    "churn",
]


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower()).strip()


def _signal_text(signal: dict[str, Any]) -> str:
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    parts = [
        _safe_text(signal.get("raw_text")),
        _safe_text(signal.get("summary")),
        _safe_text(signal.get("text")),
        _safe_text(metadata.get("raw_text")),
        _safe_text(metadata.get("summary")),
        _safe_text(metadata.get("description")),
        _safe_text(metadata.get("title")),
    ]
    return _normalize(" ".join(part for part in parts if part))


def _source_label(signal: dict[str, Any]) -> str:
    return _normalize(_safe_text(signal.get("source") or signal.get("source_type") or signal.get("type")))


def _matches_any(text: str, patterns: list[str]) -> bool:
    return any(pattern in text for pattern in patterns)


def _business_intent_category(signal: dict[str, Any]) -> str | None:
    source = _source_label(signal)
    if source in GITHUB_BLOCKLIST:
        return None

    text = _signal_text(signal)
    if not text:
        return None

    if _matches_any(text, HIRE_PATTERNS):
        return "hiring"
    if _matches_any(text, FUNDING_PATTERNS):
        return "funding"
    if _matches_any(text, PRODUCT_PATTERNS):
        return "product"
    if _matches_any(text, PARTNERSHIP_PATTERNS):
        return "partnership"
    if _matches_any(text, EXPANSION_PATTERNS):
        return "expansion"
    if _matches_any(text, LEADERSHIP_PATTERNS):
        return "leadership_hire"
    if _matches_any(text, CUSTOMER_FEEDBACK_PATTERNS):
        return "customer_feedback"

    return None


def _dedupe_key(signal: dict[str, Any]) -> tuple[str, str]:
    source = _source_label(signal)
    text = _signal_text(signal)
    if not text:
        text = _normalize(_safe_text(signal.get("summary") or signal.get("raw_text") or signal.get("name")))
    text = text[:180]
    return source, text


def filter_raw_signals(raw_signals: list[dict[str, Any]]) -> dict[str, Any]:
    kept: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for signal in raw_signals:
        if not isinstance(signal, dict):
            continue

        source = _source_label(signal)
        if source in GITHUB_BLOCKLIST:
            continue

        category = _business_intent_category(signal)
        if category is None:
            continue

        key = _dedupe_key(signal)
        if key in seen:
            continue
        seen.add(key)

        cleaned = dict(signal)
        cleaned["signal_type"] = category
        cleaned["type"] = category
        cleaned["source"] = source or _safe_text(signal.get("source"))
        cleaned.setdefault("metadata", {})
        if isinstance(cleaned.get("metadata"), dict):
            cleaned["metadata"] = {**cleaned["metadata"], "filtered_category": category}
        kept.append(cleaned)

    return {"filtered_signals": kept}
