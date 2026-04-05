from __future__ import annotations

from typing import Any


STRONG_SOURCE_TYPES = {
    "linkedin",
    "hiring",
    "news",
    "github",
    "website",
    "funding",
    "product",
    "content",
    "docs",
    "traffic",
    "review",
}

WEAK_SOURCE_TYPES = {
    "reddit",
    "forum",
    "comment",
}

EVENT_STRENGTH_MAP = {
    "Infra scaling": {"hiring_spike", "infra_scaling", "dev_activity", "github_activity"},
    "Market expansion": {"funding", "news", "company_update", "sales_expansion", "growth_phase", "marketing_expansion"},
    "Product launch": {"product_launch", "feature_update", "integration_added", "api_update", "platform_expansion"},
    "Hiring acceleration": {"hiring", "hiring_spike", "sales_expansion"},
    "Customer pressure": {"customer_pain", "feature_requests", "product_gap", "complaint"},
    "Engagement momentum": {"momentum", "content_push", "traffic_growth", "narrative_trend", "strategic_focus"},
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _event_source_groups(event: dict[str, Any], signals_by_source: dict[str, list[dict[str, Any]]] | None) -> dict[str, list[dict[str, Any]]]:
    if signals_by_source:
        return {str(source).strip().lower(): [sig for sig in signals if isinstance(sig, dict)] for source, signals in signals_by_source.items()}

    grouped: dict[str, list[dict[str, Any]]] = {}
    for signal in event.get("supporting_signals", []):
        if not isinstance(signal, dict):
            continue
        source_type = str(signal.get("source_type") or signal.get("source") or "unknown").strip().lower()
        grouped.setdefault(source_type, []).append(signal)
    return grouped


def _event_signal_types(event: dict[str, Any]) -> set[str]:
    signal_types: set[str] = set()
    for signal in event.get("supporting_signals", []):
        if not isinstance(signal, dict):
            continue
        signal_type = str(signal.get("signal_type") or "").strip()
        if signal_type:
            signal_types.add(signal_type)
    return signal_types


def _source_strength(source_type: str, signals: list[dict[str, Any]]) -> float:
    if source_type in WEAK_SOURCE_TYPES:
        return 0.35

    if source_type in STRONG_SOURCE_TYPES:
        return 1.0

    avg_score = 0.0
    if signals:
        avg_score = sum(_safe_float(sig.get("final_score") or sig.get("signal_score"), 0.0) for sig in signals) / max(1, len(signals))
    if avg_score >= 20:
        return 0.9
    if avg_score >= 12:
        return 0.75
    return 0.6


def _supporting_source_types(event: dict[str, Any], signals_by_source: dict[str, list[dict[str, Any]]] | None) -> tuple[set[str], int]:
    groups = _event_source_groups(event, signals_by_source)
    source_types: set[str] = set()
    strong_sources = 0
    for source_type, signals in groups.items():
        matched = [sig for sig in signals if str(sig.get("signal_type") or "") in _event_signal_types(event)]
        if not matched:
            continue
        source_types.add(source_type)
        if _source_strength(source_type, matched) >= 0.85:
            strong_sources += 1
    return source_types, strong_sources


def _adjustment_factor(event: dict[str, Any], signals_by_source: dict[str, list[dict[str, Any]]] | None) -> tuple[float, str]:
    base_confidence = _safe_float(event.get("confidence"), 0.0)
    source_types, strong_sources = _supporting_source_types(event, signals_by_source)
    source_count = len(source_types)

    if source_count == 0:
        return 0.75, "No independent source groups confirmed the event, so confidence was downgraded."

    if source_count == 1:
        only_source = next(iter(source_types))
        if only_source in WEAK_SOURCE_TYPES or base_confidence < 0.5:
            return 0.70, f"Only one weak source group ({only_source}) supported the event, so confidence was downgraded."
        return 0.90, f"Only one source group ({only_source}) supported the event, so confidence was slightly reduced."

    if strong_sources >= 2:
        diversity_bonus = min(0.20, 0.05 * (source_count - 1))
        return 1.00 + diversity_bonus, f"{strong_sources} strong independent source groups confirmed the event, so confidence was upgraded."

    if source_count >= 2:
        diversity_bonus = min(0.12, 0.04 * (source_count - 1))
        return 0.92 + diversity_bonus, f"Multiple source groups confirmed the event, with moderate diversity support."

    return 0.85, "Evidence was limited across sources, so confidence was reduced slightly."


def correlate_events(events: list[dict[str, Any]], signals_by_source: dict[str, list[dict[str, Any]]] | None = None) -> list[dict[str, Any]]:
    correlated: list[dict[str, Any]] = []

    for event in events:
        if not isinstance(event, dict):
            continue

        event_type = str(event.get("event_type") or "").strip()
        if not event_type:
            continue

        base_confidence = _safe_float(event.get("confidence"), 0.0)
        source_types, strong_sources = _supporting_source_types(event, signals_by_source)
        factor, reason = _adjustment_factor(event, signals_by_source)

        adjusted = base_confidence * factor

        if len(source_types) >= 2 and strong_sources >= 2:
            reason = f"{reason} LinkedIn + hiring/news/github-type confirmation increased confidence."
        elif len(source_types) == 1 and next(iter(source_types), "") in WEAK_SOURCE_TYPES:
            reason = f"{reason} Single weak-source confirmation was not enough for high confidence."

        if adjusted > base_confidence and len(source_types) >= 2:
            business = str(event.get("business_impact") or "")
            if business and not business.endswith("."):
                business += "."
            if business:
                reason = f"{reason} This matters because {business.lower()}"

        correlated.append(
            {
                "event_type": event_type,
                "adjusted_confidence": max(0.0, min(1.0, round(adjusted, 2))),
                "justification": reason,
            }
        )

    correlated.sort(key=lambda item: item["adjusted_confidence"], reverse=True)
    return correlated
