from __future__ import annotations

from collections import defaultdict
from typing import Any


EVENT_RULES: list[dict[str, Any]] = [
    {
        "event_type": "Infra scaling",
        "signal_types": {"hiring_spike", "infra_scaling", "dev_activity", "github_activity"},
        "business_impact": "The company is expanding technical capacity, which usually precedes tooling, infrastructure, and workflow purchases.",
    },
    {
        "event_type": "Market expansion",
        "signal_types": {"funding", "news", "company_update", "sales_expansion", "growth_phase", "marketing_expansion"},
        "business_impact": "The company is widening its commercial footprint, creating budget and workflow demand for revenue and operations tools.",
    },
    {
        "event_type": "Product launch",
        "signal_types": {"product_launch", "feature_update", "integration_added", "api_update", "platform_expansion"},
        "business_impact": "The company is shipping or expanding product capabilities, which often creates urgency for enablement, support, and platform support tools.",
    },
    {
        "event_type": "Hiring acceleration",
        "signal_types": {"hiring", "hiring_spike", "sales_expansion"},
        "business_impact": "The company is adding capacity quickly, which usually means new budget, new ownership, and a live need for process automation.",
    },
    {
        "event_type": "Customer pressure",
        "signal_types": {"customer_pain", "feature_requests", "product_gap", "complaint"},
        "business_impact": "Customers are signaling friction or missing functionality, which creates churn risk and a strong case for corrective action.",
    },
    {
        "event_type": "Engagement momentum",
        "signal_types": {"momentum", "content_push", "traffic_growth", "narrative_trend", "strategic_focus"},
        "business_impact": "Attention and narrative velocity are increasing, which usually means the company is in an active market-moving period.",
    },
]


EVENT_PRIORITY = [rule["event_type"] for rule in EVENT_RULES]


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _signal_reason(signal: dict[str, Any]) -> str:
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    raw_text = str(metadata.get("raw_text") or signal.get("raw_text") or "").strip()
    if raw_text:
        clipped = raw_text[:120]
        return clipped if len(raw_text) <= 120 else f"{clipped}..."

    signal_type = str(signal.get("signal_type") or "unknown")
    return signal_type.replace("_", " ")


def _base_confidence(signals: list[dict[str, Any]]) -> float:
    if not signals:
        return 0.0

    score_sum = sum(_safe_int(sig.get("final_score") or sig.get("signal_score"), 0) for sig in signals)
    avg_score = score_sum / max(1, len(signals))
    diversity = len({str(sig.get("signal_type") or "") for sig in signals if str(sig.get("signal_type") or "")})
    recency_bonus = sum(_safe_int(sig.get("recency_score"), 0) for sig in signals[:5]) / 25.0

    normalized = min(1.0, (avg_score / 25.0) * 0.45 + (diversity / 8.0) * 0.35 + recency_bonus * 0.20)
    return max(0.0, min(1.0, normalized))


def _select_supporting_signals(signals: list[dict[str, Any]], matched_types: set[str]) -> list[dict[str, Any]]:
    supporting = [sig for sig in signals if str(sig.get("signal_type") or "") in matched_types]
    supporting.sort(key=lambda sig: (_safe_int(sig.get("final_score"), 0), _safe_int(sig.get("recency_score"), 0)), reverse=True)
    return supporting[:5]


def _event_confidence(signals: list[dict[str, Any]], supporting: list[dict[str, Any]], matched_types: set[str]) -> float:
    if not supporting:
        return 0.0

    support_score = sum(_safe_int(sig.get("final_score") or sig.get("signal_score"), 0) for sig in supporting[:5])
    support_score = support_score / (5.0 * 25.0)
    type_bonus = min(1.0, len(matched_types) / 4.0)
    recency_bonus = sum(_safe_int(sig.get("recency_score"), 0) for sig in supporting[:5]) / 25.0
    repetition_bonus = 0.0

    supporting_types = [str(sig.get("signal_type") or "") for sig in supporting]
    repeated_types = {signal_type for signal_type in supporting_types if supporting_types.count(signal_type) >= 2}
    if repeated_types:
        repetition_bonus = min(0.15, 0.05 * len(repeated_types))

    confidence = (0.45 * support_score) + (0.30 * type_bonus) + (0.20 * recency_bonus) + repetition_bonus
    confidence *= 0.6 + (0.4 * _base_confidence(signals))
    return max(0.0, min(1.0, round(confidence, 2)))


def detect_company_events(validated_signals: list[dict[str, Any]], company: str | None = None) -> list[dict[str, Any]]:
    signals = [sig for sig in validated_signals if isinstance(sig, dict)]
    if not signals:
        return []

    for signal in signals:
        signal.setdefault("company", company or str(signal.get("company") or ""))

    event_candidates: list[tuple[str, set[str], str]] = []
    signal_type_counts = defaultdict(int)
    for signal in signals:
        signal_type = str(signal.get("signal_type") or signal.get("type") or "").strip()
        if signal_type:
            signal_type_counts[signal_type] += 1

    for rule in EVENT_RULES:
        matched_types = {signal_type for signal_type in signal_type_counts if signal_type in rule["signal_types"]}
        if not matched_types:
            continue

        supporting = _select_supporting_signals(signals, matched_types)
        if not supporting:
            continue

        confidence = _event_confidence(signals, supporting, matched_types)
        if confidence <= 0.0:
            continue

        event_candidates.append((rule["event_type"], matched_types, rule["business_impact"]))

    if not event_candidates:
        strongest = sorted(signals, key=lambda sig: (_safe_int(sig.get("final_score"), 0), _safe_int(sig.get("recency_score"), 0)), reverse=True)[:3]
        if not strongest:
            return []

        fallback_type = "Emerging activity"
        confidence = max(0.2, min(0.55, _base_confidence(strongest)))
        return [
            {
                "event_type": fallback_type,
                "confidence": confidence,
                "business_impact": "Signals are present but not yet strong enough to map to a specific operating event.",
                "supporting_signals": [
                    {
                        "signal_type": str(sig.get("signal_type") or "unknown"),
                        "final_score": _safe_int(sig.get("final_score") or sig.get("signal_score"), 0),
                        "recency_score": _safe_int(sig.get("recency_score"), 0),
                        "reason": _signal_reason(sig),
                    }
                    for sig in strongest
                ],
            }
        ]

    unique_events: dict[str, dict[str, Any]] = {}
    for event_type, matched_types, business_impact in event_candidates:
        supporting = _select_supporting_signals(signals, matched_types)
        confidence = _event_confidence(signals, supporting, matched_types)

        support_rows = [
            {
                "signal_type": str(sig.get("signal_type") or "unknown"),
                "final_score": _safe_int(sig.get("final_score") or sig.get("signal_score"), 0),
                "recency_score": _safe_int(sig.get("recency_score"), 0),
                "reason": _signal_reason(sig),
            }
            for sig in supporting
        ]

        existing = unique_events.get(event_type)
        if existing and confidence <= _safe_float(existing.get("confidence"), 0.0):
            continue

        unique_events[event_type] = {
            "event_type": event_type,
            "confidence": confidence,
            "business_impact": business_impact,
            "supporting_signals": support_rows,
        }

    return [unique_events[event_type] for event_type in EVENT_PRIORITY if event_type in unique_events]
