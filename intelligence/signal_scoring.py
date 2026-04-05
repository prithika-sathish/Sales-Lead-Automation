from __future__ import annotations

from typing import Any


BASE_WEIGHTS = {
    "funding": 0.9,
    "news": 0.9,
    "hiring": 0.8,
    "product launch": 0.85,
    "product_launch": 0.85,
    "github": 0.5,
    "reddit": 0.3,
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _source_weight(source_type: str, confidence_score: float) -> float:
    base = BASE_WEIGHTS.get(source_type.strip().lower(), 0.6)
    if source_type.strip().lower() == "github" and confidence_score >= 0.8:
        return 0.75
    return base


def _recency_multiplier(recency_days: float) -> float:
    if recency_days <= 30:
        return 1.0
    if recency_days <= 60:
        return 0.8
    if recency_days <= 90:
        return 0.65
    return 0.45


def _repetition_multiplier(validated_signal: dict[str, Any]) -> float:
    repetition = validated_signal.get("repetition")
    if repetition is None:
        repetition = validated_signal.get("repetition_count")
    if repetition is None:
        repetition = validated_signal.get("similar_signals")
    count = max(1.0, _safe_float(repetition, 1.0))

    if count >= 5:
        return 1.25
    if count >= 3:
        return 1.15
    if count >= 2:
        return 1.08
    return 1.0


def score_signal(validated_signal: dict[str, Any], source_type: str, recency_days: int) -> dict[str, Any]:
    confidence_score = _safe_float(validated_signal.get("confidence_score"), 0.5)
    confidence_score = max(0.0, min(1.0, confidence_score))
    recency_days_float = max(0.0, _safe_float(recency_days, 0.0))

    weight = _source_weight(source_type, confidence_score)
    if confidence_score < 0.35:
        weight *= 0.75
    elif confidence_score >= 0.75:
        weight *= 1.1

    weight *= _recency_multiplier(recency_days_float)
    weight *= _repetition_multiplier(validated_signal)

    if recency_days_float > 30:
        weight *= max(0.6, 1.0 - ((recency_days_float - 30) / 120.0))

    final_score = max(0.0, min(1.0, round(weight, 4)))

    if final_score >= 0.75:
        signal_type = "strong"
    elif final_score >= 0.45:
        signal_type = "medium"
    else:
        signal_type = "weak"

    reason_parts: list[str] = [f"base={BASE_WEIGHTS.get(source_type.strip().lower(), 0.6):.2f}"]
    reason_parts.append(f"confidence={confidence_score:.2f}")
    reason_parts.append(f"recency_days={int(recency_days_float)}")
    if recency_days_float > 30:
        reason_parts.append("recency_decay_applied")
    repetition = validated_signal.get("repetition") or validated_signal.get("repetition_count") or validated_signal.get("similar_signals")
    if repetition is not None and _safe_float(repetition, 1.0) > 1:
        reason_parts.append(f"repetition={int(_safe_float(repetition, 1.0))}")

    return {
        "final_score": final_score,
        "weight_reason": "; ".join(reason_parts) + f"; signal_type={signal_type}",
    }
