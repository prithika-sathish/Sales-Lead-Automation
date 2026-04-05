from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def _days_ago(timestamp: Any) -> float:
    parsed = _parse_timestamp(timestamp)
    if not parsed:
        return 365.0
    now = datetime.now(timezone.utc)
    delta = now - parsed
    return max(0.0, delta.total_seconds() / 86400.0)


def _extract_signals(payload: dict[str, Any]) -> list[dict[str, Any]]:
    signals = payload.get("signals")
    if isinstance(signals, list):
        return [sig for sig in signals if isinstance(sig, dict)]
    if isinstance(payload.get("validated_signal"), dict):
        return [payload["validated_signal"]]
    return []


def _signal_type(signal: dict[str, Any]) -> str:
    return str(signal.get("signal_type") or signal.get("type") or signal.get("category") or "unknown").strip().lower()


def _signal_recency(signal: dict[str, Any]) -> float:
    timestamp = signal.get("timestamp") or signal.get("created_at") or signal.get("date")
    return _days_ago(timestamp)


def _signal_weight(signal: dict[str, Any]) -> float:
    final_score = _safe_float(signal.get("final_score") or signal.get("signal_score"), 0.0)
    signal_strength = _safe_float(signal.get("signal_strength"), 0.0)
    confidence = _safe_float(signal.get("confidence_score"), 0.5)
    return max(0.0, min(1.0, (final_score / 25.0) * 0.5 + (signal_strength / 5.0) * 0.3 + confidence * 0.2))


def _spike_score(signals: list[dict[str, Any]]) -> float:
    recent = [sig for sig in signals if _signal_recency(sig) <= 14]
    very_recent = [sig for sig in signals if _signal_recency(sig) <= 7]
    if not recent:
        return 0.0

    if len(very_recent) >= 3:
        return 1.0
    if len(very_recent) >= 2:
        return 0.85
    if len(recent) >= 4 and len({id(sig) for sig in recent}) >= 3:
        return 0.75
    if len(recent) >= 2:
        return 0.55
    return 0.25


def _trend_score(signals: list[dict[str, Any]]) -> float:
    if not signals:
        return 0.0

    ordered = sorted(signals, key=_signal_recency)
    buckets: list[float] = []
    for days in [7, 14, 30, 60, 90]:
        window = [sig for sig in ordered if _signal_recency(sig) <= days]
        if window:
            buckets.append(sum(_signal_weight(sig) for sig in window) / len(window))
        else:
            buckets.append(0.0)

    if buckets[0] == buckets[1] == buckets[2] == 0.0:
        return 0.0

    early = sum(buckets[:2]) / 2.0
    mid = sum(buckets[1:4]) / 3.0
    late = sum(buckets[3:]) / 2.0

    if early >= mid >= late:
        return min(1.0, 0.45 + early * 0.4)
    if early > 0.0 and mid > 0.0 and abs(early - mid) <= 0.2:
        return min(1.0, 0.35 + mid * 0.35)
    if mid > early and late > 0.0:
        return min(1.0, 0.25 + mid * 0.3)
    return min(1.0, max(0.0, mid * 0.4))


def _stale_score(signals: list[dict[str, Any]]) -> float:
    if not signals:
        return 1.0

    ages = [_days_ago(sig.get("timestamp") or sig.get("created_at") or sig.get("date")) for sig in signals]
    avg_age = sum(ages) / len(ages)
    recent_count = sum(1 for age in ages if age <= 30)

    if recent_count == 0 and avg_age >= 90:
        return 1.0
    if avg_age >= 120:
        return 0.9
    if avg_age >= 60:
        return 0.65
    if recent_count <= 1 and avg_age >= 30:
        return 0.45
    return 0.15


def _momentum_bucket(spike: float, trend: float, stale: float) -> str:
    momentum_value = (spike * 0.45) + (trend * 0.40) + ((1.0 - stale) * 0.15)
    if momentum_value >= 0.72:
        return "high"
    if momentum_value >= 0.45:
        return "medium"
    return "low"


def detect_timing_intelligence(payload: dict[str, Any]) -> dict[str, Any]:
    signals = _extract_signals(payload)
    if not signals:
        return {
            "momentum": "low",
            "reason": "No timestamped signals were provided, so timing confidence is low.",
            "urgency_score": 0.0,
        }

    spike = _spike_score(signals)
    trend = _trend_score(signals)
    stale = _stale_score(signals)

    recent_count = sum(1 for sig in signals if _days_ago(sig.get("timestamp") or sig.get("created_at") or sig.get("date")) <= 30)
    source_counts = Counter(str(sig.get("source") or sig.get("source_type") or "unknown").strip().lower() for sig in signals)
    distinct_sources = len(source_counts)

    urgency_score = (spike * 0.45) + (trend * 0.35) + ((1.0 - stale) * 0.20)

    if recent_count >= 3 and distinct_sources >= 2:
        urgency_score += 0.10
    if any(_signal_recency(sig) > 30 for sig in signals) and recent_count <= 1:
        urgency_score -= 0.10

    urgency_score = max(0.0, min(1.0, round(urgency_score, 2)))
    momentum = _momentum_bucket(spike, trend, stale)

    if stale >= 0.9:
        reason = "Signals are old and there is not enough recent activity to support strong timing urgency."
    elif spike >= 0.85:
        reason = "A sudden recent increase across validated signals points to a timing spike."
    elif trend >= 0.6:
        reason = "Signals show consistent recent activity, which supports a steady timing trend."
    elif recent_count <= 1:
        reason = "Only one recent signal is present, so the timing picture is weak."
    else:
        reason = "Recent signals are present, but the pattern is not strong enough to call a spike or durable trend."

    if momentum == "high" and stale < 0.4:
        reason = f"{reason} The activity is recent and corroborated across multiple signals."
    elif momentum == "low" and stale >= 0.65:
        reason = f"{reason} The signal set is stale relative to the 30-day window."

    return {
        "momentum": momentum,
        "reason": reason,
        "urgency_score": urgency_score,
    }
