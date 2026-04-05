from __future__ import annotations

from typing import Any


COVERAGE_AREAS = {
    "hiring": {
        "signal_types": {"hiring", "hiring_spike", "sales_expansion"},
        "source_types": {"hiring", "linkedin", "jobs"},
    },
    "news": {
        "signal_types": {"news", "company_update", "milestone", "market_expansion"},
        "source_types": {"news", "press", "website", "content"},
    },
    "product": {
        "signal_types": {"product_launch", "feature_update", "integration_added", "api_update", "product_gap", "platform_expansion"},
        "source_types": {"product", "website", "github", "docs", "content"},
    },
    "social": {
        "signal_types": {"momentum", "narrative_trend", "content_push", "engagement"},
        "source_types": {"linkedin", "reddit", "social", "content"},
    },
    "reviews": {
        "signal_types": {"customer_pain", "feature_requests", "complaint", "review"},
        "source_types": {"review", "g2", "capterra", "trustpilot", "reddit"},
    },
    "funding": {
        "signal_types": {"funding", "milestone", "market_expansion"},
        "source_types": {"funding", "news", "press", "website"},
    },
}


AREA_PRIORITY = ["hiring", "news", "product", "social", "reviews", "funding"]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _signal_value(signal: dict[str, Any]) -> float:
    score = _safe_float(signal.get("final_score") or signal.get("signal_score"), 0.0)
    strength = _safe_float(signal.get("signal_strength"), 0.0)
    recency = _safe_float(signal.get("recency_score"), 0.0)
    source = str(signal.get("source") or signal.get("source_type") or "").strip().lower()

    value = (score / 25.0) * 0.5 + (strength / 5.0) * 0.3 + (recency / 5.0) * 0.2
    if source in {"fallback", "orchestrator"} or bool((signal.get("metadata") or {}).get("fallback")):
        value *= 0.85
    return max(0.0, min(1.0, value))


def _matches_area(signal: dict[str, Any], area: str) -> bool:
    config = COVERAGE_AREAS[area]
    signal_type = str(signal.get("signal_type") or signal.get("type") or "").strip().lower()
    source_type = str(signal.get("source") or signal.get("source_type") or "").strip().lower()

    if signal_type in config["signal_types"]:
        return True
    if source_type in config["source_types"]:
        return True

    raw_text = str((signal.get("metadata") or {}).get("raw_text") or signal.get("raw_text") or "").strip().lower()
    if area == "hiring" and any(token in raw_text for token in ["hiring", "open role", "we are hiring", "join our team", "recruit"]):
        return True
    if area == "news" and any(token in raw_text for token in ["announced", "launch", "release", "press", "funding", "raised"]):
        return True
    if area == "product" and any(token in raw_text for token in ["feature", "product", "api", "integration", "roadmap", "release"]):
        return True
    if area == "social" and any(token in raw_text for token in ["linkedin", "twitter", "reddit", "post", "engagement", "discussion"]):
        return True
    if area == "reviews" and any(token in raw_text for token in ["review", "complaint", "g2", "capterra", "trustpilot", "feedback"]):
        return True
    if area == "funding" and any(token in raw_text for token in ["funding", "series", "raised", "investment", "seed", "series a", "series b"]):
        return True

    return False


def evaluate_signal_coverage(signals: list[dict[str, Any]]) -> dict[str, Any]:
    clean_signals = [sig for sig in signals if isinstance(sig, dict)]
    if not clean_signals:
        return {
            "coverage_score": 0.0,
            "missing_areas": AREA_PRIORITY,
            "should_fetch_more": True,
        }

    area_hits: dict[str, list[dict[str, Any]]] = {area: [] for area in AREA_PRIORITY}
    source_types = set()

    for signal in clean_signals:
        source_type = str(signal.get("source") or signal.get("source_type") or "").strip().lower()
        if source_type:
            source_types.add(source_type)

        for area in AREA_PRIORITY:
            if _matches_area(signal, area):
                area_hits[area].append(signal)

    area_scores: dict[str, float] = {}
    missing_areas: list[str] = []

    for area in AREA_PRIORITY:
        hits = area_hits[area]
        if not hits:
            area_scores[area] = 0.0
            missing_areas.append(area)
            continue

        strongest = max(_signal_value(sig) for sig in hits)
        count_bonus = min(0.2, 0.05 * (len(hits) - 1))
        source_bonus = min(0.15, 0.05 * max(0, len({str(sig.get("source") or sig.get("source_type") or "").strip().lower() for sig in hits if str(sig.get("source") or sig.get("source_type") or "").strip()} ) - 1))
        area_scores[area] = max(0.0, min(1.0, strongest * 0.65 + count_bonus + source_bonus))

    weighted_sum = 0.0
    for area in AREA_PRIORITY:
        weighted_sum += area_scores[area]
    coverage_score = round(weighted_sum / len(AREA_PRIORITY), 2)

    distinct_area_count = sum(1 for area in AREA_PRIORITY if area_scores[area] >= 0.4)
    distinct_source_count = len(source_types)
    should_fetch_more = any(area_scores[area] < 0.35 for area in AREA_PRIORITY) or distinct_area_count < 4 or distinct_source_count < 3

    if coverage_score >= 0.75 and distinct_area_count >= 5:
        should_fetch_more = False

    if coverage_score < 0.3:
        should_fetch_more = True

    return {
        "coverage_score": coverage_score,
        "missing_areas": missing_areas,
        "should_fetch_more": should_fetch_more,
    }
