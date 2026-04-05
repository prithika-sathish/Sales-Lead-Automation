from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any


HIGH_INTENT_CATEGORIES = [
    "operational_pain",
    "competitor_engagement",
    "problem_discovery_signal",
    "downstream_growth",
    "urgent_trigger_event",
    "decision_maker_intent",
]

CATEGORY_PATTERNS = {
    "operational_pain": [
        "we struggled with",
        "bottleneck",
        "downtime",
        "manual process",
        "scaling issue",
        "latency problem",
        "slow",
        "incident",
        "outage",
        "reliability",
        "operational pain",
    ],
    "competitor_engagement": [
        "competitor",
        "alternative to",
        "compare",
        "vs ",
        "vendor evaluation",
        "integration",
        "api",
        "dependency",
        "follow",
        "liked",
        "commented",
    ],
    "problem_discovery_signal": [
        "how do i solve",
        "tool for",
        "any alternatives to",
        "this doesn’t scale",
        "this does not scale",
        "what is the best way",
        "looking for a solution",
        "need help with",
        "forum",
        "reddit",
        "hacker news",
        "indie hackers",
    ],
    "downstream_growth": [
        "case study",
        "testimonial",
        "customer announcement",
        "helped",
        "scale",
        "scaled",
        "expansion",
        "customer growth",
        "adoption",
    ],
    "urgent_trigger_event": [
        "security incident",
        "breach",
        "outage",
        "pr crisis",
        "churn",
        "incident",
        "downtime",
        "critical issue",
        "service disruption",
        "urgent",
    ],
    "decision_maker_intent": [
        "we need to improve",
        "looking for tools",
        "scaling our team",
        "scaling our process",
        "biggest challenge is",
        "need to improve",
        "evaluating",
        "buying",
        "switching",
        "choosing",
        "implement",
    ],
}

SOURCE_QUALITY = {
    "engineering_blog": 0.85,
    "status_page": 0.9,
    "news": 0.9,
    "press": 0.9,
    "linkedin": 0.8,
    "twitter": 0.75,
    "x": 0.75,
    "reddit": 0.75,
    "hackernews": 0.8,
    "indiehackers": 0.8,
    "forum": 0.7,
    "website": 0.7,
    "content": 0.7,
    "docs": 0.65,
    "github": 0.7,
    "jobs": 0.7,
    "review": 0.8,
}


DERIVED_CATEGORY_MAP = {
    "operational_pain": ["scaling_bottleneck", "inefficiency", "reliability_risk"],
    "competitor_engagement": ["category_awareness", "vendor_evaluation"],
    "problem_discovery_signal": ["active_problem_search"],
    "downstream_growth": ["indirect_scaling_pressure"],
    "urgent_trigger_event": ["high_priority_need"],
    "decision_maker_intent": ["executive_buying_signal"],
}


STAGE_PRIORITY = ["URGENT", "SWITCHING", "ACTIVELY_EVALUATING", "AWARE", "EXPLORING"]


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


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def _days_ago(value: Any) -> float:
    parsed = _parse_timestamp(value)
    if not parsed:
        return 365.0
    return max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds() / 86400.0)


def _source_label(signal: dict[str, Any]) -> str:
    return str(signal.get("source") or signal.get("source_type") or signal.get("type") or "unknown").strip().lower()


def _signal_text(signal: dict[str, Any]) -> str:
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    raw_text = str(metadata.get("raw_text") or signal.get("raw_text") or signal.get("summary") or "").strip()
    return raw_text.lower()


def _signal_timestamp(signal: dict[str, Any]) -> str:
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    timestamp = signal.get("timestamp") or metadata.get("timestamp") or metadata.get("created_at") or metadata.get("date")
    if timestamp:
        return str(timestamp)
    return datetime.now(timezone.utc).isoformat()


def _source_quality(source: str) -> float:
    return SOURCE_QUALITY.get(source, 0.6)


def _match_patterns(text: str, category: str, source: str) -> list[str]:
    matches: list[str] = []
    for pattern in CATEGORY_PATTERNS[category]:
        if pattern in text:
            matches.append(pattern)
    if category == "problem_discovery_signal" and source in {"reddit", "hackernews", "indiehackers", "forum"}:
        matches.append(source)
    if category == "decision_maker_intent" and source in {"linkedin", "twitter", "x"}:
        matches.append(source)
    return matches


def _confidence_and_intensity(signal: dict[str, Any], category: str, matches: list[str]) -> tuple[float, float]:
    source = _source_label(signal)
    source_q = _source_quality(source)
    recency_days = _days_ago(_signal_timestamp(signal))
    recency_factor = 1.0 if recency_days <= 30 else 0.8 if recency_days <= 60 else 0.6 if recency_days <= 90 else 0.35

    signal_strength = _safe_float(signal.get("signal_strength"), 0.0)
    final_score = _safe_float(signal.get("final_score") or signal.get("signal_score"), 0.0)
    base = (final_score / 25.0) * 0.35 + (signal_strength / 5.0) * 0.25 + source_q * 0.2 + recency_factor * 0.2
    pattern_bonus = min(0.18, 0.04 * len(matches))
    confidence = max(0.0, min(1.0, base + pattern_bonus))

    if category == "urgent_trigger_event":
        confidence += 0.08
    if category == "decision_maker_intent" and source in {"linkedin", "twitter", "x"}:
        confidence += 0.05

    confidence = max(0.0, min(1.0, confidence))

    intensity = (signal_strength / 5.0) * 0.45 + (final_score / 25.0) * 0.35 + recency_factor * 0.20
    if category in {"urgent_trigger_event", "operational_pain"}:
        intensity += 0.05
    intensity = max(0.0, min(1.0, intensity))
    return round(confidence, 2), round(intensity, 2)


def _summary_for_category(category: str, matches: list[str], text: str, source: str) -> str:
    mapping = {
        "operational_pain": "mentions internal pain or scaling friction",
        "competitor_engagement": "shows interaction with competitor or adjacent tooling",
        "problem_discovery_signal": "shows raw problem discovery in community discussion",
        "downstream_growth": "shows customer growth that can create supplier pressure",
        "urgent_trigger_event": "shows an urgent event that needs immediate response",
        "decision_maker_intent": "shows decision-maker language around solving a problem",
    }
    base = mapping.get(category, category.replace("_", " "))
    if matches:
        return f"{source}: {base} ({', '.join(matches[:3])})"
    clipped = text[:120]
    return f"{source}: {base} ({clipped})" if clipped else f"{source}: {base}"


def _bucket_signals(company_row: dict[str, Any]) -> list[dict[str, Any]]:
    signals = company_row.get("signals")
    if not isinstance(signals, list):
        return []
    return [sig for sig in signals if isinstance(sig, dict)]


def extract_high_intent_signals(company_row: dict[str, Any]) -> list[dict[str, Any]]:
    signals = _bucket_signals(company_row)
    results: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for signal in signals:
        text = _signal_text(signal)
        source = _source_label(signal)
        timestamp = _signal_timestamp(signal)
        if not text:
            continue

        for category in HIGH_INTENT_CATEGORIES:
            matches = _match_patterns(text, category, source)
            if not matches:
                continue

            confidence, intensity = _confidence_and_intensity(signal, category, matches)
            if confidence < 0.6:
                continue

            summary = _summary_for_category(category, matches, text, source)
            dedupe_key = (category, summary[:80])
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            results.append(
                {
                    "type": category,
                    "confidence_score": confidence,
                    "intensity_score": intensity,
                    "timestamp": timestamp,
                    "source": source,
                    "summary": summary,
                    "recency_days": round(_days_ago(timestamp), 1),
                    "supporting_signal_type": str(signal.get("signal_type") or signal.get("type") or "unknown"),
                }
            )

    results.sort(
        key=lambda item: (
            float(item.get("confidence_score") or 0.0),
            float(item.get("intensity_score") or 0.0),
            max(0.0, 60.0 - float(item.get("recency_days") or 0.0)),
        ),
        reverse=True,
    )
    return results


def derive_high_intent_signals(high_intent_signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for signal in high_intent_signals:
        signal_type = str(signal.get("type") or "").strip()
        if signal_type:
            grouped[signal_type].append(signal)

    derived: list[dict[str, Any]] = []
    for category in HIGH_INTENT_CATEGORIES:
        items = grouped.get(category, [])
        if not items:
            continue

        confidences = [_safe_float(item.get("confidence_score"), 0.0) for item in items]
        intensities = [_safe_float(item.get("intensity_score"), 0.0) for item in items]
        recencies = [_safe_float(item.get("recency_days"), 365.0) for item in items]
        frequency = len(items)
        avg_confidence = sum(confidences) / frequency
        avg_intensity = sum(intensities) / frequency
        recency_days = min(recencies)

        if category == "operational_pain":
            cues = " ".join(str(item.get("summary") or "").lower() for item in items)
            if any(token in cues for token in ["bottleneck", "manual", "inefficien", "latency", "scaling", "downtime"]):
                derived.append(_derived_signal("scaling_bottleneck", frequency, recency_days, avg_intensity, avg_confidence, items))
            if any(token in cues for token in ["manual", "process", "inefficien"]):
                derived.append(_derived_signal("inefficiency", frequency, recency_days, avg_intensity, avg_confidence, items))
            if any(token in cues for token in ["downtime", "outage", "latency", "reliability"]):
                derived.append(_derived_signal("reliability_risk", frequency, recency_days, avg_intensity, avg_confidence, items))
        elif category == "competitor_engagement":
            derived.append(_derived_signal("category_awareness", frequency, recency_days, avg_intensity, avg_confidence, items))
            if frequency >= 2 or avg_confidence >= 0.75:
                derived.append(_derived_signal("vendor_evaluation", frequency, recency_days, avg_intensity, avg_confidence, items))
        elif category == "problem_discovery_signal":
            derived.append(_derived_signal("active_problem_search", frequency, recency_days, avg_intensity, avg_confidence, items))
        elif category == "downstream_growth":
            derived.append(_derived_signal("indirect_scaling_pressure", frequency, recency_days, avg_intensity, avg_confidence, items))
        elif category == "urgent_trigger_event":
            derived.append(_derived_signal("high_priority_need", frequency, recency_days, avg_intensity, avg_confidence, items))
        elif category == "decision_maker_intent":
            derived.append(_derived_signal("executive_buying_signal", frequency, recency_days, avg_intensity, avg_confidence, items))

    derived.sort(key=lambda item: (float(item.get("confidence_score") or 0.0), float(item.get("intensity_score") or 0.0), -float(item.get("recency_days") or 365.0)), reverse=True)
    return derived


def _derived_signal(name: str, frequency: int, recency_days: float, intensity: float, confidence: float, items: list[dict[str, Any]]) -> dict[str, Any]:
    signal_type = name
    support_categories = sorted({str(item.get("type") or "") for item in items if str(item.get("type") or "")})
    summaries = [str(item.get("summary") or "") for item in items if str(item.get("summary") or "")]
    summary = summaries[0] if summaries else name.replace("_", " ")
    return {
        "type": signal_type,
        "confidence_score": round(max(0.0, min(1.0, confidence)), 2),
        "intensity_score": round(max(0.0, min(1.0, intensity)), 2),
        "frequency": frequency,
        "recency_days": round(recency_days, 1),
        "source": "derived",
        "summary": summary,
        "supporting_categories": support_categories,
    }


def classify_intent_stage(high_intent_signals: list[dict[str, Any]], derived_signals: list[dict[str, Any]]) -> dict[str, Any]:
    category_conf: dict[str, float] = {category: 0.0 for category in HIGH_INTENT_CATEGORIES}
    for signal in high_intent_signals:
        category = str(signal.get("type") or "").strip()
        confidence = _safe_float(signal.get("confidence_score"), 0.0)
        if category in category_conf:
            category_conf[category] = max(category_conf[category], confidence)

    derived_types = {str(item.get("type") or "") for item in derived_signals}
    if category_conf["urgent_trigger_event"] >= 0.6 or "high_priority_need" in derived_types:
        return {"intent_stage": "URGENT", "confidence": round(max(category_conf["urgent_trigger_event"], 0.85), 2)}

    if category_conf["competitor_engagement"] >= 0.6 and category_conf["decision_maker_intent"] >= 0.6:
        conf = min(0.95, (category_conf["competitor_engagement"] + category_conf["decision_maker_intent"]) / 2 + 0.12)
        return {"intent_stage": "SWITCHING", "confidence": round(conf, 2)}

    if category_conf["operational_pain"] >= 0.6 and category_conf["problem_discovery_signal"] >= 0.6:
        conf = min(0.92, (category_conf["operational_pain"] + category_conf["problem_discovery_signal"]) / 2 + 0.1)
        return {"intent_stage": "ACTIVELY_EVALUATING", "confidence": round(conf, 2)}

    if category_conf["decision_maker_intent"] >= 0.6 or category_conf["downstream_growth"] >= 0.6 or category_conf["competitor_engagement"] >= 0.6:
        strongest = max(category_conf.values())
        conf = min(0.85, strongest * 0.9 + 0.1)
        return {"intent_stage": "AWARE", "confidence": round(conf, 2)}

    weak_count = sum(1 for signal in high_intent_signals if _safe_float(signal.get("confidence_score"), 0.0) < 0.6)
    if not high_intent_signals or weak_count >= len(high_intent_signals):
        return {"intent_stage": "EXPLORING", "confidence": 0.35}

    strongest = max(category_conf.values()) if category_conf else 0.0
    return {"intent_stage": "AWARE", "confidence": round(max(0.4, strongest), 2)}


def intent_stage_reason(intent_stage: str, high_intent_signals: list[dict[str, Any]], derived_signals: list[dict[str, Any]]) -> str:
    if intent_stage == "URGENT":
        return "Urgent trigger event and operational pain indicate immediate action is required."
    if intent_stage == "SWITCHING":
        return "Decision maker intent and competitor engagement indicate the team is comparing or switching vendors."
    if intent_stage == "ACTIVELY_EVALUATING":
        return "Operational pain and problem discovery show the company is actively evaluating a solution."
    if intent_stage == "AWARE":
        return "Recent growth, decision-maker, or competitor signals indicate the company is aware of the problem and building intent."
    return "Signals are weak or scattered, so the company is still exploring the problem space."


def top_high_intent_signals(high_intent_signals: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    ordered = sorted(
        high_intent_signals,
        key=lambda item: (
            _safe_float(item.get("confidence_score"), 0.0),
            _safe_float(item.get("intensity_score"), 0.0),
            -_safe_float(item.get("recency_days"), 365.0),
        ),
        reverse=True,
    )
    return ordered[:limit]


def key_trigger_summary(intent_stage: str, high_intent_signals: list[dict[str, Any]], derived_signals: list[dict[str, Any]]) -> str:
    top_categories = [str(item.get("type") or "") for item in top_high_intent_signals(high_intent_signals, limit=3)]
    top_derived = [str(item.get("type") or "") for item in derived_signals[:2]]
    pieces = [piece for piece in [", ".join(top_categories), ", ".join(top_derived)] if piece]
    if pieces:
        return f"{intent_stage}: {' | '.join(pieces)}"
    return f"{intent_stage}: weak signal set"


def high_intent_score_boost(high_intent_signals: list[dict[str, Any]], derived_signals: list[dict[str, Any]]) -> int:
    boost = 0
    categories = {str(sig.get("type") or "") for sig in high_intent_signals}
    derived_types = {str(sig.get("type") or "") for sig in derived_signals}

    if "urgent_trigger_event" in categories or "high_priority_need" in derived_types:
        boost += 25
    if "operational_pain" in categories:
        boost += 20
    if "decision_maker_intent" in categories:
        boost += 20
    if "competitor_engagement" in categories:
        boost += 15
    if "problem_discovery_signal" in categories:
        boost += 15
    if "downstream_growth" in categories:
        boost += 10
    return boost
