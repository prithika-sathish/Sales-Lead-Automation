from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any


def _signal_id(company: str, signal_type: str, source: str, raw_text: str) -> str:
    payload = f"{company.lower()}|{signal_type.lower()}|{source.lower()}|{raw_text.strip().lower()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def derive_fused_signals(company: str, normalized_signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    hiring_signals = [sig for sig in normalized_signals if sig.get("signal_type") == "hiring"]
    github_signals = [sig for sig in normalized_signals if sig.get("signal_type") in {"github_activity", "dev_activity"}]
    has_hiring_spike = any(
        sig.get("signal_type") == "hiring"
        and int(sig.get("signal_strength") or 0) >= 3
        and int(sig.get("recency_score") or 0) >= 3
        for sig in normalized_signals
    )
    has_github_spike = any(
        sig.get("signal_type") in {"github_activity", "dev_activity"}
        and int(sig.get("signal_strength") or 0) >= 3
        and int(sig.get("recency_score") or 0) >= 3
        for sig in normalized_signals
    )
    has_complaints = any(sig.get("signal_type") == "customer_pain" for sig in normalized_signals)
    has_feature_requests = any(sig.get("signal_type") == "feature_requests" for sig in normalized_signals)
    has_traffic_growth = any(sig.get("signal_type") == "traffic_growth" for sig in normalized_signals)
    has_content_push = any(sig.get("signal_type") == "content_push" for sig in normalized_signals)
    has_api_update = any(sig.get("signal_type") == "api_update" for sig in normalized_signals)
    has_integration = any(sig.get("signal_type") == "integration_added" for sig in normalized_signals)
    founder_narratives = [
        sig
        for sig in normalized_signals
        if sig.get("signal_type") == "narrative_trend"
        and sig.get("source") in {"linkedin", "content"}
    ]
    high_engagement_velocity = any(
        int(sig.get("recency_score") or 0) >= 4
        and int(
            (
                sig.get("metadata", {}).get("engagement", {}).get("likes", 0)
                + sig.get("metadata", {}).get("engagement", {}).get("comments", 0)
            )
        )
        >= 120
        for sig in normalized_signals
        if isinstance(sig.get("metadata"), dict)
    )

    fused: list[dict[str, Any]] = []
    timestamp = datetime.now(timezone.utc).isoformat()

    if has_hiring_spike and has_github_spike:
        raw_text = "Correlated hiring and GitHub activity spikes indicate infra scaling"
        fused.append(
            {
                "company": company,
                "signal_id": _signal_id(company, "infra_scaling", "fusion", raw_text),
                "signal_type": "infra_scaling",
                "signal_strength": 5,
                "recency_score": 5,
                "final_score": 25,
                "signal_score": 25,
                "timestamp": timestamp,
                "metadata": {"fused_from": ["hiring", "github_activity"], "raw_text": raw_text},
                "source": "fusion",
            }
        )

    if has_content_push and has_hiring_spike:
        raw_text = "Combined hiring and content push indicate growth phase acceleration"
        fused.append(
            {
                "company": company,
                "signal_id": _signal_id(company, "growth_phase", "fusion", raw_text),
                "signal_type": "growth_phase",
                "signal_strength": 4,
                "recency_score": 5,
                "final_score": 20,
                "signal_score": 20,
                "timestamp": timestamp,
                "metadata": {"fused_from": ["content_push", "hiring"], "raw_text": raw_text},
                "source": "fusion",
            }
        )

    if has_api_update and has_integration:
        raw_text = "API and integration activity indicate platform expansion"
        fused.append(
            {
                "company": company,
                "signal_id": _signal_id(company, "platform_expansion", "fusion", raw_text),
                "signal_type": "platform_expansion",
                "signal_strength": 4,
                "recency_score": 5,
                "final_score": 20,
                "signal_score": 20,
                "timestamp": timestamp,
                "metadata": {"fused_from": ["api_update", "integration_added"], "raw_text": raw_text},
                "source": "fusion",
            }
        )

    if has_complaints and has_feature_requests:
        raw_text = "Complaints and feature requests correlate to product gap pressure"
        fused.append(
            {
                "company": company,
                "signal_id": _signal_id(company, "product_gap", "fusion", raw_text),
                "signal_type": "product_gap",
                "signal_strength": 5,
                "recency_score": 5,
                "final_score": 25,
                "signal_score": 25,
                "timestamp": timestamp,
                "metadata": {"fused_from": ["customer_pain", "feature_requests"], "raw_text": raw_text},
                "source": "fusion",
            }
        )

    if has_traffic_growth and has_content_push:
        raw_text = "Traffic growth and content push suggest marketing expansion"
        fused.append(
            {
                "company": company,
                "signal_id": _signal_id(company, "marketing_expansion", "fusion", raw_text),
                "signal_type": "marketing_expansion",
                "signal_strength": 4,
                "recency_score": 5,
                "final_score": 20,
                "signal_score": 20,
                "timestamp": timestamp,
                "metadata": {"fused_from": ["traffic_growth", "content_push"], "raw_text": raw_text},
                "source": "fusion",
            }
        )

    if len(hiring_signals) >= 3 and any(int(sig.get("recency_score") or 0) >= 4 for sig in hiring_signals):
        raw_text = "Repeated recent hiring signals indicate a hiring trend"
        fused.append(
            {
                "company": company,
                "signal_id": _signal_id(company, "hiring_trend", "fusion", raw_text),
                "signal_type": "hiring_trend",
                "signal_strength": 4,
                "recency_score": 5,
                "final_score": 20,
                "signal_score": 20,
                "timestamp": timestamp,
                "metadata": {"fused_from": ["hiring"], "raw_text": raw_text, "count": len(hiring_signals)},
                "source": "fusion",
            }
        )

    if len(github_signals) >= 3:
        raw_text = "Multiple recent GitHub signals indicate sustained developer activity"
        fused.append(
            {
                "company": company,
                "signal_id": _signal_id(company, "dev_activity", "fusion", raw_text),
                "signal_type": "dev_activity",
                "signal_strength": 4,
                "recency_score": 5,
                "final_score": 20,
                "signal_score": 20,
                "timestamp": timestamp,
                "metadata": {"fused_from": ["github_activity"], "raw_text": raw_text, "count": len(github_signals)},
                "source": "fusion",
            }
        )

    if not fused and len(normalized_signals) >= 5:
        raw_text = "Moderate multi-signal activity suggests emerging commercial intent"
        fused.append(
            {
                "company": company,
                "signal_id": _signal_id(company, "emerging_intent", "fusion", raw_text),
                "signal_type": "emerging_intent",
                "signal_strength": 3,
                "recency_score": 4,
                "final_score": 12,
                "signal_score": 12,
                "timestamp": timestamp,
                "metadata": {"fused_from": ["mixed"], "raw_text": raw_text},
                "source": "fusion",
            }
        )

    if founder_narratives and any(int(sig.get("signal_strength") or 0) >= 3 for sig in founder_narratives):
        raw_text = "Repeated founder-led themes indicate strategic focus"
        fused.append(
            {
                "company": company,
                "signal_id": _signal_id(company, "strategic_focus", "fusion", raw_text),
                "signal_type": "strategic_focus",
                "signal_strength": 4,
                "recency_score": 5,
                "final_score": 20,
                "signal_score": 20,
                "timestamp": timestamp,
                "metadata": {"fused_from": ["narrative_trend"], "raw_text": raw_text},
                "source": "fusion",
            }
        )

    if high_engagement_velocity:
        raw_text = "High engagement velocity across recent posts indicates momentum"
        fused.append(
            {
                "company": company,
                "signal_id": _signal_id(company, "momentum", "fusion", raw_text),
                "signal_type": "momentum",
                "signal_strength": 4,
                "recency_score": 5,
                "final_score": 20,
                "signal_score": 20,
                "timestamp": timestamp,
                "metadata": {"fused_from": ["engagement"], "raw_text": raw_text},
                "source": "fusion",
            }
        )

    return fused
