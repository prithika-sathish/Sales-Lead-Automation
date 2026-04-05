from __future__ import annotations

from typing import Any


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalize_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [_clean_text(item) for item in value if _clean_text(item)]
    if isinstance(value, str):
        return [_clean_text(part) for part in value.split(",") if _clean_text(part)]
    return []


def generate_signals(company_data: dict[str, Any], icp: dict[str, Any]) -> dict[str, Any]:
    text = _clean_text(company_data.get("text") or "").lower()
    weak = bool(company_data.get("weak"))

    score = 0
    signals: list[str] = []

    if "subscription" in text or "recurring" in text:
        score += 2
        signals.append("subscription_model")

    if "api" in text or "platform" in text:
        score += 2
        signals.append("tech")

    if "customers" in text or "enterprise" in text:
        score += 1
        signals.append("b2b")

    if "scaling" in text or "fast-growing" in text:
        score += 1
        signals.append("growth")

    if "careers" in text or "hiring" in text:
        score += 1
        signals.append("hiring")

    if weak:
        score -= 1
        signals.append("weak_data")

    return {
        "signals": signals,
        "score_factors": {
            "raw_score": score,
            "weak": weak,
        },
    }
