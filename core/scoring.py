from __future__ import annotations

from typing import Any


def _source_type(source: str) -> str:
    lowered = str(source or "").lower()
    if "google_maps" in lowered or "product_hunt" in lowered:
        return "structured"
    if "indeed" in lowered or "naukri" in lowered or "jobs" in lowered:
        return "hiring"
    return "other"


def score_companies(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    domain_counts: dict[str, int] = {}
    for row in rows:
        domain = str(row.get("domain") or "").strip().lower()
        if not domain:
            continue
        domain_counts[domain] = domain_counts.get(domain, 0) + 1

    scored: list[dict[str, object]] = []
    for row in rows:
        domain = str(row.get("domain") or "").strip().lower()
        source = str(row.get("source") or "")

        confidence = 0.0
        if domain:
            confidence += 0.4

        src_type = _source_type(source)
        if src_type == "structured":
            confidence += 0.3
        if src_type == "hiring":
            confidence += 0.2

        if domain and domain_counts.get(domain, 0) > 1:
            confidence += 0.1

        confidence = max(0.0, min(1.0, round(confidence, 3)))
        scored.append(
            {
                "name": str(row.get("name") or "").strip(),
                "domain": domain,
                "source": source,
                "confidence": confidence,
            }
        )

    scored.sort(key=lambda row: float(row.get("confidence") or 0.0), reverse=True)
    return scored


def score_company(row: dict[str, Any]) -> dict[str, Any]:
    text = " ".join(
        [
            str(row.get("description") or ""),
            " ".join(str(t) for t in (row.get("tags") or [])),
            str(row.get("category") or ""),
            str(row.get("why_it_matches") or ""),
        ]
    ).lower()

    score = 0
    reasons: list[str] = []

    activity_words = ["launch", "beta", "new", "release", "update", "recent"]
    growth_words = ["hiring", "scale", "growth", "expanding", "funding", "raised"]

    if any(word in text for word in activity_words):
        score += 25
        reasons.append("activity signal")

    if any(word in text for word in growth_words):
        score += 35
        reasons.append("hiring/growth signal")

    domain = str(row.get("domain") or "").strip()
    emails = row.get("emails") or []
    if domain:
        score += 20
        reasons.append("valid domain")
    if emails:
        score += 20
        reasons.append("contact email found")

    final_score = max(0, min(100, score))
    return {
        "score": final_score,
        "reason": ", ".join(reasons) if reasons else "limited signals",
    }
