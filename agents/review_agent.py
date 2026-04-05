from __future__ import annotations

from typing import Any

from app.apify_client import fetch_apify_query


def _signal_type_from_text(text: str) -> str:
    text_l = text.lower()
    if any(token in text_l for token in ["moving away from", "alternative to", "switch from", "migrate from"]):
        return "competitor_switch"
    if any(token in text_l for token in ["feature request", "wish", "please add", "roadmap"]):
        return "feature_requests"
    if any(token in text_l for token in ["complaint", "issue", "outage", "problem", "friction"]):
        return "customer_pain"
    return "review_mention"


def collect_review_signals(company: str) -> dict[str, Any]:
    queries = [
        f"{company} reddit review",
        f"{company} customer complaints",
        f"{company} alternative to",
        f"{company} github issues",
    ]
    signals: list[dict[str, Any]] = []

    for query in queries:
        try:
            items = fetch_apify_query(query, limit=4)
        except RuntimeError:
            continue

        for item in items:
            text = str(item.get("text") or item.get("description") or item.get("content") or "").strip()
            if not text:
                continue

            signals.append(
                {
                    "type": _signal_type_from_text(text),
                    "raw_text": text,
                    "metadata": {
                        "query": query,
                        "timestamp": str(item.get("timestamp") or item.get("date") or ""),
                        "url": str(item.get("url") or item.get("link") or ""),
                    },
                    "source": "review",
                }
            )

    return {"company": company, "signals": signals}
