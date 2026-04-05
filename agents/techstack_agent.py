from __future__ import annotations

from typing import Any

from app.apify_client import fetch_apify_query


def collect_techstack_signals(company: str) -> dict[str, Any]:
    queries = [
        f"{company} tech stack",
        f"{company} migrated to",
        f"{company} added integration",
    ]
    signals: list[dict[str, Any]] = []

    for query in queries:
        try:
            items = fetch_apify_query(query, limit=3)
        except RuntimeError:
            continue

        for item in items:
            text = str(item.get("text") or item.get("description") or item.get("content") or "").strip()
            if not text:
                continue

            text_l = text.lower()
            signal_type = "techstack_change"
            if any(token in text_l for token in ["integrat", "connector", "plugin", "marketplace"]):
                signal_type = "integration_added"

            signals.append(
                {
                    "type": signal_type,
                    "raw_text": text,
                    "metadata": {
                        "query": query,
                        "timestamp": str(item.get("timestamp") or item.get("date") or ""),
                        "url": str(item.get("url") or item.get("link") or ""),
                    },
                    "source": "techstack",
                }
            )

    return {"company": company, "signals": signals}
