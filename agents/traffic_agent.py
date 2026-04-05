from __future__ import annotations

from typing import Any

from app.apify_client import fetch_apify_query


def collect_traffic_signals(company: str) -> dict[str, Any]:
    queries = [
        f"{company} website traffic growth",
        f"{company} search trend",
        f"{company} social engagement growth",
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

            signals.append(
                {
                    "type": "traffic_growth",
                    "raw_text": text,
                    "metadata": {
                        "query": query,
                        "timestamp": str(item.get("timestamp") or item.get("date") or ""),
                        "url": str(item.get("url") or item.get("link") or ""),
                    },
                    "source": "traffic",
                }
            )

    return {"company": company, "signals": signals}
