from __future__ import annotations

from typing import Any

from app.apify_client import fetch_apify_query


def collect_producthunt_signals(company: str) -> dict[str, Any]:
    queries = [
        f"{company} product hunt launch",
        f"{company} new feature launch",
        f"{company} release notes",
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
                    "type": "product_launch",
                    "raw_text": text,
                    "metadata": {
                        "query": query,
                        "timestamp": str(item.get("timestamp") or item.get("date") or ""),
                        "url": str(item.get("url") or item.get("link") or ""),
                    },
                    "source": "product_hunt",
                }
            )

    return {"company": company, "signals": signals}
