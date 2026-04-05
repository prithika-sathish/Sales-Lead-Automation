from __future__ import annotations

from typing import Any

from app.apify_client import fetch_apify_query


def collect_website_signals(company: str) -> dict[str, Any]:
    queries = [
        f"{company} website updates",
        f"{company} integrations marketplace",
        f"{company} feature pages",
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

            signal_type = "website_update"
            text_l = text.lower()
            if any(token in text_l for token in ["integration", "connect", "plugin", "marketplace"]):
                signal_type = "integration_added"
            if any(token in text_l for token in ["documentation", "api", "guides", "feature overview"]):
                signal_type = "product_maturity"

            signals.append(
                {
                    "type": signal_type,
                    "raw_text": text,
                    "metadata": {
                        "query": query,
                        "timestamp": str(item.get("timestamp") or item.get("date") or ""),
                        "url": str(item.get("url") or item.get("link") or ""),
                    },
                    "source": "website",
                }
            )

    return {"company": company, "signals": signals}
