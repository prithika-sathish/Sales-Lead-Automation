from __future__ import annotations

from typing import Any

from app.apify_client import fetch_apify_query


def collect_docs_signals(company: str) -> dict[str, Any]:
    queries = [
        f"{company} api docs",
        f"{company} developer documentation",
        f"{company} integration guide",
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
            signal_type = "docs_update"
            if any(token in text_l for token in ["integration", "sdk", "connector", "webhook"]):
                signal_type = "integration_added"
            elif any(token in text_l for token in ["getting started", "reference", "architecture", "guides"]):
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
                    "source": "docs",
                }
            )

    return {"company": company, "signals": signals}
