from __future__ import annotations

from collections import Counter
import logging
from typing import Any

from app.apify_client import fetch_apify_query


logger = logging.getLogger(__name__)


TOPIC_KEYWORDS: dict[str, list[str]] = {
    "hiring": ["hiring", "talent", "recruit", "open role", "join our team"],
    "product_iteration": ["feature", "roadmap", "improvement", "feedback", "beta"],
    "customer_pain": ["issue", "complaint", "outage", "friction", "support"],
    "growth": ["launch", "growth", "expansion", "new market", "partnership"],
    "revenue": ["revenue", "arr", "mrr", "profit", "sales"],
}


def _extract_text(item: dict[str, Any]) -> str:
    return str(item.get("text") or item.get("content") or item.get("description") or "").strip()


def _topic_hits(text: str) -> list[str]:
    text_l = text.lower()
    hits: list[str] = []
    for topic, keywords in TOPIC_KEYWORDS.items():
        if any(keyword in text_l for keyword in keywords):
            hits.append(topic)
    return hits


def collect_content_signals(company: str) -> dict[str, Any]:
    queries = [
        f"{company} product update",
        f"{company} customer feedback",
        f"{company} growth update",
        f"{company} launch",
    ]
    signals: list[dict[str, Any]] = []
    topic_counter: Counter[str] = Counter()
    raw_count = 0
    filtered_count = 0
    filter_reasons: Counter[str] = Counter()

    for query in queries:
        logger.info("content query | company=%s query=%s", company, query)
        try:
            items = fetch_apify_query(query, limit=4)
        except RuntimeError:
            continue

        raw_count += len(items)

        for item in items:
            text = _extract_text(item)
            if not text:
                filter_reasons["empty_text"] += 1
                continue

            if len(text) < 25:
                filter_reasons["short_low_value_text"] += 1
                continue

            hits = _topic_hits(text)
            if not hits:
                # Keep medium-value company updates to increase useful signal yield.
                hits = ["company_update"]
                filter_reasons["topic_backfill_company_update"] += 1

            for topic in hits:
                topic_counter[topic] += 1

            filtered_count += 1

            signals.append(
                {
                    "type": "feature_update" if "product_iteration" in hits else "company_update",
                    "raw_text": text,
                    "metadata": {
                        "query": query,
                        "topics": hits,
                        "timestamp": str(item.get("timestamp") or item.get("date") or ""),
                        "url": str(item.get("url") or item.get("link") or ""),
                    },
                    "source": "content",
                }
            )

    if len(signals) >= 2:
        signals.append(
            {
                "type": "content_push",
                "raw_text": "Sustained content output indicates active market narrative push",
                "metadata": {"content_mentions": len(signals)},
                "source": "content",
            }
        )

    if not signals:
        try:
            fallback_items = fetch_apify_query(f"{company} blog product engineering", limit=3)
        except RuntimeError:
            fallback_items = []

        raw_count += len(fallback_items)
        for item in fallback_items:
            text = _extract_text(item)
            if not text:
                continue
            fallback_topics = _topic_hits(text)
            if not fallback_topics:
                fallback_topics = ["growth"]
            filtered_count += 1
            signals.append(
                {
                    "type": "company_update",
                    "raw_text": text,
                    "metadata": {
                        "query": "fallback:blog product engineering",
                        "topics": fallback_topics,
                        "timestamp": str(item.get("timestamp") or item.get("date") or ""),
                        "url": str(item.get("url") or item.get("link") or ""),
                        "fallback": True,
                    },
                    "source": "content",
                }
            )
            break

    if not signals:
        signals.append(
            {
                "type": "content_mention",
                "raw_text": f"Weak fallback signal: no high-value content found for {company}",
                "metadata": {
                    "query": "fallback:none",
                    "topics": ["growth"],
                    "timestamp": "",
                    "url": "",
                    "fallback": True,
                },
                "source": "content",
            }
        )
        filter_reasons["hard_fallback_injected"] += 1
        filtered_count += 1

    repeated_themes = {topic: freq for topic, freq in topic_counter.items() if freq >= 2}
    if repeated_themes:
        signals.append(
            {
                "type": "narrative_trend",
                "raw_text": f"Repeated themes detected: {repeated_themes}",
                "metadata": {
                    "themes": repeated_themes,
                    "frequency": sum(repeated_themes.values()),
                },
                "source": "content",
            }
        )

    debug = {
        "raw": raw_count,
        "filtered": filtered_count,
        "filter_reasons": dict(filter_reasons),
    }
    logger.info("content debug | company=%s raw=%s filtered=%s", company, raw_count, filtered_count)
    return {"company": company, "signals": signals, "agent_debug": {"content": debug}}
