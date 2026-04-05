from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import logging
from typing import Any

from app.apify_client import fetch_apify_query


logger = logging.getLogger(__name__)


def _extract_post_text(item: dict[str, Any]) -> str:
    return str(
        item.get("postText")
        or item.get("text")
        or item.get("content")
        or item.get("description")
        or ""
    ).strip()


def _extract_timestamp(item: dict[str, Any]) -> str:
    return str(
        item.get("timestamp")
        or item.get("createdAt")
        or item.get("publishedAt")
        or item.get("date")
        or ""
    ).strip()


def _extract_engagement(item: dict[str, Any]) -> dict[str, int]:
    likes = int(item.get("likes") or item.get("likeCount") or 0)
    comments = int(item.get("comments") or item.get("commentCount") or 0)
    return {"likes": likes, "comments": comments}


def _engagement_velocity(item: dict[str, Any]) -> float:
    engagement = _extract_engagement(item)
    total = engagement["likes"] + engagement["comments"]
    timestamp = _extract_timestamp(item)
    if not timestamp:
        return float(total)
    try:
        post_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        return float(total)

    now = datetime.now(timezone.utc)
    hours = max(1.0, (now - post_dt).total_seconds() / 3600)
    return total / hours


def _author_title(item: dict[str, Any]) -> str:
    return str(
        item.get("authorTitle")
        or item.get("authorHeadline")
        or item.get("profileHeadline")
        or item.get("authorRole")
        or ""
    ).strip()


def _is_authority_post(item: dict[str, Any]) -> bool:
    title = _author_title(item).lower()
    if not title:
        return False
    return any(token in title for token in ["founder", "co-founder", "ceo", "head ", "head of", "director"])


def _is_high_engagement_post(item: dict[str, Any]) -> bool:
    engagement = _extract_engagement(item)
    return (engagement["likes"] + engagement["comments"]) >= 50


def _topics_from_text(text: str) -> list[str]:
    text_l = text.lower()
    topics: list[str] = []
    if any(token in text_l for token in ["hiring", "open role", "recruit"]):
        topics.append("hiring")
    if any(token in text_l for token in ["launch", "released", "ga", "beta"]):
        topics.append("product")
    if any(token in text_l for token in ["funding", "series", "investor"]):
        topics.append("funding")
    if any(token in text_l for token in ["client", "customer", "revenue", "arr"]):
        topics.append("commercial")
    if any(token in text_l for token in ["scaling", "scale", "infrastructure", "platform"]):
        topics.append("scaling")
    return topics


def collect_linkedin_signals(company: str) -> dict[str, Any]:
    queries = [
        f"{company} hiring",
        f"{company} scaling",
        f"{company} funding",
        f"{company} launch",
        f"{company} milestone",
        f"{company} achievement",
        f"{company} clients",
        f"{company} revenue",
    ]

    signals: list[dict[str, Any]] = []
    topic_counter: Counter[str] = Counter()
    raw_count = 0
    filtered_count = 0
    filter_reasons: Counter[str] = Counter()
    for query in queries:
        logger.info("linkedin query | company=%s query=%s", company, query)
        try:
            items = fetch_apify_query(query, limit=3)
        except RuntimeError:
            continue

        raw_count += len(items)

        for item in items:
            if not (_is_authority_post(item) or _is_high_engagement_post(item)):
                filter_reasons["low_authority_low_engagement"] += 1
                continue

            post_text = _extract_post_text(item)
            if not post_text:
                filter_reasons["empty_text"] += 1
                continue

            engagement = _extract_engagement(item)
            if len(post_text) < 30 and (engagement["likes"] + engagement["comments"]) < 80:
                filter_reasons["generic_low_value_post"] += 1
                continue

            topics = _topics_from_text(post_text)
            for topic in topics:
                topic_counter[topic] += 1

            filtered_count += 1

            signals.append(
                {
                    "type": "linkedin_post",
                    "raw_text": post_text,
                    "metadata": {
                        "query": query,
                        "author_role": _author_title(item),
                        "founder_boost": True,
                        "topics": topics,
                        "timestamp": _extract_timestamp(item),
                        "engagement": _extract_engagement(item),
                        "engagement_velocity": _engagement_velocity(item),
                        "url": str(item.get("url") or item.get("link") or ""),
                    },
                    "source": "linkedin",
                }
            )

            velocity = _engagement_velocity(item)
            if velocity >= 10:
                momentum_type = "viral_growth" if velocity >= 25 else "high_momentum"
                signals.append(
                    {
                        "type": momentum_type,
                        "raw_text": f"{momentum_type} detected from post engagement velocity",
                        "metadata": {
                            "query": query,
                            "author_role": _author_title(item),
                            "timestamp": _extract_timestamp(item),
                            "engagement": _extract_engagement(item),
                            "engagement_velocity": velocity,
                            "url": str(item.get("url") or item.get("link") or ""),
                        },
                        "source": "linkedin",
                    }
                )

    if not signals:
        # Broader fallback query to avoid empty output.
        try:
            fallback_items = fetch_apify_query(f"{company} company updates leadership", limit=3)
        except RuntimeError:
            fallback_items = []

        raw_count += len(fallback_items)
        best: dict[str, Any] | None = None
        best_eng = -1
        for item in fallback_items:
            text = _extract_post_text(item)
            if not text:
                continue
            eng = _extract_engagement(item)
            score = eng["likes"] + eng["comments"]
            if score > best_eng:
                best_eng = score
                best = item

        if best is not None:
            filtered_count += 1
            signals.append(
                {
                    "type": "linkedin_post",
                    "raw_text": _extract_post_text(best),
                    "metadata": {
                        "query": "fallback:company updates leadership",
                        "author_role": _author_title(best),
                        "founder_boost": _is_authority_post(best),
                        "topics": _topics_from_text(_extract_post_text(best)),
                        "timestamp": _extract_timestamp(best),
                        "engagement": _extract_engagement(best),
                        "engagement_velocity": _engagement_velocity(best),
                        "url": str(best.get("url") or best.get("link") or ""),
                        "fallback": True,
                    },
                    "source": "linkedin",
                }
            )

    if not signals:
        signals.append(
            {
                "type": "linkedin_post",
                "raw_text": f"Weak fallback signal: no strong LinkedIn posts found for {company}",
                "metadata": {
                    "query": "fallback:none",
                    "author_role": "unknown",
                    "founder_boost": False,
                    "topics": ["general"],
                    "timestamp": "",
                    "engagement": {"likes": 0, "comments": 0},
                    "engagement_velocity": 0.0,
                    "url": "",
                    "fallback": True,
                },
                "source": "linkedin",
            }
        )
        filter_reasons["hard_fallback_injected"] += 1
        filtered_count += 1

    repeated_themes = {topic: freq for topic, freq in topic_counter.items() if freq >= 2}
    if repeated_themes:
        signals.append(
            {
                "type": "narrative_trend",
                "raw_text": f"Founder narrative themes repeating: {repeated_themes}",
                "metadata": {
                    "themes": repeated_themes,
                    "frequency": sum(repeated_themes.values()),
                },
                "source": "linkedin",
            }
        )

    debug = {
        "raw": raw_count,
        "filtered": filtered_count,
        "filter_reasons": dict(filter_reasons),
    }
    logger.info("linkedin debug | company=%s raw=%s filtered=%s", company, raw_count, filtered_count)
    return {"company": company, "signals": signals, "agent_debug": {"linkedin": debug}}
