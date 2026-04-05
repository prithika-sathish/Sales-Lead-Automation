from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from core.history_db import history_db


def _signal_id(company: str, signal_type: str, source: str, raw_text: str) -> str:
    payload = f"{company.lower()}|{signal_type.lower()}|{source.lower()}|{raw_text.strip().lower()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _history_runs(company: str, limit_runs: int = 6) -> list[dict[str, Any]]:
    rows = history_db.get_recent_rows(company, limit_runs=limit_runs)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        run_id = str(row.get("run_id") or "")
        if not run_id:
            continue
        grouped.setdefault(run_id, []).append(row)
    return list(grouped.values())


def _avg_hiring_count(history_runs: list[list[dict[str, Any]]]) -> float:
    if not history_runs:
        return 0.0
    counts: list[int] = []
    for run in history_runs:
        count = 0
        for row in run:
            row_type = str(row.get("signal_type") or "")
            if row_type in {"hiring", "hiring_spike", "sales_expansion"}:
                count += 1
        counts.append(count)
    return sum(counts) / max(1, len(counts))


def _avg_engagement_value(history_runs: list[list[dict[str, Any]]]) -> float:
    if not history_runs:
        return 0.0
    values: list[int] = []
    engagement_types = {"momentum", "content_push", "narrative_trend", "traffic_growth"}
    for run in history_runs:
        run_total = 0
        for row in run:
            if str(row.get("signal_type") or "") in engagement_types:
                run_total += int(row.get("value") or 0)
        values.append(run_total)
    return sum(values) / max(1, len(values))


def _repeated_topics(history_runs: list[list[dict[str, Any]]], current_topics: set[str]) -> set[str]:
    if not history_runs or not current_topics:
        return set()

    run_topic_sets: list[set[str]] = []
    for run in history_runs:
        topics: set[str] = set()
        for row in run:
            if str(row.get("signal_type") or "") != "topic_marker":
                continue
            topic = str(row.get("topic") or "").strip().lower()
            if topic:
                topics.add(topic)
        run_topic_sets.append(topics)

    repeated: set[str] = set()
    for topic in current_topics:
        run_hits = sum(1 for topic_set in run_topic_sets if topic in topic_set)
        if run_hits >= 2:
            repeated.add(topic)
    return repeated


def detect_trend_signals(company: str, normalized_signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    trends: list[dict[str, Any]] = []
    timestamp = datetime.now(timezone.utc).isoformat()

    history_runs = _history_runs(company, limit_runs=6)
    # Require multiple historical runs to avoid false trend labels from a single baseline.
    if len(history_runs) < 2:
        return trends

    recent_hiring = [
        sig
        for sig in normalized_signals
        if str(sig.get("signal_type") or "") in {"hiring", "hiring_spike", "sales_expansion"}
        and int(sig.get("recency_score") or 0) >= 4
    ]
    current_hiring_count = len(recent_hiring)
    avg_hiring = _avg_hiring_count(history_runs)
    if current_hiring_count >= 2 and current_hiring_count >= (avg_hiring + 1):
        raw_text = "Hiring signals increasing over recent window"
        trends.append(
            {
                "company": company,
                "signal_id": _signal_id(company, "hiring_trend", "trend", raw_text),
                "signal_type": "hiring_trend",
                "signal_strength": 4,
                "recency_score": 5,
                "final_score": 20,
                "signal_score": 20,
                "timestamp": timestamp,
                "metadata": {
                    "count": current_hiring_count,
                    "historical_avg": round(avg_hiring, 2),
                    "raw_text": raw_text,
                },
                "source": "trend",
            }
        )

    current_topics: set[str] = set()
    for sig in normalized_signals:
        signal_type = str(sig.get("signal_type") or "").strip().lower()
        if signal_type:
            current_topics.add(signal_type.replace("_", " "))

        metadata = sig.get("metadata") if isinstance(sig.get("metadata"), dict) else {}
        topics = metadata.get("topics")
        if not isinstance(topics, list):
            continue
        for topic in topics:
            topic_name = str(topic).strip().lower()
            if topic_name:
                current_topics.add(topic_name)

    repeated_topic_set = _repeated_topics(history_runs, current_topics)
    if repeated_topic_set:
        raw_text = "Repeated topics across runs indicate strategic focus"
        trends.append(
            {
                "company": company,
                "signal_id": _signal_id(company, "strategic_focus", "trend", raw_text),
                "signal_type": "strategic_focus",
                "signal_strength": 4,
                "recency_score": 5,
                "final_score": 20,
                "signal_score": 20,
                "timestamp": timestamp,
                "metadata": {
                    "topics": sorted(repeated_topic_set),
                    "raw_text": raw_text,
                },
                "source": "trend",
            }
        )

    engagement_signals = [
        sig
        for sig in normalized_signals
        if str(sig.get("signal_type") or "") in {"momentum", "content_push", "narrative_trend", "traffic_growth"}
        and int(sig.get("recency_score") or 0) >= 4
    ]
    current_engagement_value = sum(int(sig.get("final_score") or 0) for sig in engagement_signals)
    avg_engagement = _avg_engagement_value(history_runs)
    if current_engagement_value >= 12 and current_engagement_value >= (avg_engagement * 1.2):
        raw_text = "Engagement velocity is rising across recent company mentions"
        trends.append(
            {
                "company": company,
                "signal_id": _signal_id(company, "momentum", "trend", raw_text),
                "signal_type": "momentum",
                "signal_strength": 4,
                "recency_score": 5,
                "final_score": 20,
                "signal_score": 20,
                "timestamp": timestamp,
                "metadata": {
                    "value": current_engagement_value,
                    "historical_avg": round(avg_engagement, 2),
                    "raw_text": raw_text,
                },
                "source": "trend",
            }
        )

    return trends
