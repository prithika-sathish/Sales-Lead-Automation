from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

FEEDBACK_PATH = Path("learning/ingestion_feedback.json")


def _default_payload() -> dict[str, Any]:
    return {
        "accepted_entities": {},
        "rejected_entities": {},
        "source_stats": {},
        "query_stats": {},
        "failed_patterns": {},
        "updated_at": "",
    }


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def load_ingestion_feedback() -> dict[str, Any]:
    if not FEEDBACK_PATH.exists():
        return _default_payload()
    try:
        payload = json.loads(FEEDBACK_PATH.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            base = _default_payload()
            base.update(payload)
            return base
    except Exception:
        pass
    return _default_payload()


def _save_ingestion_feedback(payload: dict[str, Any]) -> None:
    FEEDBACK_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload["updated_at"] = datetime.now(UTC).isoformat()
    FEEDBACK_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def _update_source_stats(payload: dict[str, Any], source: str, accepted: int, rejected: int) -> None:
    source_stats = payload.get("source_stats") if isinstance(payload.get("source_stats"), dict) else {}
    row = source_stats.get(source) if isinstance(source_stats.get(source), dict) else {}
    row["accepted"] = int(row.get("accepted") or 0) + accepted
    row["rejected"] = int(row.get("rejected") or 0) + rejected
    total = max(1, row["accepted"] + row["rejected"])
    row["reliability"] = round(row["accepted"] / total, 4)
    source_stats[source] = row
    payload["source_stats"] = source_stats


def _update_query_stats(payload: dict[str, Any], query: str, accepted: int, rejected: int) -> None:
    query = _clean_text(query)
    if not query:
        return
    query_stats = payload.get("query_stats") if isinstance(payload.get("query_stats"), dict) else {}
    row = query_stats.get(query) if isinstance(query_stats.get(query), dict) else {}
    row["accepted"] = int(row.get("accepted") or 0) + accepted
    row["rejected"] = int(row.get("rejected") or 0) + rejected
    total = max(1, row["accepted"] + row["rejected"])
    row["noise_rate"] = round(row["rejected"] / total, 4)
    query_stats[query] = row
    payload["query_stats"] = query_stats


def _update_entity_bucket(bucket: dict[str, Any], entities: list[dict[str, Any]]) -> None:
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        name = _clean_text(entity.get("company_name") or entity.get("name") or "")
        if not name:
            continue
        row = bucket.get(name) if isinstance(bucket.get(name), dict) else {}
        row["count"] = int(row.get("count") or 0) + 1
        row["last_reason"] = _clean_text(entity.get("reason") or row.get("last_reason") or "")
        row["last_source"] = _clean_text(entity.get("source") or row.get("last_source") or "")
        row["last_seen"] = datetime.now(UTC).isoformat()
        bucket[name] = row


def _update_failed_patterns(payload: dict[str, Any], rejected_entities: list[dict[str, Any]]) -> None:
    failed_patterns = payload.get("failed_patterns") if isinstance(payload.get("failed_patterns"), dict) else {}
    for item in rejected_entities:
        reason = _clean_text(item.get("reason") or "unknown_reason")
        if not reason:
            continue
        failed_patterns[reason] = int(failed_patterns.get(reason) or 0) + 1
    payload["failed_patterns"] = failed_patterns


def update_ingestion_feedback(
    *,
    source: str,
    query: str,
    accepted_entities: list[dict[str, Any]],
    rejected_entities: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = load_ingestion_feedback()

    accepted_bucket = payload.get("accepted_entities") if isinstance(payload.get("accepted_entities"), dict) else {}
    rejected_bucket = payload.get("rejected_entities") if isinstance(payload.get("rejected_entities"), dict) else {}

    _update_entity_bucket(accepted_bucket, accepted_entities)
    _update_entity_bucket(rejected_bucket, rejected_entities)

    payload["accepted_entities"] = accepted_bucket
    payload["rejected_entities"] = rejected_bucket

    _update_source_stats(payload, _clean_text(source) or "unknown", len(accepted_entities), len(rejected_entities))
    _update_query_stats(payload, query, len(accepted_entities), len(rejected_entities))
    _update_failed_patterns(payload, rejected_entities)

    _save_ingestion_feedback(payload)
    return payload


def get_feedback_signals(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    state = payload if isinstance(payload, dict) else load_ingestion_feedback()

    query_stats = state.get("query_stats") if isinstance(state.get("query_stats"), dict) else {}
    bad_queries: list[str] = []
    for query, stats in query_stats.items():
        if not isinstance(stats, dict):
            continue
        accepted = int(stats.get("accepted") or 0)
        rejected = int(stats.get("rejected") or 0)
        total = accepted + rejected
        noise_rate = float(stats.get("noise_rate") or 0.0)
        if total >= 3 and noise_rate >= 0.5:
            bad_queries.append(str(query))

    source_stats = state.get("source_stats") if isinstance(state.get("source_stats"), dict) else {}
    source_reliability = {
        str(source): float(stats.get("reliability") or 0.0)
        for source, stats in source_stats.items()
        if isinstance(stats, dict)
    }

    failed_patterns_map = state.get("failed_patterns") if isinstance(state.get("failed_patterns"), dict) else {}
    failed_patterns = [
        key
        for key, _ in sorted(
            ((str(k), int(v)) for k, v in failed_patterns_map.items()),
            key=lambda item: item[1],
            reverse=True,
        )[:8]
        if _clean_text(key)
    ]

    return {
        "bad_queries": bad_queries,
        "failed_patterns": failed_patterns,
        "source_reliability": source_reliability,
    }
