from __future__ import annotations

import json
from pathlib import Path
from typing import Any

FEEDBACK_PATH = Path("learning/ingestion_feedback.json")


def _default_payload() -> dict[str, Any]:
    return {
        "bad_entities": [],
        "good_entities": [],
        "bad_queries": [],
        "good_queries": [],
    }


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _load() -> dict[str, Any]:
    if not FEEDBACK_PATH.exists():
        return _default_payload()
    try:
        payload = json.loads(FEEDBACK_PATH.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            base = _default_payload()
            base.update(payload)
            for key in ["bad_entities", "good_entities", "bad_queries", "good_queries"]:
                if not isinstance(base.get(key), list):
                    base[key] = []
            return base
    except Exception:
        pass
    return _default_payload()


def _save(payload: dict[str, Any]) -> None:
    FEEDBACK_PATH.parent.mkdir(parents=True, exist_ok=True)
    FEEDBACK_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def _dedupe_dict_items(items: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        raw = _clean_text(item.get(key) or item.get("name") or item.get("query") or "")
        if not raw:
            continue
        marker = raw.lower()
        if marker in seen:
            continue
        seen.add(marker)
        out.append(item)
    return out[-500:]


def log_entity(entity: dict[str, Any], is_good: bool) -> None:
    payload = _load()
    bucket = "good_entities" if is_good else "bad_entities"
    entry = {
        "name": _clean_text(entity.get("company_name") or entity.get("name") or ""),
        "reason": _clean_text(entity.get("reason") or ""),
        "source": _clean_text(entity.get("source") or ""),
    }
    payload[bucket].append(entry)
    payload[bucket] = _dedupe_dict_items(payload[bucket], "name")
    _save(payload)


def log_query(query: str, is_good: bool) -> None:
    payload = _load()
    bucket = "good_queries" if is_good else "bad_queries"
    entry = {"query": _clean_text(query)}
    payload[bucket].append(entry)
    payload[bucket] = _dedupe_dict_items(payload[bucket], "query")
    _save(payload)


def get_failed_queries() -> list[str]:
    payload = _load()
    return [
        _clean_text(item.get("query"))
        for item in payload.get("bad_queries", [])
        if isinstance(item, dict) and _clean_text(item.get("query"))
    ]


def get_failed_patterns() -> list[str]:
    payload = _load()
    reasons: list[str] = []
    for item in payload.get("bad_entities", []):
        if not isinstance(item, dict):
            continue
        reason = _clean_text(item.get("reason"))
        if reason:
            reasons.append(reason)
    uniq: list[str] = []
    seen: set[str] = set()
    for reason in reasons:
        lower = reason.lower()
        if lower in seen:
            continue
        seen.add(lower)
        uniq.append(reason)
    return uniq[:30]
