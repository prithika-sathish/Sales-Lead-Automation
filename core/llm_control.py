from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable

_LOCK = threading.Lock()
_LAST_CALL_TS = 0.0

_CACHE_PATH = Path(os.getenv("LLM_CACHE_PATH") or "output/llm_reasoning_cache.json")
_ALLOWED_DEFAULT = "icp_parsing,query_generation,strategy_expansion"


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _parse_allowed_tasks() -> set[str]:
    raw = _clean_text(os.getenv("LLM_ALLOWED_TASKS") or _ALLOWED_DEFAULT)
    parts = [item.strip().lower() for item in raw.split(",") if item.strip()]
    return set(parts)


def is_llm_allowed(task: str) -> bool:
    task_name = _clean_text(task).lower()
    if not task_name:
        return False
    return task_name in _parse_allowed_tasks()


def _min_interval() -> float:
    try:
        return float((os.getenv("LLM_MIN_INTERVAL") or "1.5").strip() or "1.5")
    except Exception:
        return 1.5


def run_rate_limited(fn: Callable[[], Any]) -> Any:
    global _LAST_CALL_TS
    with _LOCK:
        now = time.time()
        gap = _min_interval() - (now - _LAST_CALL_TS)
        if gap > 0:
            time.sleep(gap)
        _LAST_CALL_TS = time.time()
    return fn()


def _cache_key(task: str, payload: object) -> str:
    blob = json.dumps({"task": _clean_text(task).lower(), "payload": payload}, ensure_ascii=True, sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _load_cache() -> dict[str, Any]:
    if not _CACHE_PATH.exists():
        return {}
    try:
        data = json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_cache(payload: dict[str, Any]) -> None:
    _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _CACHE_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def cache_get(task: str, payload: object) -> Any | None:
    key = _cache_key(task, payload)
    data = _load_cache()
    return data.get(key)


def cache_set(task: str, payload: object, value: object) -> None:
    key = _cache_key(task, payload)
    data = _load_cache()
    data[key] = value
    _save_cache(data)
