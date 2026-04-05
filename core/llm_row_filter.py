from __future__ import annotations

import importlib
import importlib.util
import json
import os
import re
import time
import threading
from typing import Any

from core.llm_control import is_llm_allowed

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

if callable(load_dotenv):
    load_dotenv()

MAX_LLM_CALLS = int((os.getenv("MAX_LLM_CALLS") or "50").strip() or "50")
MAX_INPUT_LENGTH = int((os.getenv("ROW_FILTER_MAX_INPUT_LENGTH") or "300").strip() or "300")
BATCH_SIZE = int((os.getenv("ROW_FILTER_BATCH_SIZE") or "5").strip() or "5")
MIN_INTERVAL = float((os.getenv("ROW_FILTER_MIN_INTERVAL") or "1.5").strip() or "1.5")
GROQ_MODEL = (os.getenv("GROQ_MODEL") or "llama-3.3-70b-versatile").strip()
GROQ_MODEL_FALLBACK = (os.getenv("GROQ_MODEL_FALLBACK") or "llama-3.1-8b-instant").strip()

_CACHE: dict[str, dict[str, Any]] = {}
_CALLS_USED = 0
_LAST_CALL_TS = 0.0
_LOCK = threading.Lock()

SYSTEM_PROMPT = "Classify rows as relevant company signal or not. Reject jobs."


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _row_key(row: dict[str, Any]) -> str:
    title = _clean_text(row.get("title") or row.get("name") or row.get("company") or "")
    url = _clean_text(row.get("url") or row.get("website") or row.get("link") or "")
    source = _clean_text(row.get("source") or row.get("source_transport") or row.get("source_type") or "")
    return f"{title.lower()}|{url.lower()}|{source.lower()}"


def _extract_json_payload(text: str) -> dict[str, Any]:
    raw = _clean_text(text)
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.I)
        raw = re.sub(r"\s*```$", "", raw)
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        raw = raw[start : end + 1]
    try:
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _is_job_noise(title: str, description: str, source: str) -> bool:
    text = f"{title} {description} {source}".lower()
    job_tokens = [
        "job",
        "jobs",
        "career",
        "careers",
        "hiring",
        "apply",
        "remote role",
        "senior",
        "engineer",
        "vacancy",
        "opening",
    ]
    return any(token in text for token in job_tokens)


def _valid_result(obj: object) -> bool:
    return isinstance(obj, dict) and "keep" in obj and "confidence" in obj and "reason" in obj


def _rate_limited_call() -> None:
    global _LAST_CALL_TS
    with _LOCK:
        now = time.time()
        wait_for = MIN_INTERVAL - (now - _LAST_CALL_TS)
        if wait_for > 0:
            time.sleep(wait_for)
        _LAST_CALL_TS = time.time()


def _estimate_tokens(text: str) -> int:
    # Fast approximation used for logging and monitoring.
    return max(1, int(len(text) / 4))


def _extract_json_list_payload(text: str) -> list[dict[str, Any]]:
    raw = _clean_text(text)
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.I)
        raw = re.sub(r"\s*```$", "", raw)
    start = raw.find("[")
    end = raw.rfind("]")
    if start >= 0 and end > start:
        raw = raw[start : end + 1]
    try:
        payload = json.loads(raw)
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
    except Exception:
        return []
    return []


def _rate_limit_fail_result() -> dict[str, Any]:
    return {"keep": False, "confidence": 0.0, "reason": "row_filter_rate_limit"}


def _groq_classify_batch(rows: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    if importlib.util.find_spec("langchain_groq") is None or importlib.util.find_spec("langchain_core") is None:
        return {}

    api_key = (os.getenv("GROQ_API_KEY") or "").strip()
    if not api_key:
        return {}

    slim_rows: list[dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        title = _clean_text(row.get("title") or row.get("name") or row.get("company") or "")[:MAX_INPUT_LENGTH]
        source = _clean_text(row.get("source") or row.get("source_transport") or row.get("source_type") or "unknown")[:120]
        slim_rows.append({"id": idx, "title": title, "source": source})

    user_prompt = (
        "Classify each row. Return JSON list with objects {\"id\":number,\"keep\":true/false}.\n"
        f"{json.dumps(slim_rows, ensure_ascii=True)}"
    )
    print(f"ROWS_SENT_TO_LLM:{len(slim_rows)}", flush=True)
    print(f"ESTIMATED_TOKENS:{_estimate_tokens(user_prompt)}", flush=True)

    messages_mod = importlib.import_module("langchain_core.messages")
    HumanMessage = getattr(messages_mod, "HumanMessage")
    SystemMessage = getattr(messages_mod, "SystemMessage")
    ChatGroq = getattr(importlib.import_module("langchain_groq"), "ChatGroq")

    for model_name in [name for name in [GROQ_MODEL, GROQ_MODEL_FALLBACK] if name]:
        try:
            _rate_limited_call()
            llm = ChatGroq(api_key=api_key, model=model_name, temperature=0, timeout=10, max_retries=1)
            response = llm.invoke([
                SystemMessage(content=SYSTEM_PROMPT),
                HumanMessage(content=user_prompt),
            ])
            payload_list = _extract_json_list_payload(str(getattr(response, "content", "") or ""))
            if payload_list:
                mapped: dict[int, dict[str, Any]] = {}
                for item in payload_list:
                    row_id = int(item.get("id") or 0)
                    if row_id <= 0:
                        continue
                    keep = bool(item.get("keep"))
                    mapped[row_id] = {
                        "keep": keep,
                        "confidence": 0.9 if keep else 0.95,
                        "reason": "row_filter_llm_batch",
                    }
                if mapped:
                    print(f"ROW_FILTER_PROVIDER: GROQ model={model_name}", flush=True)
                    return mapped
        except Exception as exc:
            print(f"ROW_FILTER_GROQ_FAILED model={model_name}: {exc}", flush=True)
            text = str(exc).lower()
            if "rate limit" in text or "429" in text:
                time.sleep(5)
                return {idx: _rate_limit_fail_result() for idx in range(1, len(slim_rows) + 1)}
            continue

    return {}


def should_keep_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(rows, list) or not rows:
        return []

    if not is_llm_allowed("filtering"):
        deterministic: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                deterministic.append({"keep": False, "confidence": 0.0, "reason": "invalid_row"})
                continue
            title = _clean_text(row.get("title") or row.get("name") or row.get("company") or "")
            description = _clean_text(row.get("description") or row.get("snippet") or row.get("content") or "")
            source = _clean_text(row.get("source") or row.get("source_transport") or row.get("source_type") or "unknown")
            if _is_job_noise(title, description, source):
                deterministic.append({"keep": False, "confidence": 0.98, "reason": "job_noise_precheck"})
            else:
                deterministic.append({"keep": True, "confidence": 0.7, "reason": "deterministic_filter_gate"})
        return deterministic

    outputs: list[dict[str, Any]] = []
    pending_rows: list[dict[str, Any]] = []
    pending_keys: list[str] = []

    for row in rows:
        if not isinstance(row, dict):
            outputs.append({"keep": False, "confidence": 0.0, "reason": "invalid_row"})
            continue

        key = _row_key(row)
        with _LOCK:
            cached = _CACHE.get(key)
        if cached is not None:
            outputs.append(dict(cached))
            continue

        title = _clean_text(row.get("title") or row.get("name") or row.get("company") or "")
        description = _clean_text(row.get("description") or row.get("snippet") or row.get("content") or "")
        source = _clean_text(row.get("source") or row.get("source_transport") or row.get("source_type") or "unknown")

        if _is_job_noise(title, description, source):
            result = {"keep": False, "confidence": 0.98, "reason": "job_noise_precheck"}
            with _LOCK:
                _CACHE[key] = result
            outputs.append(result)
            continue

        pending_rows.append(row)
        pending_keys.append(key)
        outputs.append({"keep": False, "confidence": 0.0, "reason": "pending"})

    if not pending_rows:
        return outputs

    # Process only the pending rows in bounded batches.
    out_index = 0
    for i in range(0, len(pending_rows), max(1, BATCH_SIZE)):
        batch = pending_rows[i : i + max(1, BATCH_SIZE)]
        batch_keys = pending_keys[i : i + max(1, BATCH_SIZE)]

        global _CALLS_USED
        with _LOCK:
            if _CALLS_USED >= MAX_LLM_CALLS:
                for key in batch_keys:
                    _CACHE[key] = {"keep": False, "confidence": 0.0, "reason": "llm_call_budget_exhausted"}
                continue
            _CALLS_USED += 1

        batch_result = _groq_classify_batch(batch)
        for idx, key in enumerate(batch_keys, start=1):
            decision = batch_result.get(idx) if isinstance(batch_result, dict) else None
            if not _valid_result(decision):
                decision = {"keep": False, "confidence": 0.0, "reason": "row_filter_llm_failed"}

            normalized = {
                "keep": bool(decision.get("keep")),
                "confidence": max(0.0, min(1.0, float(decision.get("confidence") or 0.0))),
                "reason": _clean_text(decision.get("reason") or "row_filter_unknown"),
            }
            with _LOCK:
                _CACHE[key] = normalized

        out_index += len(batch)

    # Rehydrate in original order.
    rebuilt: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            rebuilt.append({"keep": False, "confidence": 0.0, "reason": "invalid_row"})
            continue
        key = _row_key(row)
        with _LOCK:
            rebuilt.append(dict(_CACHE.get(key) or {"keep": False, "confidence": 0.0, "reason": "row_filter_missing"}))
    return rebuilt


def should_keep_row(row: dict) -> dict:
    """Compatibility wrapper for single-row callers."""
    decisions = should_keep_rows([row])
    if decisions:
        return decisions[0]
    return {"keep": False, "confidence": 0.0, "reason": "invalid_row"}


def get_llm_calls_used() -> int:
    with _LOCK:
        return int(_CALLS_USED)
