from __future__ import annotations

import importlib
import json
import os
import re
from typing import Any
from urllib.parse import urlparse

import requests

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

if callable(load_dotenv):
    load_dotenv()

GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
GROQ_MODEL_FALLBACK = os.getenv("GROQ_MODEL_FALLBACK", "llama-3.1-8b-instant")


def _log(message: str) -> None:
    print(message, flush=True)

NOISE_TOKENS = {"hiring", "job", "jobs", "career", "careers", "apply", "role", "roles", "remote"}
COMMON_SUFFIXES = ["venture", "ventures", "capital", "labs", "systems", "software", "global", "europe", "india"]


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _extract_host(value: object) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = f"https://{raw}"
    try:
        host = urlparse(raw).netloc.lower()
    except Exception:
        return ""
    return host[4:] if host.startswith("www.") else host


def _root_domain(host: str) -> str:
    parts = [part for part in host.split(".") if part]
    if len(parts) < 2:
        return ""
    if len(parts) >= 3 and parts[-2] in {"co", "com", "org", "net"}:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def normalize_company_name(name: str) -> str:
    cleaned = _clean_text(name)
    cleaned = re.sub(r"[^A-Za-z0-9\s&.-]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .,-")
    if not cleaned:
        return ""
    tokens = [tok for tok in cleaned.split() if tok and tok.lower() not in NOISE_TOKENS]
    if len(tokens) == 1:
        token = tokens[0]
        lowered = token.lower()
        for suffix in COMMON_SUFFIXES:
            if lowered.endswith(suffix) and len(lowered) > len(suffix) + 2:
                prefix = token[: len(token) - len(suffix)]
                suffix_token = token[len(token) - len(suffix) :]
                tokens = [prefix, suffix_token]
                break
    out: list[str] = []
    for token in tokens:
        lower = token.lower()
        if lower in {"ai", "api", "saas", "crm", "erp", "hr"}:
            out.append(token.upper())
        elif token.isupper() and len(token) <= 4:
            out.append(token)
        else:
            out.append(token.capitalize())
    return " ".join(out)


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


def _langchain_extract(row: dict[str, Any], retries: int = 2) -> dict[str, Any] | None:
    if importlib.util.find_spec("langchain_core") is None:
        return None

    title = _clean_text(row.get("title") or row.get("name") or row.get("company") or "")
    description = _clean_text(row.get("description") or row.get("snippet") or row.get("content") or "")
    url = _clean_text(row.get("url") or row.get("website") or row.get("link") or "")
    source = _clean_text(row.get("source") or row.get("source_transport") or row.get("source_type") or "unknown")

    system_prompt = (
        "You extract REAL company entities from noisy web data. Ignore job roles, locations, generic phrases. "
        "Only return real organizations. Return ONLY strict JSON."
    )
    user_prompt = (
        "Raw Data:\n"
        f"- Title: {title}\n"
        f"- Description: {description}\n"
        f"- URL: {url}\n"
        f"- Source: {source}\n\n"
        "Rules:\n"
        "- Extract ONLY company name\n"
        "- If unclear -> confidence < 0.5\n"
        "- If not a company -> is_valid_company = false\n"
        "- Normalize spacing (e.g., Viking Venture, NOT Vikingventure)\n\n"
        "Return JSON exactly with keys: company_name, clean_name, domain, confidence, is_valid_company, reason"
    )

    api_key = (os.getenv("GROQ_API_KEY") or "").strip()
    if not api_key or importlib.util.find_spec("langchain_groq") is None:
        return None

    messages_mod = importlib.import_module("langchain_core.messages")
    HumanMessage = getattr(messages_mod, "HumanMessage")
    SystemMessage = getattr(messages_mod, "SystemMessage")

    _log("=== LLM EXTRACTION START ===")
    _log("LLM_PROVIDER: GROQ")
    ChatGroq = getattr(importlib.import_module("langchain_groq"), "ChatGroq")
    model_candidates = [GROQ_MODEL]
    if GROQ_MODEL_FALLBACK and GROQ_MODEL_FALLBACK != GROQ_MODEL:
        model_candidates.append(GROQ_MODEL_FALLBACK)

    for model_name in model_candidates:
        _log(f"LLM_MODEL: {model_name}")
        try:
            chat_model = ChatGroq(
                api_key=api_key,
                model=model_name,
                temperature=0,
                timeout=20,
            )
        except Exception as exc:
            _log(f"GROQ_INIT_FAILED model={model_name}: {exc}")
            continue

        for attempt in range(max(1, retries + 1)):
            try:
                response = chat_model.invoke([
                    SystemMessage(content=system_prompt),
                    HumanMessage(content=user_prompt),
                ])
                payload = _extract_json_payload(getattr(response, "content", ""))
                if _is_valid_llm_output(payload):
                    _log(f"LLM_RESULT: {json.dumps(payload, ensure_ascii=True)}")
                    return payload
                _log(f"GROQ_INVALID_JSON model={model_name} attempt={attempt + 1}")
            except Exception as exc:
                _log(f"GROQ_FAILED model={model_name} attempt={attempt + 1}: {exc}")
                continue

    return None


def _gemini_rest_extract(row: dict[str, Any], retries: int = 2) -> dict[str, Any] | None:
    # Gemini is intentionally disabled in the main ingestion path.
    _log("LLM_PROVIDER: GEMINI_DISABLED")
    return None


def _is_valid_llm_output(obj: object) -> bool:
    return (
        isinstance(obj, dict)
        and "company_name" in obj
        and "confidence" in obj
        and "is_valid_company" in obj
    )


def _hard_fail() -> dict[str, Any]:
    return {
        "company_name": "",
        "clean_name": "",
        "domain": "",
        "confidence": 0.0,
        "is_valid_company": False,
        "reason": "llm_failed_all_providers",
    }


def _normalize_llm_payload(payload: dict[str, Any], row: dict[str, Any]) -> dict[str, Any] | None:
    if not _is_valid_llm_output(payload):
        return None
    normalized = {
        "company_name": normalize_company_name(str(payload.get("company_name") or payload.get("clean_name") or "")),
        "clean_name": normalize_company_name(str(payload.get("clean_name") or payload.get("company_name") or "")),
        "domain": _root_domain(_extract_host(payload.get("domain") or row.get("url") or row.get("website") or row.get("link") or "")),
        "confidence": max(0.0, min(1.0, float(payload.get("confidence") or 0.0))),
        "is_valid_company": bool(payload.get("is_valid_company")) and bool(normalize_company_name(str(payload.get("company_name") or payload.get("clean_name") or ""))),
        "reason": _clean_text(payload.get("reason") or "llm_extraction"),
    }
    return normalized


def _heuristic_extract(row: dict[str, Any]) -> dict[str, Any]:
    url = _clean_text(row.get("url") or row.get("website") or row.get("link") or "")
    host = _extract_host(url)
    domain = _root_domain(host)
    domain_name = normalize_company_name((domain.split(".")[0] if domain else "").replace("-", " "))
    title = normalize_company_name(_clean_text(row.get("title") or row.get("name") or row.get("company") or ""))
    name = domain_name or title
    low = name.lower()
    bad = any(tok in low for tok in ["job", "hiring", "career", "apply", "list", "directory"])
    confidence = 0.72 if domain_name else 0.45
    if bad:
        confidence = min(confidence, 0.35)
    return {
        "company_name": name,
        "clean_name": name,
        "domain": domain,
        "confidence": confidence,
        "is_valid_company": bool(name) and not bad,
        "reason": "heuristic_fallback",
    }


def extract_company_entity(row: dict) -> dict:
    """LangChain-first entity extraction with Groq/Gemini; Gemini REST and heuristic fallback."""
    if not isinstance(row, dict):
        return {
            "company_name": "",
            "clean_name": "",
            "domain": "",
            "confidence": 0.0,
            "is_valid_company": False,
            "reason": "invalid_row",
        }

    payload = None
    try:
        payload = _langchain_extract(row, retries=2)
    except Exception as exc:
        _log(f"GROQ_FAILED: {exc}")

    if _is_valid_llm_output(payload):
        normalized = _normalize_llm_payload(payload, row)
        if normalized and normalized["is_valid_company"] and normalized["confidence"] >= 0.5:
            return normalized

    try:
        payload = _gemini_rest_extract(row, retries=2)
    except Exception as exc:
        _log(f"GEMINI_FAILED: {exc}")

    if _is_valid_llm_output(payload):
        normalized = _normalize_llm_payload(payload, row)
        if normalized and normalized["is_valid_company"] and normalized["confidence"] >= 0.5:
            return normalized

    _log("LLM_PROVIDER: HARD_FAIL")
    return _hard_fail()
