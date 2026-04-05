from __future__ import annotations

import importlib
import importlib.util
import json
import os
import re
import time
from typing import Any

from core.llm_control import cache_get
from core.llm_control import cache_set
from core.llm_control import is_llm_allowed
from core.llm_control import run_rate_limited


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalize_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [_clean_text(item) for item in value if _clean_text(item)]
    if isinstance(value, str):
        return [_clean_text(part) for part in re.split(r"[,;\n]", value) if _clean_text(part)]
    return []


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


def _default_sources() -> list[dict[str, Any]]:
    names = ["github", "reddit", "g2", "crunchbase", "jobs", "news", "product_hunt"]
    out: list[dict[str, Any]] = []
    for name in names:
        out.append(
            {
                "name": name,
                "why": "broad company discovery coverage",
                "what_to_extract": ["company_name", "url", "description", "signals"],
            }
        )
    return out


def _fallback_plan(markdown_text: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
    text = _clean_text(markdown_text).lower()
    context = context if isinstance(context, dict) else {}

    industries: list[str] = []
    if any(token in text for token in ["saas", "subscription", "recurring revenue", "billing"]):
        industries.append("saas")
    if any(token in text for token in ["fintech", "payments", "checkout"]):
        industries.append("fintech")
    if not industries:
        industries = ["b2b software"]

    keywords = []
    for token in [
        "subscription billing",
        "usage based pricing",
        "api pricing",
        "recurring revenue",
        "billing infrastructure",
    ]:
        if token in text:
            keywords.append(token)
    if not keywords:
        keywords = ["subscription billing", "usage based pricing", "recurring revenue"]

    geo = []
    for region in ["india", "usa", "uk", "europe", "apac", "global"]:
        if region in text:
            geo.append(region.title())
    if not geo:
        geo = ["Global"]

    failed = _normalize_list(context.get("failed_attempts"))
    strategies = [
        {
            "type": "keyword_based",
            "reason": "map direct ICP terms to broad company-discovery queries",
            "queries": [
                f"{geo[0]} {industries[0]} companies {keywords[0]}",
                f"{geo[0]} startups {keywords[1] if len(keywords) > 1 else keywords[0]}",
            ],
        },
        {
            "type": "signal_based",
            "reason": "target active demand and growth indicators",
            "queries": [
                f"{geo[0]} {industries[0]} companies hiring billing engineers",
                f"{geo[0]} {industries[0]} startups api pricing",
            ],
        },
        {
            "type": "tech_stack_based",
            "reason": "find companies by implementation signal",
            "queries": [
                f"{geo[0]} companies using stripe billing",
                f"{geo[0]} companies using recurring billing software",
            ],
        },
    ]

    if failed:
        strategies[0]["queries"] = [q for q in strategies[0]["queries"] if q not in set(failed)]

    return {
        "icp_struct": {
            "industries": industries,
            "company_types": ["b2b saas", "subscription businesses"],
            "geo": geo,
            "keywords": keywords,
            "pain_signals": ["manual billing", "pricing complexity", "revenue leakage"],
            "tools_used": ["stripe"],
            "exclude": ["job boards", "directories", "media listings"],
        },
        "search_strategies": strategies,
        "sources": _default_sources(),
        "extraction_hints": [
            "prefer official company pages and pricing pages",
            "capture domain from canonical url",
            "extract intent signals from hiring, pricing, and product copy",
        ],
    }


def _validate_plan(payload: dict[str, Any]) -> dict[str, Any]:
    base = _fallback_plan("", {})
    if not isinstance(payload, dict):
        return base

    icp_struct = payload.get("icp_struct") if isinstance(payload.get("icp_struct"), dict) else {}
    search_strategies = payload.get("search_strategies") if isinstance(payload.get("search_strategies"), list) else []
    sources = payload.get("sources") if isinstance(payload.get("sources"), list) else []
    extraction_hints = payload.get("extraction_hints") if isinstance(payload.get("extraction_hints"), list) else []

    result = {
        "icp_struct": {
            "industries": _normalize_list(icp_struct.get("industries")) or base["icp_struct"]["industries"],
            "company_types": _normalize_list(icp_struct.get("company_types")) or base["icp_struct"]["company_types"],
            "geo": _normalize_list(icp_struct.get("geo")) or base["icp_struct"]["geo"],
            "keywords": _normalize_list(icp_struct.get("keywords")) or base["icp_struct"]["keywords"],
            "pain_signals": _normalize_list(icp_struct.get("pain_signals")) or base["icp_struct"]["pain_signals"],
            "tools_used": _normalize_list(icp_struct.get("tools_used")),
            "exclude": _normalize_list(icp_struct.get("exclude")) or base["icp_struct"]["exclude"],
        },
        "search_strategies": [],
        "sources": [],
        "extraction_hints": _normalize_list(extraction_hints) or base["extraction_hints"],
    }

    valid_types = {"competitor_based", "signal_based", "keyword_based", "hiring_based", "tech_stack_based"}
    for item in search_strategies:
        if not isinstance(item, dict):
            continue
        strategy_type = _clean_text(item.get("type")).lower()
        if strategy_type not in valid_types:
            continue
        queries = _normalize_list(item.get("queries"))
        if not queries:
            continue
        result["search_strategies"].append(
            {
                "type": strategy_type,
                "reason": _clean_text(item.get("reason") or "structured discovery"),
                "queries": queries[:12],
            }
        )

    if not result["search_strategies"]:
        result["search_strategies"] = base["search_strategies"]

    for item in sources:
        if not isinstance(item, dict):
            continue
        name = _clean_text(item.get("name")).lower()
        if not name:
            continue
        result["sources"].append(
            {
                "name": name,
                "why": _clean_text(item.get("why") or "broad company discovery coverage"),
                "what_to_extract": ["company_name", "url", "description", "signals"],
            }
        )
    if not result["sources"]:
        result["sources"] = base["sources"]

    return result


def _llm_plan(markdown_text: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
    if importlib.util.find_spec("langchain_groq") is None or importlib.util.find_spec("langchain_core") is None:
        return {}
    api_key = _clean_text(os.getenv("GROQ_API_KEY"))
    if not api_key:
        return {}

    context = context if isinstance(context, dict) else {}
    model_name = _clean_text(os.getenv("REASONING_LLM_MODEL") or os.getenv("GROQ_MODEL") or "llama-3.3-70b-versatile")

    prompt = (
        "Return compact deterministic JSON only. No prose. No markdown.\n"
        "Schema:\n"
        "{\n"
        "  \"icp_struct\": {\n"
        "    \"industries\": [],\n"
        "    \"company_types\": [],\n"
        "    \"geo\": [],\n"
        "    \"keywords\": [],\n"
        "    \"pain_signals\": [],\n"
        "    \"tools_used\": [],\n"
        "    \"exclude\": []\n"
        "  },\n"
        "  \"search_strategies\": [\n"
        "    {\"type\":\"competitor_based|signal_based|keyword_based|hiring_based|tech_stack_based\",\"reason\":\"\",\"queries\":[]}\n"
        "  ],\n"
        "  \"sources\": [\n"
        "    {\"name\":\"github|reddit|g2|crunchbase|jobs|news|etc\",\"why\":\"\",\"what_to_extract\":[\"company_name\",\"url\",\"description\",\"signals\"]}\n"
        "  ],\n"
        "  \"extraction_hints\": []\n"
        "}\n"
        "Constraints: no companies, no leads, deterministic, compact, broad recall.\n"
        f"Previous context: {json.dumps(context, ensure_ascii=True)}\n"
        "ICP markdown:\n"
        f"{_clean_text(markdown_text)}"
    )

    messages_mod = importlib.import_module("langchain_core.messages")
    HumanMessage = getattr(messages_mod, "HumanMessage")
    SystemMessage = getattr(messages_mod, "SystemMessage")
    ChatGroq = getattr(importlib.import_module("langchain_groq"), "ChatGroq")

    def _invoke_once() -> dict[str, Any]:
        chat = ChatGroq(api_key=api_key, model=model_name, temperature=0, timeout=20, max_retries=1)
        response = chat.invoke([
            SystemMessage(content="You are an ICP reasoning engine for search planning."),
            HumanMessage(content=prompt),
        ])
        return _extract_json_payload(str(getattr(response, "content", "") or ""))

    try:
        return run_rate_limited(_invoke_once)
    except Exception as exc:
        text = str(exc).lower()
        if "429" in text or "rate limit" in text:
            time.sleep(5)
            try:
                return run_rate_limited(_invoke_once)
            except Exception:
                return {}
        return {}


def build_reasoning_plan(markdown_text: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
    task = "icp_parsing"
    payload = {
        "markdown_text": _clean_text(markdown_text),
        "context": context if isinstance(context, dict) else {},
    }

    cached = cache_get(task, payload)
    if isinstance(cached, dict):
        return _validate_plan(cached)

    if not is_llm_allowed(task):
        fallback = _fallback_plan(markdown_text, context)
        cache_set(task, payload, fallback)
        return fallback

    llm_payload = _llm_plan(markdown_text, context)
    if isinstance(llm_payload, dict) and llm_payload:
        validated = _validate_plan(llm_payload)
        cache_set(task, payload, validated)
        return validated

    fallback = _fallback_plan(markdown_text, context)
    cache_set(task, payload, fallback)
    return fallback
