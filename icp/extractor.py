from __future__ import annotations

import importlib
import json
import logging
import os
import re
from collections import Counter
from typing import Any
from app.stage_supervisor import supervise_stage


logger = logging.getLogger(__name__)

_SCHEMA_TEMPLATE: dict[str, Any] = {
    "product_type": "",
    "industry": "",
    "target_customers": "",
    "who_needs_this": "",
    "who_should_be_excluded": "",
    "company_size": "",
    "geography": "",
    "keywords": [],
    "pain_points": [],
}

_PAIN_TOKENS = [
    "pain",
    "problem",
    "challenge",
    "manual",
    "inefficient",
    "delay",
    "compliance",
    "churn",
    "cost",
    "integration",
    "accuracy",
]

_GEMINI_FALLBACK_MODELS = [
    "gemini-2.5-flash",
    "gemini-2.0-flash",
    "gemini-1.5-flash-latest",
    "gemini-1.5-pro-latest",
]


def _load_env_if_available() -> None:
    try:
        dotenv_module = importlib.import_module("dotenv")
    except Exception:  # noqa: BLE001
        return

    load_dotenv = getattr(dotenv_module, "load_dotenv", None)
    if not callable(load_dotenv):
        return

    try:
        load_dotenv()
        if not os.getenv("GEMINI_API_KEY"):
            load_dotenv(".env")
            load_dotenv(".env.local")
            load_dotenv(".env.example")
    except Exception:  # noqa: BLE001
        return


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _strip_json_fence(text: str) -> str:
    body = _clean_text(text)
    if body.startswith("```"):
        body = re.sub(r"^```(?:json)?", "", body, flags=re.I).strip()
        body = re.sub(r"```$", "", body).strip()
    return body


def _normalize_list(value: object) -> list[str]:
    if isinstance(value, list):
        items = [_clean_text(item) for item in value]
        return [item for item in items if item]
    if isinstance(value, str):
        parts = re.split(r"[,;\n]", value)
        return [item for item in (_clean_text(part) for part in parts) if item]
    return []


def _normalize_schema(raw: dict[str, Any]) -> dict[str, Any]:
    payload = dict(_SCHEMA_TEMPLATE)
    payload["product_type"] = _clean_text(raw.get("product_type"))
    payload["industry"] = _clean_text(raw.get("industry"))
    payload["target_customers"] = _clean_text(raw.get("target_customers"))
    payload["who_needs_this"] = _clean_text(raw.get("who_needs_this"))
    payload["who_should_be_excluded"] = _clean_text(raw.get("who_should_be_excluded"))
    payload["company_size"] = _clean_text(raw.get("company_size"))
    payload["geography"] = _clean_text(raw.get("geography"))
    payload["keywords"] = _normalize_list(raw.get("keywords"))
    payload["pain_points"] = _normalize_list(raw.get("pain_points"))
    return payload


def _extract_with_gemini(markdown_text: str) -> dict[str, Any]:
    _load_env_if_available()
    api_key = _clean_text(os.getenv("GEMINI_API_KEY"))
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")

    genai_module = importlib.import_module("google.genai")
    client = genai_module.Client(api_key=api_key)
    configured_model = _clean_text(os.getenv("ICP_LLM_MODEL") or os.getenv("GEMINI_MODEL"))
    model_candidates = [configured_model] if configured_model else []
    for fallback_model in _GEMINI_FALLBACK_MODELS:
        if fallback_model not in model_candidates:
            model_candidates.append(fallback_model)

    system_prompt = (
        "You extract B2B ICP from markdown for lead discovery. "
        "Return only valid JSON with keys: "
        "product_type, industry, target_customers, who_needs_this, who_should_be_excluded, "
        "company_size, geography, keywords, pain_points. "
        "who_needs_this must describe BUYER companies, not competitors. "
        "who_should_be_excluded must list competitor tool categories to filter out. "
        "keywords and pain_points must be arrays of concise strings."
    )

    user_prompt = (
        "Analyze the markdown and infer buyer-intent ICP. "
        "Focus on companies that would buy/use this product and clearly separate competitor categories.\n\n"
        "Markdown:\n"
        f"{markdown_text}"
    )

    prompt = f"{system_prompt}\n\n{user_prompt}"
    completion = None
    last_error: Exception | None = None
    for model in model_candidates:
        try:
            try:
                completion = client.models.generate_content(
                    model=model,
                    contents=prompt,
                    config={"temperature": 0},
                )
            except TypeError:
                completion = client.models.generate_content(
                    model=model,
                    contents=prompt,
                )
            break
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            continue

    if completion is None:
        raise RuntimeError(f"Gemini request failed for all models: {last_error}")

    content = _clean_text(getattr(completion, "text", "") or getattr(completion, "output_text", ""))
    if not content:
        raise RuntimeError("LLM returned empty content")

    parsed = json.loads(_strip_json_fence(content))
    if not isinstance(parsed, dict):
        raise RuntimeError("LLM output is not a JSON object")
    return _normalize_schema(parsed)


def _guess_industry(text: str) -> str:
    lowered = text.lower()
    if any(token in lowered for token in ["payroll", "hcm", "hrms", "human resources"]):
        return "hrtech"
    if any(token in lowered for token in ["fintech", "payments", "lending", "banking"]):
        return "fintech"
    if any(token in lowered for token in ["erp", "workflow", "automation", "enterprise software"]):
        return "b2b saas"
    if "saas" in lowered:
        return "saas"
    return ""


def _guess_target_customers(text: str) -> str:
    candidates = re.findall(r"(?:for|ideal for|built for|designed for)\s+([^\.\n]+)", text, flags=re.I)
    if candidates:
        return _clean_text(candidates[0])
    if "hr" in text.lower() or "people ops" in text.lower():
        return "HR teams and operations leaders"
    if "finance" in text.lower():
        return "Finance and operations teams"
    return ""


def _guess_company_size(text: str) -> str:
    lowered = text.lower()
    if any(token in lowered for token in ["startup", "smb", "small business"]):
        return "20-200"
    if any(token in lowered for token in ["mid-market", "growing company", "scale-up"]):
        return "50-1000"
    if "enterprise" in lowered:
        return "200-2000"
    return ""


def _guess_geography(text: str) -> str:
    lowered = text.lower()
    regions: list[str] = []
    for token in ["india", "singapore", "apac", "usa", "uk", "europe", "middle east"]:
        if re.search(rf"\b{re.escape(token)}\b", lowered) and token not in regions:
            regions.append(token.title())
    return ", ".join(regions)


def _guess_keywords(text: str) -> list[str]:
    lowered = text.lower()
    candidates = [
        "saas",
        "b2b",
        "payroll",
        "erp",
        "hrms",
        "hcm",
        "compliance",
        "automation",
        "api",
        "integration",
        "onboarding",
        "attendance",
        "performance",
        "benefits",
        "pricing",
    ]
    return [token for token in candidates if token in lowered][:12]


def _guess_pain_points(text: str) -> list[str]:
    sentences = re.split(r"(?<=[.!?])\s+", _clean_text(text))
    found: list[str] = []
    for sentence in sentences:
        lowered = sentence.lower()
        if any(token in lowered for token in _PAIN_TOKENS):
            cleaned = _clean_text(sentence)
            if cleaned and cleaned not in found:
                found.append(cleaned)
        if len(found) >= 8:
            break

    if found:
        return found

    lowered = text.lower()
    fallback_counter = Counter(token for token in _PAIN_TOKENS if token in lowered)
    return [token for token, _ in fallback_counter.most_common(5)]


def _extract_with_heuristics(markdown_text: str) -> dict[str, Any]:
    text = _clean_text(markdown_text)
    lowered = text.lower()
    product_type = "billing and revenue management platform" if any(
        token in lowered for token in ["billing", "subscription", "recurring revenue", "invoicing"]
    ) else "b2b saas platform"
    who_needs_this = (
        "SaaS companies with recurring revenue, subscription-based businesses, digital product companies"
    )
    who_should_be_excluded = (
        "billing platforms, invoicing SaaS, subscription management tools"
    )

    return _normalize_schema(
        {
            "product_type": product_type,
            "industry": _guess_industry(text),
            "target_customers": _guess_target_customers(text),
            "who_needs_this": who_needs_this,
            "who_should_be_excluded": who_should_be_excluded,
            "company_size": _guess_company_size(text),
            "geography": _guess_geography(text),
            "keywords": _guess_keywords(text),
            "pain_points": _guess_pain_points(text),
        }
    )


def _icp_quality_score(payload: Any) -> float:
    if not isinstance(payload, dict):
        return 0.0

    score = 0.2
    if _clean_text(payload.get("product_type")):
        score += 0.15
    if _clean_text(payload.get("industry")):
        score += 0.15
    if _clean_text(payload.get("target_customers")) or _clean_text(payload.get("who_needs_this")):
        score += 0.2
    if _clean_text(payload.get("who_should_be_excluded")):
        score += 0.1

    keywords = payload.get("keywords") if isinstance(payload.get("keywords"), list) else []
    pain_points = payload.get("pain_points") if isinstance(payload.get("pain_points"), list) else []
    if len([item for item in keywords if _clean_text(item)]) >= 3:
        score += 0.1
    if len([item for item in pain_points if _clean_text(item)]) >= 2:
        score += 0.1

    who_needs = _clean_text(payload.get("who_needs_this")).lower()
    if any(term in who_needs for term in ["company", "companies", "buyer", "business", "saas"]):
        score += 0.1

    return min(1.0, score)


def extract_icp(markdown_text: str) -> dict[str, Any]:
    """Extract a normalized ICP payload from company markdown text."""
    content = _clean_text(markdown_text)
    if not content:
        return dict(_SCHEMA_TEMPLATE)

    try:
        output, audits = supervise_stage(
            stage_name="markdown_to_icp",
            input_payload={"markdown_text": content},
            execute_stage=lambda payload: _extract_with_gemini(_clean_text(payload.get("markdown_text") or content)),
            fallback_stage=lambda payload: _extract_with_heuristics(_clean_text(payload.get("markdown_text") or content)),
            objective=(
                "Extract a buyer-focused ICP JSON object from markdown so downstream query generation can target the right companies and exclude competitors."
            ),
            min_quality=0.75,
            max_retries=2,
            quality_fn=_icp_quality_score,
        )
        if audits:
            last_audit = audits[-1]
            logger.info(
                "icp stage | attempt=%s quality=%.2f approved=%s retry=%s issues=%s",
                last_audit.attempt,
                last_audit.quality_score,
                last_audit.approved,
                last_audit.retry,
                last_audit.issues,
            )
        return output if isinstance(output, dict) else dict(_SCHEMA_TEMPLATE)
    except Exception as exc:  # noqa: BLE001
        logger.warning("icp llm extraction failed; using heuristics | err=%s", exc)
        return _extract_with_heuristics(content)
