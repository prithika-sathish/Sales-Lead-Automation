from __future__ import annotations

import json
import os
import re
from urllib import error, request

from query_engine.models import ExpandedSegment
from query_engine.models import StructuredContext


DEFAULT_OLLAMA_URL = "http://localhost:11434"
DEFAULT_OLLAMA_MODEL = "llama3"


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalize_keywords(values: list[object]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        token = _clean_text(value).lower()
        token = re.sub(r"[^a-z0-9+\-\s]", " ", token)
        token = " ".join(token.split())
        if not token or token in seen:
            continue
        seen.add(token)
        output.append(token)
    return output


def _strip_code_fences(value: str) -> str:
    text = _clean_text(value)
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text, flags=re.I).strip()
        text = re.sub(r"```$", "", text).strip()
    return text


def _ollama_generate(prompt: str, *, model: str, base_url: str, timeout_seconds: int = 45) -> str:
    endpoint = f"{base_url.rstrip('/')}/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
    }
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(endpoint, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with request.urlopen(req, timeout=timeout_seconds) as response:
        raw = response.read().decode("utf-8")
    parsed = json.loads(raw)
    return _clean_text(parsed.get("response")) if isinstance(parsed, dict) else ""


def _fallback_segments(context: StructuredContext, max_segments: int) -> list[ExpandedSegment]:
    base_keywords = [
        "subscription billing",
        "recurring revenue",
        "saas operations",
        "revenue teams",
        "finance ops",
        "customer support",
    ]
    icp_hint = context.core_icp[0] if context.core_icp else "B2B SaaS"
    candidates = [
        ExpandedSegment(
            segment_name="Direct SaaS buyers",
            description=f"Companies matching {icp_hint} with active growth.",
            why_need="Need to automate billing and revenue workflows.",
            keywords=base_keywords[:4],
        ),
        ExpandedSegment(
            segment_name="Adjacent support-led SaaS",
            description="Support-focused software teams with recurring plans.",
            why_need="Need subscription lifecycle visibility and renewal operations.",
            keywords=["helpdesk saas", "customer support software", "subscription ops"],
        ),
        ExpandedSegment(
            segment_name="Finance and RevOps teams",
            description="Teams managing revenue reporting and invoicing operations.",
            why_need="Need reliable recurring revenue and billing controls.",
            keywords=["revops", "billing automation", "invoicing saas"],
        ),
    ]
    return candidates[: max(1, min(max_segments, 12))]


def expand_segments_with_llm(
    context: StructuredContext,
    *,
    use_llm: bool,
    max_segments: int,
    model: str | None = None,
    base_url: str | None = None,
) -> list[ExpandedSegment]:
    limit = max(1, min(int(max_segments), 12))
    if not use_llm:
        return _fallback_segments(context, limit)

    ollama_model = _clean_text(model or os.getenv("QUERY_LLM_MODEL") or DEFAULT_OLLAMA_MODEL)
    ollama_url = _clean_text(base_url or os.getenv("OLLAMA_BASE_URL") or DEFAULT_OLLAMA_URL)

    prompt = (
        "You are a B2B GTM strategist.\\n\\n"
        "Given a product and its known ICP, identify ALL possible customer segments.\\n"
        "Think in: direct, adjacent, and hidden customers.\\n"
        "For each segment return: segment_name, description, why_they_need_the_product, keywords.\\n"
        "Return JSON array only.\\n\\n"
        f"product_description: {context.product_description}\\n"
        f"core_icp: {context.core_icp}\\n"
        f"regions: {context.regions}\\n"
        f"industries: {context.industries}\\n"
        f"hints: {context.hints}\\n"
    )

    try:
        raw = _ollama_generate(prompt, model=ollama_model, base_url=ollama_url)
        parsed = json.loads(_strip_code_fences(raw))
    except (error.URLError, TimeoutError, json.JSONDecodeError, ValueError):
        return _fallback_segments(context, limit)
    except Exception:
        return _fallback_segments(context, limit)

    if not isinstance(parsed, list):
        return _fallback_segments(context, limit)

    segments: list[ExpandedSegment] = []
    seen: set[str] = set()
    for row in parsed:
        if not isinstance(row, dict):
            continue
        name = _clean_text(row.get("segment_name") or row.get("segment") or "")
        description = _clean_text(row.get("description"))
        why_need = _clean_text(row.get("why they need the product") or row.get("why_they_need_the_product") or row.get("why_need"))
        keywords_raw = row.get("keywords") if isinstance(row.get("keywords"), list) else []
        keywords = _normalize_keywords(keywords_raw)
        if not name or not keywords:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        segments.append(
            ExpandedSegment(
                segment_name=name,
                description=description,
                why_need=why_need,
                keywords=keywords[:6],
            )
        )
        if len(segments) >= limit:
            break

    return segments or _fallback_segments(context, limit)
