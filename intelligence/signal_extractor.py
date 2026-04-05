from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

from app.llm import generate_json_with_gemini


logger = logging.getLogger(__name__)

TECH_KEYWORDS = [
    "salesforce",
    "hubspot",
    "segment",
    "snowflake",
    "databricks",
    "kafka",
    "aws",
    "gcp",
    "azure",
]


def _keyword_match(text: str, keywords: list[str]) -> bool:
    lowered = text.lower()
    return any(key in lowered for key in keywords)


def _heuristic_extract(company: str, domain: str, region: str, text: str) -> dict[str, Any]:
    lowered = text.lower()
    hiring = _keyword_match(lowered, ["hiring", "open role", "careers", "jobs", "join our team"])
    funding = _keyword_match(lowered, ["series a", "series b", "funding", "raised", "investor", "backed by"])
    growth = _keyword_match(lowered, ["expanding", "growth", "scale", "new office", "launch", "momentum"])

    tech_stack = [kw for kw in TECH_KEYWORDS if kw in lowered]
    summary = text[:240].strip()

    return {
        "company": company,
        "domain": domain,
        "hiring": bool(hiring),
        "funding_signal": bool(funding),
        "growth_signal": bool(growth),
        "tech_stack": tech_stack,
        "region": region,
        "summary": summary,
    }


def _build_prompt(company: str, domain: str, region: str, text: str) -> str:
    return (
        "Return ONLY valid JSON with exact schema:\n"
        '{"company":"", "domain":"", "hiring":true/false, "funding_signal":true/false, "growth_signal":true/false, '
        '"tech_stack":[], "region":"", "summary":""}\n\n'
        "Rules:\n"
        "- Use only evidence from provided content\n"
        "- Do not invent facts\n"
        "- summary must be concise and evidence grounded\n"
        f"Company: {company}\n"
        f"Domain: {domain}\n"
        f"Region: {region}\n"
        f"Content:\n{text[:14000]}"
    )


async def extract_signals_for_company(
    company: str,
    domain: str,
    region: str,
    crawled_text: str,
    llm_provider: str = "gemini",
) -> dict[str, Any]:
    text = " ".join(str(crawled_text or "").split()).strip()
    if not text:
        return {
            "company": company,
            "domain": domain,
            "hiring": False,
            "funding_signal": False,
            "growth_signal": False,
            "tech_stack": [],
            "region": region,
            "summary": "",
        }

    if llm_provider.lower() != "gemini":
        return _heuristic_extract(company, domain, region, text)

    prompt = _build_prompt(company, domain, region, text)

    try:
        data = await asyncio.to_thread(
            generate_json_with_gemini,
            system_prompt="You extract company buying signals from noisy web content. Output strict JSON only.",
            user_prompt=prompt,
            temperature=0.1,
        )
    except Exception as exc:  # noqa: BLE001
        logger.info("llm extraction failed for %s: %s", company, exc)
        return _heuristic_extract(company, domain, region, text)

    # Validate/sanitize model output before downstream scoring.
    result = {
        "company": str(data.get("company") or company).strip() or company,
        "domain": str(data.get("domain") or domain).strip() or domain,
        "hiring": bool(data.get("hiring")),
        "funding_signal": bool(data.get("funding_signal")),
        "growth_signal": bool(data.get("growth_signal")),
        "tech_stack": [str(x).strip() for x in data.get("tech_stack", []) if str(x).strip()] if isinstance(data.get("tech_stack"), list) else [],
        "region": str(data.get("region") or region).strip() or region,
        "summary": re.sub(r"\s+", " ", str(data.get("summary") or "")).strip()[:300],
    }
    return result
