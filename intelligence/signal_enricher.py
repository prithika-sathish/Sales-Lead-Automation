from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

from crawler.deep_crawler import crawl_company_domain


logger = logging.getLogger(__name__)

HIRING_KEYWORDS = ["hiring", "careers", "open role", "jobs", "join our team", "sdr", "sales development representative"]
PRODUCT_KEYWORDS = ["api", "saas", "platform", "automation", "billing", "subscription", "revenue"]
GROWTH_KEYWORDS = ["funding", "series a", "series b", "raised", "expansion", "scaling", "growth"]
B2B_KEYWORDS = ["b2b", "enterprise", "business customers", "for businesses"]
MIDMARKET_HINTS = ["startup", "growing", "scaleup", "mid-market", "mid market"]
ENTERPRISE_HINTS = ["fortune 500", "global leader", "multinational", "enterprise-grade"]


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _contains_any(text: str, keywords: list[str]) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in keywords)


def _region_match(text: str, regions: list[str]) -> bool:
    lowered = text.lower()
    for region in regions:
        token = _clean_text(region).lower()
        if token and token in lowered:
            return True
    return False


def _infer_industry(text: str) -> str:
    lowered = text.lower()
    if any(token in lowered for token in ["fintech", "payments", "banking", "card", "lending"]):
        return "fintech"
    if any(token in lowered for token in ["developer", "api", "sdk", "devops", "platform"]):
        return "devtools"
    if any(token in lowered for token in ["saas", "subscription", "software", "billing"]):
        return "saas"
    if any(token in lowered for token in ["ai", "machine learning", "llm", "automation"]):
        return "ai"
    return "unknown"


def _build_summary(text: str) -> str:
    cleaned = _clean_text(text)
    if not cleaned:
        return ""

    sentences = [segment.strip() for segment in re.split(r"(?<=[.!?])\s+", cleaned) if segment.strip()]
    preferred = [
        sentence
        for sentence in sentences
        if any(token in sentence.lower() for token in ["platform", "software", "api", "saas", "helps", "provides", "solution"])
    ]
    if preferred:
        return preferred[0][:320]
    return cleaned[:320]


def _enrich_from_text(
    *,
    company: str,
    domain: str,
    crawled_text: str,
    source_signal_types: list[str],
    regions: list[str],
) -> dict[str, Any]:
    text = _clean_text(crawled_text)
    source_tokens = " ".join(source_signal_types).lower()
    lowered = text.lower()

    hiring = _contains_any(text, HIRING_KEYWORDS) or "hiring" in source_tokens
    funding = _contains_any(text, GROWTH_KEYWORDS) or "funding" in source_tokens
    b2b = _contains_any(text, B2B_KEYWORDS) or _contains_any(text, ["saas", "api", "platform", "enterprise"]) 
    region_match = _region_match(text, regions)
    product_relevance = _contains_any(text, PRODUCT_KEYWORDS)

    hiring_roles_count = sum(1 for token in ["sales development representative", "account executive", "business development", "gtm", "growth"] if token in lowered)
    hiring_velocity = hiring_roles_count >= 2

    # Mid-size approximation from available text hints and operating signals.
    mid_size_candidate = (
        any(token in lowered for token in MIDMARKET_HINTS)
        or (hiring and b2b)
        or (funding and b2b)
        or (product_relevance and b2b)
    ) and not any(token in lowered for token in ENTERPRISE_HINTS)
    industry = _infer_industry(text)

    signals = {
        "hiring": bool(hiring),
        "funding": bool(funding),
        "b2b": bool(b2b),
        "region_match": bool(region_match),
        "hiring_velocity": bool(hiring_velocity),
    }

    return {
        "company": company,
        "domain": domain,
        "signals": signals,
        "hiring_roles_count": hiring_roles_count,
        "product_relevance": bool(product_relevance),
        "industry": industry,
        "mid_size_candidate": mid_size_candidate,
        "summary": _build_summary(text),
    }


async def enrich_company_signals(
    company_row: dict[str, Any],
    *,
    timeout_seconds: int = 25,
) -> dict[str, Any] | None:
    company = _clean_text(company_row.get("company"))
    domain = _clean_text(company_row.get("domain"))
    if not company:
        return None

    source_signal_types = [
        _clean_text(item.get("signal_type"))
        for item in (company_row.get("raw_signals") if isinstance(company_row.get("raw_signals"), list) else [])
        if isinstance(item, dict)
    ]

    source_types = [
        _clean_text(item.get("source_type")).lower()
        for item in (company_row.get("raw_signals") if isinstance(company_row.get("raw_signals"), list) else [])
        if isinstance(item, dict)
    ]

    regions = [
        _clean_text(region)
        for region in (company_row.get("regions") if isinstance(company_row.get("regions"), list) else [])
        if _clean_text(region)
    ]

    crawled_text = ""
    crawl_engine = "none"
    if domain:
        try:
            crawl_result = await crawl_company_domain(domain=domain, timeout_seconds=timeout_seconds)
            pages = crawl_result.get("pages") if isinstance(crawl_result, dict) else {}
            if isinstance(pages, dict):
                crawled_text = "\n\n".join(_clean_text(value) for value in pages.values() if _clean_text(value))
            crawl_engine = _clean_text(crawl_result.get("engine")) if isinstance(crawl_result, dict) else "none"
        except Exception as exc:  # noqa: BLE001
            logger.info("crawl failed | company=%s domain=%s err=%s", company, domain, exc)

    # If crawl has no content, still enrich from source contexts.
    if not crawled_text:
        crawled_text = "\n\n".join(
            _clean_text(item.get("context"))
            for item in (company_row.get("raw_signals") if isinstance(company_row.get("raw_signals"), list) else [])
            if isinstance(item, dict)
        )

    if not crawled_text:
        return None

    enriched = _enrich_from_text(
        company=company,
        domain=domain,
        crawled_text=crawled_text,
        source_signal_types=source_signal_types,
        regions=regions,
    )
    enriched["regions"] = regions
    enriched["sources"] = company_row.get("sources") if isinstance(company_row.get("sources"), list) else []
    enriched["source_types"] = company_row.get("source_types") if isinstance(company_row.get("source_types"), list) else []
    enriched["confidence_score"] = float(company_row.get("confidence_score") or 0.0)
    enriched["website"] = _clean_text(company_row.get("website"))
    enriched["tags"] = company_row.get("tags") if isinstance(company_row.get("tags"), list) else []
    enriched["crawl_engine"] = crawl_engine
    return enriched


async def enrich_companies_batch(
    companies: list[dict[str, Any]],
    *,
    batch_size: int = 5,
    timeout_seconds: int = 25,
) -> list[dict[str, Any]]:
    semaphore = asyncio.Semaphore(max(1, batch_size))

    async def _task(row: dict[str, Any]) -> dict[str, Any] | None:
        async with semaphore:
            return await enrich_company_signals(row, timeout_seconds=timeout_seconds)

    tasks = [asyncio.create_task(_task(row)) for row in companies if isinstance(row, dict)]
    results: list[dict[str, Any]] = []
    for item in await asyncio.gather(*tasks, return_exceptions=True):
        if isinstance(item, Exception) or item is None:
            continue
        results.append(item)
    return results
