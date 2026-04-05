from __future__ import annotations

import json
import logging
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from core.enrichment import enrich_domain
from core.icp_filter import filter_companies_by_icp
from core.merge_engine import extract_domain
from core.merge_engine import merge_and_dedupe
from core.scoring import score_company
from discovery.query_generator import build_multistrategy_queries
from icp.extractor import extract_icp
from sources.apify_leads import fetch_apify_leads
from sources.google_maps import fetch_google_maps_leads


_BANNED_NAME_TERMS = {
    "alternative",
    "alternatives",
    "comparison",
    "compare",
    "best",
    "top",
    "guide",
    "tools",
    "list",
}

_NON_LEAD_HOSTS = {
    "github.com",
    "linkedin.com",
    "x.com",
    "twitter.com",
    "facebook.com",
    "instagram.com",
    "youtube.com",
    "medium.com",
    "substack.com",
    "wordpress.com",
    "blogspot.com",
    "wikipedia.org",
    "crunchbase.com",
    "g2.com",
    "capterra.com",
    "getapp.com",
    "wellfound.com",
    "angel.co",
    "reddit.com",
    "quora.com",
}

logger = logging.getLogger(__name__)


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _domain_brand(domain: str) -> str:
    host = str(domain or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host.split(".")[0] if host else ""


def _is_blocked_lead_domain(domain: str) -> bool:
    host = str(domain or "").strip().lower()
    if not host:
        return True
    return any(host == blocked or host.endswith(f".{blocked}") for blocked in _NON_LEAD_HOSTS)


def _is_company_name_like(name: str, domain: str) -> bool:
    raw_name = str(name or "").strip()
    if not raw_name:
        return False

    words = [w for w in re.split(r"\s+", raw_name) if w]
    if len(words) > 5:
        return False

    lowered = raw_name.lower()
    if any(term in lowered for term in _BANNED_NAME_TERMS):
        return False

    compact_name = _normalize_text(raw_name)
    brand = _normalize_text(_domain_brand(domain))
    if not compact_name or not brand:
        return False

    if brand in compact_name:
        return True

    similarity = SequenceMatcher(None, compact_name, brand).ratio()
    return similarity >= 0.55


def _extract_seed_company(markdown_text: str) -> str:
    match = re.search(r"^#\s*Company:\s*(.+)$", markdown_text, flags=re.I | re.M)
    if match:
        return " ".join(str(match.group(1) or "").split()).strip(" #*-:")
    return ""


def _extract_competitor_hints(markdown_text: str) -> list[str]:
    section = re.search(r"##\s*COMPETITOR_HINTS\s*(.*?)\n(?:---|##\s+)", markdown_text, flags=re.I | re.S)
    if not section:
        return []

    hints: list[str] = []
    for line in section.group(1).splitlines():
        cleaned = line.strip()
        if not cleaned.startswith("-"):
            continue
        value = cleaned.lstrip("-").strip()
        if value and value.lower() not in {"(note: use only if needed for competitor-based queries)"}:
            hints.append(value)
    return hints


def _build_source_queries(markdown_path: str) -> tuple[list[str], dict[str, Any]]:
    path = Path(markdown_path)
    if not path.exists():
        logger.info("pipeline_input_markdown_missing %s", markdown_path)
        return (
            [
                "subscription billing saas",
                "recurring revenue software",
                "usage based billing platform",
            ],
            {},
        )

    markdown_text = path.read_text(encoding="utf-8")
    icp = extract_icp(markdown_text)
    seed_company = _extract_seed_company(markdown_text)
    competitors = _extract_competitor_hints(markdown_text)

    generated = build_multistrategy_queries(
        icp,
        seed_company=seed_company,
        competitor_names=competitors,
        regions=["India", "Singapore", "Europe", "United States"],
        industries=["SaaS", "subscription billing", "revenue management", "payments"],
        max_queries=24,
    )

    queries = [str(row.get("query") or "").strip() for row in generated if str(row.get("query") or "").strip()]
    unique_queries: list[str] = []
    seen: set[str] = set()
    for query in queries:
        key = query.lower()
        if key in seen:
            continue
        seen.add(key)
        unique_queries.append(query)
    if not unique_queries:
        unique_queries = [
            "subscription billing saas",
            "recurring revenue software",
            "usage based billing platform",
        ]

    apify_input: dict[str, Any] = {
        "limit": 300,
        "keywords": unique_queries[:8],
        "query": unique_queries[0],
        "seedCompany": seed_company,
        "competitors": competitors,
        "icp_keywords": icp.get("keywords") if isinstance(icp, dict) else [],
    }

    logger.info("pipeline_input_seed_company %s", seed_company or "")
    logger.info("pipeline_input_queries %s", len(unique_queries[:8]))
    return unique_queries[:8], apify_input


def run_deterministic_pipeline(markdown_path: str = "sample_company.md") -> list[dict[str, Any]]:
    maps_queries, apify_input = _build_source_queries(markdown_path)
    
    # Use only top 3 queries per-source to speed up Apify (it's slow with many keywords)
    apify_input["keywords"] = apify_input.get("keywords", [])[:3]
    apify_input["limit"] = 150  # Reduce per-query limit for speed
    
    apify_rows = fetch_apify_leads(limit=150, input_payload=apify_input)
    logger.info("pipeline_stage_apify_rows %s", len(apify_rows))

    maps_rows: list[dict[str, Any]] = []
    for query in maps_queries[:3]:  # Only top 3 maps queries too
        maps_rows.extend(fetch_google_maps_leads(query=query, location="", limit=50))
    logger.info("pipeline_stage_maps_rows %s", len(maps_rows))

    merged = merge_and_dedupe([apify_rows, maps_rows])
    logger.info("pipeline_stage_merged_rows %s", len(merged))
    filtered = filter_companies_by_icp(merged)
    logger.info("pipeline_stage_icp_rows %s", len(filtered))

    output: list[dict[str, Any]] = []
    for row in filtered[:50]:
        domain = extract_domain(str(row.get("domain") or row.get("url") or row.get("website") or ""))
        if not domain:
            continue
        if _is_blocked_lead_domain(domain):
            continue

        company = str(row.get("name") or "").strip()
        if not _is_company_name_like(company, domain):
            continue

        enrich = enrich_domain(domain)
        scored = score_company({**row, **enrich})

        output.append(
            {
                "company": company,
                "website": f"https://{domain}",
                "emails": enrich.get("emails", []),
                "score": scored.get("score", 0),
                "reason": scored.get("reason", ""),
                "source": str(row.get("source") or "").strip(),
            }
        )

    output.sort(key=lambda x: int(x.get("score", 0)), reverse=True)
    logger.info("pipeline_stage_output_rows %s", len(output))
    return output


def save_output(rows: list[dict[str, Any]], output_path: str = "output/full_pipeline_ranked_leads.json") -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=2, ensure_ascii=True), encoding="utf-8")


def save_company_names(rows: list[dict[str, Any]], output_path: str = "output/company_names.json") -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    names: list[str] = []
    seen: set[str] = set()
    for row in rows:
        name = str((row or {}).get("company") or "").strip()
        key = name.lower()
        if not name or key in seen:
            continue
        seen.add(key)
        names.append(name)

    path.write_text(json.dumps(names, indent=2, ensure_ascii=True), encoding="utf-8")


if __name__ == "__main__":
    rows = run_deterministic_pipeline()
    save_output(rows)
    print(f"saved {len(rows)} rows")
