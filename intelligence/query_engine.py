from __future__ import annotations

from typing import Any


DEFAULT_REGIONS = ["India", "Singapore", "USA", "Germany", "Malaysia", "Vietnam"]
DEFAULT_INDUSTRIES = ["SaaS", "fintech", "subscription billing", "revenue management", "payments"]


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _unique(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str, str, str]] = set()
    output: list[dict[str, str]] = []
    for row in rows:
        query = _clean_text(row.get("query"))
        region = _clean_text(row.get("region"))
        industry = _clean_text(row.get("industry"))
        intent = _clean_text(row.get("intent"))
        key = (query.lower(), region.lower(), industry.lower(), intent.lower())
        if not query or key in seen:
            continue
        seen.add(key)
        output.append(
            {
                "query": query,
                "region": region,
                "industry": industry,
                "intent": intent,
                "signal_type": _clean_text(row.get("signal_type")) or intent,
            }
        )
    return output


def build_expanded_queries(
    regions: list[str] | None = None,
    industries: list[str] | None = None,
    *,
    max_queries: int = 24,
) -> list[dict[str, str]]:
    region_list = [r for r in (regions or DEFAULT_REGIONS) if _clean_text(r)]
    industry_list = [i for i in (industries or DEFAULT_INDUSTRIES) if _clean_text(i)]

    templates: list[tuple[str, str]] = [
        ("unknown", "seed stage SaaS {region} hiring sales"),
        ("unknown", "early stage B2B startup hiring SDR {region}"),
        ("unknown", "YC SaaS companies hiring growth {region}"),
        ("funding", "Series A {industry} {region} funding"),
        ("funding", "recently funded {industry} startups {region}"),
        ("funding", "Series B {industry} companies {region}"),
        ("hiring", "startup hiring sales {region} {industry}"),
        ("hiring", "B2B {industry} companies hiring SDR {region}"),
        ("hiring", "companies expanding sales team {industry} {region}"),
        ("growth", "fast growing B2B {industry} companies {region}"),
        ("growth", "{industry} scaleup companies {region}"),
        ("intent", "B2B {industry} SaaS platform {region}"),
        ("intent", "mid market {industry} software company {region}"),
        ("geo", "{industry} companies in {region}"),
        ("geo", "{industry} software providers {region}"),
    ]

    rows: list[dict[str, str]] = []
    for region in region_list:
        for industry in industry_list:
            for intent, template in templates:
                rows.append(
                    {
                        "query": _clean_text(template.format(industry=industry, region=region)),
                        "region": _clean_text(region),
                        "industry": _clean_text(industry),
                        "intent": intent,
                        "signal_type": intent,
                    }
                )

    unique_rows = _unique(rows)
    limit = max(1, max_queries)
    return unique_rows[:limit]


def summarize_query_engine(queries: list[dict[str, Any]]) -> dict[str, Any]:
    by_region: dict[str, int] = {}
    by_intent: dict[str, int] = {}

    for row in queries:
        if not isinstance(row, dict):
            continue
        region = _clean_text(row.get("region"))
        intent = _clean_text(row.get("intent") or row.get("signal_type"))
        if region:
            by_region[region] = by_region.get(region, 0) + 1
        if intent:
            by_intent[intent] = by_intent.get(intent, 0) + 1

    return {
        "total_queries": len([q for q in queries if isinstance(q, dict)]),
        "by_region": by_region,
        "by_intent": by_intent,
    }
