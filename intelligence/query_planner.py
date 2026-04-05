from __future__ import annotations

from typing import Any

DEFAULT_INDUSTRIES = ["SaaS", "AI", "fintech", "devtools"]
DEFAULT_REGIONS = ["India", "USA", "Germany", "Singapore", "Malaysia", "Vietnam", "Philippines", "Europe"]


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def plan_high_signal_queries(
    regions: list[str] | None = None,
    industries: list[str] | None = None,
    max_queries: int = 6,
) -> list[dict[str, str]]:
    region_list = [r for r in (regions or DEFAULT_REGIONS) if _clean_text(r)]
    industry_list = [i for i in (industries or DEFAULT_INDUSTRIES) if _clean_text(i)]
    limit = max(1, min(max_queries, 6))

    # Keep query volume intentionally small and high signal.
    templates = [
        ("funding", "recent {industry} funding {region}"),
        ("funding", "Series A B {industry} startups {region}"),
        ("list", "B2B {industry} startups {region}"),
        ("list", "top {industry} companies {region}"),
        ("hiring", "{industry} companies hiring sales {region}"),
        ("hiring", "{industry} companies careers page {region}"),
    ]

    queries: list[dict[str, str]] = []
    for idx in range(limit):
        signal_type, template = templates[idx % len(templates)]
        industry = industry_list[idx % len(industry_list)]
        region = region_list[idx % len(region_list)]
        query = _clean_text(template.format(industry=industry, region=region))
        if not query:
            continue
        queries.append(
            {
                "query": query,
                "region": region,
                "industry": industry,
                "signal_type": signal_type,
            }
        )

    return queries


def summarize_query_plan(queries: list[dict[str, Any]]) -> dict[str, Any]:
    by_signal: dict[str, int] = {}
    by_region: dict[str, int] = {}

    for row in queries:
        if not isinstance(row, dict):
            continue
        signal = _clean_text(row.get("signal_type"))
        region = _clean_text(row.get("region"))
        if signal:
            by_signal[signal] = by_signal.get(signal, 0) + 1
        if region:
            by_region[region] = by_region.get(region, 0) + 1

    return {
        "total_queries": len([q for q in queries if isinstance(q, dict)]),
        "by_signal": by_signal,
        "by_region": by_region,
    }
