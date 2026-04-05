from __future__ import annotations

import asyncio
import json

from data_sources.aggregator import aggregate_candidates
from data_sources.serp_client import search_serp
from intelligence.query_engine import build_expanded_queries


async def main() -> None:
    query = "B2B subscription billing startups India"
    serp_rows = await search_serp(query)
    print("serp_results", len(serp_rows))
    print("serp_sample_source", serp_rows[0].get("source") if serp_rows else "none")

    queries = build_expanded_queries(
        ["India", "Singapore"],
        ["SaaS", "fintech", "subscription billing"],
        max_queries=8,
    )
    payload = await aggregate_candidates(queries, target_min=5, per_query_limit=6)
    metrics = payload.get("metrics", {})

    print("queries", metrics.get("queries_executed"))
    print("success", json.dumps(metrics.get("success_rate_per_source", {}), ensure_ascii=True))
    print("failures", json.dumps(metrics.get("failures_per_source", {}), ensure_ascii=True))
    print("source_rows", json.dumps(metrics.get("source_rows", {}), ensure_ascii=True))
    print("candidates", metrics.get("total_candidates_generated"))

    candidates = payload.get("candidates") if isinstance(payload.get("candidates"), list) else []
    print("first_candidate", candidates[0].get("company_name") if candidates else "none")


if __name__ == "__main__":
    asyncio.run(main())
