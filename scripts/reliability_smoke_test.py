from __future__ import annotations

import asyncio
import json

from data_sources.aggregator import aggregate_candidates
from intelligence.query_engine import build_expanded_queries


async def run_once(idx: int) -> dict[str, int]:
    queries = build_expanded_queries(
        regions=["India", "Singapore"],
        industries=["subscription billing"],
        max_queries=6,
    )
    payload = await aggregate_candidates(
        queries,
        target_min=5,
        per_query_limit=5,
        query_timeout_seconds=8.0,
        max_concurrent_queries=2,
    )
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
    candidates = payload.get("candidates") if isinstance(payload.get("candidates"), list) else []
    output = {
        "run": idx,
        "candidates": len(candidates),
        "queries_executed": int(metrics.get("queries_executed") or 0),
        "fallback_rows_added": int(metrics.get("fallback_rows_added") or 0),
        "serp_rows": int((metrics.get("source_rows") or {}).get("serp") or 0),
        "structured_rows": int((metrics.get("source_rows") or {}).get("structured") or 0),
        "serp_failures": int((metrics.get("failures_per_source") or {}).get("serp") or 0),
        "structured_failures": int((metrics.get("failures_per_source") or {}).get("structured") or 0),
    }
    print(json.dumps(output, ensure_ascii=True))
    return output


async def main() -> None:
    results: list[dict[str, int]] = []
    for idx in range(1, 6):
        result = await run_once(idx)
        results.append(result)

    min_candidates = min((row["candidates"] for row in results), default=0)
    max_candidates = max((row["candidates"] for row in results), default=0)
    print("summary", json.dumps({"runs": len(results), "min_candidates": min_candidates, "max_candidates": max_candidates}, ensure_ascii=True))


if __name__ == "__main__":
    asyncio.run(main())
