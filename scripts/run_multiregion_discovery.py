from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.settings import DiscoverySettings
from core.orchestrator import run_signal_driven_lead_intelligence


def _summarize(items: list[dict[str, object]]) -> dict[str, object]:
    region_counts: dict[str, int] = {}
    company_set: set[str] = set()

    for row in items:
        if not isinstance(row, dict):
            continue
        company = str(row.get("company") or "").strip()
        region = str(row.get("region") or "").strip()
        if company:
            company_set.add(company.lower())
        if region:
            region_counts[region] = region_counts.get(region, 0) + 1

    return {
        "rows": len([x for x in items if isinstance(x, dict)]),
        "unique_companies": len(company_set),
        "region_distribution": region_counts,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run multi-region lead intelligence discovery")
    parser.add_argument("--target-min", type=int, default=50, help="Minimum required ranked companies")
    parser.add_argument("--target-max", type=int, default=100, help="Maximum ranked companies")
    parser.add_argument("--max-pages", type=int, default=2, help="Max Google search pages per query")
    parser.add_argument("--batch-size", type=int, default=8, help="Async batch size for crawl/extract")
    parser.add_argument("--llm-provider", default="gemini", help="LLM provider for extraction")
    parser.add_argument("--crawl-depth", type=int, default=1, help="Reserved crawl depth setting")
    parser.add_argument("--out", default="output/companies_multiregion.json", help="Output JSON path")
    parser.add_argument("--no-save", action="store_true", help="Print only; do not save output file")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    settings = DiscoverySettings(
        max_pages=max(1, args.max_pages),
        target_min=max(1, args.target_min),
        target_max=max(max(1, args.target_min), args.target_max),
        llm_provider=str(args.llm_provider or "gemini"),
        crawl_depth=max(1, args.crawl_depth),
        batch_size=max(2, args.batch_size),
    )

    pipeline = asyncio.run(
        run_signal_driven_lead_intelligence(
            settings=settings,
            target_min=settings.target_min,
            target_max=settings.target_max,
        )
    )

    companies = pipeline.get("companies") if isinstance(pipeline.get("companies"), list) else []
    result = {
        "regions": settings.regions,
        "industries": settings.industries,
        "execution_metadata": pipeline.get("execution_metadata", {}),
        "summary": _summarize(companies),
        "companies": companies,
    }

    if not args.no_save:
        out_path = Path(args.out)
        if not out_path.is_absolute():
            out_path = ROOT_DIR / out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, indent=2, ensure_ascii=True), encoding="utf-8")
        print(f"Saved output to: {out_path}")

    print(json.dumps(result, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
