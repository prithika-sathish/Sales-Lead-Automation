from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.schemas import DiscoverEnrichedCompany
from app.schemas import LeadEngineInput
from core.icp_filter import filter_companies_by_icp
from core.multisource_discovery import fetch_companies_apify
from core.multisource_discovery import fetch_companies_playwright
from core.multisource_discovery import fetch_companies_search
from core.multisource_discovery import merge_and_dedupe
from core.orchestrator import collect_hiring_signals
from intelligence.lead_engine import generate_ranked_leads


logger = logging.getLogger(__name__)


def _flatten_for_icp(merged_companies: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for item in merged_companies:
        company = str(item.get("company") or "").strip()
        if not company:
            continue
        role_counts = item.get("role_counts") if isinstance(item.get("role_counts"), dict) else {}
        if role_counts:
            for role_name, count in role_counts.items():
                role = str(role_name or "").strip()
                repeats = max(0, int(count or 0))
                if not role or repeats <= 0:
                    continue
                rows.extend({"company": company, "role": role} for _ in range(repeats))
            continue

        roles = item.get("roles") if isinstance(item.get("roles"), list) else []
        for role in roles:
            role_name = str(role or "").strip()
            if not role_name:
                continue
            rows.append({"company": company, "role": role_name})
    return rows


def _build_hiring_records(playwright_rows: list[dict[str, Any]], apify_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    for item in playwright_rows:
        if not isinstance(item, dict):
            continue
        company = str(item.get("company") or "").strip()
        role = str(item.get("role") or "").strip()
        if not company or not role:
            continue
        records.append(
            {
                "company": company,
                "role": role,
                "location": str(item.get("location") or "").strip(),
                "date_posted": str(item.get("date_posted") or "").strip(),
                "source": "playwright",
            }
        )

    for item in apify_rows:
        if not isinstance(item, dict):
            continue
        company = str(item.get("company") or "").strip()
        role = str(item.get("role") or "").strip()
        if not company or not role:
            continue
        records.append(
            {
                "company": company,
                "role": role,
                "location": "",
                "date_posted": "",
                "source": "apify",
            }
        )

    return records


def _confidence_boost(source_count: int) -> int:
    if source_count >= 3:
        return 20
    if source_count >= 2:
        return 10
    return 0


async def run_pipeline(
    role: str = "Sales Development Representative",
    max_pages: int = 3,
    search_query: str = "companies hiring sales team OR SDR",
    search_region: str = "United States",
    apify_actor_id: str | None = None,
    idea: str = "End-to-end marketing ROI solutions",
    stakeholders: list[str] | None = None,
) -> dict[str, Any]:
    playwright_data: list[dict[str, Any]] = []
    search_data: list[dict[str, Any]] = []
    apify_data: list[dict[str, Any]] = []

    try:
        playwright_data = await fetch_companies_playwright(role=role, max_pages=max_pages)
    except Exception as exc:  # noqa: BLE001
        logger.warning("playwright discovery failed: %s", exc)

    try:
        search_data = await fetch_companies_search(query=search_query, region=search_region, max_pages=max_pages)
    except Exception as exc:  # noqa: BLE001
        logger.warning("playwright search query failed: %s", exc)

    if apify_actor_id:
        try:
            apify_data = await fetch_companies_apify(actor_id=apify_actor_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("apify discovery failed: %s", exc)

    logger.info(
        "source counts | playwright_jobs=%s playwright_search=%s apify=%s",
        len(playwright_data),
        len(search_data),
        len(apify_data),
    )

    merged = merge_and_dedupe([playwright_data, search_data, apify_data])

    if len(merged) < 20:
        return {
            "playwright_data": playwright_data,
            "search_data": search_data,
            "apify_data": apify_data,
            "merged": merged,
            "companies": [],
            "leads": [],
            "reason": "insufficient_grounded_companies",
        }

    icp_input = _flatten_for_icp(merged)
    companies = filter_companies_by_icp(icp_input)
    if not companies:
        return {
            "playwright_data": playwright_data,
            "search_data": search_data,
            "apify_data": apify_data,
            "merged": merged,
            "companies": [],
            "leads": [],
        }

    hiring_records = _build_hiring_records(playwright_data, apify_data)
    collected = collect_hiring_signals(companies=companies, discovered_jobs=hiring_records)
    bundles = collected.get("companies") if isinstance(collected.get("companies"), list) else []

    validated_companies: list[DiscoverEnrichedCompany] = []
    for item in bundles:
        if not isinstance(item, dict):
            continue
        signals = item.get("signals") if isinstance(item.get("signals"), list) else []
        if len(signals) < 2:
            continue
        validated_companies.append(
            DiscoverEnrichedCompany.model_validate(
                {
                    "company": str(item.get("company") or "").strip(),
                    "light_score": 0,
                    "signals": signals,
                    "derived_signals": [],
                    "topics": [],
                    "trend_signals": [],
                    "agent_debug": item.get("agent_debug") if isinstance(item.get("agent_debug"), dict) else {},
                }
            )
        )

    if not validated_companies:
        return {
            "playwright_data": playwright_data,
            "search_data": search_data,
            "apify_data": apify_data,
            "merged": merged,
            "companies": companies,
            "leads": [],
        }

    lead_input = LeadEngineInput(
        companies=validated_companies,
        idea=idea,
        stakeholders=stakeholders
        if stakeholders is not None
        else ["VP Sales", "Sales Director", "Business Development Director"],
    )
    leads = generate_ranked_leads(lead_input)

    source_count_by_company = {
        str(item.get("company") or ""): len(item.get("sources") or [])
        for item in merged
        if isinstance(item, dict)
    }

    boosted_leads: list[dict[str, Any]] = []
    for lead in leads:
        lead_row = lead.model_dump()
        source_count = int(source_count_by_company.get(lead.company, 0))
        boost = _confidence_boost(source_count)
        lead_row["confidence"] = max(0, min(100, int(lead_row.get("confidence") or 0) + boost))
        lead_row["source_count"] = source_count
        lead_row["cross_source_high_confidence"] = source_count >= 2
        boosted_leads.append(lead_row)

    return {
        "playwright_data": playwright_data,
        "search_data": search_data,
        "apify_data": apify_data,
        "merged": merged,
        "companies": companies,
        "execution_metadata": collected.get("execution_metadata", {}),
        "leads": boosted_leads,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run multi-source discovery lead pipeline")
    parser.add_argument("--role", default="Sales Development Representative", help="Hiring role query")
    parser.add_argument("--max-pages", type=int, default=3, help="Maximum pages for Playwright jobs")
    parser.add_argument("--search-query", default="companies hiring sales team OR SDR", help="Playwright Google search query")
    parser.add_argument("--search-region", default="United States", help="Search region label")
    parser.add_argument("--apify-actor-id", default="", help="Optional Apify actor id")
    parser.add_argument("--idea", default="End-to-end marketing ROI solutions", help="Product/solution context")
    parser.add_argument(
        "--stakeholder",
        action="append",
        dest="stakeholders",
        default=None,
        help="Target stakeholder (repeatable)",
    )
    parser.add_argument("--out", default="output/leads_multisource.json", help="Output JSON path")
    parser.add_argument("--no-save", action="store_true", help="Print only, do not write output file")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    apify_actor_id = args.apify_actor_id.strip() if isinstance(args.apify_actor_id, str) else ""
    if not apify_actor_id:
        apify_actor_id = None

    result = asyncio.run(
        run_pipeline(
            role=args.role,
            max_pages=args.max_pages,
            search_query=args.search_query,
            search_region=args.search_region,
            apify_actor_id=apify_actor_id,
            idea=args.idea,
            stakeholders=args.stakeholders,
        )
    )

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
