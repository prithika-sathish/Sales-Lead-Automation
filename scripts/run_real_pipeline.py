from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.schemas import DiscoverEnrichedCompany
from app.schemas import LeadEngineInput
from core.discovery_playwright import discover_companies_from_jobs
from core.icp_filter import filter_companies_by_icp
from core.orchestrator import collect_hiring_signals
from intelligence.lead_engine import generate_ranked_leads


async def run_pipeline(
    role: str = "Sales Development Representative",
    max_pages: int = 3,
    idea: str = "Manufacturing ERP software",
    stakeholders: list[str] | None = None,
) -> dict[str, Any]:
    companies_raw = await discover_companies_from_jobs(role=role, max_pages=max_pages)
    if not companies_raw:
        return {"companies_raw": [], "companies": [], "leads": []}

    companies = filter_companies_by_icp(companies_raw)
    if not companies:
        return {"companies_raw": companies_raw, "companies": [], "leads": []}

    collected = collect_hiring_signals(companies=companies, discovered_jobs=companies_raw)
    bundles = collected.get("companies", [])
    if not isinstance(bundles, list) or not bundles:
        return {"companies_raw": companies_raw, "companies": companies, "leads": []}

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
        return {"companies_raw": companies_raw, "companies": companies, "leads": []}

    lead_input = LeadEngineInput(
        companies=validated_companies,
        idea=idea,
        stakeholders=stakeholders
        if stakeholders is not None
        else ["VP Sales", "Sales Director", "Business Development Director"],
    )
    leads = generate_ranked_leads(lead_input)

    return {
        "companies_raw": companies_raw,
        "companies": companies,
        "execution_metadata": collected.get("execution_metadata", {}),
        "leads": [lead.model_dump() for lead in leads],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run real Playwright-based lead pipeline")
    parser.add_argument("--role", default="Sales Development Representative", help="Job role query")
    parser.add_argument("--max-pages", type=int, default=3, help="Maximum pages to scrape per source")
    parser.add_argument("--idea", default="Manufacturing ERP software", help="Product/solution context for lead generation")
    parser.add_argument(
        "--stakeholder",
        action="append",
        dest="stakeholders",
        default=None,
        help="Target stakeholder (repeatable). Example: --stakeholder 'VP Marketing'",
    )
    parser.add_argument("--out", default="output/leads.json", help="Output JSON path")
    parser.add_argument("--no-save", action="store_true", help="Print only, do not write output file")
    args = parser.parse_args()

    result = asyncio.run(
        run_pipeline(
            role=args.role,
            max_pages=args.max_pages,
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
