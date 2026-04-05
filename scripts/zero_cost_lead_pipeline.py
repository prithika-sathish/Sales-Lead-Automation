from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from ingestion.orchestrator import run_ingestion


logger = logging.getLogger(__name__)

DEFAULT_COMPANIES = [
    "HirePro",
    "Keka",
    "Darwinbox",
    "GreytHR",
    "Pocket HRMS",
    "Qandle",
    "ZingHR",
    "sumHR",
    "Akrivia HCM",
    "HROne",
    "PeopleStrong",
]


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")


def _score_company(row: dict[str, Any], regions: list[str]) -> dict[str, Any]:
    company = _clean_text(row.get("company"))
    signals = row.get("signals") if isinstance(row.get("signals"), dict) else {}

    hiring_events = signals.get("hiring") if isinstance(signals.get("hiring"), list) else []
    event_events = signals.get("events") if isinstance(signals.get("events"), list) else []
    tech_events = signals.get("tech") if isinstance(signals.get("tech"), list) else []

    hiring = len(hiring_events) > 0
    funding = any(_clean_text(item.get("event_type")).lower() == "funding" for item in event_events if isinstance(item, dict))
    signal_text = " ".join(
        [
            _clean_text(item.get("role"))
            for item in hiring_events
            if isinstance(item, dict)
        ]
        + [
            _clean_text(item.get("title"))
            for item in event_events
            if isinstance(item, dict)
        ]
    ).lower()
    b2b_keywords = ["saas", "software", "platform", "enterprise", "b2b", "api", "erp", "hcm", "hrms", "payroll"]
    b2b = len(tech_events) > 0 or any(token in signal_text for token in b2b_keywords)

    region_tokens = {_clean_text(r).lower() for r in regions if _clean_text(r)}
    combined_text = " ".join(
        [
            _clean_text(item.get("location"))
            for item in hiring_events
            if isinstance(item, dict)
        ]
        + [
            _clean_text(item.get("title"))
            for item in event_events
            if isinstance(item, dict)
        ]
    ).lower()
    region_match = any(token in combined_text for token in region_tokens)

    score = 0
    if hiring:
        score += 40
    if funding:
        score += 30
    if b2b:
        score += 20
    if region_match:
        score += 10

    reasons: list[str] = []
    if hiring:
        reasons.append("hiring signal detected")
    if funding:
        reasons.append("funding signal detected")
    if b2b:
        reasons.append("tech stack signal detected")
    if region_match:
        reasons.append("region match")

    domain = f"{company.lower().replace(' ', '')}.com" if company else ""

    return {
        "company": company,
        "domain": domain,
        "score": score,
        "signals": {
            "hiring": hiring,
            "funding": funding,
            "b2b": b2b,
            "region_match": region_match,
        },
        "reason": "; ".join(reasons),
    }


def _filter_and_rank(rows: list[dict[str, Any]], *, regions: list[str], max_leads: int) -> list[dict[str, Any]]:
    scored = [_score_company(row, regions) for row in rows if isinstance(row, dict)]
    filtered = [
        row
        for row in scored
        if row["signals"]["b2b"] and (row["signals"]["hiring"] or row["signals"]["funding"])
    ]
    filtered.sort(key=lambda item: (int(item.get("score") or 0), _clean_text(item.get("company")).lower()), reverse=True)
    return filtered[: max(1, int(max_leads))]


async def _run_pipeline(
    *,
    companies: list[str],
    regions: list[str],
    max_leads: int,
    strict_real_only: bool,
) -> list[dict[str, Any]]:
    payload = {
        "companies": companies,
        "regions": regions,
        "max_leads": max_leads,
    }
    unified = await run_ingestion(payload, strict_real_only=strict_real_only)
    logger.info("[RESULT COUNT] unified: %s", len(unified))
    return _filter_and_rank(unified, regions=regions, max_leads=max_leads)


def run_zero_cost_pipeline(
    output_path: str = "output/zero_cost_leads.json",
    *,
    companies: list[str] | None = None,
    regions: list[str] | None = None,
    max_leads: int = 10,
    strict_real_only: bool = False,
) -> list[dict[str, Any]]:
    configure_logging()

    target_companies = [c for c in (companies or DEFAULT_COMPANIES) if _clean_text(c)]
    target_regions = [r for r in (regions or ["India", "Singapore"]) if _clean_text(r)]

    rows = asyncio.run(
        _run_pipeline(
            companies=target_companies,
            regions=target_regions,
            max_leads=max_leads,
            strict_real_only=strict_real_only,
        )
    )

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(json.dumps(rows, indent=2, ensure_ascii=True), encoding="utf-8")

    logger.info("[RESULT COUNT] final_top_n: %s", len(rows))
    logger.info("[FINAL LEADS] %s", json.dumps(rows, ensure_ascii=True))
    return rows


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deterministic source-driven lead ingestion pipeline")
    parser.add_argument("--output", default="output/zero_cost_leads.json")
    parser.add_argument("--max-leads", type=int, default=10)
    parser.add_argument("--regions", default="India,Singapore")
    parser.add_argument("--companies", default=",".join(DEFAULT_COMPANIES))
    parser.add_argument("--strict-real-only", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    companies = [token.strip() for token in str(args.companies).split(",") if token.strip()]
    regions = [token.strip() for token in str(args.regions).split(",") if token.strip()]
    run_zero_cost_pipeline(
        output_path=str(args.output),
        companies=companies,
        regions=regions,
        max_leads=max(1, int(args.max_leads)),
        strict_real_only=bool(args.strict_real_only),
    )
