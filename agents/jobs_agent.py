from __future__ import annotations

from collections import Counter
import logging
from typing import Any

from app.apify_client import fetch_apify_query


logger = logging.getLogger(__name__)


def _extract_role(item: dict[str, Any]) -> str:
    return str(item.get("role") or item.get("title") or item.get("jobTitle") or "").strip()


def _infer_seniority(role: str) -> str:
    role_l = role.lower()
    if any(token in role_l for token in ["chief", "vp", "head", "director"]):
        return "leadership"
    if any(token in role_l for token in ["senior", "staff", "principal", "manager"]):
        return "senior"
    if any(token in role_l for token in ["intern", "junior", "associate"]):
        return "junior"
    return "mid"


def collect_jobs_signals(company: str) -> dict[str, Any]:
    signals: list[dict[str, Any]] = []
    queries = [
        (f"{company} linkedin jobs", "linkedin"),
        (f"{company} indeed jobs", "indeed"),
    ]
    collected_roles: list[tuple[str, str]] = []
    raw_count = 0
    filtered_count = 0
    filter_reasons: Counter[str] = Counter()

    for query, source in queries:
        logger.info("jobs query | company=%s query=%s", company, query)
        try:
            items = fetch_apify_query(query, limit=4)
        except RuntimeError:
            continue

        raw_count += len(items)

        for item in items:
            role = _extract_role(item)
            if not role:
                filter_reasons["missing_role"] += 1
                continue
            collected_roles.append((role, source))
            filtered_count += 1

    if not collected_roles:
        debug = {
            "raw": raw_count,
            "filtered": filtered_count,
            "filter_reasons": dict(filter_reasons),
        }
        logger.info("jobs debug | company=%s raw=%s filtered=%s", company, raw_count, filtered_count)
        return {"company": company, "signals": [], "agent_debug": {"jobs": debug}}

    role_counts: Counter[tuple[str, str]] = Counter(collected_roles)
    for (role, source), freq in role_counts.items():
        signals.append(
            {
                "type": "hiring",
                "raw_text": role,
                "metadata": {
                    "role": role,
                    "seniority": _infer_seniority(role),
                    "frequency": freq,
                    "evidence": role,
                },
                "source": source,
            }
        )

    debug = {
        "raw": raw_count,
        "filtered": filtered_count,
        "filter_reasons": dict(filter_reasons),
    }
    logger.info("jobs debug | company=%s raw=%s filtered=%s", company, raw_count, filtered_count)
    return {"company": company, "signals": signals, "agent_debug": {"jobs": debug}}
