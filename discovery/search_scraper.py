from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import parse_qs, urlparse

from app.stage_supervisor import supervise_stage_async
from data_sources.serp_client import search_serp


logger = logging.getLogger(__name__)

BLOCKED_DOMAINS = {
    "bing.com",
    "google.com",
    "yahoo.com",
    "linkedin.com",
    "indeed.com",
    "glassdoor.com",
    "facebook.com",
    "twitter.com",
    "instagram.com",
}


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _extract_domain(href: str) -> str:
    url = _clean_text(href)
    if not url:
        return ""

    if url.startswith("/url?"):
        parsed = urlparse(url)
        real = parse_qs(parsed.query).get("q", [""])[0]
        url = real or url

    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _normalize_result_url(href: str) -> str:
    value = _clean_text(href)
    if not value:
        return ""

    if value.startswith("/url?"):
        parsed = urlparse(value)
        real = parse_qs(parsed.query).get("q", [""])[0]
        value = _clean_text(real)

    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"}:
        return ""
    return value


def _is_blocked_search_result(url: str, domain: str) -> bool:
    d = _clean_text(domain).lower()
    if not d or d in BLOCKED_DOMAINS:
        return True

    value = _clean_text(url).lower()
    if not value:
        return True

    if "bing.com/search" in value or "google.com/search" in value or "yahoo.com/search" in value:
        return True
    if "bing.com/ck/a" in value or "google.com/url?" in value:
        return True
    return False


def filter_search_results(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    filtered: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        domain = _extract_domain(str(row.get("url") or row.get("domain") or ""))
        url = _normalize_result_url(str(row.get("url") or ""))
        company = _clean_text(row.get("company"))
        if not company:
            continue
        if _is_blocked_search_result(url, domain):
            continue

        filtered.append(
            {
                **row,
                "company": company,
                "domain": domain,
                "url": url,
            }
        )
    return filtered


def _company_from_title(title: str, domain: str) -> str:
    text = _clean_text(title)
    if text:
        first = re.split(r"\s+[\-|:]\s+", text)[0].strip()
        if first:
            return first

    if domain:
        base = domain.split(".")[0]
        return base.replace("-", " ").replace("_", " ").title()

    return ""


def _search_results_quality_score(rows: list[dict[str, str]]) -> float:
    if not isinstance(rows, list) or not rows:
        return 0.0

    domains = {str(row.get("domain") or "").strip().lower() for row in rows if isinstance(row, dict)}
    domains.discard("")
    score = 0.2
    if 3 <= len(rows) <= 12:
        score += 0.3
    elif len(rows) > 0:
        score += 0.15
    if len(domains) >= 3:
        score += 0.2
    blocked_hits = sum(1 for row in rows if _is_blocked_search_result(str(row.get("url") or ""), str(row.get("domain") or "")))
    if blocked_hits == 0:
        score += 0.15
    if all(len(_clean_text(row.get("snippet"))) >= 10 for row in rows if isinstance(row, dict)):
        score += 0.1
    return min(1.0, score)


async def scrape_google_results(query: str, region: str, max_pages: int = 2) -> list[dict[str, str]]:
    async def _run_stage(payload: dict[str, Any]) -> list[dict[str, str]]:
        refined_query = _clean_text(payload.get("query") or query)
        refined_region = _clean_text(payload.get("region") or region)
        raw_rows = await search_serp(refined_query)

        results: list[dict[str, str]] = []
        seen_domains: set[str] = set()
        for row in raw_rows:
            if not isinstance(row, dict):
                continue
            title = _clean_text(row.get("title"))
            snippet = _clean_text(row.get("snippet"))
            url = _normalize_result_url(_clean_text(row.get("link")))
            domain = _extract_domain(url)
            if not domain or domain in seen_domains:
                continue
            if _is_blocked_search_result(url, domain):
                continue

            company = _company_from_title(title, domain)
            if not company:
                continue

            seen_domains.add(domain)
            results.append(
                {
                    "company": company,
                    "domain": domain,
                    "snippet": snippet or title,
                    "source": _clean_text(row.get("source") or "serp"),
                    "region": refined_region,
                    "query": refined_query,
                    "url": url,
                }
            )

        filtered = filter_search_results(results)
        logger.info("search results (api) | query=%s region=%s count=%s", refined_query, refined_region, len(filtered))
        return filtered

    supervised_rows, audits = await supervise_stage_async(
        stage_name="serp_scrape",
        input_payload={"query": query, "region": region, "max_pages": max_pages},
        execute_stage=_run_stage,
        fallback_stage=lambda payload: [],
        objective=(
            "Fetch company-level search results for the target ICP. Avoid list pages, directories, jobs pages, and generic media pages."
        ),
        min_quality=0.7,
        max_retries=2,
        quality_fn=_search_results_quality_score,
    )

    if audits:
        last_audit = audits[-1]
        logger.info(
            "serp stage | attempt=%s quality=%.2f approved=%s retry=%s issues=%s",
            last_audit.attempt,
            last_audit.quality_score,
            last_audit.approved,
            last_audit.retry,
            last_audit.issues,
        )

    return supervised_rows if isinstance(supervised_rows, list) else []
