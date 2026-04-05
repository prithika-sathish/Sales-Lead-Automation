from __future__ import annotations

import asyncio
import logging
from typing import Any
from urllib.parse import urlparse

from data_sources.serp_client import search_serp
from data_sources.structured_sources import fetch_structured_sources
from utils.entity_validation import is_real_company_entity


logger = logging.getLogger(__name__)

CONFIDENCE_BY_SOURCE_TYPE = {
    "structured": 0.95,
    "serp": 0.75,
    "crawl": 0.55,
}

FALLBACK_COMPANIES = [
    ("Chargebee", "https://www.chargebee.com", "Subscription billing platform for SaaS businesses"),
    ("Zuora", "https://www.zuora.com", "Subscription management and recurring billing platform"),
    ("Paddle", "https://www.paddle.com", "Revenue delivery and subscription billing infrastructure"),
    ("Recurly", "https://recurly.com", "Recurring billing and subscription analytics software"),
    ("ChargeOver", "https://chargeover.com", "Automated recurring billing and invoicing software"),
    ("Billwerk", "https://www.billwerk.plus", "Subscription management and recurring payment platform"),
    ("Razorpay", "https://razorpay.com", "Payments and subscription stack for businesses"),
    ("Zoho Subscriptions", "https://www.zoho.com/subscriptions", "Recurring billing for subscription businesses"),
    ("Billsby", "https://www.billsby.com", "Subscription billing and recurring payment management"),
    ("FastSpring", "https://fastspring.com", "Merchant of record and subscription commerce platform"),
    ("2Checkout", "https://www.2checkout.com", "Global payment processing and subscription billing"),
    ("Fusebill", "https://www.fusebill.com", "Automated subscription billing and revenue management"),
    ("SaaSOptics", "https://www.saasoptics.com", "Subscription analytics and recurring revenue operations"),
    ("Maxio", "https://www.maxio.com", "B2B SaaS billing, finance, and analytics operations"),
    ("OneBill", "https://www.onebillsoftware.com", "Usage-based subscription billing and monetization"),
    ("Kill Bill", "https://killbill.io", "Open source subscription billing and payments platform"),
    ("Chargebacks911", "https://chargebacks911.com", "Revenue recovery and payment dispute management"),
    ("PayU", "https://payu.in", "Payments platform with recurring billing support"),
    ("Paytm Payment Gateway", "https://business.paytm.com", "Business payments and subscription checkout"),
    ("Cashfree", "https://www.cashfree.com", "Business payments with subscriptions and auto-collect"),
    ("Juspay", "https://juspay.in", "Enterprise payments orchestration and checkout infrastructure"),
    ("PayPal Subscriptions", "https://www.paypal.com", "Recurring billing and subscription payments"),
    ("Stripe Billing", "https://stripe.com", "Subscription billing and recurring revenue tooling"),
    ("Braintree", "https://www.braintreepayments.com", "Payment gateway with recurring billing APIs"),
    ("Mollie", "https://www.mollie.com", "European payments and recurring billing support"),
    ("GoCardless", "https://gocardless.com", "Direct debit payments and recurring collections"),
    ("Checkout.com", "https://www.checkout.com", "Global payments platform for digital businesses"),
    ("BlueSnap", "https://home.bluesnap.com", "Payment orchestration and subscription billing"),
    ("Adyen", "https://www.adyen.com", "Enterprise payments and recurring billing infrastructure"),
    ("Chargezoom", "https://www.chargezoom.com", "Integrated billing and payments automation"),
    ("Orb", "https://www.withorb.com", "Usage-based billing platform for modern SaaS"),
    ("Metronome", "https://metronome.com", "Usage-based billing and pricing infrastructure"),
    ("Sequence", "https://www.sequencehq.com", "Revenue and billing workflow automation for SaaS"),
    ("Subbly", "https://www.subbly.co", "Subscription commerce and recurring billing platform"),
    ("Younium", "https://www.younium.com", "B2B subscription management and billing"),
    ("Sage Intacct", "https://www.sage.com", "Financial operations with recurring revenue modules"),
    ("FreshBooks", "https://www.freshbooks.com", "Billing and invoicing with recurring payment support"),
]

EXCLUDED_RESULT_DOMAINS = {
    "reddit.com",
    "quora.com",
    "wikipedia.org",
    "linkedin.com",
    "facebook.com",
    "x.com",
    "twitter.com",
    "youtube.com",
    "instagram.com",
    "medium.com",
}

ENTERPRISE_FALLBACK_BLOCKLIST = {
    "chargebee",
    "zuora",
    "paddle",
    "stripe",
    "zoho",
    "razorpay",
    "paypal",
    "adyen",
    "checkout",
    "sage",
    "freshbooks",
    "payu",
    "paytm",
    "amazon",
    "google",
    "microsoft",
    "oracle",
    "ibm",
    "salesforce",
}


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _extract_domain(value: object) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    parsed = urlparse(text)
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _guess_company_name(title: str, link: str) -> str:
    text = _clean_text(title)
    if text:
        candidate = text.split("-")[0].split(":")[0].strip()
        if candidate:
            return candidate
    domain = _extract_domain(link)
    if not domain:
        return ""
    return domain.split(".")[0].replace("-", " ").replace("_", " ").title()


def _normalize_serp_rows(query_row: dict[str, str], rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    region = _clean_text(query_row.get("region"))
    industry = _clean_text(query_row.get("industry"))
    signal_type = _clean_text(query_row.get("signal_type"))

    for row in rows:
        if not isinstance(row, dict):
            continue
        title = _clean_text(row.get("title"))
        link = _clean_text(row.get("link"))
        snippet = _clean_text(row.get("snippet"))
        domain = _extract_domain(link)
        company_name = _guess_company_name(title, link)
        if not company_name or not domain:
            continue
        if domain in EXCLUDED_RESULT_DOMAINS:
            continue
        if not is_real_company_entity(company=company_name, domain=domain, description=snippet or title, url=link):
            continue

        normalized.append(
            {
                "company_name": company_name,
                "domain": domain,
                "website": link,
                "description": snippet or title,
                "title": title,
                "snippet": snippet,
                "tags": [industry, signal_type],
                "query": _clean_text(query_row.get("query")),
                "source": _clean_text(row.get("source") or "serp"),
                "source_type": "serp",
                "signal_type": signal_type,
                "region": region,
                "industry": industry,
                "confidence_score": CONFIDENCE_BY_SOURCE_TYPE["serp"],
            }
        )

    return normalized


def _normalize_structured_rows(query_row: dict[str, str], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    region = _clean_text(query_row.get("region"))
    industry = _clean_text(query_row.get("industry"))
    signal_type = _clean_text(query_row.get("signal_type"))

    for row in rows:
        if not isinstance(row, dict):
            continue
        company_name = _clean_text(row.get("company_name"))
        website = _clean_text(row.get("website"))
        description = _clean_text(row.get("description"))
        source = _clean_text(row.get("source") or "structured")
        domain = _extract_domain(website)
        if not company_name:
            continue
        if not is_real_company_entity(company=company_name, domain=domain, description=description, url=website):
            continue

        normalized.append(
            {
                "company_name": company_name,
                "domain": domain,
                "website": website,
                "description": description,
                "title": company_name,
                "snippet": description,
                "tags": row.get("tags") if isinstance(row.get("tags"), list) else [industry],
                "query": _clean_text(query_row.get("query")),
                "source": source,
                "source_type": "structured",
                "signal_type": signal_type,
                "region": region,
                "industry": industry,
                "confidence_score": CONFIDENCE_BY_SOURCE_TYPE["structured"],
            }
        )

    return normalized


def _dedupe_by_domain(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}

    for row in rows:
        if not isinstance(row, dict):
            continue
        company_name = _clean_text(row.get("company_name"))
        domain = _clean_text(row.get("domain"))
        if not company_name:
            continue

        key = domain or company_name.lower()
        existing = merged.get(key)
        if existing is None:
            row["sources"] = [row.get("source")]
            row["source_types"] = [row.get("source_type")]
            merged[key] = row
            continue

        existing_sources = existing.get("sources") if isinstance(existing.get("sources"), list) else []
        if row.get("source") and row.get("source") not in existing_sources:
            existing_sources.append(row.get("source"))
        existing["sources"] = existing_sources

        existing_types = existing.get("source_types") if isinstance(existing.get("source_types"), list) else []
        if row.get("source_type") and row.get("source_type") not in existing_types:
            existing_types.append(row.get("source_type"))
        existing["source_types"] = existing_types

        existing["confidence_score"] = max(float(existing.get("confidence_score") or 0.0), float(row.get("confidence_score") or 0.0))

        description = _clean_text(existing.get("description"))
        if not description:
            existing["description"] = _clean_text(row.get("description"))

        website = _clean_text(existing.get("website"))
        if not website:
            existing["website"] = _clean_text(row.get("website"))

    return list(merged.values())


def _build_fallback_rows(
    *,
    query_rows: list[dict[str, str]],
    existing_domains: set[str],
    needed: int,
) -> list[dict[str, Any]]:
    if needed <= 0:
        return []

    fallback_rows: list[dict[str, Any]] = []
    region = _clean_text(query_rows[0].get("region")) if query_rows else ""
    industry = _clean_text(query_rows[0].get("industry")) if query_rows else ""
    signal_type = _clean_text(query_rows[0].get("signal_type")) if query_rows else "intent"
    query = _clean_text(query_rows[0].get("query")) if query_rows else ""

    for company_name, website, description in FALLBACK_COMPANIES:
        domain = _extract_domain(website)
        domain_label = domain.split(".")[0].lower() if domain else ""
        if not domain or domain in existing_domains:
            continue
        if domain_label in ENTERPRISE_FALLBACK_BLOCKLIST:
            continue

        fallback_rows.append(
            {
                "company_name": company_name,
                "domain": domain,
                "website": website,
                "description": description,
                "title": company_name,
                "snippet": description,
                "tags": [industry or "subscription billing", signal_type or "intent"],
                "query": query,
                "source": "deterministic_fallback",
                "source_type": "structured",
                "signal_type": signal_type or "intent",
                "region": region,
                "industry": industry or "subscription billing",
                "confidence_score": 0.65,
            }
        )

        if len(fallback_rows) >= needed:
            break

    return fallback_rows


async def aggregate_candidates(
    queries: list[dict[str, str]],
    *,
    target_min: int,
    per_query_limit: int = 10,
    query_timeout_seconds: float = 20.0,
    max_concurrent_queries: int = 4,
) -> dict[str, Any]:
    query_rows = [row for row in queries if isinstance(row, dict) and _clean_text(row.get("query"))]
    metrics: dict[str, Any] = {
        "queries_executed": len(query_rows),
        "success_rate_per_source": {"serp": 0.0, "structured": 0.0},
        "failures_per_source": {"serp": 0, "structured": 0},
        "source_rows": {"serp": 0, "structured": 0},
        "fallback_rows_added": 0,
        "total_candidates_generated": 0,
        "acquisition_success_rate": 0.0,
    }

    all_rows: list[dict[str, Any]] = []
    serp_success = 0
    structured_success = 0
    semaphore = asyncio.Semaphore(max(1, max_concurrent_queries))

    async def _process_query(query_row: dict[str, str]) -> tuple[list[dict[str, Any]], bool, bool, int, int]:
        query = _clean_text(query_row.get("query"))
        if not query:
            return [], False, False, 0, 0

        async def _safe_source_call(coro: Any, *, source_name: str, timeout_seconds: float = 8.0) -> tuple[list[Any], bool, int]:
            try:
                rows = await asyncio.wait_for(coro, timeout=timeout_seconds)
                valid_rows = rows if isinstance(rows, list) else []
                return valid_rows, True, 0
            except asyncio.TimeoutError:
                logger.warning("[TIMEOUT] %s", source_name)
                return [], False, 1
            except Exception as exc:
                logger.warning("[SOURCE FAILED] %s reason=%s", source_name, exc)
                return [], False, 1

        async with semaphore:
            source_results = await asyncio.gather(
                _safe_source_call(
                    search_serp(query, timeout_seconds=min(float(query_timeout_seconds), 10.0), retries=2),
                    source_name="serp",
                    timeout_seconds=min(float(query_timeout_seconds), 10.0),
                ),
                _safe_source_call(
                    fetch_structured_sources(query, per_source_limit=max(5, per_query_limit)),
                    source_name="structured",
                    timeout_seconds=8.0,
                ),
                return_exceptions=True,
            )

            serp_rows: list[dict[str, str]] = []
            structured_rows: list[dict[str, Any]] = []
            serp_ok = False
            structured_ok = False
            serp_failed_count = 0
            structured_failed_count = 0

            if len(source_results) >= 1 and not isinstance(source_results[0], Exception):
                serp_rows = source_results[0][0]
                serp_ok = source_results[0][1]
                serp_failed_count = source_results[0][2]
            elif len(source_results) >= 1:
                logger.warning("[SOURCE FAILED] serp")
                serp_failed_count = 1

            if len(source_results) >= 2 and not isinstance(source_results[1], Exception):
                structured_rows = source_results[1][0]
                structured_ok = source_results[1][1]
                structured_failed_count = source_results[1][2]
            elif len(source_results) >= 2:
                logger.warning("[SOURCE FAILED] structured")
                structured_failed_count = 1

            normalized_serp = _normalize_serp_rows(query_row, serp_rows[: max(1, per_query_limit)])
            normalized_structured = _normalize_structured_rows(query_row, structured_rows[: max(1, per_query_limit)])
            combined = normalized_serp + normalized_structured
            return combined, bool(serp_ok), bool(structured_ok), int(serp_failed_count), int(structured_failed_count)

    results = await asyncio.gather(*[_process_query(row) for row in query_rows], return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            metrics["failures_per_source"]["serp"] = int(metrics["failures_per_source"]["serp"]) + 1
            metrics["failures_per_source"]["structured"] = int(metrics["failures_per_source"]["structured"]) + 1
            continue

        rows, serp_ok, structured_ok, serp_failed_count, structured_failed_count = result
        if serp_ok:
            serp_success += 1
        if structured_ok:
            structured_success += 1

        metrics["failures_per_source"]["serp"] = int(metrics["failures_per_source"]["serp"]) + serp_failed_count
        metrics["failures_per_source"]["structured"] = int(metrics["failures_per_source"]["structured"]) + structured_failed_count

        serp_count = len([row for row in rows if _clean_text(row.get("source_type")) == "serp"])
        structured_count = len([row for row in rows if _clean_text(row.get("source_type")) == "structured"])
        metrics["source_rows"]["serp"] = int(metrics["source_rows"]["serp"]) + serp_count
        metrics["source_rows"]["structured"] = int(metrics["source_rows"]["structured"]) + structured_count
        all_rows.extend(rows)

    deduped = _dedupe_by_domain(all_rows)
    logger.info("[RESULT COUNT] acquisition_deduped: %s", len(deduped))

    if query_rows:
        metrics["success_rate_per_source"]["serp"] = round(serp_success / len(query_rows), 3)
        metrics["success_rate_per_source"]["structured"] = round(structured_success / len(query_rows), 3)
    metrics["acquisition_success_rate"] = round(
        (
            float(metrics["success_rate_per_source"]["serp"])
            + float(metrics["success_rate_per_source"]["structured"])
        )
        / 2,
        3,
    )

    if float(metrics["acquisition_success_rate"]) < 0.3:
        raise Warning("Pipeline unhealthy")

    if len(deduped) < target_min:
        existing_domains = {_clean_text(row.get("domain")) for row in deduped if isinstance(row, dict)}
        needed = max(0, int(target_min) - len(deduped))
        fallback_rows = _build_fallback_rows(query_rows=query_rows, existing_domains=existing_domains, needed=needed)
        if fallback_rows:
            deduped = _dedupe_by_domain(deduped + fallback_rows)
            metrics["fallback_rows_added"] = len(fallback_rows)
            metrics["source_rows"]["structured"] = int(metrics["source_rows"]["structured"]) + len(fallback_rows)
            logger.warning(
                "fallback top-up applied | added=%s target_min=%s final_candidates=%s",
                len(fallback_rows),
                target_min,
                len(deduped),
            )

    metrics["total_candidates_generated"] = len(deduped)

    if len(deduped) < target_min:
        logger.warning(
            "candidate floor not met | candidates=%s target_min=%s", len(deduped), target_min
        )

    logger.info(
        "aggregator metrics | queries=%s serp_rows=%s structured_rows=%s fallback_rows=%s candidates=%s",
        metrics["queries_executed"],
        metrics["source_rows"]["serp"],
        metrics["source_rows"]["structured"],
        metrics["fallback_rows_added"],
        metrics["total_candidates_generated"],
    )

    return {
        "candidates": deduped,
        "metrics": metrics,
    }
