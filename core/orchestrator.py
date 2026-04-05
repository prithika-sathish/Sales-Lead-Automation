from __future__ import annotations

import asyncio
import hashlib
import logging
import time
import json
from typing import Any
from typing import Callable

from agents.content_agent import collect_content_signals
from agents.docs_agent import collect_docs_signals
from agents.github_agent import collect_github_signals
from agents.jobs_agent import collect_jobs_signals
from agents.linkedin_agent import collect_linkedin_signals
from agents.producthunt_agent import collect_producthunt_signals
from agents.review_agent import collect_review_signals
from agents.techstack_agent import collect_techstack_signals
from agents.traffic_agent import collect_traffic_signals
from agents.website_agent import collect_website_signals
from app.apify_client import merge_results
from core.cache import cache
from core.history_db import history_db
from core.normalizer import normalize_signals


AgentCollector = Callable[[str], dict[str, Any]]

AGENTS: list[tuple[str, AgentCollector]] = [
    ("linkedin", collect_linkedin_signals),
    ("github", collect_github_signals),
    ("jobs", collect_jobs_signals),
    ("producthunt", collect_producthunt_signals),
    ("review", collect_review_signals),
    ("website", collect_website_signals),
    ("traffic", collect_traffic_signals),
    ("docs", collect_docs_signals),
    ("content", collect_content_signals),
    ("techstack", collect_techstack_signals),
]

MAX_CONCURRENT_COMPANIES = 5
AGENT_TIMEOUT_SECONDS = 25

HIGH_INTENT_PHRASES = {
    "actively_hiring_engineers": ["we are hiring", "open roles", "join our team", "careers page"],
    "recent_funding": ["raised $", "series a", "series b", "series c", "funding round"],
    "new_product_launch": ["launch", "released", "new feature", "product update"],
    "rapid_growth_mentions": ["rapid growth", "growing fast", "momentum", "expansion"],
    "expansion_hiring": ["expanding hiring", "hiring expansion", "building the team", "grow our team"],
}

MID_INTENT_TYPES = {
    "active_github_repo",
    "repo_activity_spike",
    "integration_added",
    "api_update",
    "product_maturity",
    "website_update",
    "company_update",
    "content_push",
    "traffic_growth",
    "github_activity",
    "github_mention",
    "dev_activity",
}

WEAK_INTENT_TYPES = {
    "review_mention",
    "customer_pain",
    "content_mention",
    "has_website",
    "basic_online_presence",
}


logger = logging.getLogger(__name__)


def _build_hiring_signal_id(company: str, role: str, source: str, date_posted: str) -> str:
    payload = f"{company.lower()}|{role.lower()}|{source.lower()}|{date_posted.lower()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def collect_hiring_signals(companies: list[str], discovered_jobs: list[dict[str, Any]]) -> dict[str, Any]:
    clean_companies: list[str] = []
    seen: set[str] = set()
    for company in companies:
        name = str(company or "").strip()
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        clean_companies.append(name)

    if not clean_companies:
        return {"companies": [], "execution_metadata": {"total_time": "0.00s", "agents_used": ["jobs_only"], "failed_agents": []}}

    jobs_by_company: dict[str, list[dict[str, Any]]] = {}
    for row in discovered_jobs:
        if not isinstance(row, dict):
            continue
        company = str(row.get("company") or "").strip()
        role = str(row.get("role") or "").strip()
        source = str(row.get("source") or "").strip().lower()
        if not company or not role or source not in {"linkedin", "indeed", "playwright", "apify"}:
            continue
        jobs_by_company.setdefault(company.lower(), []).append(row)

    output_companies: list[dict[str, Any]] = []
    for company in clean_companies:
        rows = jobs_by_company.get(company.lower(), [])
        if len(rows) < 2:
            continue

        signals: list[dict[str, Any]] = []
        for row in rows:
            role = str(row.get("role") or "").strip()
            source = str(row.get("source") or "").strip().lower()
            location = str(row.get("location") or "").strip()
            date_posted = str(row.get("date_posted") or "").strip()
            evidence = role
            signal_id = _build_hiring_signal_id(company, role, source, date_posted)
            signals.append(
                {
                    "company": company,
                    "signal_id": signal_id,
                    "signal_type": "hiring",
                    "signal_strength": 3,
                    "timestamp": date_posted,
                    "recency_score": 3,
                    "final_score": 12,
                    "signal_score": 12,
                    "metadata": {
                        "raw_text": evidence,
                        "evidence": evidence,
                        "role": role,
                        "location": location,
                        "date_posted": date_posted,
                    },
                    "source": source,
                }
            )

        if len(signals) < 2:
            continue

        output_companies.append(
            {
                "company": company,
                "signal_count": len(signals),
                "high_intent_signals": signals[:10],
                "signals": signals,
                "derived_signals": [],
                "topics": [],
                "trend_signals": [],
                "failed_agents": [],
                "agent_debug": {"jobs_only": {"raw": len(rows), "filtered": len(signals)}},
            }
        )

    return {
        "companies": output_companies,
        "execution_metadata": {
            "total_time": "0.00s",
            "agents_used": ["jobs_only"],
            "failed_agents": [],
        },
    }


async def _run_agent_with_timeout(agent_name: str, agent: AgentCollector, company: str) -> dict[str, Any] | Exception:
    cache_key = f"{company.strip().lower()}::{agent_name}"
    cached = cache.get(cache_key)
    if isinstance(cached, dict):
        logger.info("cache hit | company=%s agent=%s", company, agent_name)
        return cached

    logger.info("agent start | company=%s agent=%s", company, agent_name)
    try:
        result = await asyncio.wait_for(asyncio.to_thread(agent, company), timeout=AGENT_TIMEOUT_SECONDS)
        if isinstance(result, dict):
            cache.set(cache_key, result)
        logger.info("agent end | company=%s agent=%s", company, agent_name)
        return result
    except asyncio.TimeoutError:
        logger.warning("agent timeout | company=%s agent=%s", company, agent_name)
        return TimeoutError(f"{agent_name} timed out")
    except Exception as exc:  # noqa: BLE001 - fault isolation by design
        logger.warning("agent failed | company=%s agent=%s error=%s", company, agent_name, exc)
        return exc


def _extract_topics(signals: list[dict[str, Any]]) -> list[str]:
    topic_counts: dict[str, int] = {}
    for signal in signals:
        metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
        topics = metadata.get("topics")
        if not isinstance(topics, list):
            continue
        for topic in topics:
            topic_name = str(topic).strip()
            if not topic_name:
                continue
            topic_counts[topic_name] = topic_counts.get(topic_name, 0) + 1

    ordered = sorted(topic_counts.items(), key=lambda kv: kv[1], reverse=True)
    return [topic for topic, _ in ordered[:8]]


def _contains_any(text: str, phrases: list[str]) -> bool:
    lowered = text.lower()
    return any(phrase in lowered for phrase in phrases)


def _classify_strict_signal(signal: dict[str, Any]) -> dict[str, Any] | None:
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    raw_text = str(signal.get("raw_text") or metadata.get("raw_text") or "").strip()
    signal_type = str(signal.get("signal_type") or signal.get("type") or "").strip().lower()
    source = str(signal.get("source") or "").strip().lower()
    text = " ".join([raw_text, json.dumps(metadata, ensure_ascii=False) if metadata else "", signal_type, source]).lower()

    if not raw_text:
        return None

    if _contains_any(text, HIGH_INTENT_PHRASES["actively_hiring_engineers"]):
        return {"signal_type": "actively_hiring_engineers", "bucket": "high", "weight": 5.0}
    if _contains_any(text, HIGH_INTENT_PHRASES["expansion_hiring"]):
        return {"signal_type": "expansion_hiring", "bucket": "high", "weight": 5.0}
    if _contains_any(text, HIGH_INTENT_PHRASES["recent_funding"]):
        if any(term in text for term in ["grants", "funding opportunities", "startup grants"]):
            return None
        return {"signal_type": "recent_funding", "bucket": "high", "weight": 5.0}
    if _contains_any(text, HIGH_INTENT_PHRASES["new_product_launch"]):
        return {"signal_type": "new_product_launch", "bucket": "high", "weight": 5.0}
    if _contains_any(text, HIGH_INTENT_PHRASES["rapid_growth_mentions"]):
        return {"signal_type": "rapid_growth_mentions", "bucket": "high", "weight": 5.0}

    if signal_type in MID_INTENT_TYPES:
        return {"signal_type": signal_type, "bucket": "strong", "weight": 2.0}
    if signal_type in WEAK_INTENT_TYPES:
        return {"signal_type": signal_type, "bucket": "weak", "weight": 0.5}

    if any(token in text for token in ["api available", "api reference", "tech stack", "stack detected", "github repo"]):
        return {"signal_type": "tech_stack_detected", "bucket": "strong", "weight": 2.0}

    if source in {"github", "website", "content", "review", "product_hunt"} and len(raw_text) >= 25:
        return {"signal_type": "basic_online_presence", "bucket": "weak", "weight": 0.5}

    return None


def _build_signal_profile(agent_outputs: list[dict[str, Any]]) -> dict[str, Any]:
    structured: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    for output in agent_outputs:
        for signal in output.get("signals", []):
            if not isinstance(signal, dict):
                continue
            metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
            raw_text = str(signal.get("raw_text") or metadata.get("raw_text") or "").strip()
            source = str(signal.get("source") or "").strip().lower()
            signal_type = str(signal.get("signal_type") or signal.get("type") or "").strip().lower()
            key = (signal_type, source, raw_text.lower())
            if not raw_text or key in seen:
                continue
            seen.add(key)

            classified = _classify_strict_signal(signal)
            if not classified:
                continue

            metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
            structured.append(
                {
                    "signal_type": classified["signal_type"],
                    "bucket": classified["bucket"],
                    "weight": classified["weight"],
                    "raw_text": raw_text,
                    "metadata": metadata,
                    "source": source,
                }
            )

    high_intent_signals = [signal for signal in structured if signal.get("bucket") == "high"]
    strong_signals = [signal for signal in structured if signal.get("bucket") == "strong"]
    weak_signals = [signal for signal in structured if signal.get("bucket") == "weak"]
    intent_score = (len(high_intent_signals) * 5.0) + (len(strong_signals) * 2.0) + (len(weak_signals) * 0.5)

    return {
        "signals": structured,
        "high_intent_signals": high_intent_signals,
        "signal_count": len(structured),
        "intent_score": intent_score,
        "signals_found": sorted({str(signal.get("signal_type") or "") for signal in structured if str(signal.get("signal_type") or "").strip()}),
    }


async def _run_agents_for_company(company: str) -> dict[str, Any]:
    tasks = [_run_agent_with_timeout(agent_name, agent, company) for agent_name, agent in AGENTS]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    agent_outputs: list[dict[str, Any]] = []
    failed_agents: list[str] = []
    agent_debug: dict[str, Any] = {}
    for idx, result in enumerate(results):
        agent_name = AGENTS[idx][0]
        if isinstance(result, Exception):
            failed_agents.append(f"{company}:{agent_name}")
            continue
        if isinstance(result, dict):
            agent_outputs.append(result)
            debug_data = result.get("agent_debug")
            if isinstance(debug_data, dict):
                agent_debug.update(debug_data)

    if not agent_outputs:
        logger.warning("all agents failed | company=%s", company)
        return {}

    raw_signals: list[dict[str, Any]] = []
    for output in agent_outputs:
        for signal in output.get("signals", []):
            if not isinstance(signal, dict):
                continue
            raw_signals.append(
                {
                    "company": company,
                    "type": str(signal.get("type") or ""),
                    "raw_text": str(signal.get("raw_text") or ""),
                    "metadata": signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {},
                    "source": str(signal.get("source") or ""),
                }
            )

    normalized = normalize_signals(raw_signals)
    if not normalized:
        logger.info("no normalized signals | company=%s count=0; assigning unknown fallback", company)
        fallback_signal = {
            "signal_type": "unknown",
            "bucket": "weak",
            "weight": 1.0,
            "raw_text": "No reliable signals extracted; using ingestion prior",
            "metadata": {"signal_strength": "unknown", "inferred_intent": 1.0},
            "source": "fallback",
        }
        return {
            "company": company,
            "signal_count": 1,
            "high_intent_signals": [],
            "signals": [fallback_signal],
            "derived_signals": [],
            "topics": [],
            "trend_signals": [],
            "intent_score": 1.0,
            "signals_found": ["unknown"],
            "signal_strength": "unknown",
            "inferred_intent": 1.0,
            "failed_agents": failed_agents,
            "agent_debug": agent_debug,
        }

    profile = _build_signal_profile([{"signals": normalized}])
    history_db.append_signals(company, normalized)

    logger.info(
        "signal profile | company=%s signals_found=%s high_intent_signals=%s intent_score=%s",
        company,
        profile["signals_found"],
        [sig.get("signal_type") for sig in profile["high_intent_signals"]],
        profile["intent_score"],
    )

    return {
        "company": company,
        "signal_count": profile["signal_count"],
        "high_intent_signals": profile["high_intent_signals"][:10],
        "signals": profile["signals"],
        "derived_signals": [],
        "topics": _extract_topics(normalized),
        "trend_signals": [],
        "intent_score": profile["intent_score"],
        "signals_found": profile["signals_found"],
        "signal_strength": "strong" if profile["high_intent_signals"] else ("medium" if profile["signal_count"] >= 2 else "weak"),
        "inferred_intent": float(profile["intent_score"]),
        "failed_agents": failed_agents,
        "agent_debug": agent_debug,
    }


async def collect_signals_pipeline(domains: list[str]) -> dict[str, Any]:
    start = time.perf_counter()
    company_seed = merge_results(domains, per_domain_limit=3, total_limit=12)
    companies: list[str] = []
    seen: set[str] = set()

    for item in company_seed:
        company = str(item.get("company") or "").strip()
        if not company:
            continue
        key = company.lower()
        if key in seen:
            continue
        seen.add(key)
        companies.append(company)

    if not companies:
        return {
            "companies": [],
            "execution_metadata": {
                "total_time": "0.00s",
                "agents_used": [name for name, _ in AGENTS],
                "failed_agents": [],
            },
        }

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_COMPANIES)

    async def _process(company: str) -> dict[str, Any]:
        async with semaphore:
            return await _run_agents_for_company(company)

    tasks = [asyncio.create_task(_process(company)) for company in companies]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    merged_companies: list[dict[str, Any]] = []
    failed_agents: list[str] = []
    for result in results:
        if isinstance(result, Exception):
            continue
        if isinstance(result, dict) and result.get("company"):
            merged_companies.append(result)
            failed_agents.extend([str(item) for item in result.get("failed_agents", []) if str(item)])

    elapsed = f"{(time.perf_counter() - start):.2f}s"
    return {
        "companies": merged_companies,
        "execution_metadata": {
            "total_time": elapsed,
            "agents_used": [name for name, _ in AGENTS],
            "failed_agents": sorted(set(failed_agents)),
        },
    }


async def collect_signals_for_companies(companies: list[str]) -> dict[str, Any]:
    start = time.perf_counter()
    clean_companies: list[str] = []
    seen: set[str] = set()
    for company in companies:
        name = company.strip()
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        clean_companies.append(name)

    if not clean_companies:
        return {
            "companies": [],
            "execution_metadata": {
                "total_time": "0.00s",
                "agents_used": [name for name, _ in AGENTS],
                "failed_agents": [],
            },
        }

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_COMPANIES)

    async def _process(company: str) -> dict[str, Any]:
        async with semaphore:
            return await _run_agents_for_company(company)

    tasks = [asyncio.create_task(_process(company)) for company in clean_companies]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    merged_companies: list[dict[str, Any]] = []
    failed_agents: list[str] = []
    for result in results:
        if isinstance(result, Exception):
            continue
        if isinstance(result, dict) and result.get("company"):
            merged_companies.append(result)
            failed_agents.extend([str(item) for item in result.get("failed_agents", []) if str(item)])

    elapsed = f"{(time.perf_counter() - start):.2f}s"
    return {
        "companies": merged_companies,
        "execution_metadata": {
            "total_time": elapsed,
            "agents_used": [name for name, _ in AGENTS],
            "failed_agents": sorted(set(failed_agents)),
        },
    }


async def _retry_async(coro_factory: Callable[[], Any], *, retries: int, delay_seconds: float, stage: str) -> Any:
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return await coro_factory()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            logger.warning("%s failed (attempt %s/%s): %s", stage, attempt + 1, retries + 1, exc)
            if attempt < retries:
                await asyncio.sleep(delay_seconds)
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"{stage} failed")


async def run_multilayer_lead_discovery(
    *,
    settings: Any | None = None,
    target_min: int | None = None,
    target_max: int | None = None,
) -> dict[str, Any]:
    from config.settings import DiscoverySettings
    from crawler.deep_crawler import crawl_company_domain
    from discovery.query_generator import generate_region_queries
    from discovery.query_generator import generate_region_search_fallback_queries
    from discovery.search_scraper import filter_search_results
    from discovery.search_scraper import scrape_google_results
    from intelligence.scorer import rank_companies
    from intelligence.signal_extractor import extract_signals_for_company
    from utils.dedup import dedupe_by_domain
    from utils.dedup import remove_low_quality_entries

    config = settings if isinstance(settings, DiscoverySettings) else DiscoverySettings()
    config.batch_size = 1
    if isinstance(target_min, int) and target_min > 0:
        config.target_min = target_min
    if isinstance(target_max, int) and target_max > 0:
        config.target_max = target_max

    start = time.perf_counter()
    stage_stats: dict[str, int] = {
        "search_results": 0,
        "after_domain_filter": 0,
        "after_dedupe": 0,
        "after_crawl": 0,
        "after_rank": 0,
    }

    def _apply_site_exclusions(query: str) -> str:
        base = str(query or "").strip()
        if not base:
            return ""
        exclusions = " -site:linkedin.com -site:indeed.com -site:glassdoor.com"
        return f"{base}{exclusions}"

    base_queries = generate_region_queries(config.regions, config.industries, include_expansion=False)
    expansion_queries = generate_region_queries(config.regions, config.industries, include_expansion=True)
    fallback_queries = generate_region_search_fallback_queries(config.regions)

    # Keep Bing query volume low to reduce anti-bot pressure.
    base_queries = base_queries[:6]
    expansion_queries = expansion_queries[:6]
    fallback_queries = fallback_queries[:6]

    search_semaphore = asyncio.Semaphore(1)

    async def _run_search(query_row: dict[str, str], page_limit: int) -> list[dict[str, str]]:
        async with search_semaphore:
            query = str(query_row.get("query") or "").strip()
            region = str(query_row.get("region") or "").strip()
            if not query or not region:
                return []

            effective_query = _apply_site_exclusions(query)
            return await _retry_async(
                lambda: scrape_google_results(query=effective_query, region=region, max_pages=page_limit),
                retries=1,
                delay_seconds=0.8,
                stage=f"search:{region}",
            )

    search_rows: list[dict[str, Any]] = []
    for row in base_queries:
        result = await _run_search(row, config.max_pages)
        if isinstance(result, list):
            search_rows.extend([item for item in result if isinstance(item, dict)])

    stage_stats["search_results"] = len(search_rows)
    filtered_rows = filter_search_results(search_rows)
    stage_stats["after_domain_filter"] = len(filtered_rows)
    deduped = dedupe_by_domain(filtered_rows)
    deduped = remove_low_quality_entries(deduped)
    stage_stats["after_dedupe"] = len(deduped)

    if len(deduped) < config.target_min:
        logger.warning("deduped below target_min after base search: %s < %s", len(deduped), config.target_min)
        for row in expansion_queries:
            result = await _run_search(row, config.max_pages + 1)
            if isinstance(result, list):
                search_rows.extend([item for item in result if isinstance(item, dict)])

        stage_stats["search_results"] = len(search_rows)
        filtered_rows = filter_search_results(search_rows)
        stage_stats["after_domain_filter"] = len(filtered_rows)
        deduped = dedupe_by_domain(filtered_rows)
        deduped = remove_low_quality_entries(deduped)
        stage_stats["after_dedupe"] = len(deduped)

    if len(filtered_rows) < 10:
        logger.warning("filtered rows low-quality threshold triggered: %s", len(filtered_rows))
        for row in fallback_queries:
            result = await _run_search(row, config.max_pages + 1)
            if isinstance(result, list):
                search_rows.extend([item for item in result if isinstance(item, dict)])

        stage_stats["search_results"] = len(search_rows)
        filtered_rows = filter_search_results(search_rows)
        stage_stats["after_domain_filter"] = len(filtered_rows)
        deduped = dedupe_by_domain(filtered_rows)
        deduped = remove_low_quality_entries(deduped)
        stage_stats["after_dedupe"] = len(deduped)

    candidates = deduped[: max(config.target_max * 2, max(10, config.target_min))]

    crawl_semaphore = asyncio.Semaphore(max(2, min(16, config.batch_size)))

    async def _crawl_extract(item: dict[str, Any]) -> dict[str, Any] | None:
        async with crawl_semaphore:
            company = str(item.get("company") or "").strip()
            domain = str(item.get("domain") or "").strip()
            regions = item.get("regions") if isinstance(item.get("regions"), list) else []
            region = str(regions[0]) if regions else ""
            if not company or not domain:
                return None

            crawl_result = await _retry_async(
                lambda: crawl_company_domain(domain=domain, timeout_seconds=config.request_timeout_seconds),
                retries=1,
                delay_seconds=0.5,
                stage=f"crawl:{domain}",
            )

            pages = crawl_result.get("pages") if isinstance(crawl_result, dict) else {}
            if not isinstance(pages, dict):
                pages = {}

            merged_text = "\n\n".join(str(value) for value in pages.values() if str(value).strip())
            if not merged_text:
                return None

            signal = await _retry_async(
                lambda: extract_signals_for_company(
                    company=company,
                    domain=domain,
                    region=region,
                    crawled_text=merged_text,
                    llm_provider=config.llm_provider,
                ),
                retries=1,
                delay_seconds=0.4,
                stage=f"extract:{domain}",
            )

            if not isinstance(signal, dict):
                return None

            signal["sources"] = item.get("sources") if isinstance(item.get("sources"), list) else []
            signal["regions"] = item.get("regions") if isinstance(item.get("regions"), list) else []
            signal["crawl_engine"] = str(crawl_result.get("engine") or "") if isinstance(crawl_result, dict) else ""
            signal["snippet"] = str((item.get("snippets") or [""])[0]) if isinstance(item.get("snippets"), list) else ""
            return signal

    tasks = [asyncio.create_task(_crawl_extract(item)) for item in candidates]
    extracted: list[dict[str, Any]] = []
    for result in await asyncio.gather(*tasks, return_exceptions=True):
        if isinstance(result, Exception) or result is None:
            continue
        extracted.append(result)

    # Remove entries with empty signal footprint.
    extracted = [
        row
        for row in extracted
        if bool(row.get("hiring"))
        or bool(row.get("funding_signal"))
        or bool(row.get("growth_signal"))
        or (isinstance(row.get("tech_stack"), list) and len(row.get("tech_stack")) > 0)
    ]
    stage_stats["after_crawl"] = len(extracted)

    ranked = rank_companies(extracted)
    ranked = ranked[: config.target_max]
    stage_stats["after_rank"] = len(ranked)

    logger.info("pipeline_counts %s", stage_stats)

    elapsed = f"{(time.perf_counter() - start):.2f}s"
    return {
        "companies": ranked,
        "execution_metadata": {
            "total_time": elapsed,
            "search_results": stage_stats["search_results"],
            "after_domain_filter": stage_stats["after_domain_filter"],
            "deduped_companies": stage_stats["after_dedupe"],
            "crawled_companies": stage_stats["after_crawl"],
            "ranked_companies": stage_stats["after_rank"],
            "target_min": config.target_min,
            "target_max": config.target_max,
            "warning": "partial_results" if len(ranked) < config.target_min else "",
        },
    }


async def run_signal_driven_lead_intelligence(
    *,
    settings: Any | None = None,
    target_min: int | None = None,
    target_max: int | None = None,
    validation_mode: bool = False,
    strict_target_min: bool = False,
) -> dict[str, Any]:
    from config.settings import DiscoverySettings
    from discovery.source_aggregator import aggregate_companies_with_metrics
    from intelligence.lead_scorer import build_ranked_fallback_from_source_rows
    from intelligence.lead_scorer import score_and_rank_companies
    from intelligence.query_engine import build_expanded_queries
    from intelligence.query_engine import summarize_query_engine
    from intelligence.signal_enricher import enrich_companies_batch
    from utils.dedup import resolve_company_entities

    config = settings if isinstance(settings, DiscoverySettings) else DiscoverySettings()
    if isinstance(target_min, int) and target_min > 0:
        config.target_min = target_min
    if isinstance(target_max, int) and target_max > 0:
        config.target_max = target_max

    started = time.perf_counter()
    base_query_count = max(12, min(24, config.target_max * 2))
    if validation_mode:
        base_query_count = min(base_query_count, 8)
    per_query_limit = 6 if validation_mode else 12
    query_timeout_seconds = 10.0 if validation_mode else 20.0
    max_concurrent_queries = 2 if validation_mode else 4
    queries = build_expanded_queries(config.regions, config.industries, max_queries=base_query_count)
    query_plan = summarize_query_engine(queries)

    source_rows: list[dict[str, Any]] = []
    acquisition_metrics: dict[str, Any] = {}
    try:
        acquisition = await aggregate_companies_with_metrics(
            queries,
            per_query_limit=per_query_limit,
            target_min=config.target_min,
            query_timeout_seconds=query_timeout_seconds,
            max_concurrent_queries=max_concurrent_queries,
        )
        source_rows = acquisition.get("rows") if isinstance(acquisition.get("rows"), list) else []
        acquisition_metrics = acquisition.get("metrics") if isinstance(acquisition.get("metrics"), dict) else {}
    except Warning as exc:  # noqa: B028
        logger.warning("[SOURCE FAILED] acquisition reason=%s", exc)
        try:
            recovery = await aggregate_companies_with_metrics(
                queries,
                per_query_limit=max(3, per_query_limit - 2),
                target_min=config.target_min,
                query_timeout_seconds=max(float(query_timeout_seconds), 15.0),
                max_concurrent_queries=1,
            )
            source_rows = recovery.get("rows") if isinstance(recovery.get("rows"), list) else []
            acquisition_metrics = recovery.get("metrics") if isinstance(recovery.get("metrics"), dict) else {}
        except Warning as recovery_exc:  # noqa: B028
            logger.warning("[SOURCE FAILED] acquisition_recovery reason=%s", recovery_exc)
            source_rows = []
            acquisition_metrics = {
                "queries_executed": len(queries),
                "success_rate_per_source": {"serp": 0.0, "structured": 0.0},
                "failures_per_source": {"serp": len(queries), "structured": len(queries)},
                "source_rows": {"serp": 0, "structured": 0},
                "fallback_rows_added": 0,
                "total_candidates_generated": 0,
                "acquisition_success_rate": 0.0,
                "warning": "Pipeline unhealthy",
            }
    logger.info("[RESULT COUNT] acquisition: %s", len(source_rows))

    # If one source underperforms, run a wider query set to maintain candidate floor.
    if len(source_rows) < config.target_min:
        expanded_queries = build_expanded_queries(
            config.regions,
            config.industries,
            max_queries=min(12, base_query_count + 4) if validation_mode else max(base_query_count + 8, min(36, config.target_min * 3)),
        )
        try:
            expanded_acquisition = await aggregate_companies_with_metrics(
                expanded_queries,
                per_query_limit=per_query_limit,
                target_min=config.target_min,
                query_timeout_seconds=query_timeout_seconds,
                max_concurrent_queries=max_concurrent_queries,
            )
        except Warning as exc:  # noqa: B028
            logger.warning("[SOURCE FAILED] expanded_acquisition reason=%s", exc)
            expanded_acquisition = {"rows": [], "metrics": acquisition_metrics}
        expanded_rows = expanded_acquisition.get("rows") if isinstance(expanded_acquisition.get("rows"), list) else []
        if len(expanded_rows) > len(source_rows):
            source_rows = expanded_rows
            queries = expanded_queries
            query_plan = summarize_query_engine(queries)
            acquisition_metrics = expanded_acquisition.get("metrics") if isinstance(expanded_acquisition.get("metrics"), dict) else acquisition_metrics

    if strict_target_min and len(source_rows) < config.target_min:
        invariant_payload = {
            "message": "target_min invariant not met after acquisition fallback",
            "target_min": config.target_min,
            "source_rows": len(source_rows),
            "query_plan": query_plan,
            "acquisition": acquisition_metrics,
        }
        logger.error("strict_target_min violation | payload=%s", invariant_payload)
        raise RuntimeError(str(invariant_payload))

    entities = resolve_company_entities(source_rows)
    max_enrichment_candidates = max(config.target_max * 4, config.target_min * 3)
    entities = entities[: max_enrichment_candidates]
    logger.info("[RESULT COUNT] entities: %s", len(entities))

    # Always continue with available entities; no hard empty threshold gate.
    enriched = await enrich_companies_batch(
        entities,
        batch_size=max(1, config.batch_size),
        timeout_seconds=max(10, config.request_timeout_seconds),
    )
    logger.info("[RESULT COUNT] enriched: %s", len(enriched))

    ranked = score_and_rank_companies(
        enriched,
        target_min=max(1, int(config.target_min)),
        target_max=max(1, int(config.target_max)),
    )

    if ranked and len(ranked) < int(config.target_min):
        existing_domains = {str(item.get("domain") or "").strip().lower() for item in ranked if isinstance(item, dict)}
        supplemental = build_ranked_fallback_from_source_rows(
            source_rows,
            existing_domains=existing_domains,
            target_count=max(0, int(config.target_min) - len(ranked)),
            allowed_regions=config.regions,
        )
        if supplemental:
            ranked.extend(supplemental)

    if not ranked and source_rows:
        ranked = build_ranked_fallback_from_source_rows(
            source_rows,
            existing_domains=set(),
            target_count=max(1, int(config.target_min)),
            allowed_regions=config.regions,
        )

    if source_rows and len(ranked) < int(config.target_min):
        enterprise_labels = {
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
        seen_domains = {str(item.get("domain") or "").strip().lower() for item in ranked if isinstance(item, dict)}
        for row in source_rows:
            if len(ranked) >= int(config.target_min):
                break
            if not isinstance(row, dict):
                continue
            domain = str(row.get("domain") or "").strip().lower()
            company = str(row.get("company_name") or row.get("company") or "").strip()
            signal_type = str(row.get("signal_type") or "").strip().lower()
            region = str(row.get("region") or "").strip().lower()
            domain_label = domain.split(".")[0] if domain else ""
            if not domain or not company or domain in seen_domains:
                continue
            if domain_label in enterprise_labels:
                continue
            hiring = "hiring" in signal_type
            funding = "funding" in signal_type
            region_match = region in {str(value).strip().lower() for value in config.regions}
            ranked.append(
                {
                    "company": company,
                    "domain": domain,
                    "score": 45 + (10 if region_match else 0) + (10 if funding else 0) + (10 if hiring else 0),
                    "signals": {
                        "hiring": bool(hiring),
                        "funding": bool(funding),
                        "b2b": True,
                        "region_match": bool(region_match),
                    },
                    "reason": "Fallback ranked candidate from resilient acquisition",
                }
            )
            seen_domains.add(domain)

    if isinstance(config.target_max, int) and config.target_max > 0:
        ranked = ranked[: max(config.target_max, config.target_min)]

    logger.info("[RESULT COUNT] ranked: %s", len(ranked))

    elapsed = f"{(time.perf_counter() - started):.2f}s"
    health_warning = str(acquisition_metrics.get("warning") or "").strip()
    warning_value = health_warning or ("partial_results" if len(ranked) < config.target_min else "")
    return {
        "companies": ranked,
        "execution_metadata": {
            "total_time": elapsed,
            "query_plan": query_plan,
            "acquisition": acquisition_metrics,
            "source_rows": len(source_rows),
            "entities": len(entities),
            "enriched": len(enriched),
            "ranked": len(ranked),
            "dropoffs": {
                "source_to_entities": max(0, len(source_rows) - len(entities)),
                "entities_to_enriched": max(0, len(entities) - len(enriched)),
                "enriched_to_ranked": max(0, len(enriched) - len(ranked)),
            },
            "target_min": config.target_min,
            "target_max": config.target_max,
            "warning": warning_value,
        },
    }


def validate_and_score_companies(
    enriched_companies: list[dict[str, Any]],
    icp: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Apply strict validation and scoring to enriched companies.
    
    Workflow:
    1. Normalize company names (split merged entities, clean up)
    2. Validate company (reject non-companies, generic terms, large enterprises)
    3. Score company based on signals and ICP match
    4. Filter by minimum score threshold (40)
    
    Returns: (validated_scored_companies, validation_metrics)
    """
    from lead_engine import (
        split_and_normalize,
        is_valid_company,
        score_company,
        filter_by_score,
    )

    metrics = {
        "input_count": len(enriched_companies),
        "split_merged": 0,
        "invalid_rejected": 0,
        "low_score_rejected": 0,
        "output_count": 0,
    }

    # Step 1: Normalize and split merged entities
    normalized_companies: list[dict[str, Any]] = []
    for company_data in enriched_companies:
        company_name = str(company_data.get("company") or "").strip()
        if not company_name:
            continue

        # Try to split merged entities
        split_names = split_and_normalize(company_name)
        if len(split_names) > 1:
            metrics["split_merged"] += len(split_names) - 1

        # Create entries for each normalized name
        for norm in split_names:
            normalized_companies.append(
                {
                    **company_data,
                    "company": norm.name,
                    "original_name": norm.original,
                    "suffixes": norm.suffixes,
                }
            )

    # Step 2: Validate companies
    signals_list = icp.get("keywords", []) if isinstance(icp.get("keywords"), list) else []

    validated_companies: list[dict[str, Any]] = []
    for company_data in normalized_companies:
        company_name = str(company_data.get("company") or "").strip()
        signals = company_data.get("signal_data", {}).get("signals", [])

        # Check validity
        validation_result = is_valid_company(
            company_name,
            signals=signals,
            occurrence_count=company_data.get("occurrence_count", 1),
        )

        if not validation_result.is_valid:
            metrics["invalid_rejected"] += 1
            continue

        validated_companies.append(company_data)

    # Step 3: Score companies
    scored_companies: list[dict[str, Any]] = []
    for company_data in validated_companies:
        company_name = str(company_data.get("company") or "").strip()
        signals = company_data.get("signal_data", {}).get("signals", [])
        occurrence = company_data.get("occurrence_count", 1)

        score_result = score_company(
            company_name,
            signals=signals,
            occurrence_count=occurrence,
            icp_keywords=signals_list,
        )

        # Add scoring information
        company_data["score"] = score_result.score
        company_data["confidence"] = score_result.confidence
        company_data["score_reason"] = score_result.reason
        company_data["score_breakdown"] = score_result.breakdown

        # Estimate size category (simplified)
        company_lower = company_name.lower()
        if any(known in company_lower for known in ["google", "microsoft", "amazon", "apple"]):
            company_data["size_category"] = "enterprise"
        elif len(company_lower) < 20:
            company_data["size_category"] = "small"
        else:
            company_data["size_category"] = "mid"

        scored_companies.append(company_data)

    # Step 4: Filter by minimum score threshold (40)
    final_companies, filtered_out = filter_by_score(
        scored_companies,
        min_score=40,
        score_key="score",
    )

    metrics["low_score_rejected"] = len(filtered_out)
    metrics["output_count"] = len(final_companies)

    logger.info(
        "[VALIDATION] split_merged=%d invalid_rejected=%d low_score=%d final=%d",
        metrics["split_merged"],
        metrics["invalid_rejected"],
        metrics["low_score_rejected"],
        metrics["output_count"],
    )

    return final_companies, metrics


def run_full_pipeline(markdown_path: str) -> list[dict[str, Any]]:
    from pathlib import Path

    from config.settings import DiscoverySettings
    from config.settings import QueryConfig
    from discovery.source_aggregator import aggregate_companies_with_metrics
    from icp.extractor import extract_icp
    from query_engine.query_planner import generate_queries
    from intelligence.lead_scorer import build_ranked_fallback_from_source_rows
    from intelligence.lead_scorer import score_and_rank_companies
    from intelligence.signal_enricher import enrich_companies_batch
    from utils.dedup import resolve_company_entities

    markdown_file = Path(str(markdown_path or "")).expanduser()
    if not markdown_file.exists():
        logger.warning("[SOURCE FAILED] markdown reason=file_not_found path=%s", markdown_file)
        return []

    config = DiscoverySettings()

    markdown_text = markdown_file.read_text(encoding="utf-8")
    logger.info("[RESULT COUNT] step_markdown_chars: %s", len(markdown_text))

    icp = extract_icp(markdown_text)
    logger.info("[RESULT COUNT] step_icp_keywords: %s", len(icp.get("keywords") if isinstance(icp.get("keywords"), list) else []))

    query_plan = generate_queries(
        str(markdown_file),
        mode="hybrid",
        max_queries=96,
        config=QueryConfig(mode="hybrid", max_queries=96, use_llm=True, max_segments=10),
    )

    logger.info("[RESULT COUNT] step_queries: %s", len(query_plan))
    print("Queries:", [row.get("query") for row in query_plan[:40]])

    acquisition = asyncio.run(
        aggregate_companies_with_metrics(
            query_plan,
            per_query_limit=8,
            target_min=config.target_min,
            query_timeout_seconds=float(config.request_timeout_seconds),
            max_concurrent_queries=2,
        )
    )
    search_candidates = acquisition.get("rows") if isinstance(acquisition.get("rows"), list) else []
    logger.info("[RESULT COUNT] step_candidates: %s", len(search_candidates))
    print("After discovery:", len(search_candidates))
    if len(search_candidates) < 10:
        logger.warning("[SANITY WARNING] low_candidate_count=%s", len(search_candidates))
    if not search_candidates:
        return []

    source_entities = resolve_company_entities(search_candidates)
    logger.info("[RESULT COUNT] step_entities: %s", len(source_entities))
    print("After entity resolution:", len(source_entities))
    if len(source_entities) < 10:
        logger.warning("[SANITY WARNING] low_entity_count=%s", len(source_entities))

    enriched_companies = asyncio.run(
        enrich_companies_batch(
            source_entities,
            batch_size=max(1, int(config.batch_size)),
            timeout_seconds=max(10, int(config.request_timeout_seconds)),
        )
    )
    logger.info("[RESULT COUNT] step_enriched: %s", len(enriched_companies))
    print("After enrichment:", len(enriched_companies))

    ranked = score_and_rank_companies(
        enriched_companies,
        target_min=max(1, int(config.target_min)),
        target_max=max(1, int(config.target_max)),
    )

    if len(ranked) < 5:
        fallback_ranked = build_ranked_fallback_from_source_rows(
            source_entities,
            existing_domains={str(item.get("domain") or "").strip().lower() for item in ranked if isinstance(item, dict)},
            target_count=max(0, 5 - len(ranked)),
            allowed_regions=[str(region) for region in config.regions],
        )
        ranked.extend(fallback_ranked)

    logger.info("[RESULT COUNT] step_ranked: %s", len(ranked))
    print("After scoring/ranking:", len(ranked))

    return ranked
