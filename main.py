from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

from core.dedupe import dedupe_companies
from core.domain_resolver import extract_companies_from_directory_url
from core.domain_resolver import extract_domain
from core.domain_resolver import is_blocked_domain
from core.domain_resolver import is_directory_like_url
from core.domain_resolver import is_plausible_company_name
from core.domain_resolver import resolve_company_domain
from core.classifier import SOURCE_WEIGHTS
from core.classifier import classify_company
from core.classifier import enrich_company
from core.ingestion_orchestrator import orchestrate_ingestion
from core.orchestrator import collect_signals_for_companies
from discovery.query_generator import record_query_feedback
from discovery.reasoning_engine import build_reasoning_plan
from icp.extractor import extract_icp


logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

if callable(load_dotenv):
    load_dotenv()

# Test mode - set TEST_MODE=1 to use a limited low-credit run
TEST_MODE = (os.getenv("TEST_MODE") or "0").strip() == "1"
GLOBAL_TIME_LIMIT = int((os.getenv("GLOBAL_TIME_LIMIT") or "180").strip() or "180")
MAX_ENRICH = int((os.getenv("MAX_ENRICH") or "100").strip() or "100")
ENRICH_TIME_LIMIT = int((os.getenv("ENRICH_TIME_LIMIT") or "120").strip() or "120")
ENRICH_WORKERS = int((os.getenv("ENRICH_WORKERS") or "5").strip() or "5")

MEGA_CORP_DOMAINS = {
    "adobe.com",
    "microsoft.com",
    "oracle.com",
    "sap.com",
    "ibm.com",
    "reliance.com",
    "ril.com",
    "amex.com",
    "americanexpress.com",
    "amexglobalbusinesstravel.com",
    "accenture.com",
    "deloitte.com",
    "tcs.com",
    "infosys.com",
    "wipro.com",
}

MEGA_CORP_NAME_TERMS = {
    "microsoft",
    "adobe",
    "reliance",
    "american express",
    "oracle",
    "sap",
    "ibm",
    "accenture",
    "deloitte",
    "tata consultancy",
    "infosys",
    "wipro",
}

JOB_TITLE_TOKENS = {
    "account executive",
    "implementation lead",
    "software architect",
    "consultant",
    "engineer",
    "manager",
    "job",
    "jobs",
    "vacancies",
    "apply now",
    "hiring",
}

GENERIC_NAME_TOKENS = {
    "info",
    "summit",
    "pricing",
    "monetization",
    "apply now",
}

LIST_PAGE_TOKENS = {
    "list of",
    "top ",
    "best ",
    "directory",
    "companies in",
    "startups in",
    "hub profile",
    "sector in",
    "market trends",
}

ICP_REJECT_DOMAIN_TOKENS = {
    "ventures",
    "capital",
    "vc",
    "jobs",
    "careers",
    "funding",
    "events",
    "conference",
    "glassdoor",
    "indeed",
    "reddit",
    "tracxn",
    "crunchbase",
}

ICP_REJECT_CONTEXT_TOKENS = {
    "jobs",
    "hiring platform",
    "startup directory",
    "conference",
    "event",
    "funding portal",
    "media",
    "blog",
}

ICP_ACCEPT_TOKENS = {
    "saas",
    "b2b",
    "software",
    "platform",
    "api",
    "subscription",
    "pricing",
    "product",
    "service",
}


def main() -> None:
    """
    Complete end-to-end lead discovery pipeline with enrichment:
    1. Extract ICP from markdown
    2. Generate high-intent queries
    3. Run ingestion layer (20+ sources: structured, semi-structured, unstructured)
    4. Save to ingested_leads.json (intermediate checkpoint)
    5. Run validation & filtering pipeline (name quality, domain blocking, directory expansion)
    6. Deduplicate by domain
    7. Run ENRICHMENT ORCHESTRATOR (10 signal agents):
       - LinkedIn Agent: hiring signals, growth indicators
       - GitHub Agent: code activity, project signals
       - Jobs Agent: job posting activity, hiring pace
       - ProductHunt Agent: product launch signals
       - Review Agent: product quality signals
       - Website Agent: tech stack, traffic signals
       - Traffic Agent: popularity signals
       - Docs Agent: documentation quality
       - Content Agent: content marketing signals
       - TechStack Agent: technology adoption signals
    8. Re-rank by multi-signal confidence score
    9. Output final enriched leads.json (sorted by confidence)
    """
    
    started_at = time.time()

    # Step 1: Extract ICP
    markdown_path = os.getenv("ICP_MARKDOWN_PATH", "sample_company.md")
    text = Path(markdown_path).read_text(encoding="utf-8") if Path(markdown_path).exists() else ""
    reasoning_plan = build_reasoning_plan(text, {}) if text else {}
    icp = _reasoning_plan_to_icp(reasoning_plan) if reasoning_plan else (extract_icp(text) if text else {})
    
    sample_company = Path(markdown_path).stem if Path(markdown_path).exists() else "sample"
    logger.info(f"Extracted ICP: {icp}")
    
    if TEST_MODE:
        logger.info("🧪 TEST MODE ENABLED: Using only 3 sources (Google Maps, Product Hunt, Jobs) with 5 rows each")
        logger.info("💾 Signal enrichment skipped to save API credits")
    
    # Step 2: Generate queries from reasoning plan (single-call reasoning output)
    logger.info("Generating queries...")
    try:
        queries = _queries_from_reasoning_plan(reasoning_plan)
        if not queries:
            queries = _fallback_queries_from_icp(icp)
        if not queries:
            queries = ["saas subscription billing companies"]
        logger.info(f"Generated {len(queries)} queries")
    except Exception as e:
        logger.warning(f"Query generation failed: {e}, using default")
        queries = ["saas subscription billing companies"]
    
    # Step 3: Run ingestion orchestration (NEW)
    logger.info("Running ingestion orchestration...")
    ingested_rows = orchestrate_ingestion(queries, sample_company)
    if _deadline_exceeded(started_at):
        _write_output([])
        return
    
    if not ingested_rows:
        logger.warning("No rows ingested from any source!")
        output_path = Path("output/leads.json")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("[]", encoding="utf-8")
        print("[]")
        return
    
    logger.info(f"Total ingested rows: {len(ingested_rows)}")

    # Domain-level consolidation layer (after ingestion, before filtering).
    merged_ingested_rows = _merge_entities_by_domain(ingested_rows)
    logger.info(f"After domain merge: {len(merged_ingested_rows)}")
    
    # Step 4: Save ingested_leads.json (for inspection)
    ingested_path = Path("output/ingested_leads.json")
    ingested_path.parent.mkdir(parents=True, exist_ok=True)
    ingested_path.write_text(json.dumps(merged_ingested_rows, indent=2, ensure_ascii=True), encoding="utf-8")
    logger.info(f"Saved ingested leads to {ingested_path}")
    
    # Step 5: Run EXISTING validation/filtering pipeline (UNCHANGED)
    logger.info("Running validation and filtering pipeline...")
    
    # Convert ingested rows to format expected by existing pipeline
    normalized = _convert_ingested_to_normalized(merged_ingested_rows)
    
    if not normalized:
        logger.warning("No rows after normalization!")
        output_path = Path("output/leads.json")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("[]", encoding="utf-8")
        print("[]")
        return
    
    # Apply existing validation logic
    resolved: list[dict[str, Any]] = []
    for row in normalized:
        name = str(row.get("name") or "").strip()
        raw_url = str(row.get("raw_url") or "").strip()
        domain = str(row.get("domain") or "").strip().lower() or None
        context = str(row.get("context") or row.get("raw_fields", {}).get("snippet") or row.get("raw_fields", {}).get("description") or row.get("raw_fields", {}).get("tagline") or "").strip()
        source_type = str(row.get("source_type") or "").strip().lower()
        page_domain = extract_domain(raw_url or "") or ""
        list_or_blog_page = _is_list_or_blog_page(name, context, raw_url, source_type)

        if not is_plausible_company_name(name):
            continue
        if not _is_relevant_company(name, context, domain or "", row, icp):
            continue

        # If this is a list/blog page, keep listed external companies only.
        if list_or_blog_page:
            expanded = extract_companies_from_directory_url(raw_url, max_results=30) if raw_url else []
            for item in expanded:
                candidate_name = str(item.get("name") or "").strip()
                candidate_domain = str(item.get("domain") or "").strip().lower()
                if not candidate_name or not is_plausible_company_name(candidate_name):
                    continue
                if not candidate_domain or is_blocked_domain(candidate_domain):
                    continue
                if page_domain and (candidate_domain == page_domain or candidate_domain.endswith(f".{page_domain}")):
                    continue
                if not _is_relevant_company(candidate_name, context, candidate_domain, row, icp):
                    continue
                resolved.append(
                    {
                        "name": candidate_name,
                        "domain": candidate_domain,
                        "source": str(row.get("source") or ""),
                        "source_type": str(row.get("source_type") or ""),
                        "confidence": float(row.get("confidence") or 0.5),
                        "confidence_reasons": row.get("confidence_reasons", []),
                        "is_icp_match": bool(row.get("is_icp_match") if row.get("is_icp_match") is not None else True),
                        "entity_category": str(row.get("entity_category") or "other"),
                    }
                )
            continue

        # For non-list pages, still block candidates that are clearly page-shell domains.
        if domain and page_domain and (domain == page_domain or domain.endswith(f".{page_domain}")) and source_type in {"listicle", "directory"}:
            continue

        if not domain:
            domain = resolve_company_domain(name)
        if not domain:
            continue
        if is_blocked_domain(domain):
            continue
        resolved.append(
            {
                "name": name,
                "domain": domain,
                "source": str(row.get("source") or ""),
                "source_type": str(row.get("source_type") or ""),
                "context": context,
                "raw_fields": row.get("raw_fields") or {},
                "confidence": float(row.get("confidence") or 0.5),
                "confidence_reasons": row.get("confidence_reasons", []),
                "is_icp_match": bool(row.get("is_icp_match") if row.get("is_icp_match") is not None else True),
                "entity_category": str(row.get("entity_category") or "other"),
            }
        )

    logger.info(f"Resolved rows: {len(resolved)}")
    
    # Dedup
    deduped = dedupe_companies(resolved)
    logger.info(f"After dedup: {len(deduped)}")

    # Step 6: PRE-FILTER + CHEAP SCORING + TOP-K
    pre_filtered = [row for row in deduped if _pre_filter_candidate(row)]
    for row in pre_filtered:
        row["score"] = _cheap_score_candidate(row)

    shortlisted = sorted(pre_filtered, key=lambda x: float(x.get("score") or 0.0), reverse=True)[:MAX_ENRICH]
    logger.info(f"Shortlisted for enrichment: {len(shortlisted)} (max={MAX_ENRICH})")

    if _deadline_exceeded(started_at):
        _write_output([])
        return

    # Step 7: ENRICHMENT (PARALLEL + TIME-BOUND) + ALWAYS-ON signal agents
    enrich_started = time.time()
    signal_map: dict[str, dict[str, Any]] = {}
    company_names = [str(item.get("name") or "").strip() for item in shortlisted if str(item.get("name") or "").strip()]
    if company_names:
        try:
            signal_results = asyncio.run(collect_signals_for_companies(company_names))
            for company_result in signal_results.get("companies", []):
                if not isinstance(company_result, dict):
                    continue
                company_name = str(company_result.get("company") or "").strip().lower()
                if company_name:
                    signal_map[company_name] = company_result
        except Exception as exc:
            logger.warning(f"Signal agent layer failed: {exc}")

    enriched_candidates = _bounded_parallel_enrich(shortlisted, start=enrich_started)

    # Step 8: ROLE CLASSIFICATION + FINAL FILTER
    enriched_leads: list[dict[str, Any]] = []
    for lead in enriched_candidates:
        if _deadline_exceeded(started_at):
            break

        context = str(lead.get("context") or "").strip()
        signal_payload = signal_map.get(str(lead.get("name") or "").strip().lower(), {})
        lead_signals = signal_payload.get("signals") if isinstance(signal_payload, dict) else []
        lead_high_intent = signal_payload.get("high_intent_signals") if isinstance(signal_payload, dict) else []
        lead_signal_count = int((signal_payload.get("signal_count") if isinstance(signal_payload, dict) else 0) or lead.get("signal_count") or 0)
        lead_intent_score = float((signal_payload.get("intent_score") if isinstance(signal_payload, dict) else 0.0) or lead.get("intent_score") or 0.0)
        lead_signals_found = signal_payload.get("signals_found") if isinstance(signal_payload, dict) else []
        lead_signal_strength = str((signal_payload.get("signal_strength") if isinstance(signal_payload, dict) else "") or lead.get("signal_strength") or "unknown").strip().lower() or "unknown"
        lead_ingestion_score = float(lead.get("ingestion_score") or 0.0)
        lead_agent_debug = signal_payload.get("agent_debug") if isinstance(signal_payload, dict) else {}

        role = classify_company(str(lead.get("name") or ""), context)
        if role == "infra":
            continue
        if not _final_name_ok(str(lead.get("name") or ""), str(lead.get("domain") or ""), context):
            continue

        transport = str((lead.get("raw_fields") or {}).get("source_transport") or "apify").strip().lower()
        source_weight = SOURCE_WEIGHTS.get(transport, 0.5)

        base_score = float(lead.get("confidence") or 0.5)
        cheap_boost = min(0.25, float(lead.get("score") or 0.0) * 0.05)
        final_score = min(1.0, (base_score * source_weight) + cheap_boost)

        icp_score = _score_icp_relevance(
            str(lead.get("name") or ""),
            str(lead.get("domain") or ""),
            context,
            str(lead.get("entity_category") or "other"),
            lead_signals if isinstance(lead_signals, list) else [],
            lead_high_intent if isinstance(lead_high_intent, list) else [],
        )

        lead_signal_names = sorted(
            {
                str(sig.get("signal_type") or sig.get("type") or sig.get("bucket") or "").strip()
                for sig in (lead_signals if isinstance(lead_signals, list) else [])
                if isinstance(sig, dict) and str(sig.get("signal_type") or sig.get("type") or sig.get("bucket") or "").strip()
            }
        )

        has_strong_signals = bool(lead_high_intent) or any(
            str(sig.get("bucket") or "") in {"high", "strong"}
            for sig in (lead_signals if isinstance(lead_signals, list) else [])
            if isinstance(sig, dict)
        )

        # Graceful scoring fallback: when signals are weak/missing, use ingestion prior as base intent proxy.
        base_intent_from_ingestion = min(3.0, max(0.0, lead_ingestion_score))
        if (not has_strong_signals) and (lead_intent_score <= 0.0 or lead_signal_strength == "unknown"):
            lead_intent_score = max(lead_intent_score, base_intent_from_ingestion)

        low_confidence_lead = False
        if has_strong_signals:
            if lead_intent_score < 2.0 or icp_score < 0.5:
                logger.info(
                    "drop lead | company=%s reason=strong_signal_threshold signals_found=%s high_intent_signals=%s intent_score=%s icp_score=%s",
                    lead.get("name"),
                    lead_signals_found,
                    [sig.get("signal_type") for sig in lead_high_intent if isinstance(sig, dict)],
                    lead_intent_score,
                    icp_score,
                )
                continue
        else:
            low_confidence_lead = True
            if lead_intent_score < 1.2 or icp_score < 0.3:
                logger.info(
                    "drop lead | company=%s reason=weak_signal_threshold signals_found=%s high_intent_signals=%s intent_score=%s icp_score=%s",
                    lead.get("name"),
                    lead_signals_found,
                    [sig.get("signal_type") for sig in lead_high_intent if isinstance(sig, dict)],
                    lead_intent_score,
                    icp_score,
                )
                continue

        if lead_signal_count <= 0 and lead_signal_strength == "unknown":
            low_confidence_lead = True

        confidence_label = "high" if final_score >= 0.75 else ("medium" if final_score >= 0.5 else "low")
        reasoning: list[str] = []
        if str(lead.get("entity_category") or "") in {"saas", "product", "marketplace"}:
            reasoning.append("Matched ICP industry")
        if any(token in str(context).lower() for token in ["hiring", "funding", "pricing", "subscription"]):
            reasoning.append("Appeared in high-intent query")
        if low_confidence_lead:
            reasoning.append("Signal incomplete but inferred intent")
        if not reasoning:
            reasoning.append("Passed adaptive threshold with available evidence")

        enriched_company = enrich_company(
            {
                "name": lead.get("name"),
                "domain": lead.get("domain"),
                "source": lead.get("source"),
                "source_type": lead.get("source_type"),
                "context": context,
                "role": role,
                "is_icp_match": bool(lead.get("is_icp_match") if lead.get("is_icp_match") is not None else True),
                "entity_category": str(lead.get("entity_category") or "other"),
                "icp_score": icp_score,
                "confidence": final_score,
                "final_score": final_score,
                "confidence_reasons": lead.get("confidence_reasons", []),
                "signals": lead_signals if isinstance(lead_signals, list) else [],
                "high_intent_signals": lead_high_intent if isinstance(lead_high_intent, list) else [],
                "signal_count": lead_signal_count,
                "intent_score": lead_intent_score,
                "ingestion_score": lead_ingestion_score,
                "signals_found": lead_signals_found if isinstance(lead_signals_found, list) else [],
                "signal_strength": lead_signal_strength,
                "low_confidence_lead": low_confidence_lead,
                "confidence_label": confidence_label,
                "reasoning": reasoning,
                "agent_metadata": lead_agent_debug if isinstance(lead_agent_debug, dict) else {},
                "score": float(lead.get("score") or 0.0),
            }
        )

        enriched_company["confidence"] = final_score
        enriched_company["final_score"] = final_score
        enriched_company["role"] = role
        enriched_company["source_weight"] = source_weight
        enriched_company["intent_score"] = lead_intent_score
        enriched_company["icp_score"] = icp_score
        enriched_company["ingestion_score"] = lead_ingestion_score
        enriched_company["signal_strength"] = lead_signal_strength
        enriched_company["low_confidence_lead"] = low_confidence_lead
        enriched_company["confidence_label"] = confidence_label
        enriched_company["reasoning"] = reasoning
        enriched_company["signals_found"] = lead_signals_found if isinstance(lead_signals_found, list) else []
        enriched_company["signal_types"] = lead_signal_names
        enriched_leads.append(enriched_company)

    # Graceful degradation: preserve recall by emitting low-confidence leads when strict gating yields none.
    if not enriched_leads:
        for candidate in shortlisted[:5]:
            candidate_name = str(candidate.get("name") or "").strip()
            candidate_domain = str(candidate.get("domain") or "").strip()
            candidate_context = str(candidate.get("context") or "").strip()
            if not candidate_name or not _final_name_ok(candidate_name, candidate_domain, candidate_context):
                continue

            fallback_ingestion_score = float(candidate.get("ingestion_score") or 0.0)
            fallback_intent_score = max(1.2, min(3.0, fallback_ingestion_score if fallback_ingestion_score > 0 else 1.2))
            fallback_role = classify_company(candidate_name, candidate_context)
            if fallback_role == "infra":
                continue
            fallback_icp = _score_icp_relevance(
                candidate_name,
                candidate_domain,
                candidate_context,
                str(candidate.get("entity_category") or "other"),
                [],
                [],
            )
            if fallback_icp < 0.3:
                continue

            fallback_company = enrich_company(
                {
                    "name": candidate_name,
                    "domain": candidate_domain,
                    "source": candidate.get("source"),
                    "source_type": candidate.get("source_type"),
                    "context": candidate_context,
                    "role": fallback_role,
                    "confidence": float(candidate.get("confidence") or 0.35),
                    "final_score": float(candidate.get("confidence") or 0.35),
                    "intent_score": fallback_intent_score,
                    "icp_score": fallback_icp,
                    "ingestion_score": fallback_ingestion_score,
                    "signal_strength": "unknown",
                    "signals": [],
                    "high_intent_signals": [],
                    "signal_count": 0,
                    "low_confidence_lead": True,
                    "confidence_label": "low",
                    "reasoning": [
                        "Matched ICP industry",
                        "Signal incomplete but inferred intent",
                    ],
                    "signals_found": [],
                }
            )
            fallback_company["confidence"] = float(candidate.get("confidence") or 0.35)
            fallback_company["final_score"] = float(candidate.get("confidence") or 0.35)
            fallback_company["intent_score"] = fallback_intent_score
            fallback_company["icp_score"] = fallback_icp
            fallback_company["ingestion_score"] = fallback_ingestion_score
            fallback_company["signal_strength"] = "unknown"
            fallback_company["low_confidence_lead"] = True
            fallback_company["confidence_label"] = "low"
            fallback_company["reasoning"] = ["Matched ICP industry", "Signal incomplete but inferred intent"]
            fallback_company["signals_found"] = []
            enriched_leads.append(fallback_company)
    
    # Sort by final score
    enriched_leads.sort(key=lambda x: float(x.get("confidence", 0)), reverse=True)
    
    logger.info(f"Final enriched leads: {len(enriched_leads)}")

    try:
        accepted_for_feedback = [
            row
            for row in enriched_leads
            if isinstance(row, dict)
            and not bool(row.get("low_confidence_lead"))
            and str(row.get("confidence_label") or "").lower() in {"high", "medium"}
        ]
        if accepted_for_feedback:
            record_query_feedback(queries, accepted_for_feedback)
    except Exception as exc:
        logger.warning(f"Failed to store query feedback: {exc}")

    # Step 9: Output final enriched leads.json
    _write_output(enriched_leads)


def _convert_ingested_to_normalized(ingested_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Convert ingested rows (with source_type, confidence_reasons, raw_fields)
    to format compatible with existing pipeline.
    """
    normalized = []
    
    for row in ingested_rows:
        if not isinstance(row, dict):
            continue
        
        # Already normalized, just ensure required fields
        normalized.append({
            "name": str(row.get("name") or "").strip(),
            "domain": row.get("domain"),
            "description": row.get("description") or row.get("context") or "",
            "source": str(row.get("source") or "").strip(),
            "sources": row.get("sources") or [],
            "source_count": int(row.get("source_count") or 0),
            "source_type": str(row.get("source_type") or "semi_structured"),
                "context": row.get("context") or row.get("raw_fields", {}).get("snippet") or row.get("raw_fields", {}).get("description") or row.get("raw_fields", {}).get("tagline") or "",
            "confidence": float(row.get("confidence") or 0.5),
            "ingestion_score": float(row.get("ingestion_score") or 0.0),
            "confidence_reasons": row.get("confidence_reasons") or [],
            "entity_type": str(row.get("entity_type") or "REAL_COMPANY"),
            "is_container": bool(row.get("is_container") or False),
            "is_icp_match": bool(row.get("is_icp_match") if row.get("is_icp_match") is not None else True),
            "entity_category": str(row.get("entity_category") or "other"),
            "signals": row.get("signals") or [],
            "high_intent_signals": row.get("high_intent_signals") or [],
            "signal_count": int(row.get("signal_count") or 0),
            "raw_url": row.get("raw_url"),
            "raw_fields": row.get("raw_fields") or {}
        })
    
    return normalized


def _merge_entities_by_domain(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}

    for row in rows:
        if not isinstance(row, dict):
            continue
        domain = str(row.get("domain") or "").strip().lower()
        if not domain:
            raw_url = str(row.get("raw_url") or row.get("source_url") or (row.get("raw_fields") or {}).get("url") or (row.get("raw_fields") or {}).get("link") or (row.get("raw_fields") or {}).get("website") or "").strip()
            domain = str(extract_domain(raw_url) or "").strip().lower()
        if not domain:
            continue
        grouped.setdefault(domain, []).append(row)

    merged: list[dict[str, Any]] = []

    for domain, items in grouped.items():
        name_counter = Counter()
        source_types = Counter()
        contexts: list[str] = []
        sources: set[str] = set()
        signals: set[str] = set()
        hi_signals: set[str] = set()
        confidence_reasons: set[str] = set()
        signal_count_sum = 0
        ingestion_score_max = 0.0
        best_conf = 0.0
        first_valid_name = ""
        first_slug = ""
        first_raw_fields: dict[str, Any] = {}
        first_raw_url = ""

        for item in items:
            name = str(item.get("name") or "").strip()
            if name:
                if not first_valid_name and _final_name_ok(name, domain, str(item.get("context") or "")):
                    first_valid_name = name
                name_counter[name] += 1

            src = str(item.get("source") or "").strip()
            raw_src = str((item.get("raw_fields") or {}).get("source") or "").strip()
            # Prefer source evidence from raw_fields (single-source provenance),
            # and only split pipe-delimited values when no raw source is present.
            source_tokens: list[str] = []
            if raw_src:
                source_tokens = [raw_src]
            elif src:
                source_tokens = [part.strip() for part in src.split("|") if part.strip()]

            for token in source_tokens:
                sources.add(token)

            source_type = str(item.get("source_type") or "").strip()
            if source_type:
                source_types[source_type] += 1

            context = str(item.get("context") or "").strip()
            if context:
                contexts.append(context)

            for sig in item.get("signals") or []:
                if str(sig).strip():
                    signals.add(str(sig).strip())
            for sig in item.get("high_intent_signals") or []:
                if str(sig).strip():
                    hi_signals.add(str(sig).strip())

            signal_count_sum += int(item.get("signal_count") or 0)
            ingestion_score_max = max(ingestion_score_max, float(item.get("ingestion_score") or 0.0))
            best_conf = max(best_conf, float(item.get("confidence") or 0.0))

            for reason in item.get("confidence_reasons") or []:
                reason_text = str(reason).strip()
                if reason_text:
                    confidence_reasons.add(reason_text)

            if not first_slug:
                first_slug = str(item.get("slug") or "").strip()
            if not first_raw_fields and isinstance(item.get("raw_fields"), dict):
                first_raw_fields = item.get("raw_fields") or {}
            if not first_raw_url:
                first_raw_url = str(item.get("raw_url") or item.get("source_url") or "").strip()

        chosen_name = first_valid_name
        if not chosen_name and name_counter:
            chosen_name = name_counter.most_common(1)[0][0]

        if not chosen_name:
            continue

        source_list = sorted(sources)
        source_count = len(source_list)
        merged_conf = min(1.0, best_conf + (0.05 * max(0, source_count - 1)))
        entity_category, is_icp_match = _classify_icp_relevance(chosen_name, domain, max(contexts, key=len) if contexts else "")

        if source_count > 1:
            confidence_reasons.add("multi-source aggregation boost")
        if not confidence_reasons:
            confidence_reasons.add("merged by domain")

        merged.append(
            {
                "name": chosen_name,
                "domain": domain,
                "slug": first_slug or domain.split(".")[0],
                "source": "|".join(source_list),
                "sources": source_list,
                "source_count": source_count,
                "source_type": source_types.most_common(1)[0][0] if source_types else "article",
                "context": max(contexts, key=len) if contexts else "",
                "signals": sorted(signals),
                "high_intent_signals": sorted(hi_signals),
                "signal_count": signal_count_sum,
                "ingestion_score": ingestion_score_max,
                "confidence": merged_conf,
                "confidence_reasons": sorted(confidence_reasons),
                "is_icp_match": is_icp_match,
                "entity_category": entity_category,
                "entity_type": str(items[0].get("entity_type") or "REAL_COMPANY"),
                "is_container": bool(items[0].get("is_container") or False),
                "raw_url": first_raw_url or None,
                "raw_fields": first_raw_fields,
            }
        )

    return merged


def _pre_filter_candidate(candidate: dict[str, Any]) -> bool:
    name = str(candidate.get("name") or "").strip().lower()
    ctx = str(candidate.get("context") or "").strip().lower()
    domain = str(candidate.get("domain") or "").strip().lower()

    if len(name) < 3:
        return False
    if any(token in name for token in ["top", "best", "list", "directory"]):
        return False
    if any(token in ctx for token in ["payment platform", "billing platform", "merchant of record", "directory", "list of companies"]):
        return False
    if any(token in name for token in JOB_TITLE_TOKENS):
        return False
    if any(token in ctx for token in ["job vacancies", "linkedin jobs", "indeed", "glassdoor", "cutshort", "naukri"]):
        return False
    if any(token in name for token in MEGA_CORP_NAME_TERMS):
        return False
    if any(domain == blocked or domain.endswith(f".{blocked}") for blocked in MEGA_CORP_DOMAINS):
        return False
    if candidate.get("is_icp_match") is False:
        return False
    return True


def _cheap_score_candidate(candidate: dict[str, Any]) -> int:
    score = 0
    ctx = str(candidate.get("context") or "").lower()

    if "saas" in ctx:
        score += 2
    if "pricing" in ctx or "subscription" in ctx:
        score += 2
    if str(candidate.get("source_type") or "").lower() == "structured":
        score += 3
    return score


def _bounded_parallel_enrich(shortlisted: list[dict[str, Any]], *, start: float) -> list[dict[str, Any]]:
    if not shortlisted:
        return []

    enriched: list[dict[str, Any]] = []

    def _safe_enrich(candidate: dict[str, Any]) -> dict[str, Any]:
        try:
            enriched_candidate = dict(candidate)
            enriched_candidate["signals"] = []
            enriched_candidate["high_intent_signals"] = []
            enriched_candidate["signal_count"] = 0
            enriched_candidate["agent_metadata"] = {}
            return enrich_company(enriched_candidate)
        except Exception:
            return candidate

    with ThreadPoolExecutor(max_workers=max(1, ENRICH_WORKERS)) as executor:
        futures = [executor.submit(_safe_enrich, candidate) for candidate in shortlisted]
        for future in as_completed(futures, timeout=max(10, ENRICH_TIME_LIMIT)):
            if time.time() - start > ENRICH_TIME_LIMIT:
                break
            try:
                enriched.append(future.result(timeout=10))
            except Exception:
                continue

    return enriched


def _final_name_ok(name: str, domain: str = "", context: str = "") -> bool:
    cleaned = str(name or "").strip()
    if not cleaned:
        return False
    if len(cleaned) < 3 or len(cleaned) > 80:
        return False
    lowered = cleaned.lower()
    if lowered in GENERIC_NAME_TOKENS:
        return False
    if len(cleaned.split()) == 1 and lowered in {"info", "summit", "pricing", "monetization", "support", "hire"}:
        return False
    if any(token in lowered for token in JOB_TITLE_TOKENS):
        return False

    host = str(domain or "").strip().lower()
    if any(host == blocked or host.endswith(f".{blocked}") for blocked in MEGA_CORP_DOMAINS):
        return False

    ctx = str(context or "").lower()
    if any(token in ctx for token in ["job vacancies", "linkedin jobs", "indeed", "glassdoor", "cutshort", "naukri"]):
        return False
    return True


def _deadline_exceeded(started_at: float) -> bool:
    if (time.time() - started_at) <= GLOBAL_TIME_LIMIT:
        return False
    logger.warning(f"GLOBAL_TIME_LIMIT reached ({GLOBAL_TIME_LIMIT}s). Exiting gracefully.")
    return True


def _write_output(rows: list[dict[str, Any]]) -> None:
    output_path = Path("output/leads.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(rows, indent=2, ensure_ascii=True), encoding="utf-8")
    print(json.dumps(rows, indent=2, ensure_ascii=True))


def _is_relevant_company(name: str, context: str, domain: str, row: dict[str, Any], icp: dict[str, Any]) -> bool:
    row_text = " ".join(
        [
            str(name or ""),
            str(context or ""),
            str(domain or ""),
            str(row.get("source") or ""),
            str(row.get("source_type") or ""),
        ]
    ).lower()

    icp_terms = [
        str(icp.get("product_type") or ""),
        str(icp.get("industry") or ""),
        str(icp.get("who_needs_this") or ""),
        str(icp.get("target_customers") or ""),
        " ".join(str(item) for item in icp.get("keywords") or []),
        "billing",
        "subscription",
        "revenue",
        "recurring",
        "pricing",
        "payments",
        "fintech",
        "monetization",
        "invoice",
        "usage based",
        "api",
        "saas",
        "chargebee",
        "recurly",
        "paddle",
    ]

    terms = [term.lower() for term in icp_terms if str(term).strip()]
    return any(term in row_text for term in terms)


def _is_list_or_blog_page(name: str, context: str, raw_url: str, source_type: str) -> bool:
    lowered = " ".join([str(name or ""), str(context or ""), str(raw_url or ""), str(source_type or "")]).lower()
    if source_type in {"listicle", "directory"}:
        return True
    if raw_url and is_directory_like_url(raw_url):
        return True
    if any(token in lowered for token in LIST_PAGE_TOKENS):
        return True
    if "blog" in lowered and ("companies" in lowered or "startups" in lowered):
        return True
    return False


def _reasoning_plan_to_icp(plan: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(plan, dict):
        return {}
    icp_struct = plan.get("icp_struct") if isinstance(plan.get("icp_struct"), dict) else {}
    industries = icp_struct.get("industries") if isinstance(icp_struct.get("industries"), list) else []
    geo = icp_struct.get("geo") if isinstance(icp_struct.get("geo"), list) else []
    keywords = icp_struct.get("keywords") if isinstance(icp_struct.get("keywords"), list) else []
    pain = icp_struct.get("pain_signals") if isinstance(icp_struct.get("pain_signals"), list) else []
    company_types = icp_struct.get("company_types") if isinstance(icp_struct.get("company_types"), list) else []
    exclude = icp_struct.get("exclude") if isinstance(icp_struct.get("exclude"), list) else []
    return {
        "industry": str(industries[0]) if industries else "",
        "industries": [str(item) for item in industries],
        "geography": ", ".join(str(item) for item in geo),
        "regions": [str(item) for item in geo],
        "keywords": [str(item) for item in keywords],
        "pain_points": [str(item) for item in pain],
        "who_needs_this": ", ".join(str(item) for item in company_types),
        "who_should_be_excluded": ", ".join(str(item) for item in exclude),
    }


def _queries_from_reasoning_plan(plan: dict[str, Any]) -> list[str]:
    if not isinstance(plan, dict):
        return []
    strategies = plan.get("search_strategies") if isinstance(plan.get("search_strategies"), list) else []
    queries: list[str] = []
    seen: set[str] = set()
    for item in strategies:
        if not isinstance(item, dict):
            continue
        raw = item.get("queries") if isinstance(item.get("queries"), list) else []
        for query in raw:
            clean = " ".join(str(query or "").split()).strip()
            if not clean:
                continue
            key = clean.lower()
            if key in seen:
                continue
            seen.add(key)
            queries.append(clean)
    return queries[:24]


def _fallback_queries_from_icp(icp: dict[str, Any]) -> list[str]:
    if not isinstance(icp, dict):
        return []
    industries = icp.get("industries") if isinstance(icp.get("industries"), list) else []
    keywords = icp.get("keywords") if isinstance(icp.get("keywords"), list) else []
    regions = icp.get("regions") if isinstance(icp.get("regions"), list) else []
    industry = str(industries[0]) if industries else "saas"
    keyword = str(keywords[0]) if keywords else "subscription billing"
    region = str(regions[0]) if regions else "Global"
    return [
        f"{region} {industry} companies {keyword}",
        f"{region} {industry} startups usage based pricing",
        f"{region} companies hiring billing engineers",
        f"{region} companies using stripe billing",
        f"{region} recurring revenue saas companies",
    ]


def _classify_icp_relevance(name: str, domain: str, context: str) -> tuple[str, bool]:
    lowered_domain = str(domain or "").lower()
    lowered_name = str(name or "").lower()
    lowered_ctx = str(context or "").lower()
    combined = " ".join([lowered_name, lowered_domain, lowered_ctx])

    if any(token in lowered_domain for token in ["venture", "ventures", "capital", "vc"]):
        return "vc", False
    if any(token in combined for token in ["job board", "jobs", "careers", "hiring platform"]):
        return "job_board", False
    if any(token in combined for token in ["blog", "media", "news", "startup directory", "directory"]):
        return "media", False
    if any(token in combined for token in ["funding portal", "grants", "conference", "event"]):
        return "other", False

    if any(token in lowered_domain for token in ICP_REJECT_DOMAIN_TOKENS):
        return "other", False
    if any(token in lowered_ctx for token in ICP_REJECT_CONTEXT_TOKENS):
        return "other", False

    if any(token in combined for token in ICP_ACCEPT_TOKENS):
        return "saas", True

    return "other", False


def _score_icp_relevance(
    name: str,
    domain: str,
    context: str,
    entity_category: str,
    signals: list[dict[str, Any]],
    high_intent_signals: list[dict[str, Any]],
) -> float:
    lowered_name = str(name or "").lower()
    lowered_domain = str(domain or "").lower()
    lowered_context = str(context or "").lower()
    combined = " ".join([lowered_name, lowered_domain, lowered_context])

    score = 0.15

    if entity_category == "saas":
        score += 0.45
    elif entity_category in {"marketplace", "product"}:
        score += 0.3
    elif entity_category in {"vc", "job_board", "media", "other"}:
        score -= 0.15

    positive_terms = [
        "platform",
        "software",
        "api",
        "automation",
        "dashboard",
        "subscription",
        "workflow",
        "integrations",
        "saas",
    ]
    if any(term in combined for term in positive_terms):
        score += 0.2

    if any(token in combined for token in ["blog", "newsletter", "news", "directory", "jobs", "careers", "venture capital", "vc"]):
        score -= 0.35

    if any(signal.get("bucket") == "high" for signal in high_intent_signals if isinstance(signal, dict)):
        score += 0.15
    if len(signals) > 1:
        score += 0.1

    if any(term in lowered_domain for term in ["jobs", "careers", "blog", "news", "media"]):
        score -= 0.15

    return max(0.0, min(1.0, score))


if __name__ == "__main__":
    main()

