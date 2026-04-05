from __future__ import annotations

import json
import importlib
import logging
import os
import re
import requests
from datetime import UTC, datetime
from itertools import product
from pathlib import Path
from typing import Any

from app.stage_supervisor import supervise_stage

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

if callable(load_dotenv):
    load_dotenv()

DEFAULT_PATTERNS = [
    "{industry} startups hiring in {region}",
    "{industry} companies careers page {region}",
    "fast growing {industry} companies {region}",
    "recent startup funding {region} {industry}",
]

EXPANSION_PATTERNS = [
    "{industry} outbound sales hiring {region}",
    "{industry} business development jobs {region}",
    "best {industry} startups to work for {region}",
]

LOW_QUALITY_FALLBACK_PATTERNS = [
    "SaaS companies hiring engineers {region}",
    "subscription billing companies {region}",
    "usage based pricing startups {region}",
]

DIRECTORY_SEARCH_TEMPLATES = [
    "SaaS companies using subscription billing",
    "subscription management software companies",
    "B2B SaaS companies using recurring revenue",
    "billing infrastructure startups",
]

TECH_SEARCH_TEMPLATES = [
    "companies using Stripe billing",
    "companies using subscription billing software",
    "companies with recurring revenue model SaaS",
]

HIRING_SEARCH_TEMPLATES = [
    "hiring billing engineer SaaS companies",
    "subscription platform hiring backend engineers",
]

COMPETITOR_SEARCH_TEMPLATES = [
    "companies similar to {company}",
    "{company} competitors SaaS",
]

RETRIEVAL_TERMS = ["companies", "startups", "saas", "platforms", "vendors"]
BUYING_SIGNAL_TERMS = [
    "hiring",
    "engineers",
    "billing",
    "payments",
    "subscription",
    "usage based",
    "api pricing",
    "fast growing",
    "series a",
    "series b",
    "series c",
    "infrastructure",
    "complex",
]
CURATED_RESOURCE_TERMS = ["list", "directory", "vendor", "vendors", "blog", "blogs", "companies", "startups", "platforms"]
COMPANY_FOCUSED_TERMS = ["companies", "company", "startups", "platforms", "vendors", "software", "saas"]
FORBIDDEN_QUERY_TERMS = ["top", "best", "list", "directory", "examples"]
QUERY_PATTERNS = [
    "SaaS companies using Stripe",
    "startups with subscription pricing",
    "B2B SaaS companies hiring engineers",
    "AI SaaS tools pricing page",
    "companies offering subscription services",
    "tools with monthly pricing SaaS",
    "subscription billing SaaS companies",
    "usage based billing startups",
    "revenue analytics SaaS companies",
    "billing infrastructure companies",
]


logger = logging.getLogger(__name__)
QUERY_FEEDBACK_PATH = Path("output/query_feedback.json")
INGESTION_FEEDBACK_PATH = Path("learning/ingestion_feedback.json")


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalize_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [_clean_text(item) for item in value if _clean_text(item)]
    if isinstance(value, str):
        return [_clean_text(part) for part in value.split(",") if _clean_text(part)]
    return []


def _sanitize_term(value: str, *, max_words: int = 6) -> str:
    cleaned = _clean_text(value)
    cleaned = cleaned.replace("#", " ")
    cleaned = cleaned.replace("-", " ")
    cleaned = " ".join(cleaned.split())
    words = [word for word in cleaned.split() if word]
    if not words:
        return ""
    return " ".join(words[:max_words])


def _parse_regions(value: object) -> list[str]:
    if isinstance(value, list):
        regions = [_clean_text(item) for item in value if _clean_text(item)]
        return regions or ["Global"]

    text = _clean_text(value)
    if not text:
        return ["Global"]

    parts = [part.strip() for part in text.replace(";", ",").split(",") if part.strip()]
    return parts or ["Global"]


def _dedupe_queries(queries: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for query in queries:
        clean = _clean_text(query)
        if not clean:
            continue
        key = clean.lower()
        if key in seen:
            continue
        seen.add(key)
        output.append(clean)
    return output


def _strip_code_fences(text: str) -> str:
    cleaned = _clean_text(text)
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.I)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _extract_json_payload(text: str) -> str:
    cleaned = _strip_code_fences(text)
    if cleaned.startswith("{") and cleaned.endswith("}"):
        return cleaned

    match = re.search(r"\{.*\}", cleaned, flags=re.S)
    if match:
        return match.group(0)

    return cleaned


def _contains_retrieval_term(query: str) -> bool:
    lowered = _clean_text(query).lower()
    return any(term in lowered for term in RETRIEVAL_TERMS)


def _contains_buying_signal(query: str) -> bool:
    lowered = _clean_text(query).lower()
    return any(term in lowered for term in BUYING_SIGNAL_TERMS)


def _normalize_regions(value: object) -> list[str]:
    return [item for item in _parse_regions(value) if _clean_text(item) and _clean_text(item).lower() != "global"]


def _contains_region(query: str, regions: list[str]) -> bool:
    lowered = _clean_text(query).lower()
    return any(_clean_text(region).lower() in lowered for region in regions)


def _contains_curated_resource_term(query: str) -> bool:
    lowered = _clean_text(query).lower()
    return any(term in lowered for term in CURATED_RESOURCE_TERMS)


def _contains_company_focus_term(query: str) -> bool:
    lowered = _clean_text(query).lower()
    return any(term in lowered for term in COMPANY_FOCUSED_TERMS)


def _contains_forbidden_term(query: str) -> bool:
    lowered = _clean_text(query).lower()
    return any(term in lowered for term in FORBIDDEN_QUERY_TERMS)


def _fallback_icp_queries(icp_data: dict[str, Any]) -> list[str]:
    industries = _normalize_list(icp_data.get("industries")) or ["SaaS"]
    keywords = _normalize_list(icp_data.get("keywords"))
    regions = _normalize_regions(icp_data.get("regions"))
    base_industry = industries[0]
    keyword_a = keywords[0] if keywords else "subscription billing"
    keyword_b = keywords[1] if len(keywords) > 1 else "usage based pricing"
    region_a = regions[0] if regions else ""

    base_queries = [
        f"{base_industry} companies using {keyword_a}",
        f"{base_industry} startups with {keyword_b}",
        "SaaS companies hiring billing engineers",
        "SaaS companies hiring payments engineers",
        "fast growing SaaS companies subscription pricing",
        "Series A SaaS startups recurring revenue",
        "Series B SaaS companies usage based pricing",
        "subscription billing platforms SaaS companies",
        "recurring revenue vendors B2B SaaS companies",
        "usage based pricing platforms SaaS startups",
        "SaaS companies API pricing models",
        "B2B SaaS companies complex billing infrastructure",
        f"companies with {keyword_b} SaaS platforms",
        "startup SaaS monetization platforms",
        "SaaS vendor pages subscription billing platforms",
    ]
    if region_a:
        base_queries.extend(
            [
                f"{region_a} SaaS companies hiring billing engineers",
                f"{region_a} fast growing SaaS startups subscription pricing",
            ]
        )

    return _dedupe_queries(base_queries)


def _post_process_query_output(raw_queries: list[str], icp_data: dict[str, Any]) -> list[str]:
    regions = _normalize_regions(icp_data.get("regions"))
    deduped = _dedupe_queries(raw_queries)
    filtered = [query for query in deduped if _contains_retrieval_term(query)]
    filtered = [query for query in filtered if _contains_company_focus_term(query)]
    filtered = [query for query in filtered if not _contains_forbidden_term(query)]
    filtered = [query for query in filtered if len(query.split()) <= 11]

    with_signals = [query for query in filtered if _contains_buying_signal(query)]
    without_signals = [query for query in filtered if not _contains_buying_signal(query)]
    prioritized = with_signals + without_signals

    if len(prioritized) < 12:
        prioritized = _dedupe_queries(prioritized + _fallback_icp_queries(icp_data))
        prioritized = [query for query in prioritized if _contains_retrieval_term(query)]
        prioritized = [query for query in prioritized if _contains_company_focus_term(query)]
        prioritized = [query for query in prioritized if not _contains_forbidden_term(query)]

    if regions:
        region_boost: list[str] = []
        for idx, query in enumerate(prioritized[:10]):
            if _contains_region(query, regions):
                continue
            region = regions[idx % len(regions)]
            compact_query = _clean_text(query)
            if len(compact_query.split()) > 9:
                compact_query = " ".join(compact_query.split()[:9])
            region_variant = f"{region} {compact_query}"
            if len(region_variant.split()) <= 11:
                region_boost.append(region_variant)
        prioritized = _dedupe_queries(region_boost + prioritized)

    prioritized = [query for query in prioritized if len(query.split()) <= 11]

    return prioritized[:12]


def _query_set_quality_score(output: Any) -> float:
    queries = output.get("queries") if isinstance(output, dict) else []
    if not isinstance(queries, list) or not queries:
        return 0.0

    cleaned = [_clean_text(query) for query in queries if _clean_text(query)]
    if not cleaned:
        return 0.0

    score = 0.25
    if 8 <= len(cleaned) <= 12:
        score += 0.35
    elif len(cleaned) >= 6:
        score += 0.2

    if all(len(query.split()) <= 11 for query in cleaned):
        score += 0.15
    if any(_contains_buying_signal(query) for query in cleaned):
        score += 0.15
    if not any(_contains_forbidden_term(query) for query in cleaned):
        score += 0.1

    return min(1.0, score)


def _tokenize_query(text: str) -> set[str]:
    return {part for part in re.split(r"[^a-z0-9]+", _clean_text(text).lower()) if part}


def _query_similarity(a: str, b: str) -> float:
    ta = _tokenize_query(a)
    tb = _tokenize_query(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / max(len(ta), len(tb))


def _load_query_feedback() -> dict[str, Any]:
    if not QUERY_FEEDBACK_PATH.exists():
        return {"accepted_queries": {}}
    try:
        payload = json.loads(QUERY_FEEDBACK_PATH.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            accepted = payload.get("accepted_queries")
            if isinstance(accepted, dict):
                return {"accepted_queries": accepted}
    except Exception:
        pass
    return {"accepted_queries": {}}


def _save_query_feedback(feedback: dict[str, Any]) -> None:
    QUERY_FEEDBACK_PATH.parent.mkdir(parents=True, exist_ok=True)
    QUERY_FEEDBACK_PATH.write_text(json.dumps(feedback, indent=2, ensure_ascii=True), encoding="utf-8")


def _load_ingestion_feedback() -> dict[str, Any]:
    if not INGESTION_FEEDBACK_PATH.exists():
        return {
            "bad_queries": [],
            "failed_patterns": [],
            "source_reliability": {},
        }
    try:
        from learning.feedback_manager import get_failed_patterns
        from learning.feedback_manager import get_failed_queries

        return {
            "bad_queries": get_failed_queries(),
            "failed_patterns": get_failed_patterns(),
            "source_reliability": {},
        }
    except Exception:
        pass

    try:
        from learning.feedback_updater import get_feedback_signals
        from learning.feedback_updater import load_ingestion_feedback

        payload = load_ingestion_feedback()
        return get_feedback_signals(payload)
    except Exception:
        return {
            "bad_queries": [],
            "failed_patterns": [],
            "source_reliability": {},
        }


def _llm_refine_query_list(queries: list[str], icp_data: dict[str, Any]) -> list[str]:
    cleaned = _dedupe_queries([_clean_text(query) for query in queries if _clean_text(query)])
    if not cleaned:
        return []

    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if not api_key:
        return cleaned

    ingestion_feedback = _load_ingestion_feedback()
    prompt = (
        "You optimize B2B lead-discovery queries. Return JSON with key 'queries'.\n"
        "Tasks:\n"
        "1) remove duplicates/redundancy\n"
        "2) expand with close synonyms\n"
        "3) prioritize high-commercial-intent queries first\n"
        "4) avoid patterns that previously produced noisy/non-company entities\n"
        "Rules:\n"
        "- keep queries concise\n"
        "- no listicle/news-only intent\n"
        "- max 18 queries\n\n"
        f"ICP:\n{json.dumps(icp_data, ensure_ascii=True)}\n\n"
        f"Known bad queries:\n{json.dumps(ingestion_feedback.get('bad_queries') or [], ensure_ascii=True)}\n\n"
        f"Failed ingestion patterns:\n{json.dumps(ingestion_feedback.get('failed_patterns') or [], ensure_ascii=True)}\n\n"
        f"Input queries:\n{json.dumps(cleaned, ensure_ascii=True)}"
    )

    try:
        from google import genai

        model_name = (os.getenv("GEMINI_MODEL") or "gemini-2.5-flash").strip()
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(model=model_name, contents=prompt)
        text = str(getattr(response, "text", "") or getattr(response, "output_text", "") or "")
        parsed = json.loads(_extract_json_payload(text))
        refined = parsed.get("queries") if isinstance(parsed, dict) else []
        if isinstance(refined, list):
            normalized = _dedupe_queries([_clean_text(item) for item in refined if _clean_text(item)])
            return normalized[:18] or cleaned
    except Exception:
        return cleaned

    return cleaned


def build_adaptive_queries(icp_data: dict[str, Any], feedback_log: dict[str, Any] | None = None) -> list[str]:
    if not isinstance(icp_data, dict):
        return []

    industries = _normalize_list(icp_data.get("industries") or icp_data.get("industry")) or ["SaaS"]
    keywords = _normalize_list(icp_data.get("keywords")) or ["subscription"]
    competitors = _normalize_list(icp_data.get("competitors") or icp_data.get("competitor_hints"))
    regions = _normalize_regions(icp_data.get("regions") or icp_data.get("geography")) or ["global"]
    roles = ["engineers", "backend engineers", "sales"]

    high_intent: list[str] = []
    expansion: list[str] = []
    broad: list[str] = []

    for role in roles[:2]:
        high_intent.append(f"companies hiring {role}")
    for industry in industries[:3]:
        high_intent.append(f"{industry} startups funding")
    for keyword in keywords[:4]:
        high_intent.append(f"{keyword} SaaS companies")

    for competitor in competitors[:5]:
        expansion.append(f"{competitor} alternatives")
        expansion.append(f"companies like {competitor}")

    for industry in industries[:3]:
        for region in regions[:3]:
            broad.append(f"{industry} companies in {region}")

    candidate_queries = _dedupe_queries(high_intent + expansion + broad)
    refined = _llm_refine_query_list(candidate_queries, icp_data)

    feedback = feedback_log if isinstance(feedback_log, dict) else _load_query_feedback()
    accepted_queries = feedback.get("accepted_queries") if isinstance(feedback.get("accepted_queries"), dict) else {}

    boosted: list[tuple[float, str]] = []
    for query in refined:
        score = 0.0
        for accepted_query, stats in accepted_queries.items():
            if not isinstance(stats, dict):
                continue
            accepted_count = float(stats.get("accepted_count") or 0)
            sim = _query_similarity(query, str(accepted_query))
            score += sim * accepted_count
        if _contains_buying_signal(query):
            score += 0.5
        boosted.append((score, query))

    boosted.sort(key=lambda item: item[0], reverse=True)
    return [query for _, query in boosted[:18]]


def refine_queries_with_feedback(
    icp_data: dict[str, Any],
    base_queries: list[str],
    *,
    bad_queries: list[str] | None = None,
    failed_patterns: list[str] | None = None,
    max_queries: int = 12,
) -> list[str]:
    """Regenerate cleaner ingestion queries using ICP + historical failure feedback."""
    cleaned_base = _dedupe_queries(base_queries)
    if not cleaned_base:
        cleaned_base = _fallback_icp_queries(icp_data)

    all_bad = _dedupe_queries((bad_queries or []) + (_load_ingestion_feedback().get("bad_queries") or []))
    all_failed_patterns = _dedupe_queries((failed_patterns or []) + (_load_ingestion_feedback().get("failed_patterns") or []))

    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if not api_key:
        filtered = [q for q in cleaned_base if q not in set(all_bad)]
        return _post_process_query_output(filtered, icp_data)[: max(3, int(max_queries))]

    prompt = (
        "You improve B2B lead discovery queries for company extraction quality. "
        "Return JSON only with key 'queries'.\n"
        "Objective: maximize real company websites; minimize noisy text, jobs-only pages, and directories.\n"
        "Constraints:\n"
        "- Output 8 to 12 concise intent-aware queries\n"
        "- Each query should target discoverable real companies\n"
        "- Avoid generic listicle terms and previously noisy patterns\n\n"
        f"ICP: {json.dumps(icp_data, ensure_ascii=True)}\n"
        f"Base queries: {json.dumps(cleaned_base, ensure_ascii=True)}\n"
        f"Bad queries: {json.dumps(all_bad, ensure_ascii=True)}\n"
        f"Failed patterns: {json.dumps(all_failed_patterns, ensure_ascii=True)}"
    )

    try:
        from google import genai

        model_name = (os.getenv("GEMINI_MODEL") or "gemini-2.5-flash").strip()
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(model=model_name, contents=prompt)
        text = str(getattr(response, "text", "") or getattr(response, "output_text", "") or "")
        parsed = json.loads(_extract_json_payload(text))
        generated = parsed.get("queries") if isinstance(parsed, dict) else []
        if isinstance(generated, list):
            out = _post_process_query_output([_clean_text(q) for q in generated], icp_data)
            out = [query for query in out if query not in set(all_bad)]
            return out[: max(3, int(max_queries))]
    except Exception:
        pass

    fallback = [query for query in cleaned_base if query not in set(all_bad)]
    return _post_process_query_output(fallback, icp_data)[: max(3, int(max_queries))]


def refine_queries_with_llm(icp: dict[str, Any], previous_failures: list[str]) -> list[str]:
    """GPT-researcher-style lightweight loop using Groq (preferred) or Gemini fallback."""
    icp_payload = icp if isinstance(icp, dict) else {}
    failed = _dedupe_queries(previous_failures)
    seed = _fallback_icp_queries(icp_payload)

    prompt = (
        "You generate high-quality B2B company discovery queries.\n"
        "Avoid: generic queries, job-heavy queries.\n"
        "Focus: companies, products, SaaS, APIs.\n"
        "Return ONLY JSON with key queries and 5-10 precise queries.\n\n"
        f"ICP: {json.dumps(icp_payload, ensure_ascii=True)}\n"
        f"Previous failed queries: {json.dumps(failed, ensure_ascii=True)}\n"
        f"Seed queries: {json.dumps(seed, ensure_ascii=True)}"
    )

    # Preferred: LangChain + Groq
    if (os.getenv("GROQ_API_KEY") or "").strip() and importlib.util.find_spec("langchain_groq") is not None and importlib.util.find_spec("langchain_core") is not None:
        try:
            messages_mod = importlib.import_module("langchain_core.messages")
            HumanMessage = getattr(messages_mod, "HumanMessage")
            SystemMessage = getattr(messages_mod, "SystemMessage")
            ChatGroq = getattr(importlib.import_module("langchain_groq"), "ChatGroq")

            print("QUERY_LLM_USED: GROQ", flush=True)

            groq_models = [
                (os.getenv("GROQ_MODEL") or "llama-3.3-70b-versatile").strip(),
                (os.getenv("GROQ_MODEL_FALLBACK") or "llama-3.1-8b-instant").strip(),
            ]
            for model_name in [model for model in groq_models if model]:
                try:
                    chat = ChatGroq(
                        api_key=(os.getenv("GROQ_API_KEY") or "").strip(),
                        model=model_name,
                        temperature=0,
                        timeout=20,
                    )
                    response = chat.invoke(
                        [
                            SystemMessage(content="You refine sales lead discovery queries with high precision."),
                            HumanMessage(content=prompt),
                        ]
                    )
                    parsed = json.loads(_extract_json_payload(str(getattr(response, "content", "") or "")))
                    queries = parsed.get("queries") if isinstance(parsed, dict) else []
                    if isinstance(queries, list):
                        cleaned = _post_process_query_output([_clean_text(q) for q in queries], icp_payload)
                        if cleaned:
                            return cleaned[:10]
                except Exception:
                    continue
        except Exception:
            pass

    return _post_process_query_output([q for q in seed if q not in set(failed)], icp_payload)[:10]


def record_query_feedback(queries: list[str], accepted_leads: list[dict[str, Any]]) -> None:
    feedback = _load_query_feedback()
    accepted_queries = feedback.get("accepted_queries") if isinstance(feedback.get("accepted_queries"), dict) else {}

    seen_in_run: set[str] = set()
    for lead in accepted_leads:
        if not isinstance(lead, dict):
            continue
        raw_fields = lead.get("raw_fields") if isinstance(lead.get("raw_fields"), dict) else {}
        lead_query = _clean_text(raw_fields.get("query") or raw_fields.get("search_query") or "")
        if lead_query:
            seen_in_run.add(lead_query)

    if not seen_in_run:
        seen_in_run = {_clean_text(query) for query in queries if _clean_text(query)}

    now = datetime.now(UTC).isoformat()
    for query in seen_in_run:
        stats = accepted_queries.get(query) if isinstance(accepted_queries.get(query), dict) else {}
        stats["accepted_count"] = int(stats.get("accepted_count") or 0) + 1
        stats["last_seen"] = now
        accepted_queries[query] = stats

    feedback["accepted_queries"] = accepted_queries
    _save_query_feedback(feedback)


def _generate_icp_query_set(icp_data: dict[str, Any]) -> dict[str, list[str]]:
    if not isinstance(icp_data, dict):
        return {"queries": []}

    prompt = (
        "You are a B2B sales lead query strategist.\n\n"
        "Your queries will be used in web scraping systems (like Apify), so they MUST return pages containing real companies with websites.\n\n"
        "INPUT:\n\n"
        f"{json.dumps(icp_data, ensure_ascii=True)}\n\n"
        "icp_data includes:\n\n"
        "product_description\n"
        "keywords\n"
        "industries\n"
        "competitors\n"
        "regions (optional)\n\n"
        "TASK:\n\n"
        "Generate short, high-intent search queries to find real company pages, product pages, hiring pages, pricing pages, and official sites matching the ICP.\n\n"
        "STRICT RULES:\n\n"
        "Each query MUST include at least one of:\n"
        "companies, startups, SaaS, platforms, vendors\n"
        "Include BUYING SIGNALS if possible:\n"
        "hiring (engineers, billing, payments)\n"
        "pricing (subscription, usage-based, API pricing)\n"
        "growth (fast growing, Series A/B/C)\n"
        "infrastructure complexity\n"
        "Include REGION if provided in the icp_data\n"
        "Do NOT generate list, directory, roundup, review, blog, article, top, best, or example queries.\n"
        "Do NOT generate queries aimed at curated resource pages or media pages.\n"
        "Do NOT generate competitor-only queries.\n"
        "Queries must be short, clean, and searchable\n"
        "Generate 8-12 queries per input\n\n"
        "OUTPUT:\n"
        "Return ONLY JSON:\n\n"
        "{\n"
        "  \"queries\": [\"...\"]\n"
        "}"
    )

    llm_queries: list[str] = []
    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if api_key:
        try:
            from google import genai

            model_name = (os.getenv("GEMINI_MODEL") or "gemini-2.5-flash").strip()
            client = genai.Client(api_key=api_key)
            response = client.models.generate_content(model=model_name, contents=prompt)
            text = str(getattr(response, "text", "") or getattr(response, "output_text", "") or "")
            parsed = json.loads(_extract_json_payload(text))
            if isinstance(parsed, dict) and isinstance(parsed.get("queries"), list):
                llm_queries = [str(item) for item in parsed.get("queries") if _clean_text(item)]
        except Exception:
            llm_queries = []

    final_queries = _post_process_query_output(llm_queries, icp_data)
    final_queries = final_queries[:12]
    if len(final_queries) < 8:
        final_queries = _post_process_query_output(final_queries + _fallback_icp_queries(icp_data), icp_data)[:12]
    return {"queries": final_queries}


def build_icp_query_set(icp_data: dict[str, Any]) -> dict[str, list[str]]:
    if not isinstance(icp_data, dict):
        return {"queries": []}

    supervised_output, audits = supervise_stage(
        stage_name="query_generation",
        input_payload=dict(icp_data),
        execute_stage=_generate_icp_query_set,
        fallback_stage=lambda payload: {"queries": _post_process_query_output(_fallback_icp_queries(payload), payload)},
        objective=(
            "Refine the markdown-derived ICP into short, high-intent company search queries that surface real businesses, "
            "not listicles, directories, or generic blog pages."
        ),
        min_quality=0.75,
        max_retries=2,
        quality_fn=_query_set_quality_score,
    )

    if audits:
        last_audit = audits[-1]
        logger.info(
            f"query stage | attempt={last_audit.attempt} quality={last_audit.quality_score:.2f} "
            f"approved={last_audit.approved} retry={last_audit.retry} issues={last_audit.issues}"
        )

    return supervised_output if isinstance(supervised_output, dict) else {"queries": []}


def _primary_industry(industries: list[str]) -> str:
    return _clean_text(industries[0]) if industries else "SaaS"


def _infer_use_case_terms(icp: dict[str, Any]) -> tuple[str, str]:
    text = " ".join(
        [
            _clean_text(icp.get("product_type")),
            _clean_text(icp.get("who_needs_this")),
            _clean_text(icp.get("target_customers")),
            " ".join(_normalize_list(icp.get("keywords"))),
            " ".join(_normalize_list(icp.get("pain_points"))),
        ]
    ).lower()

    if any(token in text for token in ["support", "helpdesk", "customer experience", "ticket"]):
        return ("customer support", "helpdesk software")
    if any(token in text for token in ["billing", "subscription", "recurring", "revenue"]):
        return ("subscription billing", "recurring revenue")
    if any(token in text for token in ["payments", "fintech", "checkout"]):
        return ("payment infrastructure", "fintech operations")
    return ("customer support", "helpdesk software")


def _parse_geography_regions(value: object) -> list[str]:
    if isinstance(value, list):
        return [_clean_text(item).strip("[]'\"") for item in value if _clean_text(item)]

    text = _clean_text(value).strip("[]")
    if not text:
        return []

    parts = [part.strip().strip("'\"") for part in text.replace(";", ",").split(",") if part.strip()]
    return [part for part in parts if part]


def _compact_buyers(value: str) -> str:
    text = _clean_text(value)
    if not text:
        return ""

    splitters = [".", " that ", " who ", " and "]
    lowered = text.lower()
    for splitter in splitters:
        idx = lowered.find(splitter)
        if idx > 0:
            text = _clean_text(text[:idx])
            break

    if "subscription" in text.lower() or "recurring" in text.lower():
        return "subscription-based digital businesses"
    if "saas" in text.lower():
        return "b2b saas businesses"

    return " ".join(text.split()[:8])


def _compact_buyer_phrase(value: str) -> str:
    text = _clean_text(value)
    if not text:
        return ""

    # Keep only the primary buyer segment before long explanatory clauses.
    for separator in [".", ";", ":", " that ", " who ", " experiencing ", " managing ", " hiring "]:
        marker = separator if separator.strip() else separator
        if marker in text.lower():
            parts = text.split(separator, 1)
            text = _clean_text(parts[0])
            break

    lowered = text.lower()
    if "subscription" in lowered or "recurring" in lowered:
        return "subscription-based digital businesses"
    if "saas" in lowered:
        return "b2b saas companies"
    if "ecommerce" in lowered:
        return "ecommerce companies"

    words = text.split()
    return " ".join(words[:8]) if words else ""


def generate_queries(icp: dict[str, Any]) -> list[str]:
    if not isinstance(icp, dict):
        return []

    industries = _normalize_list(icp.get("industries")) or [_clean_text(icp.get("industry")) or "SaaS"]
    keywords = _normalize_list(icp.get("keywords"))
    regions = _normalize_regions(icp.get("regions") or icp.get("geography")) or ["Global"]
    buyers_raw = _clean_text(icp.get("who_needs_this")) or _clean_text(icp.get("target_customers")) or "B2B SaaS companies"
    buyers = _compact_buyers(buyers_raw) or "B2B SaaS companies"
    primary_industry = _clean_text(industries[0]) or "SaaS"
    primary_region = _clean_text(regions[0]) or "Global"
    keyword_a = _sanitize_term(keywords[0], max_words=3) if keywords else "subscription billing"
    keyword_b = _sanitize_term(keywords[1], max_words=3) if len(keywords) > 1 else "usage based billing"

    seed_queries = [
        f"{primary_region} {primary_industry} companies {keyword_a}",
        f"{primary_region} {primary_industry} startups {keyword_b}",
        f"{buyers} {primary_region}",
        f"{primary_region} companies using Stripe Billing",
        f"{primary_region} subscription billing startups",
        f"{primary_region} usage based billing companies",
        f"{primary_region} recurring revenue SaaS companies",
        f"{primary_region} fintech SaaS companies hiring billing engineers",
        f"{primary_region} companies hiring payments engineers",
        f"{primary_region} api pricing software companies",
        f"{primary_region} revenue analytics SaaS companies",
        f"{primary_region} billing infrastructure companies",
    ]

    seed_queries.extend(QUERY_PATTERNS)

    llm_payload = build_icp_query_set(
        {
            "product_description": _clean_text(icp.get("product_description") or icp.get("product_type") or ""),
            "keywords": keywords,
            "industries": industries,
            "regions": regions,
            "competitors": _normalize_list(icp.get("competitors") or icp.get("competitor_hints") or []),
        }
    )
    adaptive_queries = build_adaptive_queries(
        {
            "industries": industries,
            "keywords": keywords,
            "competitors": _normalize_list(icp.get("competitors") or icp.get("competitor_hints") or []),
            "regions": regions,
        },
        feedback_log=_load_query_feedback(),
    )
    llm_queries = llm_payload.get("queries") if isinstance(llm_payload, dict) else []
    combined = seed_queries + adaptive_queries + ([str(item) for item in llm_queries] if isinstance(llm_queries, list) else [])
    combined = [query.replace("[", "").replace("]", "").replace("'", "") for query in combined]
    ingestion_feedback = _load_ingestion_feedback()
    llm_retry_queries = refine_queries_with_llm(
        {
            "regions": regions,
            "industries": industries,
            "keywords": keywords,
            "who_needs_this": buyers_raw,
        },
        ingestion_feedback.get("bad_queries") if isinstance(ingestion_feedback, dict) and isinstance(ingestion_feedback.get("bad_queries"), list) else [],
    )
    refined_with_feedback = refine_queries_with_feedback(
        {
            "regions": regions,
            "industries": industries,
            "keywords": keywords,
            "who_needs_this": buyers_raw,
        },
        llm_retry_queries + combined,
        bad_queries=ingestion_feedback.get("bad_queries") if isinstance(ingestion_feedback, dict) else [],
        failed_patterns=ingestion_feedback.get("failed_patterns") if isinstance(ingestion_feedback, dict) else [],
        max_queries=12,
    )

    final_queries = _post_process_query_output(refined_with_feedback or combined, {
        "regions": regions,
        "industries": industries,
        "keywords": keywords,
        "who_needs_this": buyers_raw,
    })
    return final_queries[:12]


def generate_region_queries(
    regions: list[str],
    industries: list[str],
    include_expansion: bool = False,
) -> list[dict[str, str]]:
    patterns = list(DEFAULT_PATTERNS)
    if include_expansion:
        patterns.extend(EXPANSION_PATTERNS)

    queries: list[dict[str, str]] = []
    for region, industry in product(regions, industries):
        for pattern in patterns:
            query = pattern.format(industry=industry, region=region).strip()
            if not query:
                continue
            queries.append({"query": query, "region": region, "industry": industry})

    return queries


def generate_region_search_fallback_queries(regions: list[str]) -> list[dict[str, str]]:
    queries: list[dict[str, str]] = []
    for region in regions:
        for pattern in LOW_QUALITY_FALLBACK_PATTERNS:
            query = pattern.format(region=region).strip()
            if not query:
                continue
            queries.append({"query": query, "region": region, "industry": "fallback"})
    return queries


def build_multistrategy_queries(
    icp: dict[str, Any],
    *,
    seed_company: str = "",
    competitor_names: list[str] | None = None,
    regions: list[str] | None = None,
    industries: list[str] | None = None,
    max_variations: int = 5,
    max_queries_per_seed: int = 10,
    max_seed_companies: int = 6,
    max_queries: int = 96,
) -> list[dict[str, str]]:
    if not isinstance(icp, dict):
        return []

    _ = max_variations
    _ = max_queries_per_seed
    _ = max_seed_companies

    region_list = [_clean_text(item) for item in (regions or []) if _clean_text(item)] or ["Global"]
    industry_list = [_clean_text(item) for item in (industries or []) if _clean_text(item)] or ["SaaS"]
    primary_region = region_list[0]
    primary_industry = _primary_industry(industry_list)

    icp_data = {
        "product_description": _clean_text(icp.get("product_description") or icp.get("product_type") or ""),
        "keywords": _normalize_list(icp.get("keywords")),
        "industries": industry_list,
        "regions": region_list,
        "competitors": [_clean_text(name) for name in (competitor_names or []) if _clean_text(name)],
        "seed_company": _clean_text(seed_company),
    }

    query_set = build_icp_query_set(icp_data)
    queries = query_set.get("queries") if isinstance(query_set, dict) else []
    if not isinstance(queries, list):
        return []

    rows: list[dict[str, str]] = []
    for idx, query in enumerate([_clean_text(item) for item in queries if _clean_text(item)]):
        rows.append(
            {
                "query": query,
                "region": primary_region,
                "industry": primary_industry,
                "strategy": "llm_icp",
                "priority": str(max(1, 15 - idx)),
                "variation": "1",
                "seed_company": _clean_text(seed_company),
                "buyer_phrase": _compact_buyer_phrase(
                    _clean_text(icp.get("who_needs_this"))
                    or _clean_text(icp.get("target_customers"))
                    or "B2B SaaS companies"
                ),
            }
        )

    return rows[: max(1, int(max_queries))]
