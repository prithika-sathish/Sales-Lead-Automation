import json
import asyncio
import inspect
import logging
import os
import time
import importlib
from datetime import UTC, datetime
from typing import List, Dict, Any
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

if callable(load_dotenv):
    load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test mode flag - set TEST_MODE=1 to use only 3 key sources with minimal rows
TEST_MODE = (os.getenv("TEST_MODE") or "0").strip() == "1"
TEST_SOURCES = ["google_maps", "product_hunt", "jobs"]  # Only these 3
TEST_ROWS_PER_SOURCE = 5  # Minimal rows for testing
SOURCE_TIMEOUT_SECONDS = int((os.getenv("SOURCE_TIMEOUT_SECONDS") or "20").strip() or "20")
INGESTION_TOTAL_TIMEOUT_SECONDS = int((os.getenv("INGESTION_TOTAL_TIMEOUT_SECONDS") or "150").strip() or "150")
SOURCE_MAX_RETRIES = int((os.getenv("SOURCE_MAX_RETRIES") or "2").strip() or "2")
MAX_LLM_CALLS = int((os.getenv("MAX_LLM_CALLS") or "50").strip() or "50")
TARGET_VALID_ROWS = int((os.getenv("TARGET_VALID_ROWS") or "40").strip() or "40")

ALL_SOURCES = [
    "google_maps",
    "product_hunt",
    "opencorporates",
    "jobs",
    "github",
    "app_stores",
    "blogs",
    "capterra",
    "clutch",
    "crunchbase",
    "g2",
    "marketplaces",
    "news",
    "reddit_source",
    "twitter_source",
    "yc",
]

SOURCE_FETCH_NAME_OVERRIDES = {
    "yc": "fetch_y_combinator_leads",
}


def _function_accepts_query_list(func: Any) -> bool:
    """Detect whether a source fetch function expects a list of queries."""
    try:
        sig = inspect.signature(func)
    except Exception:
        return False

    params = list(sig.parameters.values())
    if not params:
        return False

    first = params[0]
    name_hint = first.name.lower() in {"queries", "query_list"}
    ann = str(first.annotation)
    type_hint = "list" in ann.lower()
    return name_hint or type_hint


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def normalize_company(name: str) -> str:
    return " ".join(name.strip().title().split())


def _avg_entity_confidence(rows_by_source: dict[str, list[dict[str, Any]]]) -> float:
    values: list[float] = []
    for rows in rows_by_source.values():
        for row in rows:
            if not isinstance(row, dict):
                continue
            values.append(_safe_float(row.get("entity_confidence") or row.get("confidence"), 0.0))
    if not values:
        return 0.0
    return sum(values) / max(1, len(values))


def _token_overlap_score(text: str, terms: list[str]) -> float:
    lowered = _clean_text(text).lower()
    clean_terms = [_clean_text(term).lower() for term in terms if _clean_text(term)]
    if not clean_terms:
        return 0.0
    matches = sum(1 for term in clean_terms if term in lowered)
    return min(1.0, matches / max(1, len(clean_terms)))


def _broaden_query_via_llm(query: str) -> str:
    base_query = _clean_text(query)
    if not base_query:
        return "saas companies"
    return f"{base_query} companies"


def _with_retries(func: Any, *args: Any, retries: int = SOURCE_MAX_RETRIES, base_delay: float = 0.75) -> Any:
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return func(*args)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= retries:
                break
            time.sleep(base_delay * (2**attempt))
    if last_error is not None:
        raise last_error
    raise RuntimeError("retry executor failed")


def _normalize_alt_rows(rows: list[dict[str, Any]], provider: str, query: str) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = _clean_text(row.get("title") or row.get("name") or row.get("company") or "")
        link = _clean_text(row.get("link") or row.get("url") or row.get("website") or "")
        snippet = _clean_text(row.get("snippet") or row.get("content") or row.get("description") or "")
        output.append(
            {
                "name": title,
                "title": title,
                "url": link,
                "link": link,
                "website": link,
                "snippet": snippet,
                "description": snippet,
                "source": provider,
                "source_transport": provider,
                "query": query,
            }
        )
    return output


def _llm_select_discovery_providers(query: str) -> list[str]:
    providers = ["serper", "serpapi", "tavily", "google_cse", "duckduckgo"]
    api_key = _clean_text(os.getenv("GEMINI_API_KEY"))
    if not api_key:
        return providers

    try:
        from google import genai

        model_name = _clean_text(os.getenv("GEMINI_MODEL") or "gemini-2.5-flash")
        client = genai.Client(api_key=api_key)
        prompt = (
            "Choose best search providers order for this B2B discovery query. "
            "Return JSON with key providers (list). Allowed values: serper, serpapi, tavily, google_cse, duckduckgo.\n"
            f"query: {_clean_text(query)}"
        )
        response = client.models.generate_content(model=model_name, contents=prompt)
        text = str(getattr(response, "text", "") or getattr(response, "output_text", "") or "")
        start = text.find("{")
        end = text.rfind("}")
        payload = json.loads(text[start : end + 1]) if start >= 0 and end > start else {}
        selected = payload.get("providers") if isinstance(payload, dict) else []
        if isinstance(selected, list):
            normalized = [str(item).strip().lower() for item in selected if str(item).strip().lower() in providers]
            if normalized:
                return list(dict.fromkeys(normalized))
    except Exception:
        return providers

    return providers


def _llm_ingestion_quality(query: str, rows: list[dict[str, Any]]) -> tuple[float, bool, str]:
    if not rows:
        return 0.0, True, "no_rows"

    domains = {str(row.get("domain") or "").strip().lower() for row in rows if isinstance(row, dict)}
    domains.discard("")
    heuristic_score = 0.2
    if len(rows) >= 3:
        heuristic_score += 0.25
    if len(domains) >= 2:
        heuristic_score += 0.25
    if any("saas" in _clean_text(row.get("context") or row.get("description") or "").lower() for row in rows if isinstance(row, dict)):
        heuristic_score += 0.15
    if any(token in _clean_text(row.get("name") or "").lower() for token in ["top", "list", "directory", "jobs"] for row in rows if isinstance(row, dict)):
        heuristic_score -= 0.1
    heuristic_score = max(0.0, min(1.0, heuristic_score))

    return heuristic_score, False, "heuristic_only"


def _collect_alternative_discovery_rows(query: str) -> list[dict[str, Any]]:
    from sources.apify_common import basic_http_search
    from sources.apify_common import google_custom_search
    from sources.apify_common import serper_search
    from sources.apify_common import tavily_search as apify_tavily_search

    providers = _llm_select_discovery_providers(query)
    aggregated: list[dict[str, Any]] = []
    for provider in providers:
        try:
            if provider == "serper":
                aggregated.extend(_normalize_alt_rows(serper_search(query), "serper", query))
            elif provider == "serpapi":
                try:
                    from data_sources.serp_client import search_serp

                    rows = asyncio.run(search_serp(query, retries=2, timeout_seconds=12.0))
                    aggregated.extend(_normalize_alt_rows(rows, "serpapi", query))
                except Exception:
                    continue
            elif provider == "tavily":
                from discovery.tavily_search import fallback_search as discovery_tavily_fallback

                rows = discovery_tavily_fallback(query, max_results=10)
                aggregated.extend(_normalize_alt_rows(rows, "tavily", query))
                aggregated.extend(_normalize_alt_rows(apify_tavily_search(query), "tavily", query))
            elif provider == "google_cse":
                aggregated.extend(_normalize_alt_rows(google_custom_search(query), "google_cse", query))
            elif provider == "duckduckgo":
                aggregated.extend(_normalize_alt_rows(basic_http_search(query), "duckduckgo", query))
        except Exception:
            continue
    return aggregated


class IngestionOrchestrator:
    """Orchestrates multi-source company ingestion with retry logic and error isolation."""
    
    def __init__(self):
        self.sources_executed = {}
        self.total_raw_rows = 0
        self.total_normalized_rows = 0
        self.total_with_domain = 0
        self.all_rows = []
        self._disabled_sources: set[str] = set()
        self._source_health: list[dict[str, Any]] = []
        self._icp_context = self._load_icp_context()
        self._entity_cache: dict[str, list[dict[str, Any]]] = {}

    def _load_icp_context(self) -> dict[str, Any]:
        markdown_path = _clean_text(os.getenv("ICP_MARKDOWN_PATH") or "sample_company.md")
        try:
            from pathlib import Path
            from icp.extractor import extract_icp

            path = Path(markdown_path)
            if not path.exists():
                return {"keywords": [], "industries": [], "competitors": []}
            text = path.read_text(encoding="utf-8")
            icp = extract_icp(text) if text else {}
            if not isinstance(icp, dict):
                icp = {}
            return {
                "keywords": icp.get("keywords") if isinstance(icp.get("keywords"), list) else [],
                "industries": icp.get("industries") if isinstance(icp.get("industries"), list) else [str(icp.get("industry") or "")],
                "competitors": icp.get("competitors") if isinstance(icp.get("competitors"), list) else [],
            }
        except Exception:
            return {"keywords": [], "industries": [], "competitors": []}

    def _attach_ingestion_score(self, row: dict[str, Any], query: str) -> dict[str, Any]:
        text = " ".join(
            [
                str(row.get("name") or ""),
                str(row.get("context") or ""),
                str(row.get("description") or ""),
                str(query or ""),
            ]
        )
        keyword_score = _token_overlap_score(text, self._icp_context.get("keywords", [])) * 2.0
        industry_score = _token_overlap_score(text, self._icp_context.get("industries", [])) * 2.0
        competitor_similarity_score = _token_overlap_score(text, self._icp_context.get("competitors", [])) * 1.5
        ingestion_score = round(keyword_score + industry_score + competitor_similarity_score, 3)

        updated = dict(row)
        updated["ingestion_score"] = ingestion_score
        updated["ingestion_score_components"] = {
            "keyword_match_score": round(keyword_score, 3),
            "industry_match_score": round(industry_score, 3),
            "competitor_similarity_score": round(competitor_similarity_score, 3),
        }
        return updated

    def _to_unified_schema(self, row: dict[str, Any], source_name: str, query: str) -> dict[str, Any]:
        name = _clean_text(row.get("name") or row.get("title") or row.get("company") or "")
        domain = _clean_text(row.get("domain") or "")
        description = _clean_text(row.get("context") or row.get("description") or row.get("snippet") or "")
        confidence = _safe_float(row.get("confidence") or 0.4, 0.4)

        unified = dict(row)
        unified["name"] = name
        unified["company_name"] = _clean_text(unified.get("company_name") or name)
        unified["domain"] = domain
        unified["description"] = description
        unified["context"] = description or _clean_text(unified.get("context") or "")
        unified["source"] = _clean_text(unified.get("source") or source_name)
        unified["confidence"] = confidence
        unified["entity_confidence"] = _safe_float(unified.get("entity_confidence") or confidence, confidence)
        unified.setdefault("raw_fields", {})
        if isinstance(unified["raw_fields"], dict):
            unified["raw_fields"]["query"] = query
            unified["raw_fields"]["ingested_at"] = datetime.now(UTC).isoformat()
        return unified
    
    def run(self, queries: List[str], sample_company: str = None) -> List[Dict[str, Any]]:
        """
        Execute ingestion from all sources using provided queries.
        
        Args:
            queries: List of search queries (from query_generator)
            sample_company: Optional company name for context
        
        Returns:
            List of normalized, scored rows with confidence_reasons
        """
        logger.info(f"Starting ingestion for {len(queries)} queries...")
        
        # Dynamic source loader
        source_modules = self._load_source_modules()
        
        # Execute all sources in parallel with hard timeout + fail-soft behavior.
        active_queries = list(queries)
        results_by_source = self.run_sources_parallel(source_modules, active_queries, timeout=SOURCE_TIMEOUT_SECONDS)

        # GPT-researcher-style loop: regenerate and retry when yield/quality is low.
        retry_rounds = 0
        min_rows = max(3, int((os.getenv("INGESTION_MIN_ROWS") or "8").strip() or "8"))
        while retry_rounds < 2:
            total_rows = sum(len(rows) for rows in results_by_source.values())
            avg_confidence = _avg_entity_confidence(results_by_source)
            if total_rows >= min_rows and avg_confidence >= 0.6:
                break

            retry_rounds += 1
            try:
                from discovery.query_generator import refine_queries_with_llm
                from learning.feedback_manager import get_failed_queries
                from learning.feedback_manager import log_query

                bad_queries = get_failed_queries()
                retry_queries = refine_queries_with_llm(self._icp_context, bad_queries or active_queries)
                if not retry_queries:
                    break

                for q in active_queries[:4]:
                    log_query(q, is_good=False)

                active_queries = retry_queries[: max(3, len(active_queries))]
                results_by_source = self.run_sources_parallel(source_modules, active_queries, timeout=SOURCE_TIMEOUT_SECONDS)
            except Exception:
                break
        
        # Aggregate all rows
        for source_name, rows in results_by_source.items():
            self.total_raw_rows += len(rows)
            self.all_rows.extend(rows)

        # Pre-signal preparation: mark entities seen in multiple sources.
        domain_counts: dict[str, int] = {}
        for row in self.all_rows:
            if not isinstance(row, dict):
                continue
            domain = str(row.get("domain") or "").strip().lower()
            if not domain:
                continue
            domain_counts[domain] = domain_counts.get(domain, 0) + 1

        for row in self.all_rows:
            if not isinstance(row, dict):
                continue
            domain = str(row.get("domain") or "").strip().lower()
            if not domain or domain_counts.get(domain, 0) < 2:
                continue
            signals = row.get("signals") if isinstance(row.get("signals"), list) else []
            reasons = row.get("confidence_reasons") if isinstance(row.get("confidence_reasons"), list) else []
            if "multi_source_presence" not in signals:
                signals.append("multi_source_presence")
            if "multi-source presence detected" not in reasons:
                reasons.append("multi-source presence detected")
            row["signals"] = signals
            row["confidence_reasons"] = reasons
            row["signal_count"] = int(len(signals) + len(row.get("high_intent_signals") or []))
        
        logger.info(f"\nAggregation Summary:")
        logger.info(f"  Total raw rows: {self.total_raw_rows}")
        logger.info(f"  Total rows with domain: {self.total_with_domain}")
        
        return self.all_rows
    
    def _load_source_modules(self) -> Dict[str, Any]:
        """Dynamically load all source modules."""
        sources = {}
        
        # In TEST_MODE, only load specified sources
        if TEST_MODE:
            logger.info(f"🧪 TEST MODE: Loading only {TEST_SOURCES}")
        
        source_names = list(TEST_SOURCES if TEST_MODE else ALL_SOURCES)

        for source_name in source_names:
            try:
                sources[source_name] = importlib.import_module(f"sources.{source_name}")
            except Exception as e:
                logger.warning(f"Failed to load {source_name}: {e}")
        
        logger.info(f"Loaded {len(sources)} sources: {list(sources.keys())}")
        return sources

    def run_sources_parallel(self, source_modules: Dict[str, Any], queries: List[str], timeout: int = 15) -> Dict[str, List[Dict[str, Any]]]:
        results_by_source: Dict[str, List[Dict[str, Any]]] = {}
        executor = ThreadPoolExecutor(max_workers=5)
        try:
            source_items = [
                (source_name, source_module)
                for source_name, source_module in source_modules.items()
                if source_name not in self._disabled_sources
            ]
            # Lower priority for jobs source to reduce noise and LLM burn.
            source_items.sort(key=lambda item: 1 if str(item[0]).lower() == "jobs" else 0)
            future_to_source = {
                executor.submit(self._execute_source, source_name, source_module, queries): source_name
                for source_name, source_module in source_items
            }

            run_deadline = time.time() + min(
                max(timeout * max(1, len(future_to_source)), timeout),
                max(30, INGESTION_TOTAL_TIMEOUT_SECONDS),
            )

            while future_to_source and time.time() < run_deadline:
                done, _ = wait(list(future_to_source.keys()), timeout=0.25, return_when=FIRST_COMPLETED)
                if not done:
                    continue
                for future in done:
                    source_name = future_to_source.pop(future)
                    try:
                        rows = future.result()
                        rows = rows if isinstance(rows, list) else []
                    except Exception as exc:
                        logger.warning(f"⚠ {source_name}: skipped ({exc})")
                        rows = []
                    results_by_source[source_name] = rows
                    logger.info(f"✓ {source_name}: {len(rows)} rows")

            for future, source_name in list(future_to_source.items()):
                future.cancel()
                results_by_source[source_name] = []
                logger.warning(f"⚠ {source_name}: timeout>{timeout}s, skipped")
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        return results_by_source
    
    def _execute_source(self, source_name: str, source_module: Any, queries: List[str]) -> List[Dict]:
        """Execute a single source with error handling."""
        from sources.apify_common import mark_failed, mark_success, normalize_min_schema, search_fallback, should_skip
        from core.llm_row_filter import get_llm_calls_used
        from core.llm_row_filter import should_keep_rows
        from core.entity_extractor import extract_companies_from_record
        from core.entity_extractor import extract_company_domain_signals
        from core.llm_entity_extractor import extract_company_entity
        from core.llm_entity_extractor import normalize_company_name as llm_normalize_company
        from core.entity_validator import compute_weighted_ingestion_score
        from core.entity_validator import validate_entity
        from core.entity_validator import normalize_company_name
        from core.entity_validator import validate_company_entity
        from discovery.query_generator import refine_queries_with_llm
        from learning.feedback_updater import get_feedback_signals
        from learning.feedback_updater import load_ingestion_feedback
        from learning.feedback_updater import update_ingestion_feedback
        from learning.feedback_manager import log_entity
        from learning.feedback_manager import log_query

        if should_skip(source_name):
            logger.info(f"⏭ {source_name}: skipped by circuit breaker")
            return []

        if get_llm_calls_used() >= MAX_LLM_CALLS:
            logger.info("LLM call budget exhausted before source=%s, skipping source", source_name)
            return []

        apify_success = False
        fallback_used = False
        try:
            # Call source's main fetch function
            func_name = SOURCE_FETCH_NAME_OVERRIDES.get(source_name, f"fetch_{source_name.lower().replace(' ', '_')}_leads")
            
            # In TEST_MODE, limit queries
            if TEST_MODE:
                queries = queries[:2]  # Only use first 2 queries in test mode
            
            if hasattr(source_module, func_name):
                fetch_func = getattr(source_module, func_name)
            elif hasattr(source_module, 'fetch_leads'):
                fetch_func = getattr(source_module, 'fetch_leads')
            else:
                logger.warning(f"Source {source_name} has no fetch function")
                return []

            rows: list[dict[str, Any]] = []

            # For each query, run source fetch with bounded retries.
            source_queries = (queries[:3] if queries else ["saas companies"])
            query_rows: list[tuple[str, dict[str, Any]]] = []

            nested_executor = ThreadPoolExecutor(max_workers=1)
            try:
                if _function_accepts_query_list(fetch_func):
                    future = nested_executor.submit(lambda: _with_retries(fetch_func, source_queries))
                    fetched = future.result(timeout=max(3, SOURCE_TIMEOUT_SECONDS))
                    if isinstance(fetched, list):
                        for item in fetched:
                            if isinstance(item, dict):
                                item["query"] = source_queries[0] if source_queries else ""
                                query_rows.append((str(item.get("query") or ""), item))
                else:
                    for query in source_queries:
                        future = nested_executor.submit(lambda q=query: _with_retries(fetch_func, str(q)))
                        fetched = future.result(timeout=max(3, SOURCE_TIMEOUT_SECONDS))
                        if isinstance(fetched, list):
                            for item in fetched:
                                if isinstance(item, dict):
                                    item["query"] = str(query)
                                    query_rows.append((str(query), item))
            except Exception:
                query_rows = []
            finally:
                nested_executor.shutdown(wait=False, cancel_futures=True)

            rows = [item for _, item in query_rows]

            if rows:
                apify_success = any(str((row or {}).get("source_transport") or "apify").lower() == "apify" for row in rows if isinstance(row, dict))
            
            # Limit rows in TEST_MODE
            if TEST_MODE and isinstance(rows, list):
                rows = rows[:TEST_ROWS_PER_SOURCE]

            # Mandatory tertiary fallback if source returned nothing.
            if not rows:
                fallback_used = True
                fallback_rows: list[dict[str, Any]] = []
                for query in (queries[:2] if queries else ["saas companies"]):
                    fallback_rows.extend(search_fallback(str(query)))

                # Relax and retry with LLM-broadened query when needed.
                if not fallback_rows:
                    for query in (queries[:2] if queries else ["saas companies"]):
                        broadened = _broaden_query_via_llm(str(query))
                        fallback_rows.extend(search_fallback(broadened))

                rows = normalize_min_schema(fallback_rows, source_name)

            if not rows:
                mark_failed(source_name)
                self._source_health.append({"source": source_name, "apify_success": apify_success, "fallback_used": fallback_used, "rows": 0})
                return []

            print(f"ROWS_BEFORE_FILTER:{len(rows)} source={source_name}", flush=True)
            seen_rows: set[str] = set()
            unique_rows: list[dict[str, Any]] = []
            for record in rows:
                if not isinstance(record, dict):
                    continue
                row_key = _clean_text(record.get("url") or record.get("website") or record.get("link") or record.get("title") or record.get("name") or "").lower()
                if not row_key or row_key in seen_rows:
                    continue
                seen_rows.add(row_key)
                unique_rows.append(record)

            filtered_rows: list[dict[str, Any]] = []
            batch_size = max(1, int((os.getenv("ROW_FILTER_BATCH_SIZE") or "5").strip() or "5"))
            for i in range(0, len(unique_rows), batch_size):
                if get_llm_calls_used() >= MAX_LLM_CALLS:
                    logger.info("LLM budget hit while filtering rows in source=%s", source_name)
                    break

                batch = unique_rows[i : i + batch_size]
                decisions = should_keep_rows(batch)
                for record, decision in zip(batch, decisions):
                    keep = bool((decision or {}).get("keep"))
                    confidence = _safe_float((decision or {}).get("confidence"), 0.0)
                    if (not keep) or confidence < 0.5:
                        continue
                    filtered_rows.append(record)
                    if len(filtered_rows) >= TARGET_VALID_ROWS:
                        break

                if len(filtered_rows) >= TARGET_VALID_ROWS:
                    break

            print(f"ROWS_AFTER_FILTER:{len(filtered_rows)} source={source_name}", flush=True)
            print(f"LLM_CALLS_USED:{get_llm_calls_used()} source={source_name}", flush=True)
            rows = filtered_rows

            if not rows:
                mark_failed(source_name)
                self._source_health.append({"source": source_name, "apify_success": apify_success, "fallback_used": fallback_used, "rows": 0})
                return []

            # Entity extraction layer (documents/pages -> company candidates)
            extracted_candidates: list[dict[str, Any]] = []
            for record in rows:
                if not isinstance(record, dict):
                    continue
                cache_key = _clean_text(record.get("url") or record.get("website") or record.get("link") or record.get("title") or record.get("name") or "").lower()
                cached_entities = self._entity_cache.get(cache_key)
                extracted = cached_entities if isinstance(cached_entities, list) else extract_companies_from_record(record)
                if cache_key and cache_key not in self._entity_cache:
                    self._entity_cache[cache_key] = extracted if isinstance(extracted, list) else []
                if extracted:
                    extracted_candidates.extend(extracted)
                else:
                    extracted_candidates.append(
                        {
                            "name": str(record.get("title") or record.get("name") or "").strip(),
                            "source_type": "fallback",
                            "confidence": 0.2,
                            "entity_confidence": 0.2,
                            "context": str(record.get("snippet") or record.get("description") or record.get("tagline") or "").strip(),
                            "source_url": str(record.get("url") or record.get("website") or record.get("link") or "").strip() or None,
                            "raw_fields": record,
                        }
                    )

            # ENHANCED: lead normalization + light upstream validation + dedupe
            from core.normalizer import dedupe_company_candidates
            from core.normalizer import normalize_company_candidates
            from core.normalizer import passes_minimum_company_validation

            normalized = normalize_company_candidates(extracted_candidates, source_name)
            normalized = [row for row in normalized if passes_minimum_company_validation(row)]
            normalized = dedupe_company_candidates(normalized)

            container_count = sum(1 for row in normalized if str(row.get("entity_type") or "") == "SOURCE_CONTAINER")
            normalized = [row for row in normalized if str(row.get("entity_type") or "REAL_COMPANY") != "SOURCE_CONTAINER"]

            # If a source returned rows but none survived normalization, use fallback search.
            if not normalized:
                fallback_used = True
                fallback_rows: list[dict[str, Any]] = []
                for query in (queries[:2] if queries else ["saas companies"]):
                    fallback_rows.extend(search_fallback(str(query)))

                if not fallback_rows:
                    for query in (queries[:2] if queries else ["saas companies"]):
                        fallback_rows.extend(search_fallback(_broaden_query_via_llm(str(query))))

                fallback_seed = normalize_min_schema(fallback_rows, source_name)
                extracted_candidates = []
                for record in fallback_seed:
                    if not isinstance(record, dict):
                        continue
                    extracted = extract_companies_from_record(record)
                    if extracted:
                        extracted_candidates.extend(extracted)
                    else:
                        extracted_candidates.append(
                            {
                                "name": str(record.get("title") or record.get("name") or "").strip(),
                                "source_type": "fallback",
                                "confidence": 0.2,
                                "entity_confidence": 0.2,
                                "context": str(record.get("snippet") or record.get("description") or "").strip(),
                                "source_url": str(record.get("url") or record.get("link") or record.get("website") or "").strip() or None,
                                "raw_fields": record,
                            }
                        )

                normalized = normalize_company_candidates(extracted_candidates, source_name)
                normalized = [row for row in normalized if passes_minimum_company_validation(row)]
                normalized = dedupe_company_candidates(normalized)
                container_count += sum(1 for row in normalized if str(row.get("entity_type") or "") == "SOURCE_CONTAINER")
                normalized = [row for row in normalized if str(row.get("entity_type") or "REAL_COMPANY") != "SOURCE_CONTAINER"]

            # Final fail-safe: use trusted discovery surfaces if still empty.
            if not normalized:
                fallback_used = True
                trusted_rows: list[dict[str, Any]] = []
                for query in (queries[:1] if queries else ["saas companies"]):
                    trusted_query = f"site:crunchbase.com OR site:ycombinator.com OR site:producthunt.com {query}"
                    trusted_rows.extend(search_fallback(trusted_query))

                trusted_seed = normalize_min_schema(trusted_rows, source_name)
                trusted_candidates: list[dict[str, Any]] = []
                for record in trusted_seed:
                    extracted = extract_companies_from_record(record) if isinstance(record, dict) else []
                    if extracted:
                        trusted_candidates.extend(extracted)

                normalized = normalize_company_candidates(trusted_candidates, source_name)
                normalized = [row for row in normalized if passes_minimum_company_validation(row)]
                normalized = dedupe_company_candidates(normalized)
                container_count += sum(1 for row in normalized if str(row.get("entity_type") or "") == "SOURCE_CONTAINER")
                normalized = [row for row in normalized if str(row.get("entity_type") or "REAL_COMPANY") != "SOURCE_CONTAINER"]

            # Soft ingestion filtering and unified schema normalization.
            unified_rows: list[dict[str, Any]] = []
            accepted_entities: list[dict[str, Any]] = []
            rejected_entities: list[dict[str, Any]] = []
            for row in normalized:
                if not isinstance(row, dict):
                    continue
                query_for_row = _clean_text((row.get("raw_fields") if isinstance(row.get("raw_fields"), dict) else {}).get("query") or (queries[0] if queries else ""))
                unified = self._to_unified_schema(row, source_name, query_for_row)
                if not unified.get("name"):
                    continue
                if not unified.get("domain") and not unified.get("description"):
                    continue
                unified = self._attach_ingestion_score(unified, query_for_row)

                validation = validate_company_entity(
                    {
                        "company_name": unified.get("company_name") or unified.get("name"),
                        "domain_hint": unified.get("domain"),
                        "source": unified.get("source"),
                    },
                    {
                        "url": unified.get("source_url") or unified.get("url") or unified.get("website") or unified.get("link"),
                        "source": unified.get("source"),
                        "source_transport": unified.get("source_transport"),
                        "source_type": unified.get("source_type"),
                        "domain": unified.get("domain"),
                    },
                )
                lightweight_validation = validate_entity(
                    {
                        "clean_name": unified.get("company_name") or unified.get("name"),
                        "domain": unified.get("domain"),
                        "url": unified.get("source_url") or unified.get("url") or unified.get("website") or unified.get("link"),
                    }
                )
                entity_confidence = _safe_float(unified.get("entity_confidence") or unified.get("confidence"), 0.0)
                validation_passed = bool(validation.get("validation_passed")) and bool(lightweight_validation.get("validated"))
                reason = _clean_text(validation.get("reason") or "validated")
                final_score = compute_weighted_ingestion_score(
                    entity_confidence,
                    _safe_float(unified.get("ingestion_score"), 0.0),
                    validation_passed,
                )

                unified["company_name"] = validation.get("company_name") or unified.get("company_name") or unified.get("name")
                unified["name"] = unified.get("company_name") or unified.get("name")
                unified["domain"] = validation.get("domain") or unified.get("domain")
                unified["entity_confidence"] = entity_confidence
                unified["validation_passed"] = validation_passed
                unified["validation"] = validation_passed
                unified["reason"] = reason
                unified["final_score"] = final_score
                unified["proofs"] = validation.get("proofs") if isinstance(validation.get("proofs"), list) else []

                if entity_confidence < 0.5 or not validation_passed:
                    log_entity(
                        {
                            "company_name": unified.get("company_name") or unified.get("name"),
                            "source": unified.get("source") or source_name,
                            "reason": reason if entity_confidence >= 0.5 else "low_entity_confidence",
                        },
                        is_good=False,
                    )
                    rejected_entities.append(
                        {
                            "company_name": unified.get("company_name") or unified.get("name"),
                            "source": unified.get("source") or source_name,
                            "reason": reason if entity_confidence >= 0.5 else "low_entity_confidence",
                        }
                    )
                    continue

                accepted_entities.append(
                    {
                        "company_name": unified.get("company_name") or unified.get("name"),
                        "source": unified.get("source") or source_name,
                        "reason": reason,
                    }
                )
                log_entity(
                    {
                        "company_name": unified.get("company_name") or unified.get("name"),
                        "source": unified.get("source") or source_name,
                        "reason": reason,
                    },
                    is_good=True,
                )
                unified_rows.append(unified)

            normalized = unified_rows
            update_ingestion_feedback(
                source=source_name,
                query=_clean_text(queries[0] if queries else ""),
                accepted_entities=accepted_entities,
                rejected_entities=rejected_entities,
            )

            # LLM quality gate for ingestion output; when poor, use all discovery alternatives and merge.
            primary_query = _clean_text(queries[0] if queries else "saas companies")
            quality_score, needs_more, quality_reason = _llm_ingestion_quality(primary_query, normalized)
            logger.info(
                "ingestion quality | source=%s query=%s score=%.2f needs_more=%s reason=%s",
                source_name,
                primary_query,
                quality_score,
                needs_more,
                quality_reason,
            )

            if needs_more:
                feedback_state = load_ingestion_feedback()
                feedback_signals = get_feedback_signals(feedback_state)
                failure_queries = feedback_signals.get("bad_queries") if isinstance(feedback_signals, dict) else []
                retry_queries = refine_queries_with_llm(
                    {
                        "keywords": self._icp_context.get("keywords") or [],
                        "industries": self._icp_context.get("industries") or [],
                        "competitors": self._icp_context.get("competitors") or [],
                        "regions": ["global"],
                    },
                    failure_queries if isinstance(failure_queries, list) else [primary_query],
                )
                if not retry_queries:
                    retry_queries = [_broaden_query_via_llm(primary_query)]
                for q in (queries[:2] if queries else [primary_query]):
                    log_query(str(q), is_good=False)

                alt_unified_rows: list[dict[str, Any]] = []
                for alt_query in retry_queries[:2]:
                    alt_query = _clean_text(alt_query) or _broaden_query_via_llm(primary_query)
                    alt_raw_rows = _collect_alternative_discovery_rows(alt_query)
                    if not alt_raw_rows:
                        continue

                    alt_candidates: list[dict[str, Any]] = []
                    for record in alt_raw_rows:
                        if not isinstance(record, dict):
                            continue
                        extracted = extract_companies_from_record(record)
                        if extracted:
                            alt_candidates.extend(extracted)
                        else:
                            alt_candidates.append(
                                {
                                    "name": str(record.get("title") or record.get("name") or "").strip(),
                                    "source_type": "fallback",
                                    "confidence": 0.2,
                                    "entity_confidence": 0.2,
                                    "context": str(record.get("snippet") or record.get("description") or "").strip(),
                                    "source_url": str(record.get("url") or record.get("link") or record.get("website") or "").strip() or None,
                                    "raw_fields": record,
                                }
                            )

                    alt_normalized = normalize_company_candidates(alt_candidates, source_name)
                    alt_normalized = [row for row in alt_normalized if passes_minimum_company_validation(row)]
                    alt_normalized = dedupe_company_candidates(alt_normalized)
                    alt_normalized = [row for row in alt_normalized if str(row.get("entity_type") or "REAL_COMPANY") != "SOURCE_CONTAINER"]

                    for row in alt_normalized:
                        if not isinstance(row, dict):
                            continue
                        unified = self._to_unified_schema(row, source_name, alt_query)
                        if not unified.get("name"):
                            continue
                        if not unified.get("domain") and not unified.get("description"):
                            continue
                        alt_unified_rows.append(self._attach_ingestion_score(unified, alt_query))

                    if alt_unified_rows:
                        break

                if alt_unified_rows:
                    merged_by_domain: dict[str, dict[str, Any]] = {}
                    for row in normalized + alt_unified_rows:
                        domain_key = _clean_text(row.get("domain") or "").lower() or _clean_text(row.get("name") or "").lower()
                        if not domain_key:
                            continue
                        existing = merged_by_domain.get(domain_key)
                        if not existing:
                            merged_by_domain[domain_key] = row
                        else:
                            if _safe_float(row.get("ingestion_score"), 0.0) > _safe_float(existing.get("ingestion_score"), 0.0):
                                merged_by_domain[domain_key] = row
                    normalized = list(merged_by_domain.values())
            
            # Domain resolution is deferred to the main validation stage to keep ingestion bounded.
            self.total_with_domain += sum(1 for row in normalized if row.get('domain'))
            
            self.total_normalized_rows += len(normalized)
            self.sources_executed[source_name] = {
                'raw_count': len(rows),
                'extracted_count': len(extracted_candidates),
                'normalized_count': len(normalized),
                'with_domain': sum(1 for r in normalized if r.get('domain')),
                'containers_filtered': int(container_count),
            }

            mark_success(source_name)
            self._source_health.append({
                "source": source_name,
                "apify_success": bool(apify_success),
                "fallback_used": bool(fallback_used),
                "rows": len(normalized),
            })
            logger.info(json.dumps(self._source_health[-1]))
            
            return normalized
        
        except Exception as e:
            err = str(e).lower()
            if "404" in err or "not found" in err:
                self._disabled_sources.add(source_name)
            mark_failed(source_name)
            self._source_health.append({"source": source_name, "apify_success": False, "fallback_used": True, "rows": 0})
            logger.info(json.dumps(self._source_health[-1]))
            logger.warning(f"⚠ {source_name}: skipped ({str(e)})")
            return []


def orchestrate_ingestion(queries: List[str], sample_company: str = None) -> List[Dict[str, Any]]:
    """Public API for ingestion orchestration."""
    orchestrator = IngestionOrchestrator()
    return orchestrator.run(queries, sample_company)
