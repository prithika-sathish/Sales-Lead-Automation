from __future__ import annotations

import json
import logging
import os
import re
import importlib
from difflib import SequenceMatcher
from pathlib import Path
from collections import Counter
from typing import Any, TypedDict
from urllib.parse import urlparse

from discovery.tavily_search import fallback_search, search_tavily


logger = logging.getLogger(__name__)


class CompanyCandidate(TypedDict):
    name: str
    website: str
    source_query: str
    snippet: str
    penalty: int
    domain_score: int


class ExtractedEntity(TypedDict):
    company_name: str
    source_url: str
    source_query: str
    confidence: float
    website: str
    name: str


BLOCKED_TITLE_KEYWORDS = [
    "top",
    "best",
    "list",
    "guide",
    "report",
    "pdf",
    "insights",
    "trends",
    "companies in",
    "directory",
    "association",
    "institute",
    "foundation",
    "council",
    "forum",
    "community",
    "2025",
    "2026",
]

COMMON_WORDS = {
    "market",
    "growth",
    "report",
    "analysis",
    "guide",
    "blog",
    "companies",
    "company",
    "startup",
    "startups",
    "top",
    "best",
    "directory",
    "list",
    "business",
    "businesses",
    "subscription",
    "subscriptions",
    "digital",
    "model",
    "models",
    "ideas",
    "size",
    "start",
    "reach",
    "ecommerce",
    "e-commerce",
    "newsletter",
    "newsletters",
}

GEO_FRAGMENTS = {
    "ny",
    "u.s.",
    "us",
    "u.k.",
    "uk",
    "india",
    "europe",
    "asia",
    "apac",
    "emea",
    "chennai",
    "middle east",
    "north america",
    "south east asia",
    "southeast asia",
    "latin america",
}

BLOCKED_DOMAINS = [
    "forbes",
    "medium",
    "blog",
    "news",
    "analytics",
    "report",
    "education",
    "magazine",
    "indeed",
    "glassdoor",
    "naukri",
    "linkedin.com/jobs",
]

COMPETITOR_KEYWORDS = [
    "billing",
    "subscription management",
    "revenue platform",
    "invoicing software",
    "payment infrastructure",
    "billing saas",
]


_EXCLUDED_DOMAINS = {
    "google.com",
    "youtube.com",
    "facebook.com",
    "instagram.com",
    "x.com",
    "twitter.com",
    "linkedin.com",
    "wikipedia.org",
    "reddit.com",
    "crunchbase.com",
    "ycombinator.com",
    "angel.co",
    "wellfound.com",
    "github.com",
    "news.ycombinator.com",
}


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _clean_document_text(value: object) -> str:
    text = _clean_text(value)
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"\b\d+[\d,.]*\b", " ", text)
    text = re.sub(r"[^A-Za-z0-9&\-.,:;()\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _normalize_company_name(name: str) -> str:
    cleaned = _clean_text(name)
    cleaned = re.sub(r"^(top\s*\d+|top|best|list\s+of)\s+", "", cleaned, flags=re.I).strip()
    cleaned = re.sub(r"\s*\((?:u\.s\.|us|uk|india|europe|apac|emea)\)$", "", cleaned, flags=re.I).strip()
    cleaned = re.sub(r"\s*\([^)]*$", "", cleaned).strip()
    cleaned = cleaned.strip("-–—:;,.")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.title()


def _is_bad_fragment(name: str) -> bool:
    lowered = _clean_text(name).lower()
    tokens = [token for token in re.split(r"\W+", lowered) if token]
    if len(lowered) < 2:
        return True
    if lowered in COMMON_WORDS:
        return True
    if any(word in lowered for word in ["market", "report", "analysis", "guide", "blog"]):
        return True
    if lowered in GEO_FRAGMENTS:
        return True
    if re.fullmatch(r"[\w\.-]+", lowered) and len(lowered) <= 2:
        return True
    if any(fragment == lowered for fragment in ["u.s.", "u.k.", "ny", "usa"]):
        return True
    if tokens and all(token in COMMON_WORDS or token in GEO_FRAGMENTS for token in tokens):
        return True
    return False


def _has_generic_phrase(name: str) -> bool:
    lowered = _clean_text(name).lower()
    generic_keywords = ["market", "size", "growth", "ideas", "products", "examples", "report", "analysis"]
    return any(keyword in lowered for keyword in generic_keywords)


def _looks_like_geography(name: str) -> bool:
    lowered = _clean_text(name).lower()
    geo_terms = [
        "india",
        "usa",
        "europe",
        "chennai",
        "middle east",
        "north america",
        "asia",
        "apac",
        "emea",
        "u.s.",
        "u.k.",
        "uk",
        "us",
    ]
    return any(term in lowered for term in geo_terms)


def _contains_generic_phrase(name: str) -> bool:
    lowered = _clean_text(name).lower()
    generic_parts = ["subscription", "business", "digital"]
    return any(part in lowered for part in generic_parts)


def _is_invalid_company_name(name: str) -> bool:
    normalized = _normalize_company_name(name)
    if not normalized:
        return True
    if len(normalized) < 3:
        return True
    if normalized.lower() in COMMON_WORDS:
        return True
    if _has_generic_phrase(normalized):
        return True
    if _looks_like_geography(normalized):
        return True
    if _contains_generic_phrase(normalized):
        return True
    if normalized.islower():
        return True
    if _name_word_count(normalized) > 3:
        return True
    if any(normalized.lower().endswith(suffix) for suffix in ("ing", "ed")):
        return True
    if any(token in _clean_text(normalized).lower() for token in ["blog", "report", "insights"]):
        return True
    if any(word in normalized.lower() for word in ["&"]):
        if any(word in normalized.lower() for word in ["cross", "upsell", "growth", "business"]):
            return True
    return False


def _company_confidence_score(name: str, text: str, source_url: str) -> float:
    normalized = _normalize_company_name(name)
    if not normalized or _is_invalid_company_name(normalized):
        return 0.0

    score = 0.0
    lowered_name = normalized.lower()
    lowered_text = _clean_text(text).lower()

    if any(part[:1].isupper() for part in _clean_text(name).split() if part):
        score += 1.0

    if normalized in text:
        score += 1.0

    if any(token in lowered_text for token in ["company", "startup", "platform"]):
        score += 1.0

    if any(token in lowered_text for token in ["companies include", "top players", "company list", "key companies", "list of"]):
        score += 2.0

    if lowered_name and lowered_name in lowered_text:
        score += 0.5

    return round(score, 2)


def _fuzzy_company_key(name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]", "", _normalize_company_name(name).lower())
    return normalized


def _is_similar_company(existing: str, incoming: str) -> bool:
    left = _fuzzy_company_key(existing)
    right = _fuzzy_company_key(incoming)
    if not left or not right:
        return False
    if left == right:
        return True
    return SequenceMatcher(None, left, right).ratio() >= 0.88


def _should_keep_company(name: str, source_url: str, text: str, *, source_domain: str) -> bool:
    normalized = _normalize_company_name(name)
    if not normalized:
        return False
    if _is_invalid_company_name(normalized):
        return False
    if _matches_source_domain(normalized, source_url):
        return False
    if _clean_text(normalized).lower() == _clean_text(source_domain).lower():
        return False
    if normalized.lower() in COMMON_WORDS:
        return False
    if len(normalized) < 3:
        return False
    return True


def _name_word_count(name: str) -> int:
    return len([part for part in _clean_text(name).split() if part])


def _looks_entity_like(name: str) -> bool:
    count = _name_word_count(name)
    if not (1 <= count <= 4):
        return False
    if _clean_text(name).islower():
        return False
    lowered = _clean_text(name).lower()
    if _is_bad_fragment(lowered):
        return False
    return True


def _load_spacy_nlp() -> Any:
    try:
        spacy = importlib.import_module("spacy")
    except Exception:  # noqa: BLE001
        return None

    for model_name in ("en_core_web_sm", "en_core_web_md", "en_core_web_lg"):
        try:
            return spacy.load(model_name)
        except Exception:  # noqa: BLE001
            continue
    return None


_SPACY_NLP = _load_spacy_nlp()


def _source_domain(url: str) -> str:
    parsed = urlparse(_clean_text(url))
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _domain_contains_company(domain: str, company_name: str) -> bool:
    domain_tokens = re.split(r"[\.-]", _clean_text(domain).lower())
    company_tokens = [token for token in re.split(r"\W+", _clean_text(company_name).lower()) if token]
    company_tokens = [token for token in company_tokens if token not in COMMON_WORDS]
    if not domain_tokens or not company_tokens:
        return False
    joined_domain = " ".join(domain_tokens)
    return any(token in joined_domain for token in company_tokens if len(token) >= 3)


def _matches_source_domain(entity_name: str, source_url: str) -> bool:
    entity_tokens = [token for token in re.split(r"\W+", _clean_text(entity_name).lower()) if token]
    domain_label = _normalize_domain(source_url).split(".")[0].lower()
    if not entity_tokens or not domain_label:
        return False
    entity_joined = "".join(entity_tokens)
    domain_joined = re.sub(r"[^a-z0-9]", "", domain_label)
    return entity_joined == domain_joined or domain_label in entity_tokens


def _extract_list_entities(text: str) -> list[str]:
    hits: list[str] = []
    patterns = [
        r"companies\s+include\s+([^\.\n]+)",
        r"top\s+players\s*[:\-]\s*([^\.\n]+)",
        r"company\s+list\s*[:\-]\s*([^\.\n]+)",
        r"key\s+companies\s*[:\-]\s*([^\.\n]+)",
        r"list\s+of\s+([^\.\n]+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I)
        if not match:
            continue
        segment = match.group(1)
        segment = re.sub(r"\b(?:companies|company|players|startups|brands|vendors)\b", "", segment, flags=re.I)
        parts = re.split(r",|;|\band\b|/", segment, flags=re.I)
        for part in parts:
            candidate = _normalize_company_name(part)
            if candidate and candidate not in hits and _looks_entity_like(candidate):
                hits.append(candidate)

    return hits


def _extract_org_entities(text: str) -> list[str]:
    candidates: list[str] = []
    cleaned = _clean_document_text(text)
    if not cleaned:
        return candidates

    if _SPACY_NLP is not None:
        try:
            doc = _SPACY_NLP(cleaned)
            for ent in getattr(doc, "ents", []):
                if getattr(ent, "label_", "") != "ORG":
                    continue
                candidate = _normalize_company_name(ent.text)
                if candidate and candidate not in candidates and _looks_entity_like(candidate):
                    candidates.append(candidate)
        except Exception:  # noqa: BLE001
            pass

    # Regex/list-pattern fallback for pages that enumerate companies.
    for candidate in _extract_list_entities(cleaned):
        if candidate not in candidates:
            candidates.append(candidate)

    return candidates


def _candidate_confidence(company_name: str, text: str, source_url: str) -> float:
    score = 0.0
    lowered_text = _clean_text(text).lower()
    lowered_name = _clean_text(company_name).lower()

    if _SPACY_NLP is not None:
        try:
            doc = _SPACY_NLP(_clean_document_text(text))
            if any(_clean_text(getattr(ent, "text", "")).lower() == lowered_name for ent in getattr(doc, "ents", []) if getattr(ent, "label_", "") == "ORG"):
                score += 0.55
        except Exception:  # noqa: BLE001
            pass

    if lowered_name and lowered_name in lowered_text:
        score += 0.15

    if sum(1 for token in re.split(r"\W+", lowered_name) if token and token in lowered_text) >= 2:
        score += 0.1

    freq = len(re.findall(re.escape(company_name), text, flags=re.I))
    if freq > 1:
        score += min(0.1, freq * 0.02)

    return round(min(score, 0.99), 2)


def _normalize_domain(url: str) -> str:
    parsed = urlparse(_clean_text(url))
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _looks_company_like_domain(domain: str) -> bool:
    if not domain or domain in _EXCLUDED_DOMAINS:
        return False
    if domain.endswith(".gov") or domain.endswith(".edu"):
        return False
    if any(domain.endswith(f".{blocked}") for blocked in _EXCLUDED_DOMAINS):
        return False
    return bool(re.search(r"\.[a-z]{2,}$", domain))


def _guess_company_name(title: str, website: str) -> str:
    text = _clean_text(title)
    if text:
        parts = [part.strip() for part in re.split(r"\s*[\-|:]\s*", text) if part.strip()]
        first = parts[0] if parts else ""
        first_lower = first.lower()
        if any(first_lower.startswith(prefix) for prefix in ["top ", "best ", "list of "]) and len(parts) > 1:
            first = parts[1]
        first = re.sub(r"^(top\s*\d+|top|best|list\s+of)\s+", "", first, flags=re.I).strip()
        first = _clean_text(first.split(",")[0])
        if first and len(first.split()) <= 8:
            return first

    domain = _normalize_domain(website)
    token = domain.split(".")[0] if domain else ""
    token = token.replace("-", " ").replace("_", " ")
    return " ".join(part.capitalize() for part in token.split())


def _extract_entity_names(title: str, snippet: str) -> list[str]:
    text = _clean_text(f"{title} | {snippet}")
    if not text:
        return []

    parts = [part.strip() for part in re.split(r"[|:]", text) if part.strip()]
    corpus = " ".join(parts)
    raw_chunks = [chunk.strip() for chunk in re.split(r",|\band\b|;", corpus, flags=re.I) if chunk.strip()]

    entities: list[str] = []
    for chunk in raw_chunks:
        candidate = re.sub(r"^(top\s*\d+|top|best|list\s+of)\s+", "", chunk, flags=re.I).strip()
        candidate = re.sub(r"\s+companies?$", "", candidate, flags=re.I).strip()
        candidate = re.sub(r"\s+startups?$", "", candidate, flags=re.I).strip()
        candidate = _clean_text(candidate)
        if candidate and candidate not in entities:
            entities.append(candidate)
        if len(entities) >= 10:
            break

    return entities


def is_valid_candidate(name: str, url: str) -> bool:
    name_l = _clean_text(name).lower()
    url_l = _clean_text(url).lower()

    if any(k in name_l for k in BLOCKED_TITLE_KEYWORDS):
        return False

    if any(d in url_l for d in BLOCKED_DOMAINS):
        return False

    if url_l.endswith(".pdf"):
        return False

    return True


def is_not_competitor(name: str, snippet: str) -> bool:
    text = f"{_clean_text(name)} {_clean_text(snippet)}".lower()
    return not any(keyword in text for keyword in COMPETITOR_KEYWORDS)


def _is_entity_like_name(name: str) -> bool:
    clean = _clean_text(name)
    if not clean:
        return False
    words = [word for word in clean.split() if word]
    if not (1 <= len(words) <= 4):
        return False

    lowered = clean.lower()
    blocked_terms = ["market", "report", "analysis", "guide", "blog"]
    if any(term in lowered for term in blocked_terms):
        return False

    return True


def _domain_priority(domain: str) -> int:
    lowered = _clean_text(domain).lower()
    score = 0

    if lowered.endswith(".com"):
        score += 3

    startup_directory_tokens = [
        "tracxn",
        "wellfound",
        "angel",
        "g2",
        "producthunt",
        "startup",
        "crunch",
        "seedtable",
        "topstartups",
        "builtin",
        "failory",
        "crunchbase",
        "venture",
    ]
    if any(token in lowered for token in startup_directory_tokens):
        score += 2

    low_quality_tokens = ["blog", "news", "market", "research", "report", "insight", "article", "magazine"]
    if any(token in lowered for token in low_quality_tokens):
        score -= 3

    return score


def _query_priority(query: str, *, strategy: str = "", priority: str = "") -> int:
    text = _clean_text(query).lower()
    score = 0

    if strategy == "competitor":
        score += 6
    elif strategy == "directory":
        score += 5
    elif strategy == "technology":
        score += 4
    elif strategy == "hiring":
        score += 3
    elif strategy == "region":
        score += 2

    if any(token in text for token in ["companies similar to", "competitors", "list of", "directory", "top", "using stripe", "recurring revenue"]):
        score += 3
    if any(token in text for token in ["blog", "report", "guide", "article", "market report"]):
        score -= 2

    try:
        score += int(priority)
    except Exception:  # noqa: BLE001
        pass

    return score


def _process_rows(
    rows: list[dict[str, Any]],
    query: str,
    raw_candidates: list[ExtractedEntity],
    dedup: dict[str, ExtractedEntity],
) -> None:
    for row in rows:
        website = _clean_text(row.get("website") or row.get("url") or row.get("link"))
        title = _clean_text(row.get("name") or row.get("title"))
        snippet = _clean_text(row.get("snippet") or row.get("content"))

        if not website:
            continue

        combined_text = _clean_document_text(f"{title} {snippet}")
        candidates = _extract_org_entities(combined_text)
        list_entities = _extract_list_entities(combined_text)
        for list_entity in list_entities:
            if list_entity not in candidates:
                candidates.append(list_entity)

        source_domain = _source_domain(website)

        for entity_name in candidates:
            normalized = _normalize_company_name(entity_name)
            if not normalized or not _looks_entity_like(normalized):
                continue
            if _is_bad_fragment(normalized):
                continue
            if not _should_keep_company(normalized, website, combined_text, source_domain=source_domain):
                continue

            source_url = website
            confidence = _company_confidence_score(normalized, combined_text, source_url)
            if normalized in list_entities:
                confidence += 0.2
            if any(token in combined_text.lower() for token in ["company", "startup", "platform"]):
                confidence += 0.05
            if not is_not_competitor(normalized, snippet):
                confidence = max(0.0, confidence - 0.15)

            if confidence < 1.5:
                continue

            raw_candidates.append(
                {
                    "company_name": normalized,
                    "source_url": source_url,
                    "source_query": query,
                    "confidence": confidence,
                    "website": source_url,
                    "name": normalized,
                }
            )

            dedup_key = f"{normalized.lower()}|{source_url.lower()}"
            candidate: ExtractedEntity = {
                "company_name": normalized,
                "source_url": source_url,
                "source_query": query,
                "confidence": confidence,
                "website": source_url,
                "name": normalized,
            }

            if any(_is_similar_company(existing.get("company_name", ""), normalized) for existing in dedup.values()):
                existing_match = next(
                    (key for key, existing in dedup.items() if _is_similar_company(existing.get("company_name", ""), normalized)),
                    "",
                )
                if existing_match:
                    if float(candidate["confidence"]) > float(dedup[existing_match].get("confidence", 0.0)):
                        dedup[existing_match] = candidate
                    continue

            existing = dedup.get(dedup_key)
            if not existing or float(candidate["confidence"]) > float(existing.get("confidence", 0.0)):
                dedup[dedup_key] = candidate


def _transform_for_pipeline(entity_rows: list[ExtractedEntity]) -> list[CompanyCandidate]:
    transformed: list[CompanyCandidate] = []
    for item in entity_rows:
        source_url = _clean_text(item.get("source_url"))
        website = _clean_text(item.get("website")) or source_url
        domain_score = max(0, _domain_priority(_normalize_domain(source_url)))
        confidence = float(item.get("confidence") or 0.0)
        transformed.append(
            {
                "name": _clean_text(item.get("company_name")),
                "website": website,
                "source_query": _clean_text(item.get("source_query")) or source_url,
                "snippet": "",
                "penalty": 0 if domain_score >= 0 else abs(domain_score),
                "domain_score": int(domain_score + round(confidence * 10)),
                "company_name": _clean_text(item.get("company_name")),
                "source_url": source_url,
                "confidence": float(item.get("confidence") or 0.0),
            }
        )
    return transformed

def discover_companies(
    queries: list[str | dict[str, Any]],
    *,
    num_results_per_query: int = 20,
) -> list[CompanyCandidate]:
    api_key = _clean_text(os.getenv("TAVILY_API_KEY"))
    if not api_key:
        logger.warning("[SOURCE FAILED] tavily reason=missing_api_key")
        return []

    query_plan: list[dict[str, Any]] = []
    for item in queries:
        if isinstance(item, dict):
            query_text = _clean_text(item.get("query"))
            if not query_text:
                continue
            query_plan.append(
                {
                    "query": query_text,
                    "region": _clean_text(item.get("region")),
                    "industry": _clean_text(item.get("industry")),
                    "strategy": _clean_text(item.get("strategy")),
                    "priority": _clean_text(item.get("priority")),
                }
            )
        else:
            query_text = _clean_text(item)
            if query_text:
                query_plan.append({"query": query_text, "region": "", "industry": "", "strategy": "", "priority": "0"})

    if not query_plan:
        return []

    query_plan.sort(
        key=lambda row: _query_priority(
            str(row.get("query") or ""),
            strategy=str(row.get("strategy") or ""),
            priority=str(row.get("priority") or "0"),
        ),
        reverse=True,
    )

    dedup: dict[str, ExtractedEntity] = {}
    raw_pool: list[ExtractedEntity] = []
    raw_count = 0

    for query_row in query_plan:
        query = str(query_row.get("query") or "").strip()
        try:
            results = search_tavily(query, max_results=num_results_per_query)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[SOURCE FAILED] tavily query=%s err=%s", query, exc)
            try:
                results = fallback_search(query, max_results=num_results_per_query)
            except Exception as fallback_exc:  # noqa: BLE001
                logger.warning("[SOURCE FAILED] tavily fallback query=%s err=%s", query, fallback_exc)
                results = []

        raw_count += len(results)
        sorted_results = sorted(
            [result for result in results if isinstance(result, dict)],
            key=lambda row: _query_priority(
                str(row.get("name") or row.get("title") or ""),
                strategy=str(query_row.get("strategy") or ""),
                priority=str(query_row.get("priority") or "0"),
            ),
            reverse=True,
        )
        _process_rows(sorted_results, query, raw_pool, dedup)

    if len(dedup) < 20:
        fallback_queries = [
            {"query": "list of saas companies", "strategy": "directory", "priority": "4"},
            {"query": "list of startups using subscription model", "strategy": "directory", "priority": "4"},
            {"query": "b2b saas companies directory", "strategy": "directory", "priority": "4"},
            {"query": "companies similar to chargebee", "strategy": "competitor", "priority": "6"},
            {"query": "companies using stripe billing", "strategy": "technology", "priority": "5"},
        ]
        for query_row in fallback_queries:
            query = str(query_row.get("query") or "").strip()
            try:
                results = search_tavily(query, max_results=num_results_per_query)
            except Exception:
                results = []
            raw_count += len(results)
            _process_rows([result for result in results if isinstance(result, dict)], query, raw_pool, dedup)

    dedup_candidates = list(dedup.values())
    candidates = _transform_for_pipeline(dedup_candidates)
    candidates.sort(
        key=lambda item: float(item.get("domain_score", 0)) - float(item.get("penalty", 0)),
        reverse=True,
    )

    seen_domains: set[str] = set()
    filtered: list[CompanyCandidate] = []
    for candidate in candidates:
        domain = _normalize_domain(candidate.get("website", ""))
        if not domain or domain in seen_domains:
            continue
        seen_domains.add(domain)
        filtered.append(candidate)

    print("Queries used:", len(query_plan))
    print("Candidates found:", raw_count)

    output_dir = Path("output")
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dump_path = output_dir / "first_layer_raw_candidates.json"
    raw_dump_path.write_text(
        json.dumps(
            {
                "queries_used": query_plan,
                "raw_count": raw_count,
                "raw_candidate_count": len(raw_pool),
                "raw_candidates": raw_pool,
                "deduped_candidates": dedup_candidates,
            },
            indent=2,
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )
    print("Saved first-layer raw candidates:", raw_dump_path)

    print("Total raw candidates:", len(filtered))
    if len(raw_pool) < 30:
        print("WARNING: low recall, check queries")
    return filtered[:300]
