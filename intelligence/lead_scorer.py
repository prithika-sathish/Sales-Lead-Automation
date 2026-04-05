from __future__ import annotations

from typing import Any

from utils.entity_validation import canonical_company_name, is_real_company_entity, normalize_domain


ENTERPRISE_BLOCKLIST = {
    "stripe",
    "zuora",
    "paddle",
    "chargebee",
    "udemy",
    "ibm",
    "oracle",
    "microsoft",
    "google",
    "amazon",
    "meta",
    "salesforce",
    "adobe",
    "freshworks",
    "servicenow",
    "okta",
    "atlassian",
    "shopify",
    "snowflake",
    "datadog",
    "zoom",
    "hubspot",
    "zendesk",
}

POPULAR_COMPANY_HINTS = {
    "inc",
    "llc",
    "corp",
    "ltd",
    "group",
    "systems",
    "labs",
}

B2C_KEYWORDS = ["consumer app", "ecommerce marketplace", "direct to consumer", "social media app", "gaming app"]
BUSINESS_EVIDENCE_KEYWORDS = [
    "pricing",
    "billing",
    "subscription",
    "recurring revenue",
    "revenue",
    "saas",
    "api",
    "platform",
    "payments",
    "checkout",
    "invoice",
    "automation",
]
HARD_ACTIVITY_KEYWORDS = [
    "hiring",
    "careers",
    "jobs",
    "launch",
    "launched",
    "product update",
    "changelog",
    "expansion",
    "multi-currency",
    "global",
    "new pricing",
]


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _lower_tokens(value: object) -> list[str]:
    return [token for token in _clean_text(value).lower().replace("/", " ").replace("-", " ").split() if token]


def _first_region(row: dict[str, Any]) -> str:
    region = _clean_text(row.get("region"))
    if region:
        return region
    regions = row.get("regions") if isinstance(row.get("regions"), list) else []
    for item in regions:
        cleaned = _clean_text(item)
        if cleaned:
            return cleaned
    return ""


def _has_business_evidence(summary: str, website: str = "") -> bool:
    text = f"{summary} {website}".lower()
    return any(keyword in text for keyword in BUSINESS_EVIDENCE_KEYWORDS)


def _validate_company(company: str, domain: str, summary: str, website: str) -> bool:
    if not company or not domain:
        return False
    if not is_real_company_entity(company=company, domain=domain, description=summary, url=website):
        return False
    if not _has_business_evidence(summary, website):
        return False
    return True


def _is_likely_large_enterprise(company: str, domain: str, text: str) -> bool:
    company_l = company.lower()
    domain_label = normalize_domain(domain).split(".")[0].lower() if normalize_domain(domain) else ""
    if company_l in ENTERPRISE_BLOCKLIST or domain_label in ENTERPRISE_BLOCKLIST:
        return True
    lowered = text.lower()
    return any(token in lowered for token in ["fortune 500", "global leader", "multinational", "public company", "10,000+", "5,000+"])


def _fit_score(row: dict[str, Any]) -> tuple[int, list[str]]:
    summary = _clean_text(row.get("summary") or row.get("description") or row.get("snippet"))
    signals = row.get("signals") if isinstance(row.get("signals"), dict) else {}
    product_relevance = bool(row.get("product_relevance"))
    b2b = bool(signals.get("b2b"))
    mid_size_candidate = bool(row.get("mid_size_candidate"))

    lowered = summary.lower()
    score = 0
    reasons: list[str] = []

    if any(token in lowered for token in ["subscription", "billing", "revenue", "pricing"]):
        score += 2
        reasons.append("subscription or billing model")
    if any(token in lowered for token in ["saas", "api", "platform", "payments"]):
        score += 1
        reasons.append("SaaS/API orientation")
    if product_relevance:
        score += 1
        reasons.append("product relevance")
    if b2b:
        score += 1
        reasons.append("B2B focus")
    if mid_size_candidate:
        score += 1
        reasons.append("mid-size operating signal")

    return max(0, min(5, score)), reasons[:3]


def _intent_score(row: dict[str, Any]) -> tuple[int, list[str]]:
    summary = _clean_text(row.get("summary") or row.get("description") or row.get("snippet"))
    signals = row.get("signals") if isinstance(row.get("signals"), dict) else {}

    hiring = bool(signals.get("hiring"))
    funding = bool(signals.get("funding"))
    region_match = bool(signals.get("region_match"))
    hiring_velocity = bool(signals.get("hiring_velocity")) or int(row.get("hiring_roles_count") or 0) >= 2

    lowered = summary.lower()
    score = 0
    reasons: list[str] = []

    if hiring or hiring_velocity:
        score += 2
        reasons.append("hiring activity")
    if funding:
        score += 1
        reasons.append("growth or funding signal")
    if region_match:
        score += 1
        reasons.append("region match")
    if any(token in lowered for token in ["expansion", "multi-currency", "pricing change", "new billing", "payment change"]):
        score += 1
        reasons.append("operational change")

    return max(0, min(5, score)), reasons[:3]


def _emerging_score(row: dict[str, Any]) -> tuple[int, list[str]]:
    summary = _clean_text(row.get("summary") or row.get("description") or row.get("snippet"))
    signals = row.get("signals") if isinstance(row.get("signals"), dict) else {}
    source_types = row.get("source_types") if isinstance(row.get("source_types"), list) else []
    tags = row.get("tags") if isinstance(row.get("tags"), list) else []

    hiring_velocity = bool(signals.get("hiring_velocity")) or int(row.get("hiring_roles_count") or 0) >= 2
    product_relevance = bool(row.get("product_relevance"))
    funding = bool(signals.get("funding"))
    mid_size_candidate = bool(row.get("mid_size_candidate"))
    source_count = len({str(item or "").strip().lower() for item in source_types if str(item or "").strip()})

    lowered = summary.lower()
    score = 0
    reasons: list[str] = []

    if hiring_velocity:
        score += 2
        reasons.append("hiring velocity")
    if any(token in lowered for token in ["launched", "launch", "updated", "changelog", "new product", "new pricing"]):
        score += 1
        reasons.append("recent activity")
    if product_relevance or any(token in lowered for token in ["api", "platform", "saas", "billing", "automation"]):
        score += 1
        reasons.append("modern product stack")
    if any(token in lowered for token in ["expansion", "multi-currency", "global rollout", "international", "new region"]):
        score += 1
        reasons.append("growth expansion")
    if mid_size_candidate and source_count >= 1 and not funding:
        score += 1
        reasons.append("less popular operating company")
    if any(str(tag).lower() in {"saas", "api", "platform", "billing", "revenue", "payments"} for tag in tags):
        score += 1
        reasons.append("entity-fit tags")

    return max(0, min(5, score)), reasons[:3]


def _company_stage(fit_score: int, intent_score: int, emerging_score: int) -> str:
    if emerging_score >= 4:
        return "emerging"
    if emerging_score <= 1 and fit_score >= 3 and intent_score >= 2:
        return "established"
    return "mid"


def _build_signals(row: dict[str, Any]) -> dict[str, Any]:
    signals = row.get("signals") if isinstance(row.get("signals"), dict) else {}
    return {
        "hiring": bool(signals.get("hiring")),
        "funding": bool(signals.get("funding")),
        "b2b": bool(signals.get("b2b")),
        "region_match": bool(signals.get("region_match")),
        "hiring_velocity": bool(signals.get("hiring_velocity")) or int(row.get("hiring_roles_count") or 0) >= 2,
        "product_relevance": bool(row.get("product_relevance")),
    }


def _canonical_region(row: dict[str, Any]) -> str:
    return _first_region(row)


def _score_candidate(row: dict[str, Any], *, source_kind: str) -> dict[str, Any] | None:
    domain = normalize_domain(row.get("domain") or "")
    company = canonical_company_name(row.get("company") or row.get("company_name") or "", domain)
    summary = _clean_text(row.get("summary") or row.get("description") or row.get("snippet"))
    website = _clean_text(row.get("website") or row.get("url"))

    if not company or not domain:
        return None
    if not _validate_company(company, domain, summary, website):
        return None
    if _is_likely_large_enterprise(company, domain, summary):
        return None

    fit_score, fit_reasons = _fit_score(row)
    intent_score, intent_reasons = _intent_score(row)
    emerging_score, emerging_reasons = _emerging_score(row)

    base_score = (0.4 * fit_score) + (0.3 * intent_score) + (0.3 * emerging_score)
    final_score = int(round(base_score * 20))

    signals = _build_signals(row)
    if signals.get("hiring"):
        final_score += 5
    if signals.get("funding"):
        final_score += 3
    if not signals.get("product_relevance") and not signals.get("hiring") and not signals.get("funding"):
        final_score -= 5

    company_l = company.lower()
    domain_label = domain.split(".")[0].lower() if domain else ""
    if company_l in ENTERPRISE_BLOCKLIST or domain_label in ENTERPRISE_BLOCKLIST:
        final_score -= 15
        emerging_reasons.append("well-known brand penalty")

    final_score = max(0, min(100, final_score))
    if final_score < 40:
        return None

    region = _canonical_region(row)
    why_parts = []
    if fit_reasons:
        why_parts.append(", ".join(fit_reasons[:2]))
    if intent_reasons:
        why_parts.append(", ".join(intent_reasons[:2]))
    if emerging_reasons:
        why_parts.append(", ".join(emerging_reasons[:2]))
    if not why_parts:
        why_parts.append("high signal company match")

    size_category = _company_stage(fit_score, intent_score, emerging_score)

    return {
        "company_name": company,
        "company": company,
        "domain": domain,
        "region": region,
        "why_it_matches": "; ".join(why_parts[:3]),
        "signals": signals,
        "fit_score": fit_score,
        "intent_score": intent_score,
        "emerging_score": emerging_score,
        "final_score": final_score,
        "score": final_score,
        "size_category": size_category,
        "stage": size_category,
        "source_kind": source_kind,
    }


def _sort_key(item: dict[str, Any]) -> tuple[int, int, int, str]:
    stage_priority = {"emerging": 2, "mid": 1, "established": 0}
    stage = str(item.get("stage") or item.get("size_category") or "mid").lower()
    return (
        int(item.get("final_score") or item.get("score") or 0),
        int(item.get("emerging_score") or 0),
        stage_priority.get(stage, 1),
        str(item.get("company_name") or item.get("company") or "").lower(),
    )


def _select_balanced(rows: list[dict[str, Any]], hard_limit: int) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {"emerging": [], "mid": [], "established": []}
    for row in rows:
        stage = str(row.get("stage") or row.get("size_category") or "mid").lower()
        if stage not in buckets:
            stage = "mid"
        buckets[stage].append(row)

    for bucket in buckets.values():
        bucket.sort(key=_sort_key, reverse=True)

    quotas = {
        "emerging": max(1, int(hard_limit * 0.35)),
        "mid": max(1, int(hard_limit * 0.35)),
        "established": max(1, hard_limit - int(hard_limit * 0.35) - int(hard_limit * 0.35)),
    }

    selected: list[dict[str, Any]] = []
    used_domains: set[str] = set()

    for stage in ("emerging", "mid", "established"):
        quota = quotas[stage]
        for row in buckets[stage]:
            if len(selected) >= hard_limit or quota <= 0:
                break
            domain = str(row.get("domain") or "").strip().lower()
            if not domain or domain in used_domains:
                continue
            selected.append(row)
            used_domains.add(domain)
            quota -= 1
        quotas[stage] = quota

    if len(selected) < hard_limit:
        combined = sorted(rows, key=_sort_key, reverse=True)
        for row in combined:
            if len(selected) >= hard_limit:
                break
            domain = str(row.get("domain") or "").strip().lower()
            if not domain or domain in used_domains:
                continue
            selected.append(row)
            used_domains.add(domain)

    return selected[:hard_limit]


def score_and_rank_companies(
    companies: list[dict[str, Any]],
    *,
    target_min: int = 10,
    target_max: int = 10,
) -> list[dict[str, Any]]:
    scored: list[dict[str, Any]] = []
    for row in companies:
        if not isinstance(row, dict):
            continue
        candidate = _score_candidate(row, source_kind="enriched")
        if candidate:
            scored.append(candidate)

    scored.sort(key=_sort_key, reverse=True)
    hard_limit = max(int(target_max or 0), int(target_min or 0), 1)
    balanced = _select_balanced(scored, hard_limit)

    if len(balanced) < max(1, int(target_min or 0)):
        # Relax emerging mix slightly before dropping below target_min.
        extra: list[dict[str, Any]] = []
        seen_domains = {str(item.get("domain") or "").strip().lower() for item in balanced}
        for row in scored:
            domain = str(row.get("domain") or "").strip().lower()
            if not domain or domain in seen_domains:
                continue
            extra.append(row)
            seen_domains.add(domain)
            if len(balanced) + len(extra) >= max(1, int(target_min or 0)):
                break
        balanced.extend(extra)

    output: list[dict[str, Any]] = []
    seen_domains: set[str] = set()
    for item in balanced:
        domain = str(item.get("domain") or "").strip().lower()
        if not domain or domain in seen_domains:
            continue
        seen_domains.add(domain)
        output.append(
            {
                "company_name": str(item.get("company_name") or ""),
                "company": str(item.get("company_name") or ""),
                "domain": str(item.get("domain") or ""),
                "region": str(item.get("region") or ""),
                "why_it_matches": str(item.get("why_it_matches") or ""),
                "signals": item.get("signals") if isinstance(item.get("signals"), dict) else {},
                "fit_score": int(item.get("fit_score") or 0),
                "intent_score": int(item.get("intent_score") or 0),
                "emerging_score": int(item.get("emerging_score") or 0),
                "final_score": int(item.get("final_score") or item.get("score") or 0),
                "score": int(item.get("final_score") or item.get("score") or 0),
                "size_category": str(item.get("size_category") or "mid"),
                "stage": str(item.get("stage") or item.get("size_category") or "mid"),
            }
        )

    return output[:hard_limit]


def build_ranked_fallback_from_source_rows(
    source_rows: list[dict[str, Any]],
    *,
    existing_domains: set[str],
    target_count: int,
    allowed_regions: list[str],
) -> list[dict[str, Any]]:
    allowed = {str(item or "").strip().lower() for item in allowed_regions if str(item or "").strip()}
    fallback_candidates: list[dict[str, Any]] = []

    for row in source_rows:
        if not isinstance(row, dict):
            continue

        domain = normalize_domain(row.get("domain") or row.get("website") or "")
        if not domain or domain in existing_domains:
            continue

        company = canonical_company_name(row.get("company_name") or row.get("company") or "", domain)
        context = _clean_text(row.get("context") or row.get("description") or row.get("snippet") or "")
        if not company:
            continue

        signal_type = _clean_text(row.get("signal_type")).lower()
        source = _clean_text(row.get("source")).lower()
        region = _clean_text(row.get("region"))
        industry = _clean_text(row.get("industry"))

        signal_payload = {
            "hiring": any(token in f"{context} {signal_type}".lower() for token in ["hiring", "careers", "jobs", "sdr", "growth"]),
            "funding": any(token in f"{context} {signal_type}".lower() for token in ["funding", "series a", "series b", "raised", "expansion"]),
            "b2b": any(token in f"{context} {industry} {signal_type}".lower() for token in ["b2b", "saas", "api", "platform", "enterprise", "payments", "subscription", "billing"]),
            "region_match": bool(region and region.lower() in allowed),
            "hiring_velocity": any(token in context.lower() for token in ["open roles", "hiring", "careers", "jobs"]),
            "product_relevance": any(token in f"{context} {industry}".lower() for token in ["saas", "api", "platform", "billing", "subscription", "payments", "revenue"]),
        }

        row_payload = {
            "company_name": company,
            "domain": domain,
            "website": _clean_text(row.get("website") or row.get("url") or ""),
            "summary": context,
            "description": context,
            "snippet": context,
            "signals": signal_payload,
            "product_relevance": bool(signal_payload["product_relevance"]),
            "mid_size_candidate": bool(signal_payload["b2b"]),
            "hiring_roles_count": 2 if signal_payload["hiring_velocity"] else 0,
            "source_types": [source] if source else [],
            "regions": [region] if region else [],
            "tags": [industry] if industry else [],
        }
        scored = _score_candidate(row_payload, source_kind="fallback")
        if not scored:
            continue
        fallback_candidates.append(scored)

    fallback_candidates.sort(key=_sort_key, reverse=True)

    output: list[dict[str, Any]] = []
    seen_domains: set[str] = set(existing_domains)
    for row in fallback_candidates:
        domain = str(row.get("domain") or "").strip().lower()
        if not domain or domain in seen_domains:
            continue
        seen_domains.add(domain)
        output.append(
            {
                "company_name": str(row.get("company_name") or ""),
                "company": str(row.get("company_name") or ""),
                "domain": str(row.get("domain") or ""),
                "region": str(row.get("region") or ""),
                "why_it_matches": str(row.get("why_it_matches") or ""),
                "signals": row.get("signals") if isinstance(row.get("signals"), dict) else {},
                "fit_score": int(row.get("fit_score") or 0),
                "intent_score": int(row.get("intent_score") or 0),
                "emerging_score": int(row.get("emerging_score") or 0),
                "final_score": int(row.get("final_score") or row.get("score") or 0),
                "score": int(row.get("final_score") or row.get("score") or 0),
                "size_category": str(row.get("size_category") or "mid"),
                "stage": str(row.get("stage") or row.get("size_category") or "mid"),
            }
        )
        if len(output) >= max(0, int(target_count)):
            break

    return output
