from __future__ import annotations

import hashlib
import importlib
import json
import os
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from core.signal_fusion import derive_fused_signals


def _infer_signal_type(raw_type: str, raw_text: str) -> str:
    base = f"{raw_type} {raw_text}".lower()
    if any(token in base for token in ["feature_update", "feature", "roadmap", "release"]):
        return "feature_update"
    if any(token in base for token in ["api_update", "api", "endpoint", "webhook"]):
        return "api_update"
    if any(token in base for token in ["company_update", "update", "announcement"]):
        return "company_update"
    if any(token in base for token in ["competitor_switch", "moving away from", "alternative to", "switch from"]):
        return "competitor_switch"
    if any(token in base for token in ["integration_added", "integration", "connector", "plugin", "marketplace"]):
        return "integration_added"
    if any(token in base for token in ["sales_expansion", "sdr", "account executive", "sales hire"]):
        return "sales_expansion"
    if any(token in base for token in ["product_maturity", "docs", "api reference", "feature pages"]):
        return "product_maturity"
    if any(token in base for token in ["high_momentum", "viral_growth"]):
        return "momentum"
    if "narrative_trend" in base:
        return "narrative_trend"
    if any(token in base for token in ["feature request", "requested feature", "wish list"]):
        return "feature_requests"
    if any(token in base for token in ["traffic", "visitors", "sessions", "pageviews"]):
        return "traffic_growth"
    if any(token in base for token in ["newsletter", "blog", "content", "webinar", "campaign"]):
        return "content_push"
    if any(token in base for token in ["hiring", "job", "role", "recruit"]):
        return "hiring"
    if any(token in base for token in ["repo", "github", "commit", "release"]):
        return "github_activity"
    if any(token in base for token in ["funding", "launch", "milestone", "achievement", "revenue", "client"]):
        return "milestone"
    if any(token in base for token in ["complaint", "downtime", "issue", "churn"]):
        return "customer_pain"
    return "company_update"


def _signal_strength(raw: dict[str, Any], signal_type: str) -> int:
    metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
    text = str(raw.get("raw_text") or "").lower()
    strength = 2

    if signal_type == "hiring":
        freq = int(metadata.get("frequency") or 1)
        seniority = str(metadata.get("seniority") or "").lower()
        strength = min(5, 2 + min(freq, 3))
        if seniority in {"leadership", "senior"}:
            strength = min(5, strength + 1)

    if signal_type == "github_activity":
        stars = int(metadata.get("stars") or 0)
        forks = int(metadata.get("forks") or 0)
        if stars >= 100 or forks >= 50:
            strength = 5
        elif stars >= 30 or forks >= 15:
            strength = 4
        elif stars >= 10 or forks >= 5:
            strength = 3

    if signal_type == "milestone":
        if any(token in text for token in ["funding", "series", "raised", "launch", "revenue", "100k", "1m"]):
            strength = 4
        if any(token in text for token in ["series b", "series c", "profitable", "enterprise deal"]):
            strength = 5

    if signal_type == "customer_pain":
        if any(token in text for token in ["critical", "blocked", "urgent", "outage"]):
            strength = 5
        else:
            strength = 3

    if signal_type == "narrative_trend":
        freq = int(metadata.get("frequency") or 1)
        strength = min(5, 2 + min(freq, 3))

    if signal_type in {"competitor_switch", "integration_added", "sales_expansion", "product_maturity"}:
        strength = max(3, strength)

    if signal_type in {"feature_update", "api_update", "company_update"}:
        strength = max(2, strength)

    if signal_type in {"hiring_spike", "dev_activity", "content_push"}:
        strength = max(4, strength)

    if signal_type == "momentum":
        velocity = float(metadata.get("engagement_velocity") or 0)
        if velocity >= 25:
            strength = 5
        elif velocity >= 10:
            strength = 4

    engagement = metadata.get("engagement") if isinstance(metadata.get("engagement"), dict) else {}
    likes = int(engagement.get("likes") or 0)
    comments = int(engagement.get("comments") or 0)
    if likes + comments >= 100:
        strength = min(5, strength + 1)

    if bool(metadata.get("founder_boost")):
        strength = min(5, strength + 1)

    return max(1, min(5, strength))


def _extract_timestamp(raw: dict[str, Any]) -> str:
    metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
    timestamp = (
        metadata.get("timestamp")
        or metadata.get("created_at")
        or metadata.get("updated_at")
        or metadata.get("pushed_at")
        or metadata.get("date")
        or ""
    )
    if timestamp:
        return str(timestamp)
    return datetime.now(timezone.utc).isoformat()


def _parse_datetime(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _compute_recency_score(timestamp: str) -> int:
    parsed = _parse_datetime(timestamp)
    if not parsed:
        return 1

    now = datetime.now(timezone.utc)
    age_days = max(0, (now - parsed).days)
    if age_days < 7:
        return 5
    if age_days < 30:
        return 4
    if age_days < 90:
        return 3
    return 1


def _signal_id(company: str, signal_type: str, source: str, raw_text: str) -> str:
    payload = f"{company.lower()}|{signal_type.lower()}|{source.lower()}|{raw_text.strip().lower()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _weighted_final_score(
    signal_type: str,
    signal_strength: int,
    recency_score: int,
    source: str,
    metadata: dict[str, Any],
) -> int:
    score = signal_strength * recency_score

    if signal_strength <= 2 and recency_score <= 3:
        score = max(1, score - 3)

    is_fallback = bool(metadata.get("fallback")) or source in {"fallback", "orchestrator"}
    if is_fallback:
        score = max(1, int(round(score * 0.6)))

    strong_boosts = {
        "hiring_spike": 20,
        "infra_scaling": 20,
        "integration_added": 15,
        "product_launch": 15,
        "sales_expansion": 15,
    }
    score += strong_boosts.get(signal_type, 0)
    return max(1, score)


def normalize_signals(raw_signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen_signal_ids: set[str] = set()
    company = ""

    for raw in raw_signals:
        company = str(raw.get("company") or "").strip()
        raw_type = str(raw.get("type") or "").strip()
        raw_text = str(raw.get("raw_text") or "").strip()
        source = str(raw.get("source") or "").strip()
        metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}

        if not company or not raw_text:
            continue

        signal_type = _infer_signal_type(raw_type, raw_text)
        timestamp = _extract_timestamp(raw)
        recency_score = _compute_recency_score(timestamp)
        signal_strength = _signal_strength(raw, signal_type)
        final_score = _weighted_final_score(signal_type, signal_strength, recency_score, source, metadata)
        signal_id = _signal_id(company, signal_type, source, raw_text)

        # Keep moderate recency/strength signals to raise meaningful yield.
        if signal_strength <= 1 and recency_score <= 2:
            continue

        if signal_id in seen_signal_ids:
            continue
        seen_signal_ids.add(signal_id)

        normalized.append(
            {
                "company": company,
                "signal_type": signal_type,
                "signal_strength": signal_strength,
                "timestamp": timestamp,
                "recency_score": recency_score,
                "final_score": final_score,
                "signal_score": final_score,
                "metadata": {**metadata, "raw_text": raw_text},
                "source": source,
                "signal_id": signal_id,
            }
        )

    normalized.extend(derive_fused_signals(company, normalized))
    return normalized


_GENERIC_NAME_PATTERNS = {
    "top companies",
    "best companies",
    "company directory",
    "directory",
    "list of companies",
    "startups list",
}

_INVALID_NAME_TOKENS = {
    "apply",
    "job",
    "role",
    "pricing",
    "info",
    "login",
    "signup",
    "home",
}

_AGGREGATOR_DOMAIN_TOKENS = {
    "glassdoor",
    "indeed",
    "naukri",
    "jooble",
    "linkedin",
    "cutshort",
}

_SOURCE_CONTAINER_DOMAIN_TOKENS = {
    "crunchbase",
    "tracxn",
    "wellfound",
    "angel.co",
    "seedtable",
    "eu-startups",
    "producthunt",
    "reddit",
    "glassdoor",
    "indeed",
}

_GENERIC_CONTENT_DOMAIN_TOKENS = {
    "wikipedia",
    "medium",
    "blogspot",
    "wordpress",
    "quora",
    "news",
    "times",
    "press",
    "wire",
    "magazine",
    "journal",
    "media",
    "businesswire",
    "eu-startups",
    "startups",
    "jobs",
    "careers",
}

_GROWTH_TERMS = ("hiring", "funding", "growth", "series", "expansion")

_KNOWN_DOMAIN_NAME_OVERRIDES = {
    "ril": "Reliance Industries",
    "zoho": "Zoho",
    "tcs": "Tata Consultancy Services",
}

_GEMINI_CACHE: dict[str, tuple[bool, bool, str]] = {}
_GEMINI_CALLS = 0


def _clean_company_name(value: object) -> str:
    name = str(value or "").strip()
    name = re.sub(r"\s+", " ", name)
    return name.strip(" -|,.;:\t\r\n")


def _slugify_name(value: str) -> str:
    lowered = value.lower()
    lowered = re.sub(r"[^a-z0-9]+", "-", lowered)
    lowered = re.sub(r"-+", "-", lowered).strip("-")
    return lowered


def _is_generic_company_name(name: str) -> bool:
    lowered = name.lower().strip()
    if lowered in _GENERIC_NAME_PATTERNS:
        return True
    return any(token in lowered for token in ["top ", "best ", " directory", " list "]) and "company" in lowered


def _extract_host(value: object) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = f"https://{raw}"
    try:
        host = urlparse(raw).netloc.lower()
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def _root_domain(host: str) -> str:
    if not host:
        return ""
    parts = [part for part in host.split(".") if part]
    if len(parts) < 2:
        return ""

    while len(parts) > 2 and parts[0] in {"jobs", "info", "blog", "m", "help", "support", "docs"}:
        parts.pop(0)

    if len(parts) >= 3 and parts[-2] in {"co", "com", "org", "net"}:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def _company_name_from_domain(domain: str) -> str:
    if not domain:
        return ""
    label = domain.split(".")[0].strip().lower()
    if not label:
        return ""
    if label in _KNOWN_DOMAIN_NAME_OVERRIDES:
        return _KNOWN_DOMAIN_NAME_OVERRIDES[label]
    tokens = [token for token in label.replace("_", "-").split("-") if token]
    if not tokens:
        return ""
    return " ".join(token.capitalize() for token in tokens)


def _contains_growth_signal(context: str) -> bool:
    lowered = (context or "").lower()
    return any(token in lowered for token in _GROWTH_TERMS)


def _looks_aggregator_domain(domain: str) -> bool:
    lowered = (domain or "").lower()
    return any(token in lowered for token in _AGGREGATOR_DOMAIN_TOKENS)


def _looks_generic_content_domain(domain: str) -> bool:
    lowered = (domain or "").lower()
    return any(token in lowered for token in _GENERIC_CONTENT_DOMAIN_TOKENS)


def _classify_domain_type(domain: str) -> str:
    lowered = (domain or "").lower()
    if not lowered:
        return "UNKNOWN"
    if any(token in lowered for token in _SOURCE_CONTAINER_DOMAIN_TOKENS):
        return "SOURCE_CONTAINER"
    return "REAL_COMPANY"


def _is_valid_company_identity(name: str, domain: str, context: str) -> bool:
    lowered_name = (name or "").strip().lower()
    lowered_domain = (domain or "").strip().lower()
    domain_label = lowered_domain.split(".")[0] if lowered_domain else ""

    if len(lowered_name) < 3:
        return False
    if any(token in lowered_name for token in _INVALID_NAME_TOKENS):
        return False
    if domain_label in {"info", "pricing", "apply", "login", "signup", "home", "news", "jobs"}:
        return False
    if lowered_domain.endswith(".gov"):
        return False
    if _looks_aggregator_domain(lowered_domain):
        return False
    if _looks_generic_content_domain(lowered_domain) and not _contains_growth_signal(context):
        return False
    return True


def _gemini_assess_candidate(name: str, domain: str, context: str) -> tuple[bool, bool, str]:
    global _GEMINI_CALLS

    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if not api_key:
        return True, False, "gemini_unavailable"

    cache_key = f"{name.lower()}|{domain.lower()}"
    if cache_key in _GEMINI_CACHE:
        return _GEMINI_CACHE[cache_key]

    max_calls = int((os.getenv("GEMINI_MAX_SCREEN_CALLS") or "80").strip() or "80")
    if _GEMINI_CALLS >= max_calls:
        return True, False, "gemini_budget_exhausted"

    try:
        genai_module = importlib.import_module("google.genai")
        client = genai_module.Client(api_key=api_key)
        model_name = (os.getenv("GEMINI_MODEL") or "gemini-2.5-flash").strip()
        prompt = (
            "Classify candidate as lead or not. Return strict JSON with keys: "
            "is_lead (bool), useful_company_list (bool), reason (string).\n"
            f"name={name}\n"
            f"domain={domain}\n"
            f"context={context[:300]}\n"
        )
        response = client.models.generate_content(model=model_name, contents=prompt)
        text = str(getattr(response, "text", "") or getattr(response, "output_text", "") or "")
        match = re.search(r"\{.*\}", text, flags=re.S)
        payload = json.loads(match.group(0) if match else "{}")
        is_lead = bool(payload.get("is_lead", True))
        useful = bool(payload.get("useful_company_list", False))
        reason = str(payload.get("reason") or "gemini_screened")
        _GEMINI_CALLS += 1
        _GEMINI_CACHE[cache_key] = (is_lead, useful, reason)
        return is_lead, useful, reason
    except Exception:
        return True, False, "gemini_failed"


def normalize_company_candidates(candidates: list[dict[str, Any]], source: str) -> list[dict[str, Any]]:
    """
    Normalize extracted company candidates for upstream validation.

    - Allows missing domain.
    - Trims names.
    - Generates slug when missing.
    - Dedupes by case-insensitive name.
    """
    output: list[dict[str, Any]] = []
    seen_domains: set[str] = set()

    for row in candidates:
        if not isinstance(row, dict):
            continue

        source_url = str(row.get("source_url") or row.get("raw_url") or row.get("url") or row.get("link") or "").strip() or None
        raw_domain = str(row.get("domain") or "").strip().lower()
        domain = _root_domain(_extract_host(raw_domain or source_url or ""))
        if not domain:
            continue

        if domain in seen_domains:
            continue
        seen_domains.add(domain)

        name = _clean_company_name(_company_name_from_domain(domain))
        if not name:
            continue

        entity_type = _classify_domain_type(domain)
        is_container = entity_type == "SOURCE_CONTAINER"

        context = str(row.get("context") or "").strip() or None
        source_type = str(row.get("source_type") or "article").strip() or "article"
        extraction_method = str(row.get("extraction_method") or "regex").strip() or "regex"
        confidence = float(row.get("confidence") or 0.4)

        if is_container:
            output.append(
                {
                    "name": name,
                    "slug": _slugify_name(name),
                    "domain": domain,
                    "source": source,
                    "source_type": source_type,
                    "context": context,
                    "source_url": source_url,
                    "raw_url": source_url,
                    "confidence": max(0.3, min(0.7, confidence)),
                    "signals": ["source_container_detected"],
                    "high_intent_signals": [],
                    "signal_count": 1,
                    "confidence_reasons": ["classified as source container domain"],
                    "entity_type": "SOURCE_CONTAINER",
                    "is_container": True,
                    "extraction_method": extraction_method,
                    "raw_fields": row.get("raw_fields") if isinstance(row.get("raw_fields"), dict) else row,
                }
            )
            continue

        if not _is_valid_company_identity(name, domain, context or ""):
            continue

        is_lead, useful_list, gemini_reason = _gemini_assess_candidate(name, domain, context or "")
        if not is_lead and not useful_list:
            continue

        signals = ["domain_validated"]
        high_intent: list[str] = []
        reasons = ["derived from valid domain", "filtered non-aggregator source"]

        if _contains_growth_signal(context or ""):
            signals.append("growth_signal_detected")
            high_intent.append("growth_signal_detected")
            reasons.append("growth keywords in context")
        if useful_list:
            signals.append("useful_company_list_detected")
            reasons.append("gemini useful company list")
        if gemini_reason not in {"", "gemini_unavailable", "gemini_budget_exhausted", "gemini_failed"}:
            reasons.append(gemini_reason)

        output.append(
            {
                "name": name,
                "slug": _slugify_name(name),
                "domain": domain,
                "source": source,
                "source_type": source_type,
                "context": context,
                "source_url": source_url,
                "raw_url": source_url,
                "confidence": max(0.3, min(0.7, confidence)),
                "signals": signals,
                "high_intent_signals": high_intent,
                "signal_count": len(signals) + len(high_intent),
                "confidence_reasons": reasons or ["derived from valid domain"],
                "entity_type": entity_type,
                "is_container": is_container,
                "extraction_method": extraction_method,
                "raw_fields": row.get("raw_fields") if isinstance(row.get("raw_fields"), dict) else row,
            }
        )

    return output


def dedupe_company_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate by root domain first (one company per domain)."""
    deduped: list[dict[str, Any]] = []
    seen_domains: set[str] = set()
    names: set[str] = set()

    for row in candidates:
        if not isinstance(row, dict):
            continue
        domain = _root_domain(_extract_host(row.get("domain") or row.get("source_url") or row.get("raw_url") or ""))
        if domain:
            if domain in seen_domains:
                continue
            seen_domains.add(domain)

        name = str(row.get("name") or "").strip().lower()
        if not name:
            continue
        if name in names:
            continue
        names.add(name)
        deduped.append(row)

    return deduped


def passes_minimum_company_validation(row: dict[str, Any]) -> bool:
    """Strict upstream validation for normalized entities."""
    name = _clean_company_name(row.get("name"))
    domain = _root_domain(_extract_host(row.get("domain") or row.get("source_url") or row.get("raw_url") or ""))
    context = str(row.get("context") or "")

    if not domain:
        return False
    if not name:
        return False
    if _is_generic_company_name(name):
        return False
    if not _is_valid_company_identity(name, domain, context):
        return False

    if not isinstance(row.get("confidence_reasons"), list) or not row.get("confidence_reasons"):
        row["confidence_reasons"] = ["derived from valid domain"]
    if not isinstance(row.get("signals"), list) or not row.get("signals"):
        row["signals"] = ["domain_validated"]
    row["signal_count"] = int(len(row.get("signals") or []) + len(row.get("high_intent_signals") or []))

    return True
