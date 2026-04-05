from __future__ import annotations

import json
import os
import re
from typing import Any
from urllib.parse import urlparse

from core.llm_control import is_llm_allowed

from core.domain_resolver import extract_companies_from_directory_url

KNOWN_DOMAIN_NAME_OVERRIDES = {
    "ril": "Reliance Industries",
    "zoho": "Zoho",
    "tcs": "Tata Consultancy Services",
}

SUBDOMAIN_PREFIXES = {
    "www",
    "m",
    "blog",
    "blogs",
    "jobs",
    "job",
    "info",
    "help",
    "support",
    "docs",
    "developer",
    "developers",
}

NOISE_TERMS = {
    "hiring",
    "job",
    "jobs",
    "career",
    "careers",
    "apply",
    "remote",
    "opening",
    "openings",
    "role",
    "roles",
}


def _extract_json_payload(text: str) -> dict[str, Any]:
    cleaned = _clean_text(text)
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.I)
        cleaned = re.sub(r"\s*```$", "", cleaned)
        cleaned = cleaned.strip()
    if cleaned.startswith("{") and cleaned.endswith("}"):
        try:
            payload = json.loads(cleaned)
            if isinstance(payload, dict):
                return payload
        except Exception:
            return {}
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start >= 0 and end > start:
        try:
            payload = json.loads(cleaned[start : end + 1])
            if isinstance(payload, dict):
                return payload
        except Exception:
            return {}
    return {}


def _strip_noise_words(name: str) -> str:
    words = [part for part in re.split(r"\s+", _clean_text(name)) if part]
    if not words:
        return ""
    filtered = [part for part in words if part.lower().strip(".,:;!?") not in NOISE_TERMS]
    return " ".join(filtered).strip()


def _normalize_company_name(name: str) -> str:
    cleaned = _strip_noise_words(name)
    cleaned = re.sub(r"[^A-Za-z0-9\s&.-]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .,-")
    if not cleaned:
        return ""

    tokens: list[str] = []
    for token in cleaned.split():
        if len(token) <= 4 and token.isupper():
            tokens.append(token)
        elif token.lower() in {"ai", "api", "saas", "crm", "erp", "hr"}:
            tokens.append(token.upper())
        else:
            tokens.append(token.capitalize())
    return " ".join(tokens)


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _extract_host(value: object) -> str:
    raw = _clean_text(value).lower()
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

    # Drop common subdomains first (jobs.foo.com -> foo.com)
    while len(parts) > 2 and parts[0] in SUBDOMAIN_PREFIXES:
        parts.pop(0)

    if len(parts) >= 3 and parts[-2] in {"co", "com", "org", "net"}:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def _name_from_domain(root_domain: str) -> str:
    if not root_domain:
        return ""
    label = root_domain.split(".")[0].strip().lower()
    if not label:
        return ""

    if label in KNOWN_DOMAIN_NAME_OVERRIDES:
        return KNOWN_DOMAIN_NAME_OVERRIDES[label]

    tokens = [token for token in label.replace("_", "-").split("-") if token]
    if not tokens:
        return ""
    return " ".join(token.capitalize() for token in tokens)


def _record_url(record: dict[str, Any]) -> str:
    return _clean_text(record.get("url") or record.get("website") or record.get("link") or "")


def _context(record: dict[str, Any]) -> str:
    return _clean_text(
        record.get("snippet")
        or record.get("description")
        or record.get("tagline")
        or record.get("content")
        or record.get("title")
        or record.get("name")
        or ""
    )


def _source_type(record: dict[str, Any]) -> str:
    value = _clean_text(record.get("source_type") or "")
    if value:
        return value
    text = " ".join(
        [
            _clean_text(record.get("title") or ""),
            _clean_text(record.get("snippet") or ""),
            _clean_text(record.get("description") or ""),
        ]
    ).lower()
    if any(token in text for token in ["top", "best", "list of", "directory", "companies"]):
        return "listicle"
    return "article"


def _domain_candidate(
    *,
    domain: str,
    source_type: str,
    record: dict[str, Any],
    context: str,
    extraction_method: str,
    confidence: float,
) -> dict[str, Any] | None:
    name = _name_from_domain(domain)
    if not name:
        return None

    reasons = ["derived from valid domain", "filtered non-aggregator source"]
    if any(token in context.lower() for token in ["hiring", "funding", "growth", "series", "expansion"]):
        reasons.append("growth keywords in context")

    return {
        "name": name,
        "domain": domain,
        "source_type": source_type,
        "source_url": _record_url(record) or None,
        "context": context,
        "confidence": max(0.3, min(0.8, float(confidence))),
        "confidence_reasons": reasons,
        "signals": ["domain_validated"],
        "high_intent_signals": ["growth_signal_detected"] if "growth keywords in context" in reasons else [],
        "signal_count": 1 + (1 if "growth keywords in context" in reasons else 0),
        "extraction_method": extraction_method,
        "raw_fields": record,
    }


def _expand_list_domains(record: dict[str, Any], source_type: str, context: str) -> list[dict[str, Any]]:
    page_url = _record_url(record)
    if not page_url:
        return []

    expanded = extract_companies_from_directory_url(page_url, max_results=30)
    out: list[dict[str, Any]] = []
    for item in expanded:
        domain = _root_domain(_extract_host(item.get("domain") or ""))
        if not domain:
            continue
        candidate = _domain_candidate(
            domain=domain,
            source_type=source_type,
            record=record,
            context=context,
            extraction_method="list_expansion",
            confidence=0.72,
        )
        if candidate:
            out.append(candidate)
    return out


def _unique_by_domain(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for candidate in candidates:
        domain = _clean_text(candidate.get("domain") or "").lower()
        if not domain or domain in seen:
            continue
        seen.add(domain)
        out.append(candidate)
    return out


def extract_company_entity(row: dict) -> dict:
    """Extract a single company entity from noisy source rows using Gemini with heuristic fallback."""
    if not isinstance(row, dict):
        return {
            "company_name": "",
            "confidence": 0.0,
            "reason": "invalid_row",
            "domain_hint": "",
            "is_valid_company": False,
        }

    title = _clean_text(row.get("title") or row.get("name") or row.get("company") or "")
    description = _clean_text(row.get("description") or row.get("snippet") or row.get("content") or row.get("tagline") or "")
    source_url = _record_url(row)
    source = _clean_text(row.get("source") or row.get("source_transport") or row.get("source_type") or "unknown")
    root = _root_domain(_extract_host(source_url or row.get("domain") or ""))

    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if api_key and is_llm_allowed("extraction"):
        try:
            from google import genai

            model_name = (os.getenv("GEMINI_MODEL") or "gemini-2.5-flash").strip()
            client = genai.Client(api_key=api_key)
            prompt = (
                "Extract only a real company entity from this web discovery row. Return strict JSON with keys "
                "company_name, confidence, reason, domain_hint, is_valid_company.\n"
                "Rules:\n"
                "- Ignore job titles, locations, role names, and generic listing text.\n"
                "- If ambiguous or no clear company, set is_valid_company=false and low confidence.\n"
                "- company_name must preserve spaces for multi-word brands.\n"
                "- confidence is 0..1.\n"
                f"title: {title}\n"
                f"description: {description}\n"
                f"url: {source_url}\n"
                f"source: {source}"
            )
            response = client.models.generate_content(model=model_name, contents=prompt)
            text = str(getattr(response, "text", "") or getattr(response, "output_text", "") or "")
            payload = _extract_json_payload(text)
            company_name = _normalize_company_name(str(payload.get("company_name") or ""))
            confidence = float(payload.get("confidence") or 0.0)
            confidence = max(0.0, min(1.0, confidence))
            reason = _clean_text(payload.get("reason") or "llm_extraction")
            domain_hint = _root_domain(_extract_host(payload.get("domain_hint") or "")) or root
            is_valid = bool(payload.get("is_valid_company")) and bool(company_name)
            return {
                "company_name": company_name,
                "confidence": confidence,
                "reason": reason,
                "domain_hint": domain_hint,
                "is_valid_company": is_valid,
            }
        except Exception:
            pass

    # Heuristic fallback when Gemini is unavailable.
    domain_name = _normalize_company_name(_name_from_domain(root))
    title_name = _normalize_company_name(title)
    selected = domain_name or title_name
    lowered = selected.lower()
    invalid_tokens = {"hiring", "jobs", "careers", "remote", "salary", "review", "list"}
    has_bad_token = any(token in lowered for token in invalid_tokens)
    confidence = 0.72 if domain_name else 0.55
    if has_bad_token:
        confidence = min(confidence, 0.35)

    return {
        "company_name": selected,
        "confidence": confidence,
        "reason": "domain_heuristic" if domain_name else "title_heuristic",
        "domain_hint": root,
        "is_valid_company": bool(selected) and not has_bad_token,
    }


def extract_company_domain_signals(company_name: str, domain_hint: str, context: str, source_url: str = "") -> dict[str, Any]:
    """Infer lightweight business intent signals from domain/context with Gemini fallback."""
    company = _normalize_company_name(company_name)
    domain = _root_domain(_extract_host(domain_hint or source_url or ""))
    text = _clean_text(context)

    heuristic_signals: list[str] = []
    lowered = text.lower()
    if any(token in lowered for token in ["hiring", "career", "opening", "recruit"]):
        heuristic_signals.append("hiring_signal")
    if any(token in lowered for token in ["funding", "series a", "series b", "raised"]):
        heuristic_signals.append("funding_signal")
    if any(token in lowered for token in ["pricing", "api", "integration", "platform"]):
        heuristic_signals.append("product_signal")
    if not heuristic_signals:
        heuristic_signals.append("domain_validated")

    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if (not api_key) or (not is_llm_allowed("normalization")):
        return {"signals": heuristic_signals, "reason": "heuristic_only"}

    try:
        from google import genai

        model_name = (os.getenv("GEMINI_MODEL") or "gemini-2.5-flash").strip()
        client = genai.Client(api_key=api_key)
        prompt = (
            "Classify business discovery signals from this company candidate. Return JSON with keys "
            "signals (array of short tags), reason (short string).\n"
            "Only include signal tags relevant for sales lead intent such as hiring_signal, funding_signal, "
            "product_signal, expansion_signal, domain_validated.\n"
            f"company: {company}\n"
            f"domain: {domain}\n"
            f"context: {text}"
        )
        response = client.models.generate_content(model=model_name, contents=prompt)
        payload = _extract_json_payload(str(getattr(response, "text", "") or getattr(response, "output_text", "") or ""))
        llm_signals = payload.get("signals") if isinstance(payload.get("signals"), list) else []
        clean_signals = [
            _clean_text(item).lower().replace(" ", "_")
            for item in llm_signals
            if _clean_text(item)
        ]
        clean_signals = list(dict.fromkeys(clean_signals))[:5]
        if clean_signals:
            return {
                "signals": clean_signals,
                "reason": _clean_text(payload.get("reason") or "llm_signals"),
            }
    except Exception:
        pass

    return {"signals": heuristic_signals, "reason": "heuristic_only"}


def extract_companies_from_record(record: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(record, dict):
        return []

    context = _context(record)
    source_type = _source_type(record)
    url = _record_url(record)

    llm_entity = extract_company_entity(record)
    if llm_entity.get("is_valid_company") and _clean_text(llm_entity.get("company_name")):
        domain = _root_domain(_extract_host(llm_entity.get("domain_hint") or url or record.get("domain") or ""))
        if domain:
            signal_bundle = extract_company_domain_signals(
                str(llm_entity.get("company_name") or ""),
                domain,
                context,
                url,
            )
            reason = _clean_text(llm_entity.get("reason") or "llm_entity")
            return [
                {
                    "name": _normalize_company_name(str(llm_entity.get("company_name") or "")),
                    "domain": domain,
                    "source_type": source_type,
                    "source_url": url or None,
                    "context": context,
                    "confidence": max(0.0, min(1.0, float(llm_entity.get("confidence") or 0.0))),
                    "confidence_reasons": [reason],
                    "signals": signal_bundle.get("signals") if isinstance(signal_bundle.get("signals"), list) else ["domain_validated"],
                    "high_intent_signals": [],
                    "signal_count": 1,
                    "extraction_method": "llm_entity_extractor",
                    "raw_fields": record,
                }
            ]

    # For list/blog pages, prefer extracting listed external company domains.
    if source_type in {"listicle", "directory"}:
        expanded = _expand_list_domains(record, source_type, context)
        return _unique_by_domain(expanded)

    host = _extract_host(url or record.get("domain") or "")
    domain = _root_domain(host)
    if not domain:
        return []

    candidate = _domain_candidate(
        domain=domain,
        source_type=source_type,
        record=record,
        context=context,
        extraction_method="domain",
        confidence=0.68,
    )
    if not candidate:
        return []

    return [candidate]
