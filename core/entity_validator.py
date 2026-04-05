from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

NOISE_WORDS = {
    "hiring",
    "hire",
    "jobs",
    "job",
    "careers",
    "career",
    "apply",
    "opening",
    "openings",
    "remote",
    "location",
    "locations",
}

KNOWN_COMPANY_PRESENCE_SOURCES = {
    "product_hunt",
    "producthunt",
    "github",
    "opencorporates",
    "yc",
    "ycombinator",
}

COMPOUND_SUFFIXES = [
    "europe",
    "india",
    "global",
    "venture",
    "ventures",
    "capital",
    "labs",
    "systems",
    "software",
]


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
    if len(parts) >= 3 and parts[-2] in {"co", "com", "org", "net"}:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def normalize_company_name(name: str) -> str:
    """Normalize raw company names while preserving spaces and removing job noise."""
    cleaned = _clean_text(name)
    if not cleaned:
        return ""

    cleaned = re.sub(r"[^A-Za-z0-9\s&.-]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .,-")

    tokens = [token for token in cleaned.split() if token]
    if not tokens:
        return ""

    filtered = [token for token in tokens if token.lower() not in NOISE_WORDS]
    if not filtered:
        return ""

    expanded_tokens: list[str] = []
    for token in filtered:
        lowered = token.lower()
        split_done = False
        for suffix in COMPOUND_SUFFIXES:
            if lowered.endswith(suffix) and len(lowered) > len(suffix) + 2:
                stem = token[: len(token) - len(suffix)]
                if stem:
                    expanded_tokens.append(stem)
                    expanded_tokens.append(token[len(token) - len(suffix) :])
                    split_done = True
                    break
        if not split_done:
            expanded_tokens.append(token)

    out_tokens: list[str] = []
    for token in expanded_tokens:
        if token.lower() in {"ai", "api", "saas", "crm", "erp", "hr"}:
            out_tokens.append(token.upper())
        elif len(token) <= 4 and token.isupper():
            out_tokens.append(token)
        else:
            out_tokens.append(token.capitalize())

    return " ".join(out_tokens)


def validate_company_entity(entity: dict[str, Any], row: dict[str, Any] | None = None) -> dict[str, Any]:
    """Validate company candidate using domain and known-company-presence proofs."""
    candidate = entity if isinstance(entity, dict) else {}
    raw_row = row if isinstance(row, dict) else {}

    company_name = normalize_company_name(str(candidate.get("company_name") or candidate.get("name") or ""))
    source_url = _clean_text(raw_row.get("url") or raw_row.get("website") or raw_row.get("link") or candidate.get("source_url") or "")
    domain_hint = _clean_text(candidate.get("domain_hint") or candidate.get("domain") or raw_row.get("domain") or "")

    domain = _root_domain(_extract_host(domain_hint or source_url))
    source = _clean_text(raw_row.get("source") or raw_row.get("source_transport") or raw_row.get("source_type") or candidate.get("source") or "")

    proofs: list[str] = []

    has_domain = bool(domain)
    if has_domain:
        proofs.append("domain")

    combined_url = f"{source_url} {domain}".lower()
    has_linkedin_company = "linkedin.com/company/" in combined_url
    if has_linkedin_company:
        proofs.append("linkedin_company")

    has_known_presence = source.lower() in KNOWN_COMPANY_PRESENCE_SOURCES or any(
        marker in combined_url
        for marker in ["producthunt.com", "github.com", "opencorporates.com", "ycombinator.com"]
    )
    if has_known_presence:
        proofs.append("known_db_presence")

    validation_passed = bool(company_name) and (has_domain or has_linkedin_company or has_known_presence)
    reason = "validated" if validation_passed else "missing_company_proof"

    return {
        "company_name": company_name,
        "domain": domain,
        "validation_passed": validation_passed,
        "validation": validation_passed,
        "proofs": proofs,
        "reason": reason,
    }


def compute_weighted_ingestion_score(entity_confidence: float, ingestion_score: float, validation_passed: bool) -> float:
    """Compute weighted ingestion quality score used for confidence-based row drops."""
    entity_conf = max(0.0, min(1.0, float(entity_confidence or 0.0)))
    ing = max(0.0, min(1.0, float(ingestion_score or 0.0) / 5.0))
    val = 1.0 if bool(validation_passed) else 0.0
    return round((0.6 * entity_conf) + (0.25 * ing) + (0.15 * val), 4)


def validate_entity(entity: dict[str, Any]) -> dict[str, Any]:
    """Requested lightweight validator API for extraction layer integration."""
    candidate = entity if isinstance(entity, dict) else {}
    name = normalize_company_name(str(candidate.get("clean_name") or candidate.get("company_name") or candidate.get("name") or ""))
    url = _clean_text(candidate.get("url") or candidate.get("source_url") or "")
    domain = _root_domain(_extract_host(candidate.get("domain") or url or ""))

    lower_name = name.lower()
    generic_bad = any(token in lower_name for token in {"hiring", "job", "apply", "jobs", "careers"})
    sane_name = len(name) > 2 and not generic_bad
    non_job_url = not any(token in url.lower() for token in ["/jobs", "/careers", "job", "apply"])

    validated = bool(domain) or (bool(url) and non_job_url and sane_name)
    if validated:
        reason = "validated"
    elif not sane_name:
        reason = "invalid_company_name"
    elif not non_job_url:
        reason = "job_listing_url"
    else:
        reason = "missing_domain_or_site_proof"

    return {
        "validated": validated,
        "validation_reason": reason,
        "company_name": name,
        "domain": domain,
    }
