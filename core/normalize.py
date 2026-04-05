from __future__ import annotations

import re
from typing import Any


def _clean_text(value: object) -> str:
    text = str(value or "").strip()
    text = re.sub(r"[\u2600-\u27BF\U0001F300-\U0001FAFF]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip(" -|,.;")


def _extract_domain_hint(value: object) -> str | None:
    raw = _clean_text(value).lower()
    if not raw:
        return None
    raw = re.sub(r"^https?://", "", raw)
    raw = raw.split("/")[0].strip()
    if raw.startswith("www."):
        raw = raw[4:]
    if "." not in raw:
        return None
    return raw


def normalize_rows(rows: list[dict[str, Any]], source: str, source_type: str = None) -> list[dict[str, Any]]:
    """
    Normalize rows from any source to unified schema.
    
    Output schema:
    {
        "name": str,
        "domain": str | null,
        "source": str,
        "source_type": "structured" | "semi_structured" | "unstructured",
        "confidence": float,
        "confidence_reasons": [str],
        "raw_fields": dict
    }
    """
    output: list[dict[str, Any]] = []
    
    # Infer source_type if not provided
    if not source_type:
        source_type = _infer_source_type(source)
    
    for row in rows:
        if not isinstance(row, dict):
            continue

        name = _clean_text(
            row.get("name")
            or row.get("title")
            or row.get("company")
            or row.get("company_name")
            or row.get("companyName")
            or row.get("business_name")
            or row.get("productName")
            or ""
        )
        if not name:
            continue

        domain = _extract_domain_hint(
            row.get("domain")
            or row.get("website")
            or row.get("websiteUrl")
            or row.get("url")
            or row.get("companyWebsite")
            or ""
        )
        raw_url = _clean_text(row.get("website") or row.get("websiteUrl") or row.get("url") or row.get("link") or "")

        # Calculate initial confidence
        confidence, reasons = _calculate_confidence(domain, source_type, source, row)

        output.append({
            "name": name,
            "domain": domain,
            "source": source,
            "source_type": source_type,
            "confidence": confidence,
            "confidence_reasons": reasons,
            "raw_url": raw_url or None,
            "raw_fields": row
        })
    
    return output


def _infer_source_type(source: str) -> str:
    """Infer source type based on source name."""
    source_lower = source.lower()
    
    structured = ["google maps", "product hunt", "y combinator", "crunchbase", "opencorporates", "app stores"]
    semi_structured = ["indeed", "naukri", "clutch", "g2", "capterra", "github", "marketplaces", "jobs"]
    unstructured = ["news", "twitter", "reddit", "blogs"]
    
    for s in structured:
        if s in source_lower:
            return "structured"
    for s in semi_structured:
        if s in source_lower:
            return "semi_structured"
    for s in unstructured:
        if s in source_lower:
            return "unstructured"
    
    return "semi_structured"  # default


def _calculate_confidence(domain: str | None, source_type: str, source: str, raw_row: dict) -> tuple[float, list[str]]:
    """
    Calculate confidence score and reasons.
    
    Scoring rules:
    +0.4 → has valid domain
    +0.3 → structured source
    +0.2 → hiring signal (job portals)
    +0.1 → appears in multiple sources (detected via duplicates)
    Clamp to [0,1]
    """
    score = 0.0
    reasons = []
    
    # +0.4 if has domain
    if domain:
        score += 0.4
        reasons.append("has_domain")
    
    # +0.3 if structured source
    if source_type == "structured":
        score += 0.3
        reasons.append(f"source={source} (structured)")
    elif source_type == "semi_structured":
        score += 0.2
        reasons.append(f"source={source} (semi_structured)")
    
    # +0.2 if hiring signal detected
    hiring_signals = ["indeed", "naukri", "jobs", "hiring", "recruitment", "careers"]
    if any(signal in source.lower() for signal in hiring_signals):
        score += 0.2
        reasons.append("hiring_signal_detected")
    
    # +0.1 if appears in multiple sources (bonus, will be applied during dedup)
    # This is detected downstream
    
    # Clamp to [0, 1]
    score = min(max(score, 0.0), 1.0)
    
    return score, reasons
