from __future__ import annotations

import re
from typing import Any

from utils.entity_validation import canonical_company_name, is_real_company_entity


def normalize_company_name(name: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(name or "")).strip()
    return cleaned


def normalize_domain(domain: str) -> str:
    value = str(domain or "").strip().lower()
    value = value.replace("https://", "").replace("http://", "")
    value = value.split("/")[0]
    if value.startswith("www."):
        value = value[4:]
    return value.strip()


def dedupe_by_domain(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, list[dict[str, Any]]] = {}

    for item in items:
        if not isinstance(item, dict):
            continue
        company = normalize_company_name(str(item.get("company") or ""))
        domain = normalize_domain(str(item.get("domain") or ""))
        snippet = str(item.get("snippet") or item.get("context") or "").strip()
        source = str(item.get("source") or "").strip().lower()
        region = str(item.get("region") or "").strip()
        query = str(item.get("query") or "").strip()
        url = str(item.get("url") or "").strip()
        if not company or not domain:
            continue

        bucket = merged.setdefault(domain, [])
        row_signature = (
            company.lower(),
            snippet.lower(),
            url.lower(),
            query.lower(),
        )

        signature_exists = any(
            (
                str(existing.get("company") or "").strip().lower(),
                str(existing.get("snippet") or "").strip().lower(),
                str(existing.get("url") or "").strip().lower(),
                str(existing.get("query") or "").strip().lower(),
            )
            == row_signature
            for existing in bucket
        )
        if signature_exists:
            continue

        if len(bucket) >= 3:
            continue

        bucket.append(
            {
                "company": company,
                "domain": domain,
                "source": source,
                "region": region,
                "snippet": snippet,
                "query": query,
                "url": url,
            }
        )

    output: list[dict[str, Any]] = []
    for domain, rows in merged.items():
        sources: list[str] = []
        regions: list[str] = []
        snippets: list[str] = []
        urls: list[str] = []
        queries: list[str] = []
        for row in rows:
            src = str(row.get("source") or "").strip().lower()
            reg = str(row.get("region") or "").strip()
            snp = str(row.get("snippet") or "").strip()
            url = str(row.get("url") or "").strip()
            query = str(row.get("query") or "").strip()
            if src and src not in sources:
                sources.append(src)
            if reg and reg not in regions:
                regions.append(reg)
            if snp and snp not in snippets:
                snippets.append(snp)
            if url and url not in urls:
                urls.append(url)
            if query and query not in queries:
                queries.append(query)

        for row in rows:
            output.append(
                {
                    "company": str(row.get("company") or ""),
                    "domain": domain,
                    "sources": list(sources),
                    "regions": list(regions),
                    "snippets": list(snippets),
                    "urls": list(urls),
                    "queries": list(queries),
                }
            )

    return output


def remove_low_quality_entries(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        domain = normalize_domain(str(item.get("domain") or ""))
        company = normalize_company_name(str(item.get("company") or ""))
        snippet = str(item.get("snippet") or item.get("context") or item.get("description") or "").strip()
        url = str(item.get("url") or item.get("website") or "").strip()
        if not domain or not company:
            continue
        if not is_real_company_entity(company=company, domain=domain, description=snippet, url=url):
            continue
        output.append(item)
    return output


def resolve_company_entities(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    entities: dict[tuple[str, str], dict[str, Any]] = {}

    for item in items:
        if not isinstance(item, dict):
            continue

        company = normalize_company_name(str(item.get("company_name") or item.get("company") or ""))
        domain = normalize_domain(str(item.get("domain") or ""))
        description = str(item.get("context") or item.get("snippet") or item.get("description") or "").strip()
        website = str(item.get("website") or item.get("url") or "").strip()
        if not company:
            continue
        company = canonical_company_name(company, domain)
        if not is_real_company_entity(company=company, domain=domain, description=description, url=website):
            continue

        key = (company.lower(), domain)
        entity = entities.setdefault(
            key,
            {
                "company": company,
                "domain": domain,
                "sources": [],
                "source_types": [],
                "regions": [],
                "raw_signals": [],
                "confidence_score": 0.0,
                "website": "",
                "tags": [],
            },
        )

        source = str(item.get("source") or "").strip().lower()
        source_type = str(item.get("source_type") or "").strip().lower()
        region = str(item.get("region") or "").strip()
        signal_type = str(item.get("signal_type") or "").strip().lower()
        context = str(item.get("context") or item.get("snippet") or "").strip()
        confidence_score = float(item.get("confidence_score") or 0.0)
        website = str(item.get("website") or "").strip()
        tags = item.get("tags") if isinstance(item.get("tags"), list) else []

        if source and source not in entity["sources"]:
            entity["sources"].append(source)
        if source_type and source_type not in entity["source_types"]:
            entity["source_types"].append(source_type)
        if region and region not in entity["regions"]:
            entity["regions"].append(region)
        entity["confidence_score"] = max(float(entity.get("confidence_score") or 0.0), confidence_score)
        if website and not str(entity.get("website") or "").strip():
            entity["website"] = website
        for tag in tags:
            tag_value = str(tag or "").strip()
            if tag_value and tag_value not in entity["tags"]:
                entity["tags"].append(tag_value)

        entity["raw_signals"].append(
            {
                "source": source,
                "source_type": source_type,
                "signal_type": signal_type,
                "context": context,
                "confidence_score": confidence_score,
            }
        )

    return list(entities.values())
