from __future__ import annotations

import re
from pathlib import Path

from query_engine.models import StructuredContext


KNOWN_REGIONS = [
    "india",
    "singapore",
    "southeast asia",
    "europe",
    "united states",
    "usa",
    "uk",
    "germany",
    "apac",
]

KNOWN_INDUSTRIES = [
    "saas",
    "fintech",
    "payments",
    "subscription billing",
    "revenue management",
    "customer support",
    "helpdesk",
]

KNOWN_COMPANIES = [
    "Stripe",
    "Zendesk",
    "Intercom",
    "HubSpot",
    "Salesforce",
    "Shopify",
    "Chargebee",
    "Zuora",
    "Paddle",
    "Freshworks",
]


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalize_list(items: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for item in items:
        clean = _clean_text(item).strip("-:* ")
        if not clean:
            continue
        key = clean.lower()
        if key in seen:
            continue
        seen.add(key)
        output.append(clean)
    return output


def _split_bullets(text: str) -> list[str]:
    rows = [line.strip() for line in text.splitlines()]
    values: list[str] = []
    for row in rows:
        if not row:
            continue
        if row.startswith(("-", "*")):
            values.append(row[1:].strip())
        elif re.match(r"^\d+\.\s+", row):
            values.append(re.sub(r"^\d+\.\s+", "", row).strip())
    return _normalize_list(values)


def _parse_sections(markdown_text: str) -> dict[str, str]:
    sections: dict[str, list[str]] = {}
    current = "root"
    sections[current] = []

    for line in markdown_text.splitlines():
        heading = re.match(r"^#{1,6}\s+(.+)$", line.strip())
        if heading:
            current = heading.group(1).strip().lower()
            sections.setdefault(current, [])
            continue
        sections.setdefault(current, []).append(line)

    return {key: "\n".join(lines).strip() for key, lines in sections.items()}


def _extract_company_name(markdown_text: str) -> str:
    m = re.search(r"^#\s*company:\s*(.+)$", markdown_text, flags=re.I | re.M)
    if m:
        return _clean_text(m.group(1))
    m = re.search(r"^#\s+(.+)$", markdown_text, flags=re.M)
    return _clean_text(m.group(1)) if m else ""


def parse_markdown(markdown_text: str) -> StructuredContext:
    clean_text = _clean_text(markdown_text)
    sections = _parse_sections(markdown_text)

    def find_section(*keywords: str) -> str:
        for heading, body in sections.items():
            if any(token in heading for token in keywords):
                return body
        return ""

    product_description = _clean_text(find_section("overview", "product", "description"))
    icp_text = find_section("target customers", "icp", "who needs", "ideal customer")
    geo_text = find_section("geography", "region", "location")
    industry_text = find_section("industry", "industries", "category")
    hint_text = find_section("signals", "hints", "value proposition", "pain")

    core_icp = _split_bullets(icp_text)
    if not core_icp and icp_text:
        core_icp = _normalize_list([part.strip() for part in re.split(r",|;|\n", icp_text)])

    regions = _split_bullets(geo_text)
    if not regions:
        regions = [region.title() for region in KNOWN_REGIONS if region in clean_text.lower()]

    industries = _split_bullets(industry_text)
    if not industries:
        industries = [industry for industry in KNOWN_INDUSTRIES if industry in clean_text.lower()]

    hints = _split_bullets(hint_text)
    if not hints:
        hints = _normalize_list([part.strip() for part in re.split(r"\.|;", hint_text or clean_text) if len(part.strip()) > 20])[:8]

    known_companies = [name for name in KNOWN_COMPANIES if re.search(rf"\b{re.escape(name)}\b", markdown_text, flags=re.I)]

    return StructuredContext(
        product_description=product_description or clean_text,
        core_icp=core_icp or ["B2B SaaS companies"],
        regions=regions or ["Global"],
        industries=industries or ["SaaS"],
        hints=hints,
        company_name=_extract_company_name(markdown_text),
        known_companies=_normalize_list(known_companies),
        full_text=clean_text,
    )


def parse_markdown_file(markdown_path: str) -> StructuredContext:
    path = Path(str(markdown_path or "")).expanduser()
    if not path.exists():
        return StructuredContext()
    return parse_markdown(path.read_text(encoding="utf-8"))
