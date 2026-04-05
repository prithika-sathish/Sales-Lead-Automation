from __future__ import annotations

from typing import Any


ENTERPRISE_COMPANIES = {
    "adobe",
    "airbnb",
    "amazon",
    "anthropic",
    "apple",
    "atlassian",
    "block",
    "cisco",
    "cloudflare",
    "doordash",
    "github",
    "google",
    "hubspot",
    "intuit",
    "microsoft",
    "meta",
    "mongodb",
    "notion",
    "nvidia",
    "okta",
    "openai",
    "oracle",
    "paypal",
    "salesforce",
    "servicenow",
    "shopify",
    "snowflake",
    "square",
    "stripe",
    "twilio",
    "uber",
    "workday",
    "zoom",
    "zoominfo",
}


DOMAIN_HINTS: list[tuple[str, str, list[str]]] = [
    ("stripe", "payments / fintech", ["payments infrastructure", "merchant workflows", "developer-facing financial infrastructure"]),
    ("salesforce", "crm / enterprise software", ["enterprise SaaS", "revenue workflows", "platform ecosystem"]),
    ("shopify", "commerce infrastructure", ["merchant tooling", "commerce operations", "developer ecosystem"]),
    ("snowflake", "data platform", ["analytics infrastructure", "data warehousing", "enterprise scale"]),
    ("cloudflare", "internet infrastructure", ["edge delivery", "security", "developer platform"]),
    ("github", "developer platform", ["software development", "code collaboration", "release velocity"]),
    ("openai", "ai infrastructure", ["model platform", "developer adoption", "rapid experimentation"]),
    ("hubspot", "revenue software", ["marketing automation", "sales workflow", "customer growth"]),
    ("zoom", "communications", ["collaboration", "distributed teams", "customer engagement"]),
]


def _infer_domain(name: str) -> tuple[str, list[str]]:
    lowered = name.lower().strip()
    for token, domain, characteristics in DOMAIN_HINTS:
        if token in lowered:
            return domain, characteristics

    if any(token in lowered for token in ["pay", "bank", "fintech", "cash", "card"]):
        return "payments / fintech", ["financial workflows", "transaction scale", "compliance sensitivity"]
    if any(token in lowered for token in ["data", "analytics", "warehouse", "lake"]):
        return "data / analytics", ["data platform", "operational scale", "analytics adoption"]
    if any(token in lowered for token in ["cloud", "infra", "platform", "dev"]):
        return "developer infrastructure", ["platform scale", "developer workflows", "technical adoption"]

    return "software / technology", ["general software operations", "market-facing product", "growth-oriented execution"]


def get_company_context(company: str) -> dict[str, Any]:
    cleaned = company.strip()
    lowered = cleaned.lower()
    domain, characteristics = _infer_domain(cleaned)

    if lowered in ENTERPRISE_COMPANIES:
        estimated_scale = "enterprise"
    elif any(token in lowered for token in ["inc", "corp", "corporation", "llc", "ltd", "plc"]):
        estimated_scale = "growth"
    else:
        estimated_scale = "startup"

    if lowered in ENTERPRISE_COMPANIES:
        characteristics = list(dict.fromkeys(["widely recognized market leader", *characteristics]))

    return {
        "estimated_scale": estimated_scale,
        "domain": domain,
        "known_characteristics": characteristics,
    }