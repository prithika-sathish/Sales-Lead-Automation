from __future__ import annotations


SOURCE_WEIGHTS = {
    "apify": 0.9,
    "serper": 0.7,
    "tavily": 0.7,
    "fallback": 0.4,
}


def classify_company(name: str, context: str) -> str:
    ctx = f"{name or ''} {context or ''}".lower()

    infra_keywords = [
        "payment platform",
        "billing platform",
        "merchant of record",
        "payment processor",
        "subscription infrastructure",
        "billing infrastructure",
        "infrastructure",
        "payments infrastructure",
        "stripe",
        "paddle",
        "chargebee",
        "recurly",
    ]

    for word in infra_keywords:
        if word in ctx:
            return "infra"

    return "customer"


def enrich_company(company: dict) -> dict:
    # future: domain lookup, linkedin, tech stack
    return company