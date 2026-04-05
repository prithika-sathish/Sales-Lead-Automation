from __future__ import annotations

import re
from typing import Any


PAIN_PATTERNS = [
    "problem",
    "issue",
    "pain",
    "frustrat",
    "bottleneck",
    "downtime",
    "latency",
    "slow",
    "not working",
    "doesn't scale",
    "does not scale",
    "manual",
    "unmet need",
    "missing",
    "unhappy",
    "dissatisf",
]

COMPARISON_PATTERNS = [
    "alternative to",
    "vs ",
    "versus",
    "compare",
    "switch from",
    "switching from",
    "replacing",
    "looking for a replacement",
    "better than",
]

CHURN_PATTERNS = [
    "cancel",
    "cancelling",
    "churn",
    "leave",
    "switch",
    "moving away",
    "going back to",
    "fed up",
    "done with",
]


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower()).strip()


def _company_mentioned(post: str, company: str) -> bool:
    normalized_post = _normalize(post)
    normalized_company = _normalize(company)
    if not normalized_company:
        return False

    company_tokens = [token for token in re.split(r"[^a-z0-9]+", normalized_company) if len(token) >= 3]
    if normalized_company in normalized_post:
        return True
    if company_tokens and all(token in normalized_post for token in company_tokens[:2]):
        return True

    aliases = {
        "stripe": ["stripe", "stripe.com"],
        "salesforce": ["salesforce", "sfdc"],
        "shopify": ["shopify"],
        "sap": ["sap"],
        "oracle": ["oracle"],
        "microsoft": ["microsoft", "dynamics"],
        "netsuite": ["netsuite"],
        "odoo": ["odoo"],
        "erp": ["erp"],
    }
    for alias in aliases.get(normalized_company, []):
        if alias in normalized_post:
            return True

    return False


def _find_patterns(text: str, patterns: list[str]) -> list[str]:
    return [pattern for pattern in patterns if pattern in text]


def _build_reason(intent_type: str, matched_patterns: list[str], post: str, company: str) -> str:
    snippet = _safe_text(post)[:140]
    if len(post) > 140:
        snippet += "..."
    if intent_type == "pain":
        return f"Explicit pain described for {company}: {', '.join(matched_patterns[:3])}. Post snippet: {snippet}"
    if intent_type == "comparison":
        return f"Actionable comparison intent for {company}: {', '.join(matched_patterns[:3])}. Post snippet: {snippet}"
    if intent_type == "churn_risk":
        return f"Churn or switching intent for {company}: {', '.join(matched_patterns[:3])}. Post snippet: {snippet}"
    return f"No explicit buying intent or pain was found for {company}."


def evaluate_reddit_signal(reddit_post: str, company: str) -> dict[str, Any]:
    post = _normalize(reddit_post)
    company_name = _safe_text(company)

    if not post or not company_name:
        return {
            "accepted": False,
            "intent_type": "none",
            "confidence": 0.0,
            "reason": "Missing reddit_post or company.",
        }

    company_mentioned = _company_mentioned(post, company_name)
    if not company_mentioned:
        return {
            "accepted": False,
            "intent_type": "none",
            "confidence": 0.05,
            "reason": "The post does not explicitly mention the company or product.",
        }

    pain_hits = _find_patterns(post, PAIN_PATTERNS)
    comparison_hits = _find_patterns(post, COMPARISON_PATTERNS)
    churn_hits = _find_patterns(post, CHURN_PATTERNS)

    actionable_markers = pain_hits + comparison_hits + churn_hits
    if not actionable_markers:
        return {
            "accepted": False,
            "intent_type": "none",
            "confidence": 0.15,
            "reason": "The post mentions the company but does not show clear pain, comparison, or switching intent.",
        }

    if churn_hits:
        confidence = 0.72 if len(churn_hits) >= 2 else 0.64
        if pain_hits:
            confidence += 0.08
        return {
            "accepted": True,
            "intent_type": "churn_risk",
            "confidence": round(min(0.95, confidence), 2),
            "reason": _build_reason("churn_risk", churn_hits or actionable_markers, reddit_post, company_name),
        }

    if comparison_hits and pain_hits:
        confidence = 0.74 if len(comparison_hits) >= 2 else 0.66
        confidence += 0.06 if pain_hits else 0.0
        return {
            "accepted": True,
            "intent_type": "comparison",
            "confidence": round(min(0.94, confidence), 2),
            "reason": _build_reason("comparison", comparison_hits + pain_hits, reddit_post, company_name),
        }

    if pain_hits:
        confidence = 0.68 if len(pain_hits) >= 2 else 0.6
        return {
            "accepted": True,
            "intent_type": "pain",
            "confidence": round(min(0.9, confidence), 2),
            "reason": _build_reason("pain", pain_hits, reddit_post, company_name),
        }

    if comparison_hits:
        confidence = 0.62 if len(comparison_hits) >= 2 else 0.56
        return {
            "accepted": True,
            "intent_type": "comparison",
            "confidence": round(min(0.88, confidence), 2),
            "reason": _build_reason("comparison", comparison_hits, reddit_post, company_name),
        }

    return {
        "accepted": False,
        "intent_type": "none",
        "confidence": 0.18,
        "reason": "The post is too generic, sarcastic, or non-actionable to treat as buying intent.",
    }
