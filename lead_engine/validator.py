"""
Company validation logic.
Rejects non-companies, generic terms, and unsuitable targets.
"""

import re
from typing import Set

from .models import (
    ValidationResult,
    GENERIC_KEYWORDS,
    NON_TARGET_KEYWORDS,
    LARGE_ENTERPRISES,
)


def is_valid_company(
    name: str,
    signals: list[str] | None = None,
    occurrence_count: int = 1,
) -> ValidationResult:
    """
    Validate if a name is a real, target-worthy company.
    
    Returns ValidationResult with is_valid and reason/issues.
    """
    if not name or not isinstance(name, str):
        return ValidationResult(False, "Empty or invalid name", ["Empty name"])
    
    issues = []
    
    # Check 1: Length and basic structure
    name_lower = name.lower().strip()
    words = name_lower.split()
    
    if not words:
        return ValidationResult(False, "No words in name", ["No words"])
    
    # Single lowercase word (likely generic)
    if len(words) == 1 and not name[0].isupper():
        issues.append("Single lowercase word")
    
    # Check 2: Generic non-company keywords
    for keyword in GENERIC_KEYWORDS:
        if keyword in name_lower:
            issues.append(f"Contains generic keyword: {keyword}")
    
    # Check 3: Non-target keywords (VC, media, directories)
    for keyword in NON_TARGET_KEYWORDS:
        if keyword in name_lower:
            issues.append(f"Contains non-target keyword: {keyword}")
    
    # Check 4: Sentence-like structure
    # (more than 3 words and looks fragmented)
    if len(words) > 3:
        verb_endings = ["ing", "ed"]
        has_verb_ending = any(
            word.endswith(tuple(verb_endings)) for word in words
        )
        if has_verb_ending:
            issues.append("Looks like sentence fragment (verb-like ending)")
    
    # Check 5: Verb-like words (eliminates, creates, etc.)
    # These are often extracted from sentences, not company names
    if len(words) == 1:
        # Single word ending in -ate, -ates, -ize, -izes
        if re.search(r'(ate|ize)s?$', name_lower):
            issues.append("Looks like verb form (ends with -ate, -ize)")
        
        # Single word is too generic/common
        overly_generic_singles = [
            "ticker", "menu", "table", "report", "analysis",
            "tool", "site", "platform", "hub", "center",
            "bank", "post", "space", "vault", "flow",
        ]
        if name_lower in overly_generic_singles:
            issues.append(f"Single generic word: {name_lower}")
    
    # Check 6: Large enterprise filter
    for enterprise in LARGE_ENTERPRISES:
        if enterprise.lower() in name_lower:
            issues.append(f"Known large enterprise or unsuitable: {enterprise}")
    
    # Check 7: Weak data rule
    # If weak_data signal and only 1 occurrence, likely noise
    signals_list = signals or []
    if "weak_data" in signals_list and occurrence_count <= 1:
        issues.append("weak_data signal with single occurrence")
    
    # Check 8: Obviously fake company patterns
    if re.search(r'\b(the\s+)?best\b', name_lower, re.IGNORECASE):
        issues.append("Common list article pattern (the best...)")
    
    if re.search(r'\b(alternatives?|competitors?|similar)\b', name_lower):
        issues.append("List/comparison title pattern")
    
    # Check 9: Known non-entity patterns
    concept_words = {
        "recurring revenue", "annual recurring", "key metrics",
        "best practices", "case study", "methodology",
    }
    for concept in concept_words:
        if concept in name_lower:
            issues.append(f"Concept/abstract term: {concept}")
    
    # Final decision
    if issues:
        reason = "; ".join(issues[:2])  # First 2 issues
        return ValidationResult(False, reason, issues)
    
    return ValidationResult(True, "Valid target company")


def batch_validate_companies(
    companies: list[dict],
    signals_key: str = "signals",
    name_key: str = "company",
) -> tuple[list[dict], list[dict]]:
    """
    Batch validate company list.
    
    Returns: (valid_companies, rejected_with_reasons)
    """
    valid = []
    rejected = []
    
    for entry in companies:
        name = entry.get(name_key, "")
        signals = entry.get(signals_key, [])
        occurrence = entry.get("occurrence_count", 1)
        
        result = is_valid_company(name, signals, occurrence)
        
        if result.is_valid:
            valid.append(entry)
        else:
            rejected.append({
                **entry,
                "_validation_reason": result.reason,
                "_validation_issues": result.issues,
            })
    
    return valid, rejected
