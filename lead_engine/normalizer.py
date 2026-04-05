"""
Company name normalization and entity splitting.
"""

import re
from typing import Callable

from .models import NormalizedCompany


# Common company suffixes to strip/track
COMPANY_SUFFIXES = {
    "inc": "Inc",
    "inc.": "Inc",
    "incorporated": "Inc",
    "ltd": "Ltd",
    "ltd.": "Ltd",
    "limited": "Ltd",
    "llc": "LLC",
    "l.l.c": "LLC",
    "co": "Co",
    "co.": "Co",
    "corp": "Corp",
    "corp.": "Corp",
    "corporation": "Corp",
    "gmbh": "GmbH",
    "sa": "SA",
    "bv": "BV",
    "pty": "Pty",
}

# Splitting patterns
SPLIT_PATTERNS = [
    (r',\s+', ","),  # comma split
    (r'\s+and\s+', " and "),  # "and" split
    (r'\s+&\s+', " & "),  # ampersand split
]


def normalize_company_name(raw_name: str) -> str:
    """
    Normalize a company name:
    - title case
    - remove extra whitespace
    - deduplicate repeated tokens
    """
    if not raw_name or not isinstance(raw_name, str):
        return ""
    
    # Strip and normalize whitespace
    name = " ".join(raw_name.split())
    
    # Deduplicate consecutive repeated words
    tokens = name.split()
    deduped = []
    for token in tokens:
        if not deduped or deduped[-1].lower() != token.lower():
            deduped.append(token)
    
    normalized = " ".join(deduped)
    
    # Title case (but preserve certain patterns)
    normalized = _smart_title_case(normalized)
    
    return normalized.strip()


def _smart_title_case(text: str) -> str:
    """Title case that preserves acronyms and special patterns."""
    words = text.split()
    result = []
    
    for word in words:
        # Preserve all-caps acronyms (e.g., "API", "SDK")
        if len(word) <= 3 and word.isupper():
            result.append(word)
        # Preserve camelCase patterns (e.g., "GitHub", "JavaScript")
        elif any(c.isupper() for c in word[1:]) if len(word) > 1 else False:
            result.append(word)
        else:
            result.append(word.capitalize())
    
    return " ".join(result)


def extract_company_suffix(name: str) -> tuple[str, str | None]:
    """
    Extract company suffix (Inc, Ltd, LLC, etc.) from name.
    
    Returns: (name_without_suffix, suffix_key_lower)
    """
    words = name.split()
    if not words:
        return name, None
    
    last_word = words[-1].lower().rstrip(".,")
    
    # Check if last word is a known suffix
    for suffix_key, suffix_display in COMPANY_SUFFIXES.items():
        if last_word == suffix_key.rstrip("."):
            name_without = " ".join(words[:-1])
            return name_without.strip(), suffix_key
    
    return name, None


def split_merged_entities(raw_name: str) -> list[str]:
    """
    Split merged company entities like "Zoom, General Motors" or "Company A and Company B".
    
    Returns: List of individual company names
    """
    if not raw_name or not isinstance(raw_name, str):
        return []
    
    names = [raw_name]
    
    # Apply split patterns
    for pattern, delimiter in SPLIT_PATTERNS:
        new_names = []
        for name in names:
            if re.search(pattern, name, re.IGNORECASE):
                # Split by pattern
                parts = re.split(pattern, name, flags=re.IGNORECASE)
                new_names.extend(parts)
            else:
                new_names.append(name)
        names = new_names
    
    # Clean and deduplicate
    cleaned = []
    seen = set()
    for name in names:
        clean = normalize_company_name(name)
        if clean and clean.lower() not in seen:
            cleaned.append(clean)
            seen.add(clean.lower())
    
    return cleaned


def normalize_company(raw_name: str) -> NormalizedCompany:
    """
    Fully normalize a company name.
    
    Returns: NormalizedCompany with name, original, suffixes
    """
    if not raw_name or not isinstance(raw_name, str):
        return NormalizedCompany("", "", [])
    
    original = raw_name
    
    # Clean
    normalized = normalize_company_name(raw_name)
    
    # Extract suffix
    without_suffix, suffix = extract_company_suffix(normalized)
    
    suffixes = [COMPANY_SUFFIXES[suffix]] if suffix else []
    
    return NormalizedCompany(
        name=without_suffix if without_suffix else normalized,
        original=original,
        suffixes=suffixes,
        is_split=False,
    )


def split_and_normalize(raw_name: str) -> list[NormalizedCompany]:
    """
    Split merged entities and normalize each one.
    
    Returns: List of NormalizedCompany objects
    """
    split_names = split_merged_entities(raw_name)
    
    result = []
    for name in split_names:
        norm = normalize_company(name)
        # Mark as split if we split multiple entities
        norm.is_split = len(split_names) > 1
        result.append(norm)
    
    return result
