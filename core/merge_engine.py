from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any


def extract_domain(value: str) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("https://", "").replace("http://", "")
    return text.split("/")[0].strip()


def name_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, str(a or "").lower(), str(b or "").lower()).ratio()


def merge_and_dedupe(source_batches: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []

    for batch in source_batches:
        for row in batch:
            if isinstance(row, dict):
                merged.append(row)

    by_domain: dict[str, dict[str, Any]] = {}
    no_domain: list[dict[str, Any]] = []

    for row in merged:
        url = row.get("url") or row.get("website") or row.get("domain") or ""
        domain = extract_domain(str(url))
        name = str(row.get("name") or row.get("company") or "").strip()

        normalized = {**row, "name": name, "domain": domain}

        if domain:
            if domain not in by_domain:
                by_domain[domain] = normalized
            else:
                old = by_domain[domain]
                old_tags = set(old.get("tags") or [])
                new_tags = set(normalized.get("tags") or [])
                merged_tags = sorted(list(old_tags | new_tags))
                old["tags"] = merged_tags
                if len(str(normalized.get("description") or "")) > len(str(old.get("description") or "")):
                    old["description"] = normalized.get("description")
                by_domain[domain] = old
        else:
            no_domain.append(normalized)

    deduped = list(by_domain.values())

    # Secondary dedupe for no-domain rows by name similarity.
    final_rows: list[dict[str, Any]] = deduped[:]
    for row in no_domain:
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        if any(name_similarity(name, existing.get("name", "")) >= 0.9 for existing in final_rows):
            continue
        final_rows.append(row)

    return final_rows
