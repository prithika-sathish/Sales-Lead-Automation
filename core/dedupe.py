from __future__ import annotations

from difflib import SequenceMatcher


def _clean_name(name: str) -> str:
    return " ".join(str(name or "").lower().split()).strip()


def dedupe_companies(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    by_domain: dict[str, dict[str, object]] = {}
    no_domain: list[dict[str, object]] = []

    for row in rows:
        domain = str(row.get("domain") or "").strip().lower()
        if domain:
            existing = by_domain.get(domain)
            if not existing:
                by_domain[domain] = row
            else:
                existing_sources = set(str(existing.get("source") or "").split("|"))
                existing_sources.add(str(row.get("source") or ""))
                existing["source"] = "|".join(sorted(source for source in existing_sources if source))
        else:
            no_domain.append(row)

    output = list(by_domain.values())

    for candidate in no_domain:
        candidate_name = _clean_name(str(candidate.get("name") or ""))
        if not candidate_name:
            continue
        is_duplicate = False
        for existing in output:
            existing_name = _clean_name(str(existing.get("name") or ""))
            if not existing_name:
                continue
            if SequenceMatcher(None, candidate_name, existing_name).ratio() >= 0.9:
                is_duplicate = True
                break
        if not is_duplicate:
            output.append(candidate)

    return output
