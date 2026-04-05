from __future__ import annotations

from app.apify_client import fetch_apify_query


def _candidate(company: str, source: str, initial_signal: str) -> dict[str, str]:
    return {
        "company": company,
        "source": source,
        "initial_signal": initial_signal,
    }


def discover_candidates(domains: list[str]) -> list[dict[str, str]]:
    discovered: list[dict[str, str]] = []
    seen: set[str] = set()

    source_queries: list[tuple[str, str]] = []
    for domain in domains:
        source_queries.extend(
            [
                (f"{domain} startups companies", "apify_leads"),
                (f"{domain} github startup", "github_mentions"),
                (f"{domain} product hunt launch", "product_hunt"),
                (f"{domain} reddit startup discussion", "reddit"),
            ]
        )

    for query, source in source_queries:
        try:
            items = fetch_apify_query(query, limit=6)
        except RuntimeError:
            continue

        for item in items:
            company = str(item.get("company") or "").strip()
            if not company:
                continue

            dedupe_key = company.lower()
            if dedupe_key in seen:
                continue

            text = str(item.get("description") or item.get("activity_text") or "").strip()
            if not text:
                text = f"Mentioned via {source} query '{query}'"

            seen.add(dedupe_key)
            discovered.append(_candidate(company, source, text))

    return discovered
