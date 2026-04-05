from __future__ import annotations


def _score_signal_text(text: str) -> int:
    text_l = text.lower()
    score = 0

    if any(token in text_l for token in ["hiring", "open role", "recruit"]):
        score += 10
    if any(token in text_l for token in ["github", "repo", "commit", "release"]):
        score += 10
    if any(token in text_l for token in ["launch", "product", "feature", "announcement"]):
        score += 10
    if any(token in text_l for token in ["complaint", "issue", "outage", "friction"]):
        score += 5
    if any(token in text_l for token in ["funding", "series", "scaling", "expansion"]):
        score += 10

    return min(50, max(0, score))


def score_candidates(candidates: list[dict[str, str]]) -> list[dict[str, str | int]]:
    scored: list[dict[str, str | int]] = []
    seen: set[str] = set()
    for candidate in candidates:
        company = str(candidate.get("company") or "").strip()
        source = str(candidate.get("source") or "").strip()
        initial_signal = str(candidate.get("initial_signal") or "").strip()
        if not company:
            continue

        key = company.lower()
        if key in seen:
            continue
        seen.add(key)

        scored.append(
            {
                "company": company,
                "source": source,
                "initial_signal": initial_signal,
                "light_score": _score_signal_text(initial_signal),
            }
        )

    scored.sort(key=lambda row: int(row["light_score"]), reverse=True)
    return scored
