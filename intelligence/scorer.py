from __future__ import annotations

from typing import Any


def score_signal(signal: dict[str, Any]) -> int:
    score = 0
    if bool(signal.get("hiring")):
        score += 3
    if bool(signal.get("funding_signal")):
        score += 3
    if bool(signal.get("growth_signal")):
        score += 2

    tech_stack = signal.get("tech_stack") if isinstance(signal.get("tech_stack"), list) else []
    if len([x for x in tech_stack if str(x).strip()]) >= 2:
        score += 1

    return max(0, min(10, score))


def rank_companies(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for item in signals:
        if not isinstance(item, dict):
            continue
        scored = dict(item)
        scored["score"] = score_signal(item)
        ranked.append(scored)

    ranked.sort(key=lambda row: (-int(row.get("score") or 0), str(row.get("company") or "").lower()))
    return ranked
