from __future__ import annotations

from typing import Any


class LeadRanker:
    def score(self, signals: dict[str, bool]) -> int:
        score = 0
        if bool(signals.get("hiring")):
            score += 40
        if bool(signals.get("funding")):
            score += 30
        if bool(signals.get("b2b")):
            score += 20
        if bool(signals.get("region_match")):
            score += 10
        return score

    def rank(self, rows: list[dict]) -> list[dict]:
        ranked = list(rows)
        ranked.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
        return ranked

    def score_from_factors(self, factors: dict[str, Any]) -> int:
        raw = factors.get("raw_score", 0)
        try:
            raw_score = int(raw)
        except (TypeError, ValueError):
            raw_score = 0
        return min(raw_score * 15, 100)

    def rank_discovered_leads(self, rows: list[dict[str, Any]], *, limit: int = 50) -> list[dict[str, Any]]:
        enriched_rows: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            company = str(row.get("company") or row.get("name") or "").strip()
            website = str(row.get("website") or "").strip()
            signal_block = row.get("signal_data") if isinstance(row.get("signal_data"), dict) else {}
            factors = signal_block.get("score_factors") if isinstance(signal_block.get("score_factors"), dict) else {}
            active_signals = signal_block.get("signals") if isinstance(signal_block.get("signals"), list) else []

            if not company or not website:
                continue

            score = self.score_from_factors(factors)
            if score <= 0:
                continue

            reason_tokens = [str(signal).strip() for signal in active_signals if str(signal).strip()]

            enriched_rows.append(
                {
                    "company": company,
                    "website": website,
                    "signals": active_signals,
                    "score": score,
                    "reason": ", ".join(reason_tokens),
                    "source_query": str(row.get("source_query") or "").strip(),
                }
            )

        enriched_rows.sort(key=lambda item: int(item.get("score") or 0), reverse=True)

        deduped: list[dict[str, Any]] = []
        seen_websites: set[str] = set()
        for row in enriched_rows:
            key = str(row.get("website") or "").lower()
            if not key or key in seen_websites:
                continue
            seen_websites.add(key)
            deduped.append(row)
            if len(deduped) >= max(1, int(limit)):
                break

        return deduped
