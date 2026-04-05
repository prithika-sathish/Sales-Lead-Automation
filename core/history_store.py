from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


class SignalHistoryStore:
    def __init__(self) -> None:
        self._history: dict[str, list[dict[str, Any]]] = {}

    def append_signals(self, company: str, signals: list[dict[str, Any]]) -> None:
        company_key = company.strip().lower()
        if not company_key:
            return

        rows = self._history.setdefault(company_key, [])
        for signal in signals:
            rows.append(
                {
                    "signal_type": str(signal.get("signal_type") or ""),
                    "timestamp": str(signal.get("timestamp") or datetime.now(timezone.utc).isoformat()),
                    "value": int(signal.get("final_score") or signal.get("signal_score") or 0),
                }
            )

    def get_company_history(self, company: str) -> list[dict[str, Any]]:
        return list(self._history.get(company.strip().lower(), []))


history_store = SignalHistoryStore()
