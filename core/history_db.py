from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DB_PATH = Path(__file__).resolve().parent / "signal_history.db"


class SignalHistoryDB:
    def __init__(self, db_path: Path = DB_PATH) -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS signal_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        company TEXT NOT NULL,
                        run_id TEXT NOT NULL,
                        signal_type TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        value INTEGER NOT NULL,
                        topic TEXT,
                        created_at TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_signal_history_company_run
                    ON signal_history(company, run_id)
                    """
                )
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_signal_history_company_type
                    ON signal_history(company, signal_type)
                    """
                )

    def append_signals(self, company: str, signals: list[dict[str, Any]]) -> None:
        company_key = company.strip().lower()
        if not company_key or not signals:
            return

        run_id = datetime.now(timezone.utc).isoformat()
        created_at = run_id
        rows: list[tuple[str, str, str, str, int, str | None, str]] = []

        for signal in signals:
            signal_type = str(signal.get("signal_type") or "").strip() or "unknown"
            timestamp = str(signal.get("timestamp") or run_id)
            value = int(signal.get("final_score") or signal.get("signal_score") or 0)
            rows.append((company_key, run_id, signal_type, timestamp, value, None, created_at))
            rows.append(
                (
                    company_key,
                    run_id,
                    "topic_marker",
                    timestamp,
                    1,
                    signal_type.replace("_", " "),
                    created_at,
                )
            )

            metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
            topics = metadata.get("topics")
            if not isinstance(topics, list):
                continue
            for topic in topics:
                topic_name = str(topic).strip().lower()
                if not topic_name:
                    continue
                rows.append((company_key, run_id, "topic_marker", timestamp, 1, topic_name, created_at))

        if not rows:
            return

        with self._lock:
            with self._connect() as conn:
                conn.executemany(
                    """
                    INSERT INTO signal_history(company, run_id, signal_type, timestamp, value, topic, created_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )

    def get_recent_rows(self, company: str, limit_runs: int = 6) -> list[dict[str, Any]]:
        company_key = company.strip().lower()
        if not company_key:
            return []

        with self._lock:
            with self._connect() as conn:
                run_rows = conn.execute(
                    """
                    SELECT run_id
                    FROM signal_history
                    WHERE company = ?
                    GROUP BY run_id
                    ORDER BY MAX(created_at) DESC
                    LIMIT ?
                    """,
                    (company_key, max(1, limit_runs)),
                ).fetchall()

                run_ids = [str(row["run_id"]) for row in run_rows]
                if not run_ids:
                    return []

                placeholders = ",".join(["?"] * len(run_ids))
                query = (
                    "SELECT company, run_id, signal_type, timestamp, value, topic, created_at "
                    f"FROM signal_history WHERE company = ? AND run_id IN ({placeholders})"
                )
                rows = conn.execute(query, (company_key, *run_ids)).fetchall()

        return [dict(row) for row in rows]


history_db = SignalHistoryDB()