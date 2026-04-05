from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


CACHE_TTL_SECONDS = 3600


class InMemoryCache:
    def __init__(self) -> None:
        self._store: dict[str, tuple[datetime, Any]] = {}

    def get(self, key: str) -> Any | None:
        entry = self._store.get(key)
        if not entry:
            return None

        saved_at, value = entry
        now = datetime.now(timezone.utc)
        if now - saved_at > timedelta(seconds=CACHE_TTL_SECONDS):
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any) -> None:
        self._store[key] = (datetime.now(timezone.utc), value)


cache = InMemoryCache()
