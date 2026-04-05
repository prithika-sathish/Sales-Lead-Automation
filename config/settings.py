from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass(slots=True)
class QueryConfig:
    mode: Literal["strict", "hybrid"] = "hybrid"
    max_queries: int = 96
    use_llm: bool = True
    max_segments: int = 10


@dataclass(slots=True)
class DiscoverySettings:
    regions: list[str] = field(
        default_factory=lambda: [
            "India",
            "USA",
            "Germany",
            "Singapore",
            "Malaysia",
            "Vietnam",
            "Philippines",
            "Europe",
        ]
    )
    industries: list[str] = field(default_factory=lambda: ["SaaS", "AI", "fintech", "devtools"])
    max_pages: int = 2
    target_min: int = 50
    target_max: int = 100
    crawl_depth: int = 1
    llm_provider: str = "gemini"
    batch_size: int = 8
    request_timeout_seconds: int = 30
    query_config: QueryConfig = field(default_factory=QueryConfig)
