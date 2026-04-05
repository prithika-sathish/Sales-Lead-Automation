"""
Data models for hybrid query generation system.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


QueryMode = Literal["strict", "hybrid"]


@dataclass(slots=True)
class StructuredContext:
    """Structured information extracted from markdown."""
    product_description: str = ""
    core_icp: list[str] = field(default_factory=list)
    regions: list[str] = field(default_factory=list)
    industries: list[str] = field(default_factory=list)
    hints: list[str] = field(default_factory=list)
    company_name: str = ""
    known_companies: list[str] = field(default_factory=list)
    full_text: str = ""


@dataclass(slots=True)
class ExpandedSegment:
    """Segment discovered via LLM expansion."""
    segment_name: str
    description: str
    why_need: str
    keywords: list[str]


@dataclass
class QueryItem:
    """A single query to send to search engine."""
    query: str
    source: str
    segment: str
    priority: int
    query_type: str = "icp"  # icp | industry | behavioral | competitor


@dataclass(slots=True)
class QueryConfig:
    """Configuration for query generation."""
    mode: QueryMode = "hybrid"
    max_queries:int = 96
    use_llm: bool = True
    max_segments: int = 10
