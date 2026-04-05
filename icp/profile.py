from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ICPProfile:
    industries: list[str]
    regions: list[str]
    min_employee_count: int = 20
    max_employee_count: int = 1000
    required_keywords: list[str] = field(default_factory=list)
    excluded_companies: list[str] = field(default_factory=list)


@dataclass
class CompanySeed:
    name: str
    domain: str = ""
    notes: str = ""
