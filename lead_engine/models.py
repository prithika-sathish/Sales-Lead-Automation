"""
Data models for lead validation and scoring system.
"""

from dataclasses import dataclass, field
from typing import Literal


SizeCategory = Literal["small", "mid", "enterprise"]


@dataclass
class ValidationResult:
    """Result of company validation."""
    is_valid: bool
    reason: str = ""
    issues: list[str] = field(default_factory=list)


@dataclass
class ScoringResult:
    """Result of company scoring."""
    score: int  # 0-100
    breakdown: dict[str, int] = field(default_factory=dict)  # signal -> points
    confidence: float = 0.5  # 0.0-1.0
    reason: str = ""


@dataclass
class NormalizedCompany:
    """Normalized company entity."""
    name: str
    original: str  # Original raw name
    suffixes: list[str] = field(default_factory=list)  # Inc, Ltd, LLC, etc.
    is_split: bool = False  # True if split from merged entity


@dataclass
class ValidatedLead:
    """A validated and scored lead."""
    company: str
    original_name: str
    signals: list[str]
    score: int
    confidence: float
    reason: str
    size_category: SizeCategory
    source_query: str
    website: str = ""
    occurrence_count: int = 1


# ICP Keywords (from company profile)
CORE_ICP_KEYWORDS = [
    # Subscription/Billing
    "subscription", "recurring", "billing", "pricing",
    "saas", "usage-based", "hybrid billing",
    
    # Payment/Fintech
    "payment", "fintech", "payment processing",
    "recurring payment", "revenue", "commission",
    
    # Product Management
    "pricing", "monetization", "revenue optimization",
    "upsell", "churn", "retention",
]

# Industries relevant to the target company
TARGET_INDUSTRIES = [
    "saas", "fintech", "subscription billing",
    "revenue management", "payments", "e-commerce",
    "payment processing", "billing software"
]

# Known large enterprises to exclude
LARGE_ENTERPRISES = [
    "Google", "Microsoft", "Amazon", "Apple", "Meta",
    "Walmart", "Adobe", "Salesforce", "Oracle", "SAP",
    "IBM", "Intel", "Cisco", "Accenture", "Deloitte",
    "McKinsey", "PwC", "EY", "Bain", "BCG",
    "JP Morgan", "Goldman Sachs", "Morgan Stanley",
    "HSBC", "Barclays", "Deutsche Bank",
    "Zoom", "Slack", "Atlassian", "GitHub",
    "Netflix", "Spotify", "Disney", "Warner",
]

# Generic non-company keywords to reject
GENERIC_KEYWORDS = [
    "metrics", "services", "features", "solutions",
    "revenue", "growth", "analysis", "report", "table",
    "methodology", "content", "menu", "navigation",
    "key metrics", "best practices", "case study",
    "guide", "tutorial", "blog", "article", "resource",
]

# VC/Media/Directory keywords to reject
NON_TARGET_KEYWORDS = [
    "ventures", "capital", "fund", "vc", "investor",
    "blog", "media", "news", "magazine", "publication",
    "directory", "listing", "database", "list",
    "research", "academy", "school", "university",
]
