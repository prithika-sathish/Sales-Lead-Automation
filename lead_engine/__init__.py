"""
Lead validation and scoring engine.

Provides:
- Entity normalization (split merged companies, clean names)
- Strict company validation (reject non-companies, generic terms)
- Meaningful scoring (signal-weighted, ICP-aware, size-adjusted)
- Output filtering (quality-first, high precision)
"""

from .models import (
    ValidationResult,
    ScoringResult,
    NormalizedCompany,
    ValidatedLead,
    SizeCategory,
)
from .normalizer import (
    normalize_company_name,
    extract_company_suffix,
    split_merged_entities,
    normalize_company,
    split_and_normalize,
)
from .validator import (
    is_valid_company,
    batch_validate_companies,
)
from .scorer import (
    score_company,
    filter_by_score,
)


__all__ = [
    # Models
    "ValidationResult",
    "ScoringResult",
    "NormalizedCompany",
    "ValidatedLead",
    "SizeCategory",
    # Normalizer
    "normalize_company_name",
    "extract_company_suffix",
    "split_merged_entities",
    "normalize_company",
    "split_and_normalize",
    # Validator
    "is_valid_company",
    "batch_validate_companies",
    # Scorer
    "score_company",
    "filter_by_score",
]
