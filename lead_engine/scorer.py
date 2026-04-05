"""
Company scoring engine with meaningful signal weighting.
"""

from .models import (
    ScoringResult,
    CORE_ICP_KEYWORDS,
    TARGET_INDUSTRIES,
    LARGE_ENTERPRISES,
)


# Signal weights
SIGNAL_WEIGHTS = {
    # Core business signals
    "hiring": 20,
    "growth": 15,
    "subscription_model": 20,
    "b2b": 15,
    "tech": 10,
    
    # Product signals
    "product": 10,
    "service": 5,
    
    # Market signals
    "market_leader": 15,
    "competitive": 10,
    
    # Weakness signals
    "weak_data": -30,
}

# Size penalties
SIZE_PENALTIES = {
    "enterprise": -40,
    "well_known": -20,
}

# ICP match bonus
ICP_MATCH_BONUS = 25
INDUSTRY_MATCH_BONUS = 15

# Multi-source confidence
MULTI_SOURCE_BONUS = 10
LIST_PATTERN_BONUS = 5


def score_company(
    company_name: str,
    signals: list[str],
    occurrence_count: int = 1,
    icp_keywords: list[str] | None = None,
    industries: list[str] | None = None,
) -> ScoringResult:
    """
    Score a company based on signals, ICP match, and occurrences.
    
    Returns ScoringResult with score (0-100) and breakdown.
    """
    score = 0
    breakdown = {}
    
    # Normalize inputs
    signals_set = set(s.lower() for s in (signals or []))
    icp_keywords = icp_keywords or CORE_ICP_KEYWORDS
    industries = industries or TARGET_INDUSTRIES
    
    # ============================================
    # 1. Signal scoring
    # ============================================
    for signal in signals:
        signal_lower = signal.lower()
        
        # Direct match
        if signal_lower in SIGNAL_WEIGHTS:
            weight = SIGNAL_WEIGHTS[signal_lower]
            breakdown[f"signal:{signal}"] = weight
            score += weight
        
        # Substring matches
        else:
            for keyword, weight in SIGNAL_WEIGHTS.items():
                if keyword in signal_lower:
                    breakdown[f"signal:{signal}"] = weight
                    score += weight
                    break
    
    # ============================================
    # 2. ICP keyword matching
    # ============================================
    name_lower = company_name.lower()
    
    icp_matched = []
    for keyword in icp_keywords:
        if keyword.lower() in name_lower:
            icp_matched.append(keyword)
    
    if icp_matched:
        icp_points = min(ICP_MATCH_BONUS, ICP_MATCH_BONUS * len(icp_matched))
        breakdown["icp_match"] = icp_points
        score += icp_points
    
    # ============================================
    # 3. Industry match
    # ============================================
    industry_matched = []
    for industry in industries:
        if industry.lower() in name_lower:
            industry_matched.append(industry)
    
    if industry_matched:
        industry_points = INDUSTRY_MATCH_BONUS
        breakdown["industry_match"] = industry_points
        score += industry_points
    
    # ============================================
    # 4. Multi-source confidence
    # ============================================
    if occurrence_count >= 2:
        multi_points = MULTI_SOURCE_BONUS * min(3, occurrence_count // 2)
        breakdown["multi_source"] = multi_points
        score += multi_points
    elif occurrence_count == 1:
        # Single mention slightly penalizes
        breakdown["single_mention"] = -5
        score -= 5
    
    # ============================================
    # 5. Size penalties
    # ============================================
    # Large enterprise check
    is_large = False
    for enterprise in LARGE_ENTERPRISES:
        if enterprise.lower() in name_lower:
            breakdown["enterprise_penalty"] = SIZE_PENALTIES["enterprise"]
            score += SIZE_PENALTIES["enterprise"]
            is_large = True
            break
    
    # Well-known brand heuristic (very common in results)
    if not is_large and "well_known" in signals_set:
        breakdown["well_known_penalty"] = SIZE_PENALTIES["well_known"]
        score += SIZE_PENALTIES["well_known"]
    
    # ============================================
    # 6. Clamp score
    # ============================================
    score = max(0, min(100, score))
    
    # ============================================
    # 7. Confidence calculation
    # ============================================
    # Confidence is based on:
    # - presence of multiple positive signals
    # - lack of weak_data
    # - multiple occurrences
    
    confidence = 0.3  # baseline
    
    positive_signals = sum(
        1 for s in signals if s.lower() not in ["weak_data"]
    )
    confidence += min(0.4, positive_signals * 0.1)
    
    if "weak_data" not in signals_set:
        confidence += 0.15
    
    if occurrence_count >= 2:
        confidence += 0.15
    
    confidence = min(1.0, max(0.0, confidence))
    
    # ============================================
    # 8. Reason/summary
    # ============================================
    reasons = []
    if positive_signals >= 3:
        reasons.append(f"Strong signal match ({positive_signals} signals)")
    if icp_matched:
        reasons.append(f"ICP match: {', '.join(icp_matched[:2])}")
    if occurrence_count >= 2:
        reasons.append(f"Multi-source ({occurrence_count} occurrences)")
    if "weak_data" in signals_set:
        reasons.append("Low data quality")
    if is_large:
        reasons.append("Large enterprise (penalized)")
    
    reason = "; ".join(reasons) if reasons else "Mixed signals"
    
    return ScoringResult(
        score=int(score),
        breakdown=breakdown,
        confidence=round(confidence, 2),
        reason=reason,
    )


def filter_by_score(
    scored_companies: list[dict],
    min_score: int = 40,
    score_key: str = "score",
) -> tuple[list[dict], list[dict]]:
    """
    Filter companies by minimum score threshold.
    
    Returns: (passing, filtered_out)
    """
    passing = []
    filtered = []
    
    for company in scored_companies:
        score = company.get(score_key, 0)
        if score >= min_score:
            passing.append(company)
        else:
            filtered.append({**company, "_filtered_reason": f"Score {score} < {min_score}"})
    
    return passing, filtered
