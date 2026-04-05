from __future__ import annotations

from typing import Any

from app.llm import generate_json_with_gemini
from app.schemas import CompanySignalBundle
from app.schemas import NormalizedSignal
from app.schemas import SignalIntelligenceResult


SIGNAL_INTELLIGENCE_SYSTEM_PROMPT = (
    "You are a B2B signal intelligence strategist. Infer company stage and buying intent ONLY from provided signals. "
    "Do not hallucinate. Be specific, concise, and sales-actionable."
)

HIGH_VALUE_THRESHOLD = 12


def _compact_signal(signal: NormalizedSignal) -> str:
    return (
        f"{signal.signal_type}"
        f"|score={signal.final_score}"
        f"|strength={signal.signal_strength}"
        f"|recency={signal.recency_score}"
        f"|source={signal.source}"
    )


def _top_signals(signals: list[NormalizedSignal], limit: int = 20) -> list[NormalizedSignal]:
    ordered = sorted(signals, key=lambda s: s.final_score, reverse=True)
    return ordered[:limit]


def _high_value_signals(signals: list[NormalizedSignal]) -> list[NormalizedSignal]:
    return [sig for sig in signals if sig.final_score >= HIGH_VALUE_THRESHOLD]


def _derived_signals(signals: list[NormalizedSignal]) -> list[str]:
    derived = [sig.signal_type for sig in signals if sig.source == "fusion"]
    # Preserve order and uniqueness.
    seen: set[str] = set()
    unique: list[str] = []
    for item in derived:
        if item in seen:
            continue
        seen.add(item)
        unique.append(item)
    return unique


def _build_prompt(company: str, top_signals: list[NormalizedSignal], derived_signals: list[str]) -> str:
    compact = [_compact_signal(sig) for sig in top_signals]
    return (
        "Return ONLY valid JSON with this exact schema:\n"
        '{"stage":"early|growth|scaling|enterprise","buying_intent_score":0-100,'
        '"intent_tags":[str],"why_now":str,"recommended_pitch_angle":str}\n\n'
        "Rules:\n"
        "- infer stage from signal patterns and maturity indicators\n"
        "- base reasoning ONLY on provided signals\n"
        "- do not invent data\n"
        "- keep output specific and actionable\n\n"
        f"Company: {company}\n"
        f"Top Signals:\n- " + "\n- ".join(compact) + "\n"
        f"Derived Signals: {derived_signals}\n"
    )


def _base_intent_score(signals: list[NormalizedSignal]) -> int:
    if not signals:
        return 0
    avg_score = sum(sig.final_score for sig in signals) / len(signals)
    scaled = round(min(100, avg_score * 4))
    return max(0, min(100, scaled))


def _apply_deterministic_boosts(score: int, signals: list[NormalizedSignal]) -> int:
    boosted = score
    if any(sig.signal_type == "infra_scaling" for sig in signals):
        boosted += 10
    if any(sig.signal_type == "hiring" and sig.final_score >= 16 for sig in signals):
        boosted += 10
    if any(sig.signal_type in {"customer_pain", "product_gap"} for sig in signals):
        boosted += 5
    return min(100, boosted)


def _fallback_result(company: str, signals: list[NormalizedSignal]) -> SignalIntelligenceResult:
    top = _top_signals(signals, limit=5)
    score = _apply_deterministic_boosts(_base_intent_score(signals), signals)

    stage = "early"
    if any(sig.signal_type == "infra_scaling" for sig in signals):
        stage = "scaling"
    elif score >= 70:
        stage = "growth"

    tags: list[str] = []
    if any(sig.signal_type == "infra_scaling" for sig in signals):
        tags.append("infra_scaling")
    if any(sig.signal_type == "hiring" for sig in signals):
        tags.append("team_expansion")
    if any(sig.signal_type in {"customer_pain", "product_gap"} for sig in signals):
        tags.append("product_iteration")
    if not tags:
        tags.append("early_signal_monitoring")

    return SignalIntelligenceResult(
        company=company,
        stage=stage,
        buying_intent_score=score,
        intent_tags=tags,
        key_signals=[_compact_signal(sig) for sig in top[:3]],
        why_now="Recent high-scoring operational signals indicate a timely window for outreach.",
        recommended_pitch_angle="Lead with a signal-led hypothesis tied to their most recent operational pressure and propose a fast, low-risk pilot.",
    )


def analyze_company_signals(company_signals: CompanySignalBundle) -> SignalIntelligenceResult:
    company = company_signals.company
    top = _top_signals(company_signals.signals, limit=20)
    high_value = _high_value_signals(top)
    derived = _derived_signals(top)
    prompt = _build_prompt(company, top, derived)

    llm_score = _base_intent_score(high_value if high_value else top)
    llm_result: dict[str, Any] | None = None

    try:
        llm_result = generate_json_with_gemini(
            system_prompt=SIGNAL_INTELLIGENCE_SYSTEM_PROMPT,
            user_prompt=prompt,
            temperature=0.3,
        )
    except ValueError:
        return _fallback_result(company, top)

    stage_raw = str(llm_result.get("stage") or "early").strip().lower()
    stage = stage_raw if stage_raw in {"early", "growth", "scaling", "enterprise"} else "early"
    why_now = str(llm_result.get("why_now") or "Signal momentum indicates immediate relevance.").strip()
    angle = str(
        llm_result.get("recommended_pitch_angle")
        or "Use a concise, evidence-led outreach tied to current execution pressure."
    ).strip()

    tags_raw = llm_result.get("intent_tags")
    intent_tags = [str(item).strip() for item in tags_raw] if isinstance(tags_raw, list) else []
    intent_tags = [tag for tag in intent_tags if tag]

    score_raw = llm_result.get("buying_intent_score")
    if isinstance(score_raw, (int, float)):
        llm_score = max(0, min(100, int(score_raw)))

    final_score = _apply_deterministic_boosts(llm_score, top)
    key_signal_count = 5 if len(top) >= 5 else max(3, len(top))
    key_signals = [_compact_signal(sig) for sig in top[:key_signal_count]]

    return SignalIntelligenceResult(
        company=company,
        stage=stage,
        buying_intent_score=final_score,
        intent_tags=intent_tags,
        key_signals=key_signals,
        why_now=why_now,
        recommended_pitch_angle=angle,
    )


def analyze_companies_signals(companies: list[CompanySignalBundle]) -> list[SignalIntelligenceResult]:
    return [analyze_company_signals(company) for company in companies]
