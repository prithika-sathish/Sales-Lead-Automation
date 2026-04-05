from __future__ import annotations

from typing import Any

from app.llm import generate_json_with_gemini
from app.schemas import LeadEngineInput
from app.schemas import RankedLead
from intelligence.company_context import get_company_context
from intelligence.context_compressor import compress_company_context
from intelligence.high_intent_signals import classify_intent_stage
from intelligence.high_intent_signals import derive_high_intent_signals
from intelligence.high_intent_signals import extract_high_intent_signals
from intelligence.high_intent_signals import high_intent_score_boost
from intelligence.high_intent_signals import intent_stage_reason
from intelligence.high_intent_signals import key_trigger_summary
from intelligence.high_intent_signals import top_high_intent_signals


LEAD_ENGINE_SYSTEM_PROMPT = (
    "You are a high-performance B2B sales strategist. Be decisive, specific, and signal-grounded. "
    "Every claim must reference the provided signals. Do not use hedging language or generic phrasing."
)

HIGH_INTENT_TYPES = {
    "hiring_spike",
    "infra_scaling",
    "integration_added",
    "product_launch",
    "sales_expansion",
    "growth_phase",
    "platform_expansion",
    "dev_activity",
}

GENERIC_PHRASES = (
    "may be",
    "could be",
    "indicates activity",
    "shows growth",
    "recent signals indicate",
    "timely buying window",
)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _is_hiring_only(signals: list[dict[str, Any]]) -> bool:
    if not signals:
        return False
    return all(str(sig.get("signal_type") or "") == "hiring" for sig in signals)


def _hiring_count_score(hiring_count: int) -> int:
    if hiring_count >= 5:
        return 85
    if hiring_count >= 2:
        return 65
    return 0


def _hiring_only_lead(company: str, signals: list[dict[str, Any]], stakeholders: list[str]) -> RankedLead:
    hiring_count = len(signals)
    score = _hiring_count_score(hiring_count)
    priority = "high" if hiring_count >= 5 else "medium"
    stage = "scaling" if hiring_count >= 5 else "growth"
    top_signals = [
        (
            f"hiring | 3 | 3 | {str(sig.get('metadata', {}).get('evidence') or sig.get('metadata', {}).get('role') or sig.get('metadata', {}).get('raw_text') or '').strip()}"
        )
        for sig in signals[:5]
        if str(sig.get('metadata', {}).get('evidence') or sig.get('metadata', {}).get('role') or sig.get('metadata', {}).get('raw_text') or '').strip()
    ]

    target_persona = stakeholders[0] if stakeholders else "VP Sales"
    return RankedLead(
        company=company,
        score=score,
        priority=_priority(score) if score > 0 else priority,
        stage=stage,  # type: ignore[arg-type]
        intent_stage="AWARE",
        intent_tags=["hiring", "sales_hiring"],
        top_signals=top_signals[:5],
        why_now=f"{hiring_count} active sales hiring role(s) indicate ongoing GTM expansion and immediate process standardization needs.",
        key_trigger_summary=f"AWARE: {hiring_count} hiring signal(s) from job postings",
        target_persona=target_persona,
        pain_point="Scaling outbound execution while maintaining pipeline quality.",
        pitch_angle="Position ERP workflow standardization for faster ramp, cleaner handoffs, and predictable execution.",
        key_signals=top_signals[:5],
        confidence=min(100, 60 + (hiring_count * 8)),
    )


def _priority(score: int) -> str:
    if score >= 75:
        return "high"
    if score >= 50:
        return "medium"
    return "low"


def _compact_signal(signal: dict[str, Any]) -> str:
    return (
        f"{signal.get('signal_type', 'unknown')}"
        f"|score={int(signal.get('final_score') or 0)}"
        f"|recency={int(signal.get('recency_score') or 0)}"
        f"|source={signal.get('source', '')}"
    )


def _signal_reason(signal: dict[str, Any]) -> str:
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    raw_text = str(metadata.get("raw_text") or "").strip()
    if raw_text:
        clipped = raw_text[:120]
        return clipped if len(raw_text) <= 120 else f"{clipped}..."

    signal_type = str(signal.get("signal_type") or "unknown")
    mapping = {
        "hiring_spike": "multiple hiring signals point to team expansion",
        "hiring": "recent hiring activity points to capacity expansion",
        "infra_scaling": "infra-oriented activity points to systems scaling",
        "integration_added": "new integration evidence points to ecosystem expansion",
        "product_launch": "new launch evidence points to active product motion",
        "sales_expansion": "sales hiring or motion points to revenue scale-up",
        "growth_phase": "derived growth pattern from repeated strong signals",
        "dev_activity": "developer activity points to active build velocity",
        "momentum": "engagement lift points to rising attention",
        "strategic_focus": "repeated themes point to sustained priority",
    }
    return mapping.get(signal_type, "signal scored high relative to peers")


def _group_signals(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for signal in signals:
        signal_type = str(signal.get("signal_type") or "unknown")
        bucket = grouped.setdefault(
            signal_type,
            {
                "signal_type": signal_type,
                "count": 0,
                "max_score": 0,
                "top_signal": signal,
            },
        )
        bucket["count"] += 1
        score = int(signal.get("final_score") or 0)
        if score > int(bucket["max_score"] or 0):
            bucket["max_score"] = score
            bucket["top_signal"] = signal

    return sorted(grouped.values(), key=lambda item: (int(item["max_score"]), int(item["count"])), reverse=True)


def _top_categories(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    categories = _group_signals(signals)
    return [
        {
            "signal_type": item["signal_type"],
            "count": int(item["count"]),
            "max_score": int(item["max_score"]),
        }
        for item in categories[:3]
    ]


def _high_intent_context(company_row: dict[str, Any]) -> dict[str, Any]:
    high_intent_signals = extract_high_intent_signals(company_row)
    derived_high_intent_signals = derive_high_intent_signals(high_intent_signals)
    stage_info = classify_intent_stage(high_intent_signals, derived_high_intent_signals)
    return {
        "high_intent_signals": high_intent_signals,
        "derived_high_intent_signals": derived_high_intent_signals,
        "intent_stage": stage_info["intent_stage"],
        "intent_confidence": float(stage_info["confidence"]),
        "top_high_intent_signals": top_high_intent_signals(high_intent_signals),
        "key_trigger_summary": key_trigger_summary(stage_info["intent_stage"], high_intent_signals, derived_high_intent_signals),
    }


def _prioritize_signals(company_row: dict[str, Any]) -> list[dict[str, Any]]:
    signals = company_row.get("signals")
    if not isinstance(signals, list):
        signals = []

    ordered = sorted([sig for sig in signals if isinstance(sig, dict)], key=lambda s: int(s.get("final_score") or 0), reverse=True)
    top = ordered[:20]

    for derived in company_row.get("derived_signals", []):
        if not isinstance(derived, str):
            continue
        if any(str(sig.get("signal_type") or "") == derived for sig in top):
            continue
        top.append({"signal_type": derived, "final_score": 20, "recency_score": 5, "signal_strength": 4, "source": "derived"})

    for trend in company_row.get("trend_signals", []):
        if not isinstance(trend, str):
            continue
        if any(str(sig.get("signal_type") or "") == trend for sig in top):
            continue
        top.append({"signal_type": trend, "final_score": 20, "recency_score": 5, "signal_strength": 4, "source": "trend"})

    top.sort(key=lambda s: int(s.get("final_score") or 0), reverse=True)
    return top[:20]


def _deterministic_score(company_row: dict[str, Any], signals: list[dict[str, Any]]) -> int:
    unique_types = {str(sig.get("signal_type") or "") for sig in signals if str(sig.get("signal_type") or "")}
    high_intent_count = sum(1 for item in unique_types if item in HIGH_INTENT_TYPES)
    score = 0

    if "infra_scaling" in unique_types:
        score += 25
    if "hiring_spike" in unique_types:
        score += 25
    if "growth_phase" in unique_types:
        score += 20
    if "integration_added" in unique_types:
        score += 15
    if "product_launch" in unique_types:
        score += 15
    if "sales_expansion" in unique_types:
        score += 15
    if "customer_pain" in unique_types or "product_gap" in unique_types:
        score += 10
    if "momentum" in unique_types or "high_momentum" in unique_types or "viral_growth" in unique_types:
        score += 10
    if high_intent_count >= 2:
        score += 20
    if len(unique_types) >= 5:
        score += 10
    if company_row.get("derived_signals"):
        score = max(score, 50)

    return max(0, min(100, score))


def _generic_activity_penalty(signals: list[dict[str, Any]], high_intent_signals: list[dict[str, Any]]) -> int:
    signal_types = {str(sig.get("signal_type") or "") for sig in signals if str(sig.get("signal_type") or "")}
    if high_intent_signals:
        return 0
    generic_only = signal_types and signal_types.issubset({"company_update", "content_push", "momentum", "github_activity", "feature_update", "api_update"})
    if generic_only:
        return 10
    if len(signal_types) <= 2:
        return 5
    return 0


def _trend_score(company_row: dict[str, Any], signals: list[dict[str, Any]]) -> int:
    trend_signals = company_row.get("trend_signals") if isinstance(company_row.get("trend_signals"), list) else []
    trend_set = {str(item).strip() for item in trend_signals if str(item).strip()}

    score = 0
    if "hiring_trend" in trend_set:
        score += 35
    if "momentum" in trend_set:
        score += 30
    if "strategic_focus" in trend_set:
        score += 25

    trend_boost_from_scores = sum(int(sig.get("final_score") or 0) for sig in signals if str(sig.get("source") or "") == "trend")
    score += min(20, trend_boost_from_scores // 3)

    return max(0, min(100, score))


def _company_stage_override(company_context: dict[str, Any], company_row: dict[str, Any], signals: list[dict[str, Any]], llm_stage: str) -> str:
    if str(company_context.get("estimated_scale") or "").lower() == "enterprise":
        return "enterprise"

    signal_types = {str(sig.get("signal_type") or "") for sig in signals}
    hiring_signals = {"hiring", "hiring_spike", "sales_expansion"}
    infra_signals = {"infra_scaling", "dev_activity"}

    if signal_types.intersection(hiring_signals) and signal_types.intersection(infra_signals):
        return "scaling"

    if len(signal_types) >= 5 and len(company_row.get("derived_signals", [])) > 0:
        return "growth"

    if llm_stage in {"early", "growth", "scaling", "enterprise"}:
        return llm_stage

    return "growth" if len(signal_types) >= 4 else "early"


def _persona_from_signals(signals: list[dict[str, Any]], fallback: str) -> str:
    signal_types = {str(sig.get("signal_type") or "") for sig in signals}
    if signal_types.intersection({"infra_scaling", "dev_activity"}):
        return "CTO / Engineering Lead"
    if signal_types.intersection({"hiring", "hiring_spike", "sales_expansion", "growth_phase"}):
        return "VP Sales / RevOps"
    if signal_types.intersection({"product_launch", "integration_added", "product_gap", "feature_update"}):
        return "Product Manager / Head of Product"
    if signal_types.intersection({"customer_pain", "company_update"}):
        return "COO / Operations Lead"
    return fallback


def _why_now_from_signals(signals: list[dict[str, Any]], stage: str) -> str:
    prioritized = signals[:3]
    fragments: list[str] = []
    for signal in prioritized:
        signal_type = str(signal.get("signal_type") or "unknown")
        reason = _signal_reason(signal)
        fragments.append(f"{signal_type} because {reason}")

    what_changed = "; ".join(fragments) if fragments else "recent prioritized signals surfaced an active buying window"
    why_it_matters = (
        "These signals line up with an active scaling cycle and a concrete operational need"
        if stage in {"scaling", "enterprise"}
        else "These signals line up with a live workflow change that creates a sales trigger"
    )
    why_timing = (
        "Timing is critical because the company is already moving, so the next vendor choice will anchor the workflow"
        if stage in {"scaling", "enterprise"}
        else "Timing is critical because this is the moment before the team settles on a default approach"
    )
    return f"What changed: {what_changed}. Why it matters: {why_it_matters}. Why timing is critical: {why_timing}."


def _is_generic_text(text: str) -> bool:
    lowered = text.lower()
    return any(phrase in lowered for phrase in GENERIC_PHRASES)


def _format_key_signals(signals: list[dict[str, Any]]) -> list[str]:
    formatted: list[str] = []
    for signal in signals[:5]:
        signal_type = str(signal.get("signal_type") or "unknown")
        strength = int(signal.get("signal_strength") or 0)
        recency = int(signal.get("recency_score") or 0)
        reason = _signal_reason(signal)
        formatted.append(f"{signal_type} | {strength} | {recency} | {reason}")
    return formatted


def _confidence_from_signals(signals: list[dict[str, Any]], company_row: dict[str, Any]) -> int:
    signal_count = len(signals)
    signal_types = {str(sig.get("signal_type") or "") for sig in signals if str(sig.get("signal_type") or "")}
    diversity = len(signal_types)
    high_intent_count = sum(1 for sig in signals if str(sig.get("signal_type") or "") in HIGH_INTENT_TYPES or int(sig.get("final_score") or 0) >= 15)

    score = 30
    score += min(25, signal_count * 2)
    score += min(25, diversity * 4)
    score += min(20, high_intent_count * 5)
    if company_row.get("derived_signals"):
        score += 5
    if company_row.get("trend_signals"):
        score += 5
    return max(0, min(100, score))


def _build_prompt(
    company: str,
    idea: str,
    stakeholders: list[str],
    company_context: dict[str, Any],
    high_intent_context: dict[str, Any],
    compressed_context: dict[str, Any],
) -> str:
    compressed_signals = compressed_context.get("compressed_signals", [])
    topics = compressed_context.get("topics", [])
    derived = compressed_context.get("derived_signals", [])
    trends = compressed_context.get("trend_signals", [])
    top_categories = compressed_context.get("top_categories", [])
    high_intent_signals = high_intent_context.get("high_intent_signals", [])
    derived_high_intent_signals = high_intent_context.get("derived_high_intent_signals", [])
    intent_stage = high_intent_context.get("intent_stage", "EXPLORING")
    trigger_summary = high_intent_context.get("key_trigger_summary", "")
    stakeholders_str = stakeholders if stakeholders else ["Unknown"]

    return (
        "Return ONLY valid JSON with this exact schema:\n"
        '{"company":str,"score":0-100,"priority":"high|medium|low","stage":"early|growth|scaling|enterprise",'
        '"intent_tags":[str],"why_now":str,"target_persona":str,"pain_point":str,"pitch_angle":str,"confidence":0-100}\n\n'
        "Rules:\n"
        "- You are a high-performance B2B sales strategist\n"
        "- Every claim must reference a signal\n"
        "- Do not use the phrases may be, could be, or indicates activity\n"
        "- Do not generalize\n"
        "- Tie reasoning directly to compressed, derived, and trend signals\n"
        "- Prioritize recent signals, repeated patterns, and high-intent indicators\n"
        "- target_persona and pitch_angle must be concrete and sales-ready\n"
        "- why_now must explicitly state what changed, why it matters, and why timing is critical\n\n"
        f"Company: {company}\n"
        f"Product Idea: {idea}\n"
        f"Stakeholders: {stakeholders_str}\n"
        f"Company Context: {company_context}\n"
        f"High Intent Signals: {high_intent_signals}\n"
        f"Derived High Intent Signals: {derived_high_intent_signals}\n"
        f"Intent Stage: {intent_stage}\n"
        f"Key Trigger Summary: {trigger_summary}\n"
        f"Topics: {topics}\n"
        f"Top Signal Categories: {top_categories}\n"
        f"Derived Signals: {derived}\n"
        f"Trend Signals: {trends}\n"
        f"Compressed Signals:\n{compressed_signals}\n"
    )


def _fallback(
    company: str,
    base_score: int,
    stakeholders: list[str],
    signals: list[dict[str, Any]],
    company_context: dict[str, Any],
    high_intent_context: dict[str, Any],
    company_row: dict[str, Any],
) -> RankedLead:
    score = max(0, min(100, base_score))
    persona = _persona_from_signals(signals, stakeholders[0] if stakeholders else "Head of Operations")
    stage = _company_stage_override(company_context, company_row, signals, "early")
    intent_stage = str(high_intent_context.get("intent_stage") or "EXPLORING")
    return RankedLead(
        company=company,
        score=score,
        priority=_priority(score),
        stage=stage,
        intent_stage=intent_stage,  # type: ignore[arg-type]
        intent_tags=["signal_driven_opportunity"],
        top_signals=_format_key_signals(signals),
        why_now=_why_now_from_signals(signals, stage),
        key_trigger_summary=str(high_intent_context.get("key_trigger_summary") or "weak signal set"),
        target_persona=persona,
        pain_point="Execution pressure from active scaling and delivery expectations.",
        pitch_angle="Lead with a signal-led pilot that resolves the most visible operational bottleneck.",
        key_signals=_format_key_signals(signals),
        confidence=_confidence_from_signals(signals, company_row),
    )


def generate_ranked_leads(input_data: LeadEngineInput) -> list[RankedLead]:
    leads: list[RankedLead] = []

    for company_bundle in input_data.companies:
        row = company_bundle.model_dump()
        company = str(row.get("company") or "").strip()
        if not company:
            continue

        company_context = get_company_context(company)
        selected = _prioritize_signals(row)

        if _is_hiring_only(selected):
            leads.append(_hiring_only_lead(company, selected, input_data.stakeholders))
            continue

        high_intent_context = _high_intent_context(row)
        deterministic = _deterministic_score(row, selected)
        deterministic = max(0, min(100, deterministic + high_intent_score_boost(high_intent_context["high_intent_signals"], high_intent_context["derived_high_intent_signals"])) )
        deterministic = max(0, deterministic - _generic_activity_penalty(selected, high_intent_context["high_intent_signals"]))
        trend_score = _trend_score(row, selected)
        compressed_context = compress_company_context(row)
        compressed_context["top_categories"] = _top_categories(selected)
        prompt = _build_prompt(company, input_data.idea, input_data.stakeholders, company_context, high_intent_context, compressed_context)

        try:
            llm = generate_json_with_gemini(
                system_prompt=LEAD_ENGINE_SYSTEM_PROMPT,
                user_prompt=prompt,
                temperature=0.3,
            )
        except (RuntimeError, ValueError):
            fallback_score = round((0.8 * deterministic) + (0.2 * trend_score))
            leads.append(_fallback(company, fallback_score, input_data.stakeholders, selected, company_context, high_intent_context, row))
            continue

        llm_score_raw = llm.get("score")
        if not isinstance(llm_score_raw, (int, float)):
            llm_score_raw = llm.get("buying_intent_score")
        llm_score = int(llm_score_raw) if isinstance(llm_score_raw, (int, float)) else deterministic
        llm_score = max(0, min(100, llm_score))
        final_score = round((0.45 * deterministic) + (0.25 * llm_score) + (0.15 * trend_score) + (0.15 * high_intent_score_boost(high_intent_context["high_intent_signals"], high_intent_context["derived_high_intent_signals"])))
        if row.get("derived_signals"):
            final_score = max(final_score, 50)
        intent_stage = str(high_intent_context.get("intent_stage") or "EXPLORING")
        if intent_stage == "URGENT" and high_intent_context.get("intent_confidence", 0.0) >= 0.6:
            final_score = max(final_score, 75)
        elif intent_stage in {"SWITCHING", "ACTIVELY_EVALUATING"} and high_intent_context.get("intent_confidence", 0.0) >= 0.6:
            final_score = max(final_score, 60)
        final_score = max(0, min(100, final_score))

        stage_raw = str(llm.get("stage") or "early").lower().strip()
        stage = _company_stage_override(company_context, row, selected, stage_raw if stage_raw in {"early", "growth", "scaling", "enterprise"} else "early")

        intent_tags_raw = llm.get("intent_tags")
        intent_tags = [str(x).strip() for x in intent_tags_raw] if isinstance(intent_tags_raw, list) else []
        intent_tags = [x for x in intent_tags if x]

        why_now = f"{intent_stage}: {high_intent_context['key_trigger_summary']}"
        if len(why_now) > 180:
            why_now = f"{why_now[:177]}..."

        target_persona = str(llm.get("target_persona") or (input_data.stakeholders[0] if input_data.stakeholders else "Head of Operations")).strip()
        target_persona = _persona_from_signals(selected, target_persona)
        if intent_stage == "SWITCHING" and any(sig.get("type") == "decision_maker_intent" for sig in high_intent_context["high_intent_signals"]):
            target_persona = "Founder / CTO / Head of Sales"
        elif intent_stage == "URGENT":
            target_persona = "CTO / COO / Head of Operations"

        pain_point = str(llm.get("pain_point") or "Execution and scaling friction under rising demand.").strip()

        pitch_angle = str(llm.get("pitch_angle") or "").strip()
        if _is_generic_text(pitch_angle):
            pitch_angle = "Lead with a concrete pilot tied to the strongest current signal and quantified operational relief."

        confidence_raw = llm.get("confidence")
        confidence = int(confidence_raw) if isinstance(confidence_raw, (int, float)) else _confidence_from_signals(selected, row)
        confidence = max(confidence, _confidence_from_signals(selected, row))
        confidence = max(confidence, int(round(_safe_float(high_intent_context.get("intent_confidence"), 0.0) * 100)))
        confidence = max(0, min(100, confidence))

        ranked_top = top_high_intent_signals(high_intent_context["high_intent_signals"], limit=5)
        top_signals = [
            f"{str(sig.get('type') or 'unknown')} | conf={_safe_float(sig.get('confidence_score'), 0.0):.2f} | intensity={_safe_float(sig.get('intensity_score'), 0.0):.2f} | {str(sig.get('summary') or '')}"
            for sig in ranked_top
        ]
        if len(top_signals) < 3:
            top_signals.extend(_format_key_signals(selected)[: max(0, 3 - len(top_signals))])

        key_signals = top_signals[:5]
        if len(key_signals) < 3:
            key_signals = key_signals + ["general_activity | 0 | 1 | no strong evidence available"] * (3 - len(key_signals))

        leads.append(
            RankedLead(
                company=company,
                score=final_score,
                priority=_priority(final_score),
                stage=stage,
                intent_stage=intent_stage,  # type: ignore[arg-type]
                intent_tags=intent_tags,
                top_signals=top_signals[:5],
                why_now=why_now,
                key_trigger_summary=str(high_intent_context.get("key_trigger_summary") or "weak signal set"),
                target_persona=target_persona,
                pain_point=pain_point,
                pitch_angle=pitch_angle,
                key_signals=key_signals[:5],
                confidence=confidence,
            )
        )

    leads.sort(key=lambda lead: (-lead.score, lead.company.lower()))
    return leads
