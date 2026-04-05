from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse

from app.apify_client import merge_results
from app.llm import analyze_idea_with_gemini
from app.llm import extract_signals
from app.llm import generate_reason
from app.llm import map_domains_with_gemini
from app.llm import map_stakeholders_with_gemini
from core.discovery import discover_candidates
from core.light_scorer import score_candidates
from core.orchestrator import collect_signals_for_companies
from core.orchestrator import collect_signals_pipeline
from intelligence.lead_engine import generate_ranked_leads
from intelligence.signal_engine import analyze_companies_signals
from app.schemas import AnalyzeSignalsRequest
from app.schemas import AnalyzeSignalsResponse
from app.schemas import DiscoverEnrichRequest
from app.schemas import DiscoverEnrichResponse
from app.schemas import DiscoverEnrichedCompany
from app.schemas import GenerateLeadsRequest
from app.schemas import GenerateLeadsResponse
from app.schemas import LeadEngineInput
from app.schemas import IdeaAnalysis, IdeaRequest
from app.schemas import DomainMappingRequest
from app.schemas import DomainMappingResponse
from app.schemas import LeadRankingRequest
from app.schemas import LeadRankingResponse
from app.schemas import SignalCollectionRequest
from app.schemas import SignalItem
from app.schemas import SignalCollectionV2Response
from app.schemas import StakeholderMappingRequest
from app.schemas import StakeholderMappingResponse


app = FastAPI(title="Context Engine")


@app.get("/", response_class=PlainTextResponse)
def root() -> str:
    return "Context Engine Running"


@app.post("/analyze-idea", response_model=IdeaAnalysis)
def analyze_idea(payload: IdeaRequest) -> IdeaAnalysis:
    try:
        return analyze_idea_with_gemini(payload.idea)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/map-stakeholders", response_model=StakeholderMappingResponse)
def map_stakeholders(payload: StakeholderMappingRequest) -> StakeholderMappingResponse:
    try:
        return map_stakeholders_with_gemini(payload.context)
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/map-domains", response_model=DomainMappingResponse)
def map_domains(payload: DomainMappingRequest) -> DomainMappingResponse:
    try:
        return DomainMappingResponse.model_validate(map_domains_with_gemini(payload.context))
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/collect-signals", response_model=list[SignalItem])
def collect_signals(payload: SignalCollectionRequest) -> list[SignalItem]:
    try:
        raw_items = merge_results(payload.domains)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    try:
        return extract_signals(raw_items)
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/collect-signals-v2", response_model=SignalCollectionV2Response)
async def collect_signals_v2(payload: SignalCollectionRequest) -> SignalCollectionV2Response:
    try:
        result = await collect_signals_pipeline(payload.domains)
        return SignalCollectionV2Response.model_validate(result)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/analyze-signals", response_model=AnalyzeSignalsResponse)
def analyze_signals(payload: AnalyzeSignalsRequest) -> AnalyzeSignalsResponse:
    results = analyze_companies_signals(payload.companies)
    return AnalyzeSignalsResponse(results=results)


@app.post("/discover-and-enrich", response_model=DiscoverEnrichResponse)
async def discover_and_enrich(payload: DiscoverEnrichRequest) -> DiscoverEnrichResponse:
    candidates = discover_candidates(payload.domains)
    scored = score_candidates(candidates)
    selected = scored[: payload.top_n]
    selected_companies = [str(item.get("company") or "") for item in selected]
    light_scores = {
        str(item.get("company") or "").strip().lower(): int(item.get("light_score") or 0)
        for item in selected
    }

    try:
        enriched = await collect_signals_for_companies(selected_companies)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    companies_payload = []
    for item in enriched.get("companies", []):
        if not isinstance(item, dict):
            continue
        company_name = str(item.get("company") or "").strip()
        if not company_name:
            continue
        companies_payload.append(
            {
                "company": company_name,
                "light_score": light_scores.get(company_name.lower(), 0),
                "signals": item.get("signals", []),
                "derived_signals": item.get("derived_signals", []),
                "topics": item.get("topics", []),
                "trend_signals": item.get("trend_signals", []),
                "agent_debug": item.get("agent_debug", {}),
            }
        )

    return DiscoverEnrichResponse(
        companies=[DiscoverEnrichedCompany.model_validate(row) for row in companies_payload],
        execution_metadata=enriched.get("execution_metadata"),
    )


@app.post("/generate-leads", response_model=GenerateLeadsResponse)
async def generate_leads(payload: GenerateLeadsRequest) -> GenerateLeadsResponse:
    candidates = discover_candidates(payload.domains)
    scored = score_candidates(candidates)
    selected = scored[: payload.top_n]
    selected_companies = [str(item.get("company") or "") for item in selected]
    light_scores = {
        str(item.get("company") or "").strip().lower(): int(item.get("light_score") or 0)
        for item in selected
    }

    try:
        enriched = await collect_signals_for_companies(selected_companies)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    companies_payload = []
    for item in enriched.get("companies", []):
        if not isinstance(item, dict):
            continue
        company_name = str(item.get("company") or "").strip()
        if not company_name:
            continue
        companies_payload.append(
            {
                "company": company_name,
                "light_score": light_scores.get(company_name.lower(), 0),
                "signals": item.get("signals", []),
                "derived_signals": item.get("derived_signals", []),
                "topics": item.get("topics", []),
                "trend_signals": item.get("trend_signals", []),
            }
        )

    engine_input = LeadEngineInput(
        companies=[DiscoverEnrichedCompany.model_validate(row) for row in companies_payload],
        idea=payload.idea,
        stakeholders=payload.stakeholders,
    )
    leads = generate_ranked_leads(engine_input)
    return GenerateLeadsResponse(leads=leads)


def compute_score(lead: SignalItem) -> int:
    raw_score = 0
    signals = lead.signals

    if signals.hiring:
        raw_score += 20

    github_weights = {"low": 5, "medium": 15, "high": 30}
    raw_score += github_weights[signals.github_activity]

    if signals.product_launch:
        raw_score += 25

    heat_weights = {"low": 5, "medium": 10, "high": 20}
    raw_score += heat_weights[signals.discussion_heat]

    normalized_score = round((raw_score / 95) * 100)
    return max(0, min(100, normalized_score))


def _priority_bucket(score: int) -> str:
    if score >= 75:
        return "high"
    if score >= 50:
        return "medium"
    return "low"


@app.post("/rank-leads", response_model=list[LeadRankingResponse])
def rank_leads(payload: LeadRankingRequest) -> list[LeadRankingResponse]:
    ranked: list[dict[str, object]] = []

    for lead in payload.leads:
        score = compute_score(lead)
        priority = _priority_bucket(score)
        explanation = generate_reason(lead.model_dump(), score, priority)

        ranked.append(
            {
                "company": lead.company,
                "score": score,
                "priority": priority,
                "reason": explanation["reason"],
                "recommended_angle": explanation["recommended_angle"],
            }
        )

    ranked.sort(key=lambda item: (-int(item["score"]), str(item["company"]).lower()))
    return [LeadRankingResponse.model_validate(item) for item in ranked]