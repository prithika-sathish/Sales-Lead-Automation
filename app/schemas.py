from pydantic import BaseModel, Field
from typing import Literal


class IdeaRequest(BaseModel):
    idea: str = Field(min_length=1, description="Startup or product idea to analyze")


class IdeaAnalysis(BaseModel):
    target_companies: list[str]
    buyer_roles: list[str]
    pain_points: list[str]
    use_cases: list[str]
    value_proposition: str


class StakeholderContext(BaseModel):
    target_companies: list[str]
    buyer_roles: list[str]
    pain_points: list[str]
    use_cases: list[str]
    value_proposition: str


class StakeholderMappingRequest(BaseModel):
    context: StakeholderContext


class StakeholderMappingResponse(BaseModel):
    roles: list[str]
    role_reasoning: dict[str, str]
    priority_order: list[str]


class SignalCollectionRequest(BaseModel):
    domains: list[str] = Field(min_length=1, description="Domains to expand into Apify signal queries")


class SignalDetails(BaseModel):
    hiring: bool
    github_activity: Literal["low", "medium", "high"]
    product_launch: bool
    discussion_heat: Literal["low", "medium", "high"]


class SignalItem(BaseModel):
    company: str
    matched_domain: str
    signals: SignalDetails
    source_summary: str


class DomainMappingRequest(BaseModel):
    context: StakeholderContext


class DomainMappingResponse(BaseModel):
    domains: list[str]
    domain_reasoning: dict[str, str]
    priority_domains: list[str]


class LeadRankingRequest(BaseModel):
    leads: list[SignalItem] = Field(min_length=1, description="Signals to score and rank")


class LeadRankingResponse(BaseModel):
    company: str
    score: int
    priority: Literal["high", "medium", "low"]
    reason: str
    recommended_angle: str


class NormalizedSignal(BaseModel):
    company: str
    signal_id: str
    signal_type: str
    signal_strength: int = Field(ge=1, le=5)
    recency_score: int = Field(ge=1, le=5)
    final_score: int = Field(ge=1)
    signal_score: int = Field(ge=1)
    timestamp: str
    metadata: dict[str, object]
    source: str


class CompanySignalBundle(BaseModel):
    company: str
    signal_count: int = Field(ge=0)
    high_intent_signals: list[NormalizedSignal]
    signals: list[NormalizedSignal]
    derived_signals: list[str] = Field(default_factory=list)
    topics: list[str] = Field(default_factory=list)
    trend_signals: list[str] = Field(default_factory=list)
    agent_debug: dict[str, object] = Field(default_factory=dict)


class ExecutionMetadata(BaseModel):
    total_time: str
    agents_used: list[str]
    failed_agents: list[str]


class SignalCollectionV2Response(BaseModel):
    companies: list[CompanySignalBundle]
    execution_metadata: ExecutionMetadata | None = None


class SignalIntelligenceResult(BaseModel):
    company: str
    stage: Literal["early", "growth", "scaling", "enterprise"]
    buying_intent_score: int = Field(ge=0, le=100)
    intent_tags: list[str]
    key_signals: list[str]
    why_now: str
    recommended_pitch_angle: str


class AnalyzeSignalsRequest(BaseModel):
    companies: list[CompanySignalBundle] = Field(min_length=1)


class AnalyzeSignalsResponse(BaseModel):
    results: list[SignalIntelligenceResult]


class DiscoverCandidate(BaseModel):
    company: str
    source: str
    initial_signal: str


class ScoredCandidate(DiscoverCandidate):
    light_score: int = Field(ge=0, le=50)


class DiscoverEnrichRequest(BaseModel):
    domains: list[str] = Field(min_length=1)
    top_n: int = Field(default=20, ge=1, le=100)


class DiscoverEnrichedCompany(BaseModel):
    company: str
    light_score: int = Field(ge=0, le=50)
    signals: list[NormalizedSignal]
    derived_signals: list[str]
    topics: list[str]
    trend_signals: list[str]
    agent_debug: dict[str, object] = Field(default_factory=dict)


class DiscoverEnrichResponse(BaseModel):
    companies: list[DiscoverEnrichedCompany]
    execution_metadata: ExecutionMetadata | None = None


class LeadEngineInput(BaseModel):
    companies: list[DiscoverEnrichedCompany] = Field(min_length=1)
    idea: str = Field(min_length=1)
    stakeholders: list[str] = Field(default_factory=list)


class RankedLead(BaseModel):
    company: str
    score: int = Field(ge=0, le=100)
    priority: Literal["high", "medium", "low"]
    stage: Literal["early", "growth", "scaling", "enterprise"]
    intent_stage: Literal["EXPLORING", "AWARE", "ACTIVELY_EVALUATING", "SWITCHING", "URGENT"]
    intent_tags: list[str]
    top_signals: list[str]
    why_now: str
    key_trigger_summary: str
    target_persona: str
    pain_point: str
    pitch_angle: str
    key_signals: list[str]
    confidence: int = Field(ge=0, le=100)


class GenerateLeadsRequest(BaseModel):
    domains: list[str] = Field(min_length=1)
    idea: str = Field(min_length=1)
    stakeholders: list[str] = Field(default_factory=list)
    top_n: int = Field(default=20, ge=1, le=100)


class GenerateLeadsResponse(BaseModel):
    leads: list[RankedLead]