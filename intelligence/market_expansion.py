from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from intelligence.company_context import get_company_context


@dataclass(frozen=True)
class ExpansionCandidate:
    company: str
    reason: str
    score: float


COMPETITOR_MAP: dict[str, list[tuple[str, str]]] = {
    "payments / fintech": [
        ("Adyen", "Direct payments competitor with the same enterprise payment infrastructure motion."),
        ("Checkout.com", "Direct payments competitor serving digital commerce and platform payments."),
        ("PayPal", "Large-scale payments incumbent with overlapping checkout and merchant workflows."),
        ("Block", "Adjacent fintech platform with merchant and payments workflows."),
        ("Braintree", "Payments infrastructure competitor focused on merchant checkout and processing."),
        ("Paddle", "Billing and payments platform for software businesses."),
        ("Razorpay", "High-growth payments platform in a similar infrastructure category."),
    ],
    "crm / enterprise software": [
        ("HubSpot", "Revenue platform competitor for customer-facing workflows."),
        ("Salesforce", "Category leader with overlapping enterprise revenue operations use cases."),
        ("Pipedrive", "Sales workflow platform serving similar GTM teams."),
        ("Zoho", "Broad business software suite competing on CRM and workflow automation."),
    ],
    "commerce infrastructure": [
        ("BigCommerce", "Commerce platform competitor with similar merchant enablement needs."),
        ("WooCommerce", "Commerce infrastructure platform used by merchants and developers."),
        ("Adobe Commerce", "Enterprise commerce platform with adjacent merchant workflow needs."),
        ("Commercetools", "Composable commerce platform in the same infrastructure layer."),
    ],
    "developer platform": [
        ("GitLab", "Developer platform competitor serving software delivery teams."),
        ("Atlassian", "Developer workflow ecosystem with similar engineering buyer personas."),
        ("CircleCI", "Developer tooling platform with overlapping engineering operations needs."),
        ("Jira", "Workflow platform embedded in engineering execution."),
    ],
    "developer infrastructure": [
        ("Cloudflare", "Infrastructure platform with similar technical buyer motion."),
        ("Datadog", "Infrastructure observability platform bought by engineering teams."),
        ("Akamai", "Enterprise infrastructure provider with adjacent buyer needs."),
        ("Fastly", "Edge infrastructure platform with overlapping technical scaling themes."),
    ],
    "data / analytics": [
        ("Snowflake", "Data platform competitor and adjacent enterprise buyer universe."),
        ("Databricks", "Data/AI platform with similar enterprise scale and data workflows."),
        ("dbt Labs", "Data workflow platform with adjacent analytics operations needs."),
        ("Fivetran", "Data integration platform used by similar RevOps and analytics buyers."),
    ],
    "revenue software": [
        ("HubSpot", "Revenue software platform with overlapping buyer personas."),
        ("Salesforce", "Core enterprise revenue system with related operating workflows."),
        ("Gong", "Revenue intelligence platform for sales teams."),
        ("Apollo", "Revenue prospecting platform used by GTM teams."),
    ],
    "communications": [
        ("Zoom", "Collaboration platform with overlapping distributed-team workflows."),
        ("Teams", "Enterprise collaboration platform with adjacent communication use cases."),
        ("Slack", "Workplace communication platform used across similar teams."),
        ("Webex", "Enterprise communications platform with similar collaboration buyers."),
    ],
    "software / technology": [
        ("Notion", "General software platform with cross-functional operations use cases."),
        ("Intercom", "Customer workflow software used by product and support teams."),
        ("Zendesk", "Customer operations platform with similar workflow buyers."),
        ("Monday.com", "Work management platform used by cross-functional teams."),
    ],
}


ROLE_COMPANY_MAP: dict[str, list[tuple[str, str]]] = {
    "cto": [
        ("Cloudflare", "Strong technical buyer profile and infrastructure focus."),
        ("Datadog", "Engineering-led buyer motion and infrastructure scaling need."),
        ("Snowflake", "Technical platform buying motion with infrastructure depth."),
        ("MongoDB", "Developer-facing technical stack with engineering ownership."),
    ],
    "engineering": [
        ("GitHub", "Engineering-heavy buyer environment and technical workflow focus."),
        ("Cloudflare", "Infrastructure teams own the tooling and scaling workflow."),
        ("Datadog", "Engineering and platform teams are primary owners."),
        ("Atlassian", "Engineering workflow-heavy organization."),
    ],
    "product": [
        ("Notion", "Product-led organization with cross-functional workflow pressure."),
        ("Intercom", "Product teams own customer workflow and launch motion."),
        ("Amplitude", "Product analytics platform aligned with PM ownership."),
        ("Coda", "Product-driven collaboration environment."),
    ],
    "sales": [
        ("Salesforce", "Sales-led motion with CRM and pipeline ownership."),
        ("HubSpot", "GTM-heavy teams with clear sales workflow ownership."),
        ("Gong", "Sales operations and revenue leadership use case alignment."),
        ("Apollo", "Outbound and revenue operations buyer alignment."),
    ],
    "revops": [
        ("HubSpot", "RevOps-heavy organization with process automation demand."),
        ("Salesforce", "Revenue operations and system integration ownership."),
        ("6sense", "Revenue orchestration and account prioritization use case."),
        ("Outreach", "Sales execution and workflow automation fit."),
    ],
    "marketing": [
        ("Braze", "Marketing workflow and lifecycle automation owners."),
        ("Iterable", "Lifecycle and activation tooling aligned with marketing ops."),
        ("Segment", "Customer data plumbing often owned by marketing ops."),
        ("Mailchimp", "Marketing execution and campaign automation needs."),
    ],
    "finance": [
        ("NetSuite", "Finance systems and operational budget ownership."),
        ("Oracle", "Enterprise finance and operations system buyer."),
        ("Brex", "Finance team tooling and spend automation workflows."),
        ("Ramp", "Finance operations and procurement efficiency use case."),
    ],
}


TECH_COMPANY_MAP: dict[str, list[tuple[str, str]]] = {
    "salesforce": [
        ("Salesforce", "Companies using the same CRM stack are relevant because they face similar operational workflows."),
        ("HubSpot", "Same sales stack ecosystem and revenue ops motion."),
        ("Gong", "Revenue tooling ecosystems often overlap with Salesforce-heavy teams."),
        ("Outreach", "Sales execution stack alignment."),
    ],
    "hubspot": [
        ("HubSpot", "Companies with HubSpot-like GTM stack tend to value workflow automation."),
        ("Apollo", "Outbound stack adjacency and GTM automation fit."),
        ("6sense", "Revenue orchestration stack overlap."),
        ("Clearbit", "Data enrichment and workflow integration adjacency."),
    ],
    "snowflake": [
        ("Snowflake", "Data platform users often share scale and analytics pain points."),
        ("Databricks", "Modern data stack adjacency and data engineering motion."),
        ("dbt Labs", "Data transformation workflows and analytics teams."),
        ("Fivetran", "Data pipeline and integration stack adjacency."),
    ],
    "aws": [
        ("Cloudflare", "Infrastructure and delivery teams with similar cloud stack needs."),
        ("Datadog", "Cloud observability and platform operations adjacency."),
        ("MongoDB", "Cloud-native application stack overlap."),
        ("Confluent", "Cloud infrastructure and streaming architecture adjacency."),
    ],
    "kubernetes": [
        ("Cloudflare", "Platform engineering and infrastructure-heavy teams."),
        ("Datadog", "Platform observability and SRE stack overlap."),
        ("Snyk", "DevSecOps adjacency in cloud-native stacks."),
        ("GitLab", "DevOps and deployment workflow alignment."),
    ],
    "react": [
        ("Vercel", "Frontend platform adjacency for modern product teams."),
        ("Next.js", "Frontend/product engineering stack overlap."),
        ("GitHub", "Developer workflow and web product teams."),
        ("Contentful", "Frontend-driven product delivery and content workflow adjacency."),
    ],
    "postgres": [
        ("Supabase", "Postgres-centric developer workflow adjacency."),
        ("Cockroach Labs", "Database scaling and infra concerns."),
        ("Neon", "Modern Postgres workflow and developer adoption."),
        ("MongoDB", "Data layer adjacency for product engineering teams."),
    ],
}


ADJACENT_INDUSTRY_MAP: dict[str, list[tuple[str, str]]] = {
    "payments / fintech": [
        ("Shopify", "Merchants and commerce platforms adjacent to payments infrastructure."),
        ("Square", "Merchant services and small-business financial workflows."),
        ("Plaid", "Financial infrastructure adjacent to payments workflows."),
        ("Brex", "Modern finance workflows adjacent to fintech operations."),
    ],
    "crm / enterprise software": [
        ("Zendesk", "Customer workflow and support operations adjacency."),
        ("ServiceNow", "Enterprise workflow and operations adjacency."),
        ("Workday", "Large enterprise workflow and system ownership."),
        ("Monday.com", "Cross-functional workflow automation adjacency."),
    ],
    "commerce infrastructure": [
        ("BigCommerce", "Commerce operations adjacent to merchant growth."),
        ("Wix", "Merchant and SMB digital commerce adjacency."),
        ("Squarespace", "Website/commerce workflow overlap."),
        ("ShipBob", "Commerce fulfillment and operations adjacency."),
    ],
    "developer platform": [
        ("Atlassian", "Adjacent engineering workflow and collaboration category."),
        ("GitLab", "CI/CD and developer workflow adjacency."),
        ("CircleCI", "Software delivery and deployment adjacency."),
        ("JetBrains", "Developer tooling adjacency across engineering teams."),
    ],
    "data / analytics": [
        ("Looker", "Analytics consumption and decision workflow adjacency."),
        ("Mode", "Business analytics and operational reporting adjacency."),
        ("Sigma Computing", "Cloud analytics and enterprise reporting adjacency."),
        ("Metabase", "Self-serve analytics and internal data workflow adjacency."),
    ],
    "software / technology": [
        ("Notion", "Cross-functional software workflow adjacency."),
        ("Intercom", "Product and support workflow adjacency."),
        ("Zendesk", "Customer operations adjacency."),
        ("Coda", "Collaborative workflow adjacency."),
    ],
}


ROLE_KEYWORDS = {
    "sales": ["sales", "revops", "revenue", "pipeline", "gtm", "outbound", "prospect", "account executive"],
    "engineering": ["engineering", "infra", "platform", "developer", "devops", "sre", "cto", "backend"],
    "product": ["product", "pm", "roadmap", "launch", "feature", "ux", "design"],
    "marketing": ["marketing", "demand", "brand", "growth", "content", "campaign"],
    "finance": ["finance", "billing", "payments", "controller", "cfo", "procurement", "rev rec"],
    "hr": ["hr", "people", "talent", "recruit", "hiring", "people ops", "recruiter"],
    "security": ["security", "compliance", "risk", "governance", "secops"],
}


TECH_KEYWORDS = {
    "salesforce": ["salesforce"],
    "hubspot": ["hubspot"],
    "snowflake": ["snowflake"],
    "aws": ["aws", "amazon web services"],
    "kubernetes": ["kubernetes", "k8s"],
    "react": ["react"],
    "postgres": ["postgres", "postgresql"],
    "datadog": ["datadog"],
    "zendesk": ["zendesk"],
    "segment": ["segment"],
}


def _clean_company_name(company: str) -> str:
    return company.strip()


def _tokenize(text: str) -> set[str]:
    cleaned = text.lower().replace("/", " ").replace("-", " ")
    tokens = {token for token in cleaned.split() if token}
    return tokens


def _seed_domain(company: str) -> str:
    context = get_company_context(company)
    return str(context.get("domain") or "software / technology")


def _score_reason(reason: str, source: str, weight: float) -> float:
    score = weight
    if source == "competitor":
        score += 0.20
    elif source == "similar":
        score += 0.12
    elif source == "role":
        score += 0.10
    elif source == "tech":
        score += 0.08
    elif source == "adjacent":
        score += 0.05
    if len(reason) > 0:
        score += 0.05
    return min(score, 1.0)


def _add_candidate(candidates: dict[str, ExpansionCandidate], company: str, reason: str, score: float) -> None:
    normalized = _clean_company_name(company)
    if not normalized:
        return

    key = normalized.lower()
    existing = candidates.get(key)
    if existing and existing.score >= score:
        return

    candidates[key] = ExpansionCandidate(company=normalized, reason=reason, score=score)


def _seed_company_expansions(company: str, candidates: dict[str, ExpansionCandidate]) -> None:
    domain = _seed_domain(company)
    context = get_company_context(company)
    characteristics = " ".join(str(item).lower() for item in context.get("known_characteristics", []))

    for competitor, reason in COMPETITOR_MAP.get(domain, []):
        _add_candidate(candidates, competitor, f"Competitor to {company} in {domain}: {reason}", _score_reason(reason, "competitor", 0.82))

    for similar, reason in COMPETITOR_MAP.get(domain, []):
        _add_candidate(candidates, similar, f"Similar company to {company} in the same category: {reason}", _score_reason(reason, "similar", 0.74))

    if "enterprise" in str(context.get("estimated_scale") or ""):
        for adjacent, reason in ADJACENT_INDUSTRY_MAP.get(domain, []):
            _add_candidate(candidates, adjacent, f"Adjacent to {company} because enterprise buyers in {domain} often evaluate {adjacent}: {reason}", _score_reason(reason, "adjacent", 0.68))

    if any(token in characteristics for token in ["developer", "technical", "platform"]):
        for adjacent, reason in ADJACENT_INDUSTRY_MAP.get("developer platform", []):
            _add_candidate(candidates, adjacent, f"Adjacent technical company for {company}: {reason}", _score_reason(reason, "adjacent", 0.65))


def _role_expansions(icp_description: str, candidates: dict[str, ExpansionCandidate]) -> None:
    tokens = _tokenize(icp_description)
    matched_roles: list[str] = []
    for role, keywords in ROLE_KEYWORDS.items():
        if any(keyword in icp_description.lower() for keyword in keywords):
            matched_roles.append(role)

    for role in matched_roles:
        for company, reason in ROLE_COMPANY_MAP.get(role, []):
            _add_candidate(candidates, company, f"Hiring similar {role} roles suggests this company has the same buyer motion: {reason}", _score_reason(reason, "role", 0.70))

    if not matched_roles:
        if any(token in tokens for token in {"growth", "scale", "pipeline", "revenue", "sales"}):
            for company, reason in ROLE_COMPANY_MAP["sales"]:
                _add_candidate(candidates, company, f"ICP implies sales-led expansion: {reason}", _score_reason(reason, "role", 0.68))
        if any(token in tokens for token in {"engineering", "infra", "platform", "developer", "api"}):
            for company, reason in ROLE_COMPANY_MAP["engineering"]:
                _add_candidate(candidates, company, f"ICP implies technical hiring or platform work: {reason}", _score_reason(reason, "role", 0.68))


def _tech_expansions(icp_description: str, candidates: dict[str, ExpansionCandidate]) -> None:
    lowered = icp_description.lower()
    matched_techs: list[str] = []
    for tech, keywords in TECH_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            matched_techs.append(tech)

    for tech in matched_techs:
        for company, reason in TECH_COMPANY_MAP.get(tech, []):
            _add_candidate(candidates, company, f"Companies using similar tech stack ({tech}) are relevant: {reason}", _score_reason(reason, "tech", 0.72))


def _adjacent_expansions(initial_companies: list[str], icp_description: str, candidates: dict[str, ExpansionCandidate]) -> None:
    description_tokens = _tokenize(icp_description)
    for company in initial_companies:
        domain = _seed_domain(company)
        context = get_company_context(company)

        for adjacent, reason in ADJACENT_INDUSTRY_MAP.get(domain, []):
            _add_candidate(candidates, adjacent, f"Adjacent industry for {company} in {domain}: {reason}", _score_reason(reason, "adjacent", 0.66))

        if any(token in description_tokens for token in {"fintech", "payments", "billing", "commerce"}) and domain != "payments / fintech":
            for adjacent, reason in ADJACENT_INDUSTRY_MAP["payments / fintech"]:
                _add_candidate(candidates, adjacent, f"ICP suggests adjacent fintech/commerce opportunities: {reason}", _score_reason(reason, "adjacent", 0.64))

        if any(token in description_tokens for token in {"data", "analytics", "warehouse", "warehouse"}) and domain != "data / analytics":
            for adjacent, reason in ADJACENT_INDUSTRY_MAP["data / analytics"]:
                _add_candidate(candidates, adjacent, f"ICP suggests adjacent data/analytics opportunities: {reason}", _score_reason(reason, "adjacent", 0.64))

        if str(context.get("estimated_scale") or "") == "enterprise":
            for adjacent, reason in ADJACENT_INDUSTRY_MAP.get(domain, []):
                _add_candidate(candidates, adjacent, f"Enterprise adjacency for {company}: {reason}", _score_reason(reason, "adjacent", 0.62))


def expand_market(initial_companies: list[str], icp_description: str) -> list[dict[str, str]]:
    candidates: dict[str, ExpansionCandidate] = {}
    seeds = [company for company in initial_companies if str(company).strip()]

    for company in seeds:
        _seed_company_expansions(company, candidates)

    _role_expansions(icp_description, candidates)
    _tech_expansions(icp_description, candidates)
    _adjacent_expansions(seeds, icp_description, candidates)

    icp_tokens = _tokenize(icp_description)
    for company in seeds:
        _add_candidate(candidates, company, f"Seed company from the initial market set and ICP context for {company}.", 1.0)

    ranked = sorted(
        candidates.values(),
        key=lambda item: (item.score, len(item.reason), item.company.lower()),
        reverse=True,
    )

    diverse_output: list[ExpansionCandidate] = []
    seen_domains: set[str] = set()
    for candidate in ranked:
        domain = _seed_domain(candidate.company)
        if candidate.company.lower() in {seed.lower() for seed in seeds}:
            diverse_output.append(candidate)
            continue
        if len(diverse_output) >= 40:
            break
        if domain in seen_domains and len(diverse_output) < 12:
            diverse_output.append(candidate)
        elif domain not in seen_domains:
            diverse_output.append(candidate)
            seen_domains.add(domain)

    if not diverse_output:
        return []

    output: list[dict[str, str]] = []
    output_seen: set[str] = set()
    for candidate in diverse_output:
        key = candidate.company.lower()
        if key in output_seen:
            continue
        output_seen.add(key)
        output.append({"company": candidate.company, "reason": candidate.reason})

    return output
