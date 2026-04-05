from __future__ import annotations

import json
import os
import re
from typing import Any

from google import genai
from dotenv import load_dotenv



from app.schemas import IdeaAnalysis
from app.schemas import StakeholderContext
from app.schemas import StakeholderMappingResponse
from app.schemas import SignalItem


SYSTEM_PROMPT = (
    "You are a B2B SaaS strategist. Convert vague startup ideas into precise ICP, "
    "pain points, and value propositions. Be specific, non-generic, and business-relevant."
)

DOMAIN_SYSTEM_PROMPT = (
    "You are a B2B go-to-market strategist. Given a product context, identify the best industries and market segments where this product can be sold. Base your reasoning on pain points, use cases, and urgency of need."
)

SIGNAL_SYSTEM_PROMPT = (
    "You are an AI sales intelligence system. Analyze company activity and extract buying intent signals such as hiring, product launches, developer activity, and community discussion. Be precise and avoid guessing."
)

LEAD_RANKING_SYSTEM_PROMPT = (
    "You are a B2B sales strategist. Given company signals and score, explain why this lead is valuable and suggest a sharp outreach angle based on timing and needs."
)

MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
TEMPERATURE = 0.2
DOMAIN_TEMPERATURE = 0.3


class _ModelSession:
    def __init__(self, client: genai.Client, system_prompt: str) -> None:
        self.client = client
        self.system_prompt = system_prompt


_CLIENT: genai.Client | None = None


load_dotenv()
if not os.getenv("GEMINI_API_KEY"):
    load_dotenv(".env.example")

def _strip_code_fences(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _extract_json_payload(text: str) -> str:
    cleaned = _strip_code_fences(text)
    if cleaned.startswith("{") and cleaned.endswith("}"):
        return cleaned

    match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if match:
        return match.group(0)

    return cleaned


def _build_prompt(idea: str, retry: bool = False) -> str:
    instructions = (
        "Return ONLY valid JSON matching this exact schema:\n"
        '{"target_companies": [str], "buyer_roles": [str], "pain_points": [str], '
        '"use_cases": [str], "value_proposition": str}'
    )
    if retry:
        instructions = (
            "The previous response was invalid JSON. Return ONLY valid JSON matching this exact schema:\n"
            '{"target_companies": [str], "buyer_roles": [str], "pain_points": [str], '
            '"use_cases": [str], "value_proposition": str}'
        )

    return f"{instructions}\n\nIdea:\n{idea.strip()}"


def _build_stakeholder_prompt(context: StakeholderContext, retry: bool = False) -> str:
    instructions = (
        "Return ONLY valid JSON matching this exact schema:\n"
        '{"roles": [str], "role_reasoning": {"role_name": "why this role is relevant"}, '
        '"priority_order": [str]}\n\n'
        "Rules:\n"
        "- roles must be refined, realistic job titles\n"
        "- priority_order must rank who to contact first by buying power and relevance\n"
        "- role_reasoning must be specific and tied to pain points or use cases"
    )
    if retry:
        instructions = (
            "The previous response was invalid JSON. Return ONLY valid JSON matching this exact schema:\n"
            '{"roles": [str], "role_reasoning": {"role_name": "why this role is relevant"}, '
            '"priority_order": [str]}\n\n'
            "Rules:\n"
            "- roles must be refined, realistic job titles\n"
            "- priority_order must rank who to contact first by buying power and relevance\n"
            "- role_reasoning must be specific and tied to pain points or use cases"
        )

    return (
        f"{instructions}\n\n"
        f"Context:\n{context.model_dump_json(indent=2)}"
    )


def _build_signal_prompt(items: list[dict[str, Any]], retry: bool = False) -> str:
    instructions = (
        "Return ONLY valid JSON as an array of objects matching this exact schema:\n"
        '[{"company": str, "matched_domain": str, "signals": {"hiring": bool, "github_activity": "low" | "medium" | "high", '
        '"product_launch": bool, "discussion_heat": "low" | "medium" | "high"}, "source_summary": str}]\n\n'
        "Rules:\n"
        "- use only the provided data\n"
        "- do not guess beyond evidence\n"
        "- keep source_summary concise and specific\n"
        "- return between 5 and 10 items if possible, matching the input size"
    )
    if retry:
        instructions = (
            "The previous response was invalid JSON. Return ONLY valid JSON as an array of objects matching this exact schema:\n"
            '[{"company": str, "matched_domain": str, "signals": {"hiring": bool, "github_activity": "low" | "medium" | "high", '
            '"product_launch": bool, "discussion_heat": "low" | "medium" | "high"}, "source_summary": str}]\n\n'
            "Rules:\n"
            "- use only the provided data\n"
            "- do not guess beyond evidence\n"
            "- keep source_summary concise and specific\n"
            "- return between 5 and 10 items if possible, matching the input size"
        )

    return f"{instructions}\n\nData:\n{json.dumps(items, ensure_ascii=True, indent=2)}"


def _build_domain_prompt(context: StakeholderContext, retry: bool = False) -> str:
    instructions = (
        "Return ONLY valid JSON matching this exact schema:\n"
        '{"domains": [str], "domain_reasoning": {"domain_name": "why this domain is a strong fit"}, '
        '"priority_domains": [str]}\n\n'
        "Rules:\n"
        "- domains must be industry segments or company categories\n"
        "- domain_reasoning must connect to pain points or use cases\n"
        "- priority_domains must be ranked by likelihood to convert"
    )
    if retry:
        instructions = (
            "The previous response was invalid JSON. Return ONLY valid JSON matching this exact schema:\n"
            '{"domains": [str], "domain_reasoning": {"domain_name": "why this domain is a strong fit"}, '
            '"priority_domains": [str]}\n\n'
            "Rules:\n"
            "- domains must be industry segments or company categories\n"
            "- domain_reasoning must connect to pain points or use cases\n"
            "- priority_domains must be ranked by likelihood to convert"
        )

    return f"{instructions}\n\nContext:\n{context.model_dump_json(indent=2)}"


def _build_lead_reason_prompt(lead: dict[str, Any], score: int, priority: str, retry: bool = False) -> str:
    instructions = (
        "Return ONLY valid JSON matching this exact schema:\n"
        '{"reason": str, "recommended_angle": str}\n\n'
        "Rules:\n"
        "- reason must explain why the lead is valuable\n"
        "- recommended_angle must be a concise outreach angle\n"
        "- do not mention scoring mechanics unless useful\n"
        "- do not add markdown or extra text"
    )
    if retry:
        instructions = (
            "The previous response was invalid JSON. Return ONLY valid JSON matching this exact schema:\n"
            '{"reason": str, "recommended_angle": str}\n\n'
            "Rules:\n"
            "- reason must explain why the lead is valuable\n"
            "- recommended_angle must be a concise outreach angle\n"
            "- do not mention scoring mechanics unless useful\n"
            "- do not add markdown or extra text"
        )

    payload = {
        "company": lead.get("company", ""),
        "matched_domain": lead.get("matched_domain", ""),
        "signals": lead.get("signals", {}),
        "source_summary": lead.get("source_summary", ""),
        "score": score,
        "priority": priority,
    }
    return f"{instructions}\n\nLead:\n{json.dumps(payload, ensure_ascii=True, indent=2)}"


def _create_model() -> _ModelSession:
    return _create_model_with_prompt(SYSTEM_PROMPT)


def _create_client() -> genai.Client:
    global _CLIENT
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not set")
    if _CLIENT is None:
        _CLIENT = genai.Client(api_key=api_key)
    return _CLIENT


def _create_model_with_prompt(system_prompt: str) -> _ModelSession:
    return _ModelSession(client=_create_client(), system_prompt=system_prompt)


def _parse_analysis(payload: str) -> IdeaAnalysis:
    data: Any = json.loads(_extract_json_payload(payload))
    return IdeaAnalysis.model_validate(data)


def _parse_stakeholder_mapping(payload: str) -> StakeholderMappingResponse:
    data: Any = json.loads(_extract_json_payload(payload))
    return StakeholderMappingResponse.model_validate(data)


def _parse_signal_items(payload: str) -> list[SignalItem]:
    data: Any = json.loads(_extract_json_payload(payload))
    if not isinstance(data, list):
        raise ValueError("Expected a JSON array")
    return [SignalItem.model_validate(item) for item in data]


def _parse_domain_mapping(payload: str) -> dict[str, Any]:
    data: Any = json.loads(_extract_json_payload(payload))
    if not isinstance(data, dict):
        raise ValueError("Expected a JSON object")
    return data


def _parse_lead_reason(payload: str) -> dict[str, str]:
    data: Any = json.loads(_extract_json_payload(payload))
    if not isinstance(data, dict):
        raise ValueError("Expected a JSON object")
    reason = data.get("reason")
    recommended_angle = data.get("recommended_angle")
    if not isinstance(reason, str) or not isinstance(recommended_angle, str):
        raise ValueError("Invalid lead explanation payload")
    return {"reason": reason, "recommended_angle": recommended_angle}


def _response_text(response: Any) -> str:
    direct = getattr(response, "text", None)
    if isinstance(direct, str) and direct.strip():
        return direct

    output_text = getattr(response, "output_text", None)
    if isinstance(output_text, str) and output_text.strip():
        return output_text

    return ""


def _generate_structured_response(model: _ModelSession, prompt: str) -> str:
    full_prompt = f"{model.system_prompt}\n\n{prompt}"
    try:
        response = model.client.models.generate_content(
            model=MODEL_NAME,
            contents=full_prompt,
            config={"temperature": TEMPERATURE},
        )
    except TypeError:
        response = model.client.models.generate_content(
            model=MODEL_NAME,
            contents=full_prompt,
        )

    response_text = _response_text(response)
    if not response_text:
        raise ValueError("Empty response from Gemini")
    return response_text


def _generate_structured_response_with_temperature(
    model: _ModelSession,
    prompt: str,
    temperature: float,
) -> str:
    full_prompt = f"{model.system_prompt}\n\n{prompt}"
    try:
        response = model.client.models.generate_content(
            model=MODEL_NAME,
            contents=full_prompt,
            config={"temperature": temperature},
        )
    except TypeError:
        response = model.client.models.generate_content(
            model=MODEL_NAME,
            contents=full_prompt,
        )

    response_text = _response_text(response)
    if not response_text:
        raise ValueError("Empty response from Gemini")
    return response_text


def collect_signals_with_gemini(items: list[dict[str, Any]]) -> list[SignalItem]:
    return extract_signals(items)


def extract_signals(items: list[dict[str, Any]]) -> list[SignalItem]:
    model = _create_model_with_prompt(SIGNAL_SYSTEM_PROMPT)
    last_error: Exception | None = None

    for attempt in range(2):
        prompt = _build_signal_prompt(items, retry=attempt == 1)

        try:
            return _parse_signal_items(_generate_structured_response(model, prompt))
        except Exception as exc:  # noqa: BLE001 - retry once on invalid JSON or validation failure
            last_error = exc

    raise ValueError("Gemini returned invalid JSON after retry") from last_error


def analyze_idea_with_gemini(idea: str) -> IdeaAnalysis:
    model = _create_model()
    last_error: Exception | None = None

    for attempt in range(2):
        prompt = _build_prompt(idea, retry=attempt == 1)

        try:
            return _parse_analysis(_generate_structured_response(model, prompt))
        except Exception as exc:  # noqa: BLE001 - we intentionally retry on any parse/validation failure
            last_error = exc

    raise ValueError("Gemini returned invalid JSON after retry") from last_error


def map_stakeholders_with_gemini(context: StakeholderContext) -> StakeholderMappingResponse:
    model = _create_model()
    last_error: Exception | None = None

    for attempt in range(2):
        prompt = _build_stakeholder_prompt(context, retry=attempt == 1)

        try:
            return _parse_stakeholder_mapping(_generate_structured_response(model, prompt))
        except Exception as exc:  # noqa: BLE001 - retry once on invalid JSON or validation failure
            last_error = exc

    raise ValueError("Gemini returned invalid JSON after retry") from last_error


def map_domains_with_gemini(context: StakeholderContext) -> dict[str, Any]:
    model = _create_model_with_prompt(DOMAIN_SYSTEM_PROMPT)
    last_error: Exception | None = None

    for attempt in range(2):
        prompt = _build_domain_prompt(context, retry=attempt == 1)

        try:
            response_text = _generate_structured_response_with_temperature(
                model,
                prompt,
                DOMAIN_TEMPERATURE,
            )
            return _parse_domain_mapping(response_text)
        except Exception as exc:  # noqa: BLE001 - retry once on invalid JSON or validation failure
            last_error = exc

    raise ValueError("Gemini returned invalid JSON after retry") from last_error


def generate_reason(lead: dict[str, Any], score: int, priority: str) -> dict[str, str]:
    model = _create_model_with_prompt(LEAD_RANKING_SYSTEM_PROMPT)
    last_error: Exception | None = None

    for attempt in range(2):
        prompt = _build_lead_reason_prompt(lead, score, priority, retry=attempt == 1)

        try:
            response_text = _generate_structured_response_with_temperature(model, prompt, 0.2)
            return _parse_lead_reason(response_text)
        except Exception as exc:  # noqa: BLE001 - retry once on invalid JSON or validation failure
            last_error = exc

    return {
        "reason": f"{lead.get('company', 'This lead')} shows buying intent based on current activity and fit with the '{lead.get('matched_domain', 'target')}' domain.",
        "recommended_angle": "Lead with a concise, timely note tied to the strongest visible signal and the specific operational pain it creates.",
    }


def generate_json_with_gemini(
    *,
    system_prompt: str,
    user_prompt: str,
    temperature: float = 0.3,
) -> dict[str, Any]:
    model = _create_model_with_prompt(system_prompt)
    last_error: Exception | None = None

    for attempt in range(2):
        retry_prefix = ""
        if attempt == 1:
            retry_prefix = "The previous response was invalid JSON. Return ONLY valid JSON.\n\n"

        prompt = f"{retry_prefix}{user_prompt}"
        try:
            response_text = _generate_structured_response_with_temperature(model, prompt, temperature)
            data: Any = json.loads(_extract_json_payload(response_text))
            if not isinstance(data, dict):
                raise ValueError("Expected a JSON object")
            return data
        except Exception as exc:  # noqa: BLE001 - retry once on invalid JSON or parse issues
            last_error = exc

    raise ValueError("Gemini returned invalid JSON after retry") from last_error