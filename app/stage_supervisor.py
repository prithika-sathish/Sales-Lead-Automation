from __future__ import annotations

import asyncio
import inspect
import importlib
import json
import os
from dataclasses import dataclass, field
from typing import Any, Callable


DEFAULT_REFINER_MODEL = os.getenv("LLM_STAGE_REFINER_MODEL", "gemini-2.5-flash")
DEFAULT_JUDGE_MODEL = os.getenv("LLM_STAGE_JUDGE_MODEL", "gemini-2.5-pro")
DEFAULT_MAX_RETRIES = 2
DEFAULT_MIN_QUALITY = 0.75


@dataclass(slots=True)
class StageAudit:
    stage_name: str
    attempt: int
    refined: bool
    quality_score: float
    approved: bool
    retry: bool
    issues: list[str] = field(default_factory=list)
    rationale: str = ""


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, indent=2, default=str)


def _strip_code_fences(text: str) -> str:
    cleaned = _clean_text(text)
    if cleaned.startswith("```"):
        cleaned = cleaned.removeprefix("```json").removeprefix("```").strip()
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3].strip()
    return cleaned


def _extract_json_payload(text: str) -> str:
    cleaned = _strip_code_fences(text)
    if cleaned.startswith("{") and cleaned.endswith("}"):
        return cleaned
    if cleaned.startswith("[") and cleaned.endswith("]"):
        return cleaned
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start >= 0 and end > start:
        return cleaned[start : end + 1]
    start = cleaned.find("[")
    end = cleaned.rfind("]")
    if start >= 0 and end > start:
        return cleaned[start : end + 1]
    return cleaned


def _load_genai_client() -> Any | None:
    api_key = _clean_text(os.getenv("GEMINI_API_KEY"))
    if not api_key:
        return None

    try:
        genai_module = importlib.import_module("google.genai")
    except Exception:
        return None

    try:
        return genai_module.Client(api_key=api_key)
    except Exception:
        return None


def _response_text(response: Any) -> str:
    direct = getattr(response, "text", None)
    if isinstance(direct, str) and direct.strip():
        return direct

    output_text = getattr(response, "output_text", None)
    if isinstance(output_text, str) and output_text.strip():
        return output_text

    return ""


def _generate_json(
    *,
    model_name: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float,
) -> dict[str, Any]:
    client = _load_genai_client()
    if client is None:
        raise RuntimeError("GEMINI_API_KEY is not available")

    prompt = f"{system_prompt}\n\n{user_prompt}"
    try:
        response = client.models.generate_content(
            model=model_name,
            contents=prompt,
            config={"temperature": temperature},
        )
    except TypeError:
        response = client.models.generate_content(
            model=model_name,
            contents=prompt,
        )

    text = _response_text(response)
    if not text:
        raise ValueError("Empty response from Gemini")

    payload = json.loads(_extract_json_payload(text))
    if not isinstance(payload, dict):
        raise ValueError("Expected JSON object")
    return payload


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _default_quality_score(output: Any) -> float:
    if isinstance(output, dict):
        return 1.0 if output else 0.0
    if isinstance(output, list):
        return min(1.0, max(0.0, len(output) / 10.0))
    return 0.5 if output else 0.0


def _build_refiner_prompt(
    *,
    stage_name: str,
    objective: str,
    input_payload: Any,
    issues: list[str],
    attempt: int,
) -> tuple[str, str]:
    system_prompt = (
        "You are the input refiner in a supervised LLM pipeline. "
        "Your job is to improve the next stage input without broadening scope. "
        "Return JSON only."
    )
    user_prompt = (
        f"stage_name: {stage_name}\n"
        f"objective: {objective}\n"
        f"attempt: {attempt}\n"
        f"known_issues: {issues}\n\n"
        "Return JSON with keys: refined_input, rationale, risk_flags.\n"
        "Rules:\n"
        "- refined_input must stay within the original stage boundary\n"
        "- reduce noise, remove generic terms, and narrow the search\n"
        "- if the input is already good, return it unchanged\n\n"
        f"input_payload:\n{_json_text(input_payload)}"
    )
    return system_prompt, user_prompt


def _build_judge_prompt(
    *,
    stage_name: str,
    objective: str,
    input_payload: Any,
    output_payload: Any,
    min_quality: float,
) -> tuple[str, str]:
    system_prompt = (
        "You are the output judge in a supervised LLM pipeline. "
        "Evaluate only the provided data. Return JSON only."
    )
    user_prompt = (
        f"stage_name: {stage_name}\n"
        f"objective: {objective}\n"
        f"minimum_quality: {min_quality}\n\n"
        "Return JSON with keys: approved, quality_score, retry, issues, refined_input, rationale.\n"
        "Rules:\n"
        "- quality_score must be between 0 and 1\n"
        "- approved should be true only when the output meets the objective\n"
        "- retry should be true only if rerunning with a refined input can improve the result\n"
        "- refined_input should be the smallest useful correction, not a rewrite of the pipeline\n\n"
        f"input_payload:\n{_json_text(input_payload)}\n\n"
        f"output_payload:\n{_json_text(output_payload)}"
    )
    return system_prompt, user_prompt


async def supervise_stage_async(
    *,
    stage_name: str,
    input_payload: dict[str, Any],
    execute_stage: Callable[[dict[str, Any]], Awaitable[Any] | Any],
    fallback_stage: Callable[[dict[str, Any]], Any],
    objective: str,
    min_quality: float = DEFAULT_MIN_QUALITY,
    max_retries: int = DEFAULT_MAX_RETRIES,
    refiner_model: str | None = None,
    judge_model: str | None = None,
    quality_fn: Callable[[Any], float] | None = None,
) -> tuple[Any, list[StageAudit]]:
    current_input = dict(input_payload or {})
    audits: list[StageAudit] = []
    refiner_model_name = _clean_text(refiner_model or DEFAULT_REFINER_MODEL)
    judge_model_name = _clean_text(judge_model or DEFAULT_JUDGE_MODEL)

    for attempt in range(max(0, int(max_retries)) + 1):
        refined_input = dict(current_input)
        refined = False
        issues: list[str] = []

        try:
            system_prompt, user_prompt = _build_refiner_prompt(
                stage_name=stage_name,
                objective=objective,
                input_payload=current_input,
                issues=issues,
                attempt=attempt,
            )
            refinement = _generate_json(
                model_name=refiner_model_name,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.2,
            )
            candidate_input = refinement.get("refined_input")
            if isinstance(candidate_input, dict) and candidate_input:
                refined_input = candidate_input
                refined = True
        except Exception:
            refined_input = dict(current_input)

        output = await _maybe_await(execute_stage(refined_input))

        deterministic_score = _default_quality_score(output)
        judge_score = deterministic_score
        approved = deterministic_score >= min_quality
        retry = False
        rationale = ""

        try:
            system_prompt, user_prompt = _build_judge_prompt(
                stage_name=stage_name,
                objective=objective,
                input_payload=refined_input,
                output_payload=output,
                min_quality=min_quality,
            )
            judgment = _generate_json(
                model_name=judge_model_name,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.0,
            )
            if isinstance(judgment.get("quality_score"), (int, float)):
                judge_score = max(0.0, min(1.0, float(judgment.get("quality_score") or 0.0)))
            issues = [str(item).strip() for item in judgment.get("issues", []) if str(item).strip()] if isinstance(judgment.get("issues"), list) else []
            retry = bool(judgment.get("retry"))
            rationale = _clean_text(judgment.get("rationale"))
            approved = bool(judgment.get("approved"))
        except Exception:
            pass

        quality_score = judge_score if judge_score >= 0 else deterministic_score
        if quality_fn is not None:
            try:
                fallback_quality = max(0.0, min(1.0, float(quality_fn(output))))
                quality_score = min(1.0, (quality_score * 0.7) + (fallback_quality * 0.3))
            except Exception:
                pass

        final_approved = bool(approved and quality_score >= min_quality)
        final_retry = bool(retry or quality_score < min_quality)

        audits.append(
            StageAudit(
                stage_name=stage_name,
                attempt=attempt,
                refined=refined,
                quality_score=quality_score,
                approved=final_approved,
                retry=final_retry,
                issues=issues,
                rationale=rationale,
            )
        )

        if final_approved or attempt >= max_retries:
            if final_approved:
                return output, audits
            break

        if isinstance(output, dict) and isinstance(output.get("refined_input"), dict):
            current_input = output.get("refined_input")
        else:
            current_input = refined_input

    fallback_output = fallback_stage(input_payload)
    return fallback_output, audits


def supervise_stage(
    *,
    stage_name: str,
    input_payload: dict[str, Any],
    execute_stage: Callable[[dict[str, Any]], Awaitable[Any] | Any],
    fallback_stage: Callable[[dict[str, Any]], Any],
    objective: str,
    min_quality: float = DEFAULT_MIN_QUALITY,
    max_retries: int = DEFAULT_MAX_RETRIES,
    refiner_model: str | None = None,
    judge_model: str | None = None,
    quality_fn: Callable[[Any], float] | None = None,
) -> tuple[Any, list[StageAudit]]:
    return asyncio.run(
        supervise_stage_async(
            stage_name=stage_name,
            input_payload=input_payload,
            execute_stage=execute_stage,
            fallback_stage=fallback_stage,
            objective=objective,
            min_quality=min_quality,
            max_retries=max_retries,
            refiner_model=refiner_model,
            judge_model=judge_model,
            quality_fn=quality_fn,
        )
    )
