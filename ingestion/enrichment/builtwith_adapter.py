from __future__ import annotations

import asyncio
import importlib
import logging
import os
from typing import Any

httpx = importlib.import_module("httpx")


logger = logging.getLogger(__name__)
_BUILTWITH_URL = "https://api.builtwith.com/v20/api.json"


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalize_domain(value: str) -> str:
    text = _clean_text(value).lower()
    text = text.replace("https://", "").replace("http://", "")
    text = text.split("/")[0]
    if text.startswith("www."):
        text = text[4:]
    return text


def _company_from_domain(domain: str) -> str:
    token = domain.split(".")[0] if domain else ""
    token = token.replace("-", " ").replace("_", " ")
    return " ".join(part.capitalize() for part in token.split())


async def _fetch_domain(client: Any, domain: str, api_key: str) -> tuple[dict[str, Any], str | None]:
    params = {"KEY": api_key, "LOOKUP": domain}
    for attempt in range(3):
        try:
            response = await client.get(_BUILTWITH_URL, params=params)
        except httpx.TimeoutException:
            if attempt < 2:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            return {}, "timeout"
        except httpx.TransportError as exc:
            if attempt < 2:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            return {}, f"transport_error:{exc}"

        if response.status_code in {429, 500, 502, 503, 504} and attempt < 2:
            await asyncio.sleep(0.6 * (attempt + 1))
            continue
        if response.status_code >= 400:
            return {}, f"status_{response.status_code}"

        try:
            payload: Any = response.json()
        except Exception as exc:  # noqa: BLE001
            if attempt < 2:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            return {}, f"invalid_json:{exc}"
        break
    else:
        return {}, "retries_exhausted"

    results = payload.get("Results") if isinstance(payload, dict) else None
    if not isinstance(results, list) or not results:
        return {}, "schema_invalid"

    technologies: list[str] = []
    for result in results:
        paths = result.get("Result") if isinstance(result, dict) else []
        if not isinstance(paths, list):
            continue
        for path in paths:
            techs = path.get("Technologies") if isinstance(path, dict) else []
            if not isinstance(techs, list):
                continue
            for tech in techs:
                name = _clean_text(tech.get("Name") if isinstance(tech, dict) else "")
                if name and name not in technologies:
                    technologies.append(name)

    if not technologies:
        return {}, "empty_technologies"

    return {
        "company": _company_from_domain(domain),
        "domain": domain,
        "tech_stack": technologies[:40],
        "source": "builtwith",
    }, None


async def fetch_builtwith_tech(domains: list[str]) -> tuple[list[dict[str, Any]], int]:
    api_key = _clean_text(os.getenv("BUILTWITH_API_KEY"))
    if not api_key:
        logger.warning("[SOURCE FAILED] builtwith reason=missing_api_key")
        return [], 1

    domain_list = [d for d in (_normalize_domain(value) for value in domains) if d]
    if not domain_list:
        return [], 0

    failures = 0
    timeout = httpx.Timeout(5.0, read=5.0)
    output: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        tasks = [asyncio.create_task(_fetch_domain(client, domain, api_key)) for domain in domain_list]
        gathered = await asyncio.gather(*tasks, return_exceptions=True)

        for idx, result in enumerate(gathered):
            domain = domain_list[idx]
            if isinstance(result, Exception):
                failures += 1
                logger.warning("[SOURCE FAILED] builtwith domain=%s reason=exception detail=%s", domain, result)
                continue

            payload, error = result
            if error:
                failures += 1
                logger.warning("[SOURCE FAILED] builtwith domain=%s reason=%s", domain, error)
                continue

            output.append(payload)

    if not output:
        logger.warning("[EMPTY RESPONSE] builtwith")

    logger.info("[RESULT COUNT] builtwith: %s", len(output))
    return output, failures
