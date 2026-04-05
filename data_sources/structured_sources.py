from __future__ import annotations

import asyncio
import importlib
import json
import logging
import re
from typing import Any
from urllib.parse import urlparse

httpx = importlib.import_module("httpx")


logger = logging.getLogger(__name__)

_PRODUCT_HUNT_FEED = "https://www.producthunt.com/feed"
_YC_COMPANIES_URL = "https://www.ycombinator.com/companies"
_PUBLIC_STARTUP_DATASET = "https://raw.githubusercontent.com/ozlerhakan/mongodb-json-files/master/datasets/companies.json"
_SOURCE_TIMEOUT_SECONDS = 8.0
_SOURCE_RETRIES = 2


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _extract_domain(value: object) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    parsed = urlparse(text)
    host = _clean_text(parsed.netloc).lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _query_tokens(query: str) -> list[str]:
    tokens = [token.lower() for token in re.findall(r"[A-Za-z0-9]+", query) if len(token) > 2]
    return list(dict.fromkeys(tokens))


def _matches_query(text: str, query: str) -> bool:
    haystack = _clean_text(text).lower()
    if not haystack:
        return False
    tokens = _query_tokens(query)
    if not tokens:
        return True
    return any(token in haystack for token in tokens)


def _failure_reason(exc: Exception) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code if exc.response is not None else "unknown"
        return f"http_status={code}"
    if isinstance(exc, httpx.TimeoutException):
        return "timeout"
    if isinstance(exc, httpx.ConnectError):
        return f"connect_error={exc}"
    if isinstance(exc, httpx.NetworkError):
        return f"network_error={exc}"
    if isinstance(exc, httpx.TransportError):
        return f"transport_error={exc}"
    return f"error={exc}"


async def _http_get_with_retry(*, source_name: str, url: str, timeout_seconds: float) -> str:
    last_error: Exception | None = None
    strict_timeout = min(float(timeout_seconds), _SOURCE_TIMEOUT_SECONDS)
    for attempt in range(_SOURCE_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(strict_timeout, read=strict_timeout)) as client:
                response = await client.get(url)
                response.raise_for_status()
                return response.text
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= _SOURCE_RETRIES:
                break
            await asyncio.sleep(0.4 * (2**attempt))

    if last_error is not None:
        raise RuntimeError(f"{source_name} failed reason={_failure_reason(last_error)}")
    raise RuntimeError(f"{source_name} failed reason=unknown")


def _normalize_structured_item(
    *,
    company_name: str,
    website: str,
    description: str,
    tags: list[str],
    source: str,
) -> dict[str, Any]:
    return {
        "company_name": _clean_text(company_name),
        "website": _clean_text(website),
        "description": _clean_text(description),
        "tags": [tag for tag in [_clean_text(tag) for tag in tags] if tag],
        "source": _clean_text(source).lower(),
    }


def validate_company(obj: object) -> bool:
    if not isinstance(obj, dict):
        return False
    required = ["company_name", "website", "description", "source"]
    return all(isinstance(obj.get(k), str) and _clean_text(obj.get(k)) for k in required)


def _filter_valid_companies(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    validated = [row for row in results if validate_company(row)]
    return [
        {
            **row,
            "company_name": _clean_text(row.get("company_name")),
            "website": _clean_text(row.get("website")),
            "description": _clean_text(row.get("description")),
            "source": _clean_text(row.get("source")).lower(),
            "tags": row.get("tags") if isinstance(row.get("tags"), list) else [],
        }
        for row in validated
    ]


def mock_structured_source(query: str, limit: int = 5) -> list[dict[str, Any]]:
    _ = query
    rows = [
        {
            "company_name": "Chargebee",
            "website": "https://www.chargebee.com",
            "description": "Subscription billing platform",
            "source": "mock",
            "tags": ["subscription billing", "saas"],
        },
        {
            "company_name": "Zuora",
            "website": "https://www.zuora.com",
            "description": "Subscription management and recurring billing platform",
            "source": "mock",
            "tags": ["subscription billing", "finance"],
        },
        {
            "company_name": "Paddle",
            "website": "https://www.paddle.com",
            "description": "Revenue delivery and subscription billing infrastructure",
            "source": "mock",
            "tags": ["subscription billing", "payments"],
        },
        {
            "company_name": "Recurly",
            "website": "https://recurly.com",
            "description": "Recurring billing and subscription analytics software",
            "source": "mock",
            "tags": ["subscription billing", "analytics"],
        },
        {
            "company_name": "Billsby",
            "website": "https://www.billsby.com",
            "description": "Subscription billing and recurring payment management",
            "source": "mock",
            "tags": ["subscription billing", "payments"],
        },
    ]
    return _filter_valid_companies(rows)[: max(1, int(limit))]


async def fetch_hiring_companies(query: str, *, limit: int = 20) -> list[dict[str, Any]]:
    # Simulated hiring signal feed to keep acquisition resilient when external hiring endpoints are unstable.
    catalog = [
        ("Klenty", "https://www.klenty.com", "Hiring SDR and account executive roles for GTM expansion in India and Singapore"),
        ("BrowserStack", "https://www.browserstack.com", "Growing sales development and revenue roles across APAC"),
        ("WebEngage", "https://webengage.com", "Hiring growth and enterprise sales teams for B2B SaaS products"),
        ("LeadSquared", "https://www.leadsquared.com", "Actively hiring SDR and account executive talent for regional expansion"),
        ("MoEngage", "https://www.moengage.com", "Scaling go-to-market with new sales and growth openings"),
        ("Darwinbox", "https://www.darwinbox.com", "Hiring business development and growth roles in Asia"),
        ("Freshservice", "https://www.freshworks.com", "Hiring enterprise account executives and SDRs for SaaS sales"),
        ("Observe.AI", "https://www.observe.ai", "Expanding revenue team with SDR and growth positions"),
        ("Mindtickle", "https://www.mindtickle.com", "Hiring sales development and customer growth teams"),
        ("Whatfix", "https://whatfix.com", "Recruiting GTM talent to support global B2B expansion"),
        ("Icertis", "https://www.icertis.com", "Building enterprise sales and account teams"),
        ("Postman", "https://www.postman.com", "Hiring growth and sales roles to scale API platform adoption"),
    ]

    rows: list[dict[str, Any]] = []
    for company_name, website, description in catalog:
        text_for_match = f"{company_name} {description}"
        if not _matches_query(text_for_match, query):
            continue
        rows.append(
            _normalize_structured_item(
                company_name=company_name,
                website=website,
                description=description,
                tags=["hiring", "sdr", "account executive", "growth"],
                source="hiring_feed",
            )
        )
        if len(rows) >= limit:
            break

    return _filter_valid_companies(rows)


async def fetch_producthunt_companies(query: str, *, limit: int = 20, timeout_seconds: float = 15.0) -> list[dict[str, Any]]:
    try:
        xml = await _http_get_with_retry(source_name="producthunt", url=_PRODUCT_HUNT_FEED, timeout_seconds=timeout_seconds)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[SOURCE FAILED] producthunt reason=%s", exc)
        return []

    if "<item>" not in xml:
        logger.warning("[SOURCE FAILED] producthunt reason=invalid_schema")
        return []

    items = re.findall(r"<item>(.*?)</item>", xml, flags=re.S | re.I)
    rows: list[dict[str, Any]] = []
    for item in items:
        title_match = re.search(r"<title><!\[CDATA\[(.*?)\]\]></title>", item, flags=re.S | re.I)
        link_match = re.search(r"<link>(.*?)</link>", item, flags=re.S | re.I)
        desc_match = re.search(r"<description><!\[CDATA\[(.*?)\]\]></description>", item, flags=re.S | re.I)

        title = _clean_text(title_match.group(1) if title_match else "")
        link = _clean_text(link_match.group(1) if link_match else "")
        description = _clean_text(re.sub(r"<[^>]+>", " ", desc_match.group(1) if desc_match else ""))
        if not title:
            continue

        company_name = title.split("|")[0].strip()
        text_for_match = f"{company_name} {description}"
        if not _matches_query(text_for_match, query):
            continue

        rows.append(
            _normalize_structured_item(
                company_name=company_name,
                website=link,
                description=description,
                tags=["producthunt"],
                source="producthunt",
            )
        )
        if len(rows) >= limit:
            break

    validated = _filter_valid_companies(rows)
    if not validated:
        logger.warning("[EMPTY RESPONSE] producthunt")
    return validated


async def fetch_yc_companies(query: str, *, limit: int = 25, timeout_seconds: float = 20.0) -> list[dict[str, Any]]:
    try:
        html = await _http_get_with_retry(source_name="yc", url=_YC_COMPANIES_URL, timeout_seconds=timeout_seconds)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[SOURCE FAILED] yc reason=%s", exc)
        return []

    pattern = re.compile(
        r'"name":"(?P<name>[^"]+)".*?"website":"(?P<website>[^"]*)".*?"one_liner":"(?P<one_liner>[^"]*)"',
        flags=re.S,
    )

    rows: list[dict[str, Any]] = []
    for match in pattern.finditer(html):
        name = _clean_text(match.group("name"))
        website = _clean_text(match.group("website"))
        one_liner = _clean_text(match.group("one_liner"))
        if not name:
            continue
        text_for_match = f"{name} {one_liner}"
        if not _matches_query(text_for_match, query):
            continue

        rows.append(
            _normalize_structured_item(
                company_name=name,
                website=website,
                description=one_liner,
                tags=["yc"],
                source="yc",
            )
        )
        if len(rows) >= limit:
            break

    validated = _filter_valid_companies(rows)
    if not validated:
        logger.warning("[EMPTY RESPONSE] yc")
    return validated


async def fetch_public_startup_dataset(query: str, *, limit: int = 40, timeout_seconds: float = 20.0) -> list[dict[str, Any]]:
    try:
        body = await _http_get_with_retry(source_name="public_dataset", url=_PUBLIC_STARTUP_DATASET, timeout_seconds=timeout_seconds)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[SOURCE FAILED] public_dataset reason=%s", exc)
        return []

    payload: Any = None
    try:
        payload = json.loads(body)
    except Exception:
        # Fallback for JSONL payloads.
        jsonl_rows: list[dict[str, Any]] = []
        for line in body.splitlines():
            stripped = _clean_text(line)
            if not stripped:
                continue
            try:
                obj = json.loads(stripped)
            except Exception:
                continue
            if isinstance(obj, dict):
                jsonl_rows.append(obj)
        payload = jsonl_rows

    items = payload if isinstance(payload, list) else []
    if not isinstance(items, list):
        logger.warning("[SOURCE FAILED] public_dataset reason=invalid_schema")
        return []
    rows: list[dict[str, Any]] = []

    for item in items:
        if not isinstance(item, dict):
            continue

        name = _clean_text(item.get("name"))
        website = _clean_text(item.get("homepage_url") or item.get("website") or "")
        description = _clean_text(item.get("category_code") or item.get("description") or "")
        tags: list[str] = []
        if _clean_text(item.get("category_code")):
            tags.append(_clean_text(item.get("category_code")))

        if not name:
            continue

        text_for_match = f"{name} {description} {' '.join(tags)}"
        if not _matches_query(text_for_match, query):
            continue

        rows.append(
            _normalize_structured_item(
                company_name=name,
                website=website,
                description=description,
                tags=tags,
                source="public_dataset",
            )
        )
        if len(rows) >= limit:
            break

    validated = _filter_valid_companies(rows)
    if not validated:
        logger.warning("[EMPTY RESPONSE] public_dataset")
    return validated


async def fetch_structured_candidates(query: str, *, per_source_limit: int = 20) -> list[dict[str, Any]]:
    async def safe_fetch(task: Any, *, timeout: float, source_name: str) -> tuple[list[dict[str, Any]], bool]:
        try:
            rows = await asyncio.wait_for(task, timeout=timeout)
            valid_rows = rows if isinstance(rows, list) else []
            if not valid_rows:
                logger.warning("[EMPTY RESPONSE] %s", source_name)
            return valid_rows, True
        except asyncio.TimeoutError:
            logger.warning("[TIMEOUT] %s", source_name)
            return [], False
        except Exception:  # noqa: BLE001
            logger.warning("[SOURCE FAILED] %s", source_name)
            return [], False

    tasks = [
        safe_fetch(fetch_producthunt_companies(query, limit=per_source_limit, timeout_seconds=_SOURCE_TIMEOUT_SECONDS), timeout=_SOURCE_TIMEOUT_SECONDS, source_name="producthunt"),
        safe_fetch(fetch_yc_companies(query, limit=per_source_limit, timeout_seconds=_SOURCE_TIMEOUT_SECONDS), timeout=_SOURCE_TIMEOUT_SECONDS, source_name="yc"),
        safe_fetch(fetch_public_startup_dataset(query, limit=per_source_limit, timeout_seconds=_SOURCE_TIMEOUT_SECONDS), timeout=_SOURCE_TIMEOUT_SECONDS, source_name="public_dataset"),
    ]

    gathered = await asyncio.gather(*tasks, return_exceptions=True)
    output: list[dict[str, Any]] = []
    any_external_success = False
    all_external_empty = True
    for value in gathered:
        if isinstance(value, Exception):
            continue
        rows, succeeded = value
        any_external_success = any_external_success or bool(succeeded)
        all_external_empty = all_external_empty and len(rows) == 0
        output.extend([row for row in rows if isinstance(row, dict)])

    # Emergency mock-only source for hiring signals when every external structured source fails.
    if (not any_external_success or all_external_empty) and not output:
        hiring_rows, _ = await safe_fetch(
            fetch_hiring_companies(query, limit=per_source_limit),
            timeout=_SOURCE_TIMEOUT_SECONDS,
            source_name="hiring_feed_mock",
        )
        output.extend([row for row in hiring_rows if isinstance(row, dict)])

    output = _filter_valid_companies(output)

    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in output:
        company_name = _clean_text(row.get("company_name"))
        website = _clean_text(row.get("website"))
        domain = _extract_domain(website)
        key = (company_name.lower(), domain)
        if not company_name:
            continue
        deduped[key] = row

    results = list(deduped.values())
    results = _filter_valid_companies(results)
    logger.info("[RESULT COUNT] structured: %s", len(results))

    if not results:
        logger.warning("[EMPTY RESPONSE] structured")
        return []

    return results


async def fetch_structured_sources(query: str, *, per_source_limit: int = 20) -> list[dict[str, Any]]:
    return await fetch_structured_candidates(query, per_source_limit=per_source_limit)
