from __future__ import annotations

import asyncio
import logging
import os
from typing import Any
from urllib.parse import urlencode

import httpx
from dotenv import load_dotenv


logger = logging.getLogger(__name__)

load_dotenv()
load_dotenv(".env")
load_dotenv(".env.local")
if not os.getenv("SERPER_API_KEY") and not os.getenv("SERPAPI_API_KEY"):
    load_dotenv(".env.example")

_SERPER_URL = "https://google.serper.dev/search"
_SERPAPI_URL = "https://serpapi.com/search.json"
_DUCKDUCKGO_HTML_URL = "https://html.duckduckgo.com/html/"


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _api_key(name: str) -> str:
    return _clean_text(os.getenv(name))


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


def _is_valid_serper_payload(payload: Any) -> bool:
    return isinstance(payload, dict) and isinstance(payload.get("organic"), list)


def _is_valid_serpapi_payload(payload: Any) -> bool:
    return isinstance(payload, dict) and isinstance(payload.get("organic_results"), list)


async def _request_with_backoff(
    client: httpx.AsyncClient,
    *,
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    json_payload: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    retries: int = 2,
    base_delay: float = 0.8,
) -> httpx.Response:
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            response = await client.request(method=method, url=url, headers=headers, json=json_payload, params=params)
            response.raise_for_status()
            return response
        except (httpx.TimeoutException, httpx.HTTPStatusError, httpx.TransportError) as exc:
            last_error = exc
            if attempt >= retries:
                break
            delay = base_delay * (2**attempt)
            logger.warning(
                "serp request retry | url=%s attempt=%s delay=%.2fs reason=%s",
                url,
                attempt + 1,
                delay,
                _failure_reason(exc),
            )
            await asyncio.sleep(delay)

    if last_error is not None:
        raise last_error
    raise RuntimeError("serp request failed without error")


def _normalize_serper_items(payload: dict[str, Any]) -> list[dict[str, str]]:
    organic = payload.get("organic") if isinstance(payload, dict) else []
    rows: list[dict[str, str]] = []
    if not isinstance(organic, list):
        return rows

    for row in organic:
        if not isinstance(row, dict):
            continue
        title = _clean_text(row.get("title"))
        link = _clean_text(row.get("link"))
        snippet = _clean_text(row.get("snippet"))
        if not title or not link:
            continue
        rows.append(
            {
                "title": title,
                "link": link,
                "snippet": snippet,
                "source": "serper",
            }
        )
    return rows


def _normalize_serpapi_items(payload: dict[str, Any]) -> list[dict[str, str]]:
    organic = payload.get("organic_results") if isinstance(payload, dict) else []
    rows: list[dict[str, str]] = []
    if not isinstance(organic, list):
        return rows

    for row in organic:
        if not isinstance(row, dict):
            continue
        title = _clean_text(row.get("title"))
        link = _clean_text(row.get("link"))
        snippet = _clean_text(row.get("snippet") or row.get("snippet_highlighted_words"))
        if not title or not link:
            continue
        rows.append(
            {
                "title": title,
                "link": link,
                "snippet": snippet,
                "source": "serpapi",
            }
        )
    return rows


def _normalize_duckduckgo_items(html: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for block in html.split('<div class="result">'):
        if "result__a" not in block:
            continue
        title_match = None
        link_match = None
        snippet_match = None
        try:
            import re

            title_match = re.search(r'class="result__a"[^>]*>(.*?)</a>', block, flags=re.S | re.I)
            link_match = re.search(r'class="result__a"[^>]*href="([^"]+)"', block, flags=re.S | re.I)
            snippet_match = re.search(r'class="result__snippet"[^>]*>(.*?)</a>|class="result__snippet"[^>]*>(.*?)</div>', block, flags=re.S | re.I)
        except Exception:
            continue

        title = _clean_text(title_match.group(1) if title_match else "")
        link = _clean_text(link_match.group(1) if link_match else "")
        snippet = _clean_text((snippet_match.group(1) or snippet_match.group(2)) if snippet_match else "")
        if not title or not link:
            continue
        rows.append({"title": title, "link": link, "snippet": snippet, "source": "duckduckgo"})
    return rows


async def _search_via_serper(
    *,
    query: str,
    timeout_seconds: float,
    retries: int,
    base_delay: float,
    proxy_url: str,
) -> list[dict[str, str]]:
    api_key = _api_key("SERPER_API_KEY")
    if not api_key:
        raise RuntimeError("SERPER_API_KEY is not set")

    transport = httpx.AsyncHTTPTransport(proxy=proxy_url) if proxy_url else None
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds, read=timeout_seconds), transport=transport) as client:
        response = await _request_with_backoff(
            client,
            method="POST",
            url=_SERPER_URL,
            headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
            json_payload={"q": query, "num": 10},
            retries=retries,
            base_delay=base_delay,
        )
    payload: Any = response.json()
    if not _is_valid_serper_payload(payload):
        logger.warning("[SOURCE FAILED] serp_serper reason=invalid_schema")
        return []
    rows = _normalize_serper_items(payload)
    if not rows:
        logger.warning("[EMPTY RESPONSE] serp_serper")
    return rows


async def _search_via_serpapi(
    *,
    query: str,
    timeout_seconds: float,
    retries: int,
    base_delay: float,
    proxy_url: str,
) -> list[dict[str, str]]:
    api_key = _api_key("SERPAPI_API_KEY")
    if not api_key:
        raise RuntimeError("SERPAPI_API_KEY is not set")

    params = {
        "q": query,
        "engine": "google",
        "num": 10,
        "api_key": api_key,
    }
    transport = httpx.AsyncHTTPTransport(proxy=proxy_url) if proxy_url else None
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds, read=timeout_seconds), transport=transport) as client:
        response = await _request_with_backoff(
            client,
            method="GET",
            url=_SERPAPI_URL,
            params=params,
            retries=retries,
            base_delay=base_delay,
        )
    payload: Any = response.json()
    if not _is_valid_serpapi_payload(payload):
        logger.warning("[SOURCE FAILED] serp_serpapi reason=invalid_schema")
        return []
    rows = _normalize_serpapi_items(payload)
    if not rows:
        logger.warning("[EMPTY RESPONSE] serp_serpapi")
    return rows


async def _search_via_duckduckgo(
    *,
    query: str,
    timeout_seconds: float,
    retries: int,
    base_delay: float,
    proxy_url: str,
) -> list[dict[str, str]]:
    transport = httpx.AsyncHTTPTransport(proxy=proxy_url) if proxy_url else None
    last_error: Exception | None = None
    html = ""
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds, read=timeout_seconds), transport=transport) as client:
        for attempt in range(retries + 1):
            try:
                response = await client.post(
                    _DUCKDUCKGO_HTML_URL,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    data={"q": query},
                )
                response.raise_for_status()
                html = response.text
                break
            except (httpx.TimeoutException, httpx.HTTPStatusError, httpx.TransportError) as exc:
                last_error = exc
                if attempt >= retries:
                    break
                delay = base_delay * (2**attempt)
                await asyncio.sleep(delay)

    if last_error is not None and not html:
        raise last_error

    if "result__a" not in html:
        logger.warning("[SOURCE FAILED] serp_duckduckgo reason=invalid_schema")
        return []

    rows = _normalize_duckduckgo_items(html)
    if not rows:
        logger.warning("[EMPTY RESPONSE] serp_duckduckgo")
    return rows


async def search_serp(
    query: str,
    *,
    timeout_seconds: float = 10.0,
    retries: int = 2,
    base_delay: float = 0.8,
    proxy_url: str | None = None,
) -> list[dict[str, str]]:
    clean_query = _clean_text(query)
    if not clean_query:
        return []

    proxy = _clean_text(proxy_url or os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY"))
    serper_error: Exception | None = None
    try:
        rows = await _search_via_serper(
            query=clean_query,
            timeout_seconds=timeout_seconds,
            retries=retries,
            base_delay=base_delay,
            proxy_url=proxy,
        )
        logger.info("serp success | provider=serper query=%s results=%s", clean_query, len(rows))
        if rows:
            return rows
        logger.warning("[EMPTY RESPONSE] serp_serper")
    except Exception as exc:  # noqa: BLE001
        serper_error = exc
        logger.warning("[SOURCE FAILED] serp_serper reason=%s", _failure_reason(exc))

    serpapi_error: Exception | None = None
    try:
        rows = await _search_via_serpapi(
            query=clean_query,
            timeout_seconds=timeout_seconds,
            retries=retries,
            base_delay=base_delay,
            proxy_url=proxy,
        )
        logger.info("serp success | provider=serpapi query=%s results=%s", clean_query, len(rows))
        if not rows:
            logger.warning("[EMPTY RESPONSE] serp_serpapi")
        return rows
    except Exception as exc:  # noqa: BLE001
        serpapi_error = exc
        _ = urlencode({"q": clean_query})
        logger.warning("[SOURCE FAILED] serp_serpapi reason=%s", _failure_reason(exc))

    duck_error: Exception | None = None
    try:
        rows = await _search_via_duckduckgo(
            query=clean_query,
            timeout_seconds=timeout_seconds,
            retries=retries,
            base_delay=base_delay,
            proxy_url=proxy,
        )
        logger.info("serp success | provider=duckduckgo query=%s results=%s", clean_query, len(rows))
        if not rows:
            logger.warning("[EMPTY RESPONSE] serp_duckduckgo")
        return rows
    except Exception as exc:  # noqa: BLE001
        duck_error = exc
        logger.warning("[SOURCE FAILED] serp_duckduckgo reason=%s", _failure_reason(exc))

    if serper_error is not None or serpapi_error is not None or duck_error is not None:
        raise RuntimeError(
            f"serp providers failed | serper={_failure_reason(serper_error) if serper_error else 'none'} "
            f"serpapi={_failure_reason(serpapi_error) if serpapi_error else 'none'} "
            f"duckduckgo={_failure_reason(duck_error) if duck_error else 'none'}"
        )

    logger.warning("[EMPTY RESPONSE] serp")
    return []
