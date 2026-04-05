from __future__ import annotations

import os
import time
from html import unescape
from urllib.parse import quote_plus
from urllib.parse import urlparse
from typing import Any

import requests

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


if callable(load_dotenv):
    load_dotenv()


_APIFY_BASE = "https://api.apify.com/v2"
_DISABLED_ACTORS: set[str] = set()
FAILED_SOURCES: dict[str, int] = {}


def _derive_query(input_payload: dict[str, Any]) -> str:
    for key in ("query", "search", "searchTerm", "queries", "q"):
        value = input_payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, list) and value:
            joined = " ".join(str(item).strip() for item in value if str(item).strip())
            if joined:
                return joined
    for value in input_payload.values():
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, list):
            joined = " ".join(str(item).strip() for item in value if str(item).strip())
            if joined:
                return joined
    return "company websites"


def _token() -> str:
    return (os.getenv("APIFY_API_TOKEN") or os.getenv("APIFY_TOKEN") or "").strip()


def _request_with_retries(method: str, url: str, *, retries: int = 1, backoff_base: float = 1.25, **kwargs: Any) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.request(method=method, url=url, timeout=(5, 10), **kwargs)
            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt == retries:
                    response.raise_for_status()
                time.sleep(backoff_base ** attempt)
                continue
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt == retries:
                raise
            time.sleep(backoff_base ** attempt)
    if last_error is not None:
        raise last_error
    raise RuntimeError("request failed without explicit error")


def serper_search(query: str) -> list[dict[str, Any]]:
    key = (os.getenv("SERPER_API_KEY") or "").strip()
    if not key:
        return []
    try:
        response = requests.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": key, "Content-Type": "application/json"},
            json={"q": query, "num": 10},
            timeout=(10, 30),
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return []

    output: list[dict[str, Any]] = []
    for item in payload.get("organic") or []:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        link = str(item.get("link") or "").strip()
        snippet = str(item.get("snippet") or "").strip()
        if title or link:
            output.append(
                {
                    "title": title,
                    "snippet": snippet,
                    "link": link,
                    "url": link,
                    "source_transport": "serper",
                }
            )
    return output


def google_custom_search(query: str) -> list[dict[str, Any]]:
    api_key = (os.getenv("GOOGLE_API_KEY") or "").strip()
    cx = (os.getenv("GOOGLE_CX") or "").strip()
    if not api_key or not cx:
        return []

    try:
        response = requests.get(
            "https://www.googleapis.com/customsearch/v1",
            params={"q": f"{query} saas company", "key": api_key, "cx": cx},
            timeout=(5, 10),
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return []

    out: list[dict[str, Any]] = []
    for item in payload.get("items") or []:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        link = str(item.get("link") or "").strip()
        snippet = str(item.get("snippet") or "").strip()
        if not (title or link):
            continue
        out.append(
            {
                "name": title,
                "title": title,
                "url": link,
                "link": link,
                "snippet": snippet,
                "source_type": "search",
                "source_transport": "google_cse",
            }
        )
    return out


def tavily_search(query: str) -> list[dict[str, Any]]:
    key = (os.getenv("TAVILY_API_KEY") or "").strip()
    if not key:
        return []
    try:
        response = requests.post(
            "https://api.tavily.com/search",
            headers={"Content-Type": "application/json"},
            json={"api_key": key, "query": query, "max_results": 10, "include_answer": False},
            timeout=(10, 30),
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return []

    output: list[dict[str, Any]] = []
    for item in payload.get("results") or []:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or "").strip()
        content = str(item.get("content") or "").strip()
        if title or url:
            output.append(
                {
                    "title": title,
                    "content": content,
                    "url": url,
                    "source_transport": "tavily",
                }
            )
    return output


def basic_http_search(query: str) -> list[dict[str, Any]]:
    try:
        response = requests.get(
            f"https://duckduckgo.com/html/?q={quote_plus(query)}",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=(5, 10),
        )
        response.raise_for_status()
        html = response.text
    except Exception:
        return []

    output: list[dict[str, Any]] = []
    anchors = requests.utils.re.findall(r'<a[^>]+class="[^"]*result__a[^"]*"[^>]+href="([^"]+)"[^>]*>(.*?)</a>', html, flags=requests.utils.re.IGNORECASE | requests.utils.re.DOTALL) if False else []
    if not anchors:
        anchors = []
        for match in __import__("re").finditer(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', html, flags=__import__("re").IGNORECASE | __import__("re").DOTALL):
            link = unescape(match.group(1))
            title = __import__("re").sub(r"<[^>]+>", " ", unescape(match.group(2)))
            title = " ".join(title.split())
            if title or link:
                anchors.append((link, title))
            if len(anchors) >= 10:
                break

    for link, title in anchors[:10]:
        if not link:
            continue
        parsed = urlparse(link)
        if parsed.scheme not in {"http", "https"}:
            continue
        output.append(
            {
                "title": title,
                "snippet": "",
                "link": link,
                "url": link,
                "source_transport": "fallback",
            }
        )
    return output


def _fallback_records(query: str) -> list[dict[str, Any]]:
    records = google_custom_search(query)
    if records:
        return records
    records = serper_search(query)
    if records:
        return records
    records = tavily_search(query)
    if records:
        return records
    return basic_http_search(query)


def run_actor_with_fallback(actor_id: str, query: str, *, input_payload: dict[str, Any] | None = None, timeout_seconds: int = 420) -> list[dict[str, Any]]:
    payload = dict(input_payload or {})
    payload.setdefault("query", query)
    try:
        rows = _run_apify_actor_strict(actor_id, payload, timeout_seconds=min(timeout_seconds, 10))
        if rows:
            for row in rows:
                if isinstance(row, dict):
                    row.setdefault("source_transport", "apify")
            return rows
    except Exception:
        pass

    return _fallback_records(query)


def _run_apify_actor_strict(actor: str, input_payload: dict[str, Any], *, timeout_seconds: int = 420) -> list[dict[str, Any]]:
    if actor in _DISABLED_ACTORS:
        raise RuntimeError(f"apify actor disabled: {actor}")

    token = _token()
    if not token:
        raise RuntimeError("APIFY_API_TOKEN is not set")

    actor_id = actor.replace("/", "~")
    run_url = f"{_APIFY_BASE}/acts/{actor_id}/runs"

    try:
        run_response = _request_with_retries(
            "POST",
            run_url,
            params={"token": token, "waitForFinish": "0"},
            json=input_payload,
        )
    except requests.HTTPError as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        if status_code == 404:
            _DISABLED_ACTORS.add(actor)
        raise
    run_payload = run_response.json()
    run_id = str(((run_payload.get("data") or {}) if isinstance(run_payload, dict) else {}).get("id") or "").strip()
    if not run_id:
        raise RuntimeError(f"apify actor {actor} did not return run id")

    status_url = f"{_APIFY_BASE}/actor-runs/{run_id}"
    deadline = time.time() + max(60, int(timeout_seconds))
    status = ""
    run_data: dict[str, Any] = {}

    while time.time() < deadline:
        status_response = _request_with_retries("GET", status_url, params={"token": token})
        payload = status_response.json()
        run_data = payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else {}
        status = str(run_data.get("status") or "").upper()
        if status == "SUCCEEDED":
            break
        if status in {"FAILED", "ABORTED", "TIMED-OUT"}:
            raise RuntimeError(f"apify actor {actor} failed with status={status}")
        time.sleep(4)

    if status != "SUCCEEDED":
        raise TimeoutError(f"apify actor {actor} timed out")

    dataset_id = str(run_data.get("defaultDatasetId") or "").strip()
    if not dataset_id:
        raise RuntimeError(f"apify actor {actor} finished without dataset id")

    dataset_url = f"{_APIFY_BASE}/datasets/{dataset_id}/items"
    dataset_response = _request_with_retries(
        "GET",
        dataset_url,
        params={"token": token, "clean": "true", "format": "json"},
    )
    rows = dataset_response.json()
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def run_apify_actor(actor: str, input_payload: dict[str, Any], *, timeout_seconds: int = 420) -> list[dict[str, Any]]:
    query = _derive_query(input_payload)
    try:
        rows = _run_apify_actor_strict(actor, input_payload, timeout_seconds=min(timeout_seconds, 10))
        if rows:
            for row in rows:
                if isinstance(row, dict):
                    row.setdefault("source_transport", "apify")
            return rows
    except Exception:
        pass
    return _fallback_records(query)


def should_skip(source_name: str) -> bool:
    return FAILED_SOURCES.get(str(source_name or "").lower(), 0) >= 3


def mark_failed(source_name: str) -> None:
    key = str(source_name or "").lower()
    FAILED_SOURCES[key] = FAILED_SOURCES.get(key, 0) + 1


def mark_success(source_name: str) -> None:
    FAILED_SOURCES.pop(str(source_name or "").lower(), None)


def search_fallback(query: str) -> list[dict[str, Any]]:
    return _fallback_records(query)


def normalize_min_schema(data: list[dict[str, Any]], source: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or item.get("title") or "").strip()
        if not name:
            continue
        url = str(item.get("url") or item.get("link") or item.get("website") or "").strip() or None
        context = str(item.get("snippet") or item.get("description") or item.get("content") or item.get("tagline") or "").strip()
        out.append(
            {
                "name": name,
                "url": url,
                "link": url,
                "website": url,
                "snippet": context,
                "description": context,
                "source": source,
                "source_type": str(item.get("source_type") or item.get("source_transport") or "search").strip(),
                "source_transport": str(item.get("source_transport") or source).strip().lower() or "search",
            }
        )
    return out
