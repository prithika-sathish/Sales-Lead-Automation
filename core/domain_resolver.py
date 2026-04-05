from __future__ import annotations

import os
import re
from urllib.parse import urlparse

import requests


_BLOCKED_DOMAINS = {
    "linkedin.com",
    "crunchbase.com",
    "g2.com",
    "clutch.co",
    "facebook.com",
    "twitter.com",
    "x.com",
    "instagram.com",
    "youtube.com",
    "medium.com",
    "substack.com",
    "wordpress.com",
    "blogspot.com",
    "reddit.com",
    "quora.com",
    "wikipedia.org",
    "capterra.com",
    "getapp.com",
    "angel.co",
    "wellfound.com",
    "builtin.com",
    "forbes.com",
    "techcrunch.com",
    "google.com",
    "googletagmanager.com",
    "gstatic.com",
    "googleapis.com",
    "gmpg.org",
    "producthunt.com",
    "indeed.com",
    "naukri.com",
    "tracxn.com",
    "subta.com",
    "pitchbook.com",
    "cbinsights.com",
    "getlatka.com",
    "dealroom.co",
}

_BLOCKED_HOST_PREFIXES = {
    "blog",
    "blogs",
    "careers",
    "jobs",
    "help",
    "support",
    "docs",
    "developer",
    "developers",
    "static",
    "cdn",
    "status",
}

_NOISY_NAME_TERMS = {
    "top",
    "best",
    "list",
    "directory",
    "companies in",
    "jobs",
    "employment",
    "hiring",
    "blog",
    "article",
    "guide",
    "explore",
    "sector",
    "social network",
    "subscription service companies",
    "subscription commerce sector",
    "vs",
    "manager",
    "engineer",
    "developer",
    "specialist",
    "analyst",
    "intern",
}

_DOMAIN_CACHE: dict[str, str | None] = {}


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def extract_domain(value: object) -> str | None:
    raw = _clean_text(value).lower()
    if not raw:
        return None
    if not raw.startswith(("http://", "https://")):
        raw = f"https://{raw}"
    try:
        host = urlparse(raw).netloc.lower()
    except Exception:
        return None
    if host.startswith("www."):
        host = host[4:]
    if "." not in host:
        return None
    return host


def is_blocked_domain(domain: str | None) -> bool:
    host = extract_domain(domain or "")
    if not host:
        return True
    if any(host == blocked or host.endswith(f".{blocked}") for blocked in _BLOCKED_DOMAINS):
        return True

    first_label = host.split(".")[0]
    if first_label in _BLOCKED_HOST_PREFIXES:
        return True

    blocked_tokens = ["directory", "marketplace", "listings", "blog", "news", "tracker", "analytics"]
    return any(token in host for token in blocked_tokens)


def is_plausible_company_name(name: str) -> bool:
    value = _clean_text(name)
    if not value:
        return False
    lowered = value.lower()
    if lowered in {"saas", "fintech", "software", "platform", "startups", "companies"}:
        return False
    if any(term in lowered for term in _NOISY_NAME_TERMS):
        return False
    if any(char in value for char in [":", "|", "?", "(", ")", "/"]):
        return False
    words = value.split()
    if len(words) > 5:
        return False
    if re.search(r"\b20\d{2}\b", lowered):
        return False
    return True


def is_directory_like_url(url: str) -> bool:
    raw = _clean_text(url).lower()
    if not raw:
        return False
    return any(token in raw for token in ["/blog", "/blogs", "/article", "/articles", "/list", "/lists", "/top-"])


def _domain_to_name(domain: str) -> str:
    root = str(domain or "").split(".")[0]
    return root.replace("-", " ").replace("_", " ").title().strip() or domain


def extract_companies_from_directory_url(url: str, *, max_results: int = 30) -> list[dict[str, str]]:
    raw_url = _clean_text(url)
    if not raw_url:
        return []

    try:
        response = requests.get(raw_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=(10, 25))
        response.raise_for_status()
        html = response.text
    except Exception:
        return []

    links = re.findall(r'href=["\'](https?://[^"\']+)["\']', html, flags=re.I)
    page_domain = extract_domain(raw_url) or ""

    output: list[dict[str, str]] = []
    seen: set[str] = set()
    for link in links:
        domain = extract_domain(link)
        if not domain:
            continue
        if domain == page_domain:
            continue
        if is_blocked_domain(domain):
            continue
        if domain in seen:
            continue
        seen.add(domain)
        output.append({"name": _domain_to_name(domain), "domain": domain})
        if len(output) >= max(5, min(max_results, 50)):
            break
    return output


def _search_serper(query: str) -> list[str]:
    key = (os.getenv("SERPER_API_KEY") or "").strip()
    if not key:
        return []
    try:
        response = requests.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": key, "Content-Type": "application/json"},
            json={"q": query, "num": 6},
            timeout=(8, 18),
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return []

    urls: list[str] = []
    for row in payload.get("organic") or []:
        link = row.get("link")
        if isinstance(link, str) and link.strip():
            urls.append(link.strip())
    return urls


def _search_duckduckgo(query: str) -> list[str]:
    try:
        response = requests.get(
            "https://duckduckgo.com/html/",
            params={"q": query},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=(8, 18),
        )
        response.raise_for_status()
    except Exception:
        return []

    html = response.text
    links = re.findall(r'href="(https?://[^"]+)"', html)
    return [link for link in links if "duckduckgo.com" not in link]


def resolve_company_domain(name: str) -> str | None:
    company = _clean_text(name)
    if not company:
        return None

    cache_key = company.lower()
    if cache_key in _DOMAIN_CACHE:
        return _DOMAIN_CACHE[cache_key]

    query = f"{company} official website"
    candidates = _search_serper(query)
    if not candidates:
        candidates = _search_duckduckgo(query)

    for url in candidates:
        domain = extract_domain(url)
        if not domain or is_blocked_domain(domain):
            continue
        _DOMAIN_CACHE[cache_key] = domain
        return domain

    _DOMAIN_CACHE[cache_key] = None
    return None
