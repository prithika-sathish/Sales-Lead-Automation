from __future__ import annotations

from collections import Counter
import json
import logging
import re
import urllib.parse
from datetime import datetime, timezone
from typing import Any
from urllib import error, request

from app.apify_client import fetch_apify_query


logger = logging.getLogger(__name__)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fetch_github_repos(company: str) -> list[dict[str, Any]]:
    q = urllib.parse.quote_plus(company)
    url = f"https://api.github.com/search/repositories?q={q}&sort=updated&order=desc&per_page=20"
    req = request.Request(url, headers={"Accept": "application/vnd.github+json", "User-Agent": "signal-intel"})

    try:
        with request.urlopen(req, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (error.HTTPError, error.URLError, TimeoutError):
        return []

    items = payload.get("items", [])
    if not isinstance(items, list):
        return []
    return [repo for repo in items if isinstance(repo, dict)]


def _norm_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _is_official_repo(company: str, repo: dict[str, Any]) -> bool:
    owner = repo.get("owner") if isinstance(repo.get("owner"), dict) else {}
    owner_login = str(owner.get("login") or "")
    owner_type = str(owner.get("type") or "")

    company_norm = _norm_name(company)
    owner_norm = _norm_name(owner_login)
    if not company_norm or not owner_norm:
        return False

    direct_match = company_norm == owner_norm or company_norm in owner_norm or owner_norm in company_norm
    if direct_match:
        return True
    return owner_type.lower() == "organization" and company_norm in owner_norm


def _recent_with_quality(repo: dict[str, Any], recent_days: int = 30) -> bool:
    stars = int(repo.get("stargazers_count") or 0)
    forks = int(repo.get("forks_count") or 0)
    pushed_at = str(repo.get("pushed_at") or "")
    if not pushed_at:
        return False

    try:
        pushed_dt = datetime.fromisoformat(pushed_at.replace("Z", "+00:00"))
    except ValueError:
        return False

    return (datetime.now(timezone.utc) - pushed_dt).days <= recent_days and (stars >= 20 or forks >= 5)


def _has_strong_intent_description(repo: dict[str, Any]) -> bool:
    description = str(repo.get("description") or "").lower()
    return any(token in description for token in ["integration", "sdk", "api", "migration", "plugin"])


def collect_github_signals(company: str) -> dict[str, Any]:
    signals: list[dict[str, Any]] = []
    raw_repos = _fetch_github_repos(company)
    raw_count = len(raw_repos)
    filtered_count = 0
    filter_reasons: Counter[str] = Counter()

    for repo in raw_repos:
        repo_name = str(repo.get("full_name") or repo.get("name") or "")
        if not repo_name:
            filter_reasons["missing_repo_name"] += 1
            continue

        official = _is_official_repo(company, repo)
        stars = int(repo.get("stargazers_count") or 0)
        forks = int(repo.get("forks_count") or 0)
        if stars == 0 and forks == 0 and not official:
            filter_reasons["zero_star_zero_fork"] += 1
            continue

        high_quality_recent = _recent_with_quality(repo, recent_days=30)
        strong_intent = _has_strong_intent_description(repo)

        if not (official or high_quality_recent or strong_intent):
            filter_reasons["not_relevant"] += 1
            continue

        filtered_count += 1
        quality_tier = "official" if official else ("high_quality_recent" if high_quality_recent else "strong_intent")

        updated_at = str(repo.get("updated_at") or "")
        pushed_at = str(repo.get("pushed_at") or "")
        signal_type = "repo_activity_spike" if high_quality_recent else "github_activity"
        strength_hint = 5 if quality_tier == "official" else (4 if quality_tier == "high_quality_recent" else 3)
        signals.append(
            {
                "type": signal_type,
                "raw_text": f"Relevant repository activity: {repo_name}",
                "metadata": {
                    "repo": repo_name,
                    "updated_at": updated_at,
                    "pushed_at": pushed_at,
                    "stars": stars,
                    "forks": forks,
                    "url": str(repo.get("html_url") or ""),
                    "github_quality_tier": quality_tier,
                    "strength_hint": strength_hint,
                },
                "source": "github",
            }
        )

    # Optional secondary sweep via Apify for references to GitHub activity.
    try:
        logger.info("github query | company=%s query=%s", company, f"{company} github integration sdk api migration plugin")
        items = fetch_apify_query(f"{company} github integration sdk api migration plugin", limit=5)
    except RuntimeError:
        items = []

    mention_raw = len(items)
    mention_filtered = 0

    for item in items:
        text = str(item.get("text") or item.get("description") or item.get("content") or "").strip()
        if not text:
            continue

        text_l = text.lower()
        if not any(token in text_l for token in ["integration", "sdk", "api", "migration", "plugin"]):
            filter_reasons["weak_mention"] += 1
            continue

        mention_filtered += 1
        signals.append(
            {
                "type": "github_mention",
                "raw_text": text,
                "metadata": {
                    "timestamp": str(item.get("timestamp") or item.get("date") or _iso_now()),
                    "url": str(item.get("url") or item.get("link") or ""),
                    "strength_hint": 3,
                },
                "source": "github",
            }
        )

    if filtered_count >= 3:
        signals.append(
            {
                "type": "dev_activity",
                "raw_text": "Multiple relevant GitHub activities indicate sustained developer momentum",
                "metadata": {
                    "repo_signal_count": filtered_count,
                    "strength_hint": 4,
                },
                "source": "github",
            }
        )

    debug = {
        "raw": raw_count + mention_raw,
        "filtered": filtered_count + mention_filtered,
        "filter_reasons": dict(filter_reasons),
    }
    logger.info("github debug | company=%s raw=%s filtered=%s", company, debug["raw"], debug["filtered"])

    return {"company": company, "signals": signals, "agent_debug": {"github": debug}}
