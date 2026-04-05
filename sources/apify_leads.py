from __future__ import annotations

import json
import logging
import os
import time
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


if callable(load_dotenv):
    load_dotenv()


logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(message)s")


_NON_COMPANY_HOSTS = {
    "github.com",
    "linkedin.com",
    "x.com",
    "twitter.com",
    "facebook.com",
    "instagram.com",
    "youtube.com",
    "medium.com",
    "substack.com",
    "wordpress.com",
    "blogspot.com",
    "wikipedia.org",
    "crunchbase.com",
    "g2.com",
    "capterra.com",
    "getapp.com",
    "angel.co",
    "wellfound.com",
    "reddit.com",
    "quora.com",
}


def _to_tags(row: dict[str, Any]) -> list[str]:
    tags = row.get("tags") or row.get("labels") or []
    if isinstance(tags, list):
        return [str(tag).strip() for tag in tags if str(tag).strip()]
    if isinstance(tags, str):
        return [part.strip() for part in tags.split(",") if part.strip()]
    return []


def _host_from_url(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = f"https://{raw}"
    try:
        host = urlparse(raw).netloc.lower()
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def _is_non_company_host(url: str) -> bool:
    host = _host_from_url(url)
    if not host:
        return False
    return any(host == blocked or host.endswith(f".{blocked}") for blocked in _NON_COMPANY_HOSTS)


def normalize_apify_item(row: dict[str, Any]) -> dict[str, Any]:
    # For individual profiles, use name or username
    name = str(
        row.get("name")
        or row.get("username")
        or row.get("companyName")
        or row.get("title")
        or row.get("businessName")
        or ""
    ).strip()

    # Try to extract company from multiple fields
    company = str(
        row.get("company")
        or row.get("companyName")
        or row.get("businessName")
        or ""
    ).strip()

    # Keep profile URLs separate; lead URL should be a real company website/domain.
    profile_url = str(row.get("profile_url") or row.get("url") or "").strip()
    website = str(row.get("website") or row.get("domain") or "").strip()
    url = website

    description = str(row.get("description") or row.get("bio") or row.get("snippet") or "").strip()
    platform = str(row.get("platform") or row.get("source") or "apify").strip()

    return {
        "name": company or name,
        "company": company,  # Now capture company if available
        "source": "apify",
        "platform": platform,
        "description": description,
        "url": url,
        "profile_url": profile_url,
        "email": str(row.get("email") or "").strip(),
        "lead_score": row.get("lead_score", 0),
        "tags": _to_tags(row),
        "timestamp": datetime.now(UTC).isoformat(),
    }



def run_apify_actor(input_payload: dict[str, Any]) -> list[dict[str, Any]]:
    token = (os.getenv("APIFY_TOKEN") or os.getenv("APIFY_API_TOKEN") or "").strip()
    actor = os.getenv("APIFY_LEADS_ACTOR", "kenny256/leads-generator").strip()

    if not token:
        logger.warning("apify_token_missing")
        return []

    actor_id = actor.replace("/", "~")
    run_url = f"https://api.apify.com/v2/acts/{actor_id}/runs"

    run_response = requests.post(
        run_url,
        params={"token": token, "waitForFinish": "0"},
        json=input_payload,
        timeout=(10, 30),
    )
    run_response.raise_for_status()

    run_payload = run_response.json()
    run_data = run_payload.get("data") if isinstance(run_payload, dict) else {}
    run_id = str((run_data or {}).get("id") or "").strip()

    if not run_id:
        raise RuntimeError("apify run started without run id")

    logger.info("apify_run_started %s", run_id)

    status_url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    deadline = time.time() + 600  # 10 minutes timeout for long-running queries
    status = ""
    final_data: dict[str, Any] = {}

    while time.time() < deadline:
        try:
            status_response = requests.get(
                status_url,
                params={"token": token},
                timeout=(10, 20),
            )
            status_response.raise_for_status()
        except requests.RequestException as exc:
            code = getattr(getattr(exc, "response", None), "status_code", None)
            if code in {429, 500, 502, 503, 504}:
                logger.warning("apify_status_transient_error status=%s", code)
                time.sleep(5)
                continue
            raise
        status_payload = status_response.json()
        status_data = status_payload.get("data") if isinstance(status_payload, dict) else {}
        final_data = status_data if isinstance(status_data, dict) else {}
        status = str(final_data.get("status") or "").upper()
        logger.info("apify_status %s", status)

        if status == "SUCCEEDED":
            break

        if status in {"FAILED", "ABORTED", "TIMED-OUT"}:
            raise RuntimeError(
                f"Apify run failed status={status} details={json.dumps(final_data, ensure_ascii=True)}"
            )

        time.sleep(4)

    if status != "SUCCEEDED":
        raise TimeoutError(f"Apify run timeout after 600s run_id={run_id} status={status}")

    dataset_id = str(final_data.get("defaultDatasetId") or "").strip()
    if not dataset_id:
        raise RuntimeError(f"Apify run succeeded but dataset id missing run_id={run_id}")

    logger.info("apify_run_completed %s", dataset_id)

    dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
    dataset_response = requests.get(
        dataset_url,
        params={"token": token, "clean": "true"},
        timeout=(10, 60),
    )
    dataset_response.raise_for_status()
    rows = dataset_response.json()
    if not isinstance(rows, list):
        raise RuntimeError("Apify dataset response is not a list")

    logger.info("apify_rows_fetched %s", len(rows))
    if not rows:
        logger.warning("Apify returned 0 rows — likely input issue, not infra issue")
    return rows


def fetch_apify_leads(limit: int = 200, input_payload: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    target_limit = max(1, min(limit, 1000))
    payload_input: dict[str, Any] = dict(input_payload or {})
    payload_input["limit"] = target_limit

    try:
        payload = run_apify_actor(payload_input)
        if not payload:
            logger.info("apify_retry_scheduled")
            time.sleep(7)
            payload = run_apify_actor(payload_input)
    except Exception as exc:
        logger.exception("apify_ingestion_failed error=%s", exc)
        return []

    if not isinstance(payload, list):
        logger.warning("apify_invalid_payload_type %s", type(payload).__name__)
        return []

    payload = payload[:target_limit]

    normalized: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        row = normalize_apify_item(item)
        # Accept only company-like rows with website/domain, not social/blog directory hosts.
        if row["name"] and row.get("url") and not _is_non_company_host(str(row.get("url") or "")):
            normalized.append(row)

    return normalized


if __name__ == "__main__":
    rows = fetch_apify_leads(limit=300)
    out = Path("output/apify_leads.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rows, indent=2, ensure_ascii=True), encoding="utf-8")
    print(json.dumps({"rows": len(rows), "output": str(out)}, ensure_ascii=True))
