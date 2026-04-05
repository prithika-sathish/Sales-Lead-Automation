from __future__ import annotations

import json
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import requests

from core.merge_engine import extract_domain

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}")
ASSET_TLDS = {"png", "jpg", "jpeg", "webp", "gif", "svg", "css", "js", "ico", "woff", "woff2"}


def _parse_with_email_addresses(text: str) -> list[str]:
    script = Path("scripts/extract_emails.js")
    if not script.exists():
        return []

    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, suffix=".txt") as tmp:
        tmp.write(text)
        tmp_path = tmp.name

    try:
        proc = subprocess.run(
            ["node", str(script), tmp_path],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        raw = proc.stdout.strip() or "[]"
        parsed = json.loads(raw)
        if not isinstance(parsed, list):
            return []
        return [str(item).strip().lower() for item in parsed if str(item).strip()]
    except Exception:
        return []
    finally:
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except Exception:
            pass


def _confidence_for_emails(emails: list[str], domain: str) -> str:
    if not emails:
        return "low"
    if any(email.endswith(f"@{domain}") for email in emails if domain):
        return "high"
    return "medium"


def _is_plausible_email(email: str) -> bool:
    if "@" not in email:
        return False
    local, domain = email.rsplit("@", 1)
    if not local or not domain or "." not in domain:
        return False
    tld = domain.rsplit(".", 1)[-1].lower()
    if tld in ASSET_TLDS:
        return False
    return True


def enrich_domain(domain_or_url: str) -> dict[str, Any]:
    domain = extract_domain(domain_or_url)
    if not domain:
        return {"emails": [], "confidence": "low"}

    # Deterministic extraction from company page content.
    # External repo `email-addresses` can be added as a parser backend later.
    target_url = f"https://{domain}"
    try:
        response = requests.get(
            target_url,
            timeout=(2.5, 3.0),
            stream=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; LeadPipeline/1.0)"},
        )
        response.raise_for_status()

        chunks: list[bytes] = []
        size = 0
        for chunk in response.iter_content(chunk_size=16384):
            if not chunk:
                continue
            chunks.append(chunk)
            size += len(chunk)
            if size >= 220000:
                break
        text = b"".join(chunks).decode("utf-8", errors="ignore")
    except Exception:
        return {"emails": [], "confidence": "low"}

    # Primary extraction with proven email-addresses parser package.
    emails = _parse_with_email_addresses(text)
    if not emails:
        # Deterministic fallback to regex if node parser unavailable.
        emails = sorted(set(EMAIL_RE.findall(text)))

    filtered = [e for e in emails if not e.endswith("@example.com") and _is_plausible_email(e)]

    return {
        "emails": filtered,
        "confidence": _confidence_for_emails(filtered, domain),
    }
