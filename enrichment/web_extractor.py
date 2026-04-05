from __future__ import annotations

import logging
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalize_url(url: str) -> str:
    value = _clean_text(url)
    if not value:
        return ""
    parsed = urlparse(value)
    if parsed.scheme:
        return value
    return f"https://{value}"


def is_company_website(text: str) -> bool:
    text = _clean_text(text).lower()

    positive = [
        "product",
        "platform",
        "solutions",
        "customers",
        "pricing",
    ]
    negative = ["blog", "news", "article", "top 10", "guide"]

    score = sum(1 for p in positive if p in text)
    penalty = sum(1 for n in negative if n in text)

    return score >= 2 and penalty <= 1


def extract_company_info(url: str) -> dict:
    """Fetch and extract structured website content from a company homepage."""
    result = {
        "title": "",
        "description": "",
        "keywords": [],
        "visible_text": "",
        "text": "",
        "weak": True,
    }

    target_url = _normalize_url(url)
    if not target_url:
        return result

    try:
        response = requests.get(
            target_url,
            timeout=10,
            headers={"User-Agent": _USER_AGENT},
            allow_redirects=True,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("web extract failed | url=%s err=%s", target_url, exc)
        return result

    soup = BeautifulSoup(response.text, "html.parser")

    title_tag = soup.find("title")
    result["title"] = _clean_text(title_tag.get_text(" ", strip=True) if title_tag else "")

    description_tag = soup.find("meta", attrs={"name": "description"})
    result["description"] = _clean_text(description_tag.get("content") if description_tag else "")

    keywords_tag = soup.find("meta", attrs={"name": "keywords"})
    keywords_raw = _clean_text(keywords_tag.get("content") if keywords_tag else "")
    if keywords_raw:
        result["keywords"] = [item for item in (_clean_text(part) for part in keywords_raw.split(",")) if item]

    for node in soup(["script", "style", "noscript"]):
        node.decompose()

    visible_text = _clean_text(soup.get_text(" ", strip=True))
    result["text"] = " ".join(part for part in [result["title"], result["description"], visible_text] if part).strip()
    result["weak"] = not is_company_website(result["text"])

    result["visible_text"] = visible_text[:100000]
    return result
