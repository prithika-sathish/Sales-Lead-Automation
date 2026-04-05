from __future__ import annotations

import re
from urllib.parse import urlparse


LIST_TITLE_PATTERNS = [
    r"\btop\s+\d+\b",
    r"\bbest\s+\d+\b",
    r"\b\d+\s+(?:best|top)\b",
    r"\btop\b",
    r"\bbest\b",
    r"\bjobs?\b",
    r"\blist\b",
    r"\bcompanies\b",
    r"\bstartups?\b",
    r"\blist of\b",
    r"\bstartups? to watch\b",
    r"\bfunded startups?\b",
    r"\bcompanies in\b",
    r"\bstartups in\b",
    r"\bdirectory\b",
    r"\breport\b",
    r"\broundup\b",
    r"\bguide\b",
    r"\barticle\b",
    r"\bblog\b",
    r"\bcomparison\b",
    r"\bplatforms?\b.*\blist\b",
]

DIRECTORY_DOMAINS = {
    "builtin.com",
    "crunchbase.com",
    "failory.com",
    "growthlist.co",
    "indianstartupnews.com",
    "seedtable.com",
    "techcrunch.com",
    "tracxn.com",
    "ycombinator.com",
    "topstartups.io",
    "producthunt.com",
    "linkedin.com",
    "medium.com",
    "substack.com",
    "substack.com",
    "forbes.com",
    "entrepreneur.com",
    "inc.com",
    "thehustle.co",
    "angel.co",
    "slashdot.org",
    "cutshort.io",
    "wellfound.com",
    "indeed.com",
    "glassdoor.com",
    "instahyre.com",
    "naukri.com",
    "monster.com",
    "wikipedia.org",
    "wikimedia.org",
}

ENTERPRISE_BLOCKLIST = {
    "stripe",
    "udemy",
    "ibm",
    "oracle",
    "microsoft",
    "google",
    "amazon",
    "meta",
    "salesforce",
    "adobe",
    "zoho",
    "razorpay",
    "freshworks",
    "servicenow",
    "okta",
    "atlassian",
    "shopify",
    "snowflake",
    "datadog",
    "zoom",
    "hubspot",
    "zendesk",
}


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def normalize_domain(value: object) -> str:
    text = _clean_text(value).lower()
    if not text:
        return ""
    if "//" in text:
        parsed = urlparse(text)
        text = parsed.netloc or text
    text = text.replace("www.", "")
    host = text.split("/")[0].strip()
    labels = [part for part in host.split(".") if part]
    common_subdomains = {"www", "m", "en", "blog", "help", "docs", "support", "app", "go", "news"}
    if len(labels) >= 3 and labels[0] in common_subdomains:
        return ".".join(labels[-2:])
    return host


def normalize_company_name_from_domain(value: object) -> str:
    domain = normalize_domain(value)
    if not domain:
        return ""
    labels = [part for part in domain.split(".") if part]
    if not labels:
        return ""
    subdomain_labels = {"www", "m", "en", "blog", "help", "docs", "support", "app", "go", "news"}
    if len(labels) >= 3 and labels[0] in subdomain_labels:
        label = labels[-2]
    else:
        label = labels[0]
    label = re.sub(r"[-_]+", " ", label).strip()
    if not label:
        return ""
    return " ".join(part[:1].upper() + part[1:] if part and not part.isupper() else part for part in label.split())


def canonical_company_name(company: object, domain: object) -> str:
    company_name = _clean_text(company)
    domain_name = normalize_company_name_from_domain(domain)
    if not company_name:
        return domain_name
    lowered = company_name.lower()
    if _looks_like_list_page(company_name):
        return domain_name
    if "|" in company_name:
        return domain_name or company_name
    if re.search(r"\b(company|companies|startup|startups|jobs?|list|directory|report|roundup|guide|article|blog|comparison)\b", lowered):
        return domain_name or company_name
    if len(company_name.split()) >= 3 and re.search(
        r"\b(solution|solutions|platform|platforms|software|saas|billing|subscription|subscriptions|payment|payments|recurring|management|automate|automation|service|services|association|foundation|institute|council|alliance|community|empowerment|network)\b",
        lowered,
    ):
        return domain_name or company_name
    return company_name


def _looks_like_list_page(text: str) -> bool:
    lowered = _clean_text(text).lower()
    if not lowered:
        return False
    return any(re.search(pattern, lowered) for pattern in LIST_TITLE_PATTERNS)


def _url_or_description_looks_like_non_company_entity(*, description: str, url: str) -> bool:
    description_text = _clean_text(description).lower()
    url_text = _clean_text(url).lower()
    if not description_text and not url_text:
        return False
    return bool(
        re.search(r"\b(job|jobs|hiring|careers|directory|report|roundup|guide|comparison|list)\b", description_text)
        or re.search(r"/(job|jobs|careers|directory|report|roundup|guide|list)(/|$)", url_text)
    )


def is_real_company_entity(*, company: str, domain: str, description: str = "", url: str = "") -> bool:
    company_name = canonical_company_name(company, domain)
    original_name = _clean_text(company)
    domain_name = normalize_domain(domain)
    description_text = _clean_text(description).lower()
    url_text = _clean_text(url).lower()

    if not company_name or not domain_name:
        return False

    if any(domain_name == blocked or domain_name.endswith(f".{blocked}") for blocked in DIRECTORY_DOMAINS):
        return False

    if domain_name.endswith(".org") and re.search(
        r"\b(association|foundation|institute|council|alliance|community|empowerment|network|society|coalition|consortium)\b",
        company_name.lower(),
    ):
        return False

    if domain_name.split(".")[0] in ENTERPRISE_BLOCKLIST:
        return False

    if _url_or_description_looks_like_non_company_entity(description=description_text, url=url_text):
        return False

    if _looks_like_list_page(description_text):
        return False

    if _looks_like_list_page(original_name) and original_name == company_name:
        return False

    if re.search(r"\b(top|best|list|report|directory|guide|roundup)\b", url_text):
        return False

    return True
