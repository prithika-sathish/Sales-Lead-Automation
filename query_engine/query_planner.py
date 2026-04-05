from __future__ import annotations

import re

from query_engine.llm_expander import expand_segments_with_llm
from query_engine.md_parser import parse_markdown_file
from query_engine.models import QueryConfig
from query_engine.models import QueryItem
from query_engine.models import QueryMode
from query_engine.models import StructuredContext

BANNED_TERMS = {"companies", "company", "list", "top", "directory", "examples", "example"}
WELL_KNOWN_COMPETITORS = {"stripe", "chargebee", "paddle"}

MAX_WORDS = 6
MIN_WORDS = 3
MAX_BEHAVIORAL_PER_SEGMENT = 2
MAX_SAAS_RATIO = 0.60

REGION_ALIASES = {
    "united states": "usa",
    "southeast asia": "sea",
}

REGION_ORDER = ["usa", "europe", "sea", "india"]

INDUSTRY_ALIASES = {
    "ai ml": ["ai startups", "machine learning startups"],
    "e learning": ["edtech startups", "learning platforms"],
    "media content": ["content startups", "media startups"],
    "marketplaces recurring": ["marketplaces", "subscription marketplaces"],
    "cloud infrastructure": ["cloud startups", "devops startups"],
    "developer tools": ["developer tools", "devtools startups"],
    "fintech": ["fintech startups"],
    "saas": ["saas startups"],
}


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalize_query(query: str) -> str:
    text = _clean_text(query).lower()
    text = text.replace("/", " ")
    text = text.replace("-", " ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return " ".join(text.split())


def _normalize_phrase(value: str) -> str:
    text = _normalize_query(value)
    text = re.sub(r"\b(companies|company|tools?|platforms?)\b$", "", text).strip()
    return text


def _compact_phrase(value: str, *, max_tokens: int = 3) -> str:
    text = _normalize_phrase(value)
    if not text:
        return ""

    weak = {
        "based",
        "digital",
        "business",
        "businesses",
        "scaling",
        "expanding",
        "internationally",
        "complexity",
        "with",
        "and",
    }
    tokens = [t for t in text.split() if t not in weak]
    if not tokens:
        tokens = text.split()
    return " ".join(tokens[:max_tokens])


def _ordered_regions(regions: list[str]) -> list[str]:
    clean = [_sanitize_region(region) for region in regions if _sanitize_region(region)]
    ordered: list[str] = []
    for region in REGION_ORDER:
        if region in clean and region not in ordered:
            ordered.append(region)
    for region in clean:
        if region not in ordered:
            ordered.append(region)
    return ordered or REGION_ORDER[:]


def _query_region(query: str) -> str:
    q = _normalize_query(query)
    if "india" in q:
        return "india"
    if "europe" in q or "eu" in q:
        return "europe"
    if "sea" in q or "southeast asia" in q:
        return "sea"
    if "usa" in q or "united states" in q or "us " in f"{q} ":
        return "usa"
    return ""


def _industry_variants(industry: str) -> list[str]:
    base = _compact_phrase(industry, max_tokens=2)
    if not base:
        return []
    return INDUSTRY_ALIASES.get(base, [f"{base} startups"])


def _word_count(query: str) -> int:
    return len(_normalize_query(query).split())


def _contains_banned(query: str) -> bool:
    q = _normalize_query(query)
    return any(re.search(rf"\b{re.escape(term)}\b", q) for term in BANNED_TERMS)


def _stem(token: str) -> str:
    t = token.lower()
    for suffix in ("ing", "ers", "er", "ed", "es", "s"):
        if len(t) > 4 and t.endswith(suffix):
            return t[: -len(suffix)]
    return t


def _token_signature(query: str) -> set[str]:
    stop = {"in", "for", "with", "and", "the", "a", "an", "to"}
    tokens = [_stem(t) for t in _normalize_query(query).split()]
    return {t for t in tokens if t and t not in stop}


def _near_duplicate(a: str, b: str) -> bool:
    na = _normalize_query(a)
    nb = _normalize_query(b)
    if na == nb:
        return True

    ta = _token_signature(a)
    tb = _token_signature(b)
    if not ta or not tb:
        return False

    overlap = len(ta & tb) / max(len(ta), len(tb))
    if overlap > 0.70:
        return True

    # Same root keywords after removing weak modifiers.
    weak = {"saas", "usa", "india", "europe", "sea"}
    ra = {t for t in ta if t not in weak}
    rb = {t for t in tb if t not in weak}
    return bool(ra) and ra == rb


def _shorten_query(query: str) -> str:
    q = _normalize_query(query)
    q = re.sub(r"\b(based)\b", "based", q)

    # Trim filler words first.
    fillers = {"software", "digital", "business", "businesses", "models", "model"}
    tokens = [t for t in q.split() if t not in fillers]

    if len(tokens) > MAX_WORDS:
        tokens = tokens[:MAX_WORDS]

    return " ".join(tokens)


def _ensure_length(query: str) -> str:
    q = _shorten_query(query)
    wc = _word_count(q)
    if wc > MAX_WORDS:
        q = " ".join(_normalize_query(q).split()[:MAX_WORDS])
    return q


def _sanitize_region(region: str) -> str:
    r = _normalize_query(region)
    return REGION_ALIASES.get(r, r)


def _alt_without_saas(text: str) -> str:
    # Keep wording natural while reducing repeated "saas" usage.
    replacements = [
        (r"\bsaas startups\b", "startups"),
        (r"\bsaas platforms\b", "platforms"),
        (r"\bsaas tools\b", "tools"),
        (r"\bsaas products\b", "products"),
        (r"\bsaas\b", "startups"),
    ]
    out = text
    for pattern, repl in replacements:
        out = re.sub(pattern, repl, out)
    return " ".join(out.split())


def _add(rows: list[QueryItem], query: str, qtype: str, priority: int, segment: str, source: str = "md") -> None:
    q = _ensure_length(query)
    parts: list[str] = []
    for token in q.split():
        if not parts or parts[-1] != token:
            parts.append(token)
    q = " ".join(parts)
    if not q:
        return
    if _contains_banned(q):
        return
    wc = _word_count(q)
    if wc < MIN_WORDS or wc > MAX_WORDS:
        return

    rows.append(QueryItem(query=q, source=source, segment=segment, priority=priority, query_type=qtype))


def _icp_queries(context: StructuredContext) -> list[QueryItem]:
    rows: list[QueryItem] = []
    seed_bank = [
        "b2b saas startups billing",
        "subscription saas startups",
        "usage based saas startups",
        "mid sized saas startups",
        "saas billing startups",
        "saas pricing startups",
        "fintech billing startups",
        "recurring revenue startups",
        "subscription billing startups",
        "usage based pricing startups",
        "b2b pricing startups",
        "saas revenue startups",
    ]

    for seed in seed_bank:
        _add(rows, seed, "icp", 5 if "billing" in seed or "usage" in seed else 4, "icp_seed")

    for icp in context.core_icp[:6]:
        base = _compact_phrase(icp, max_tokens=3)
        if not base:
            continue
        if any(term in base for term in ("billing", "pricing", "subscription", "usage", "revenue")):
            _add(rows, f"{base} startups", "icp", 4, base)
            _add(rows, f"{base} saas", "icp", 4, base)
    return rows


def _industry_region_queries(context: StructuredContext) -> list[QueryItem]:
    rows: list[QueryItem] = []
    regions = _ordered_regions(context.regions[:6] if context.regions else REGION_ORDER)
    for industry in context.industries[:8]:
        for variant in _industry_variants(industry)[:2]:
            for region in regions:
                _add(rows, f"{variant} {region}", "industry", 4, variant)
    return rows


def _behavioral_queries(context: StructuredContext) -> list[QueryItem]:
    rows: list[QueryItem] = []

    intents = {
        "hiring": [
            "saas startups hiring engineers",
            "startups hiring engineers",
        ],
        "tools": [
            "startups using stripe",
            "saas using stripe billing",
        ],
        "pricing": [
            "usage based pricing startups",
            "subscription pricing startups",
        ],
        "billing": [
            "saas billing complexity",
            "invoice automation startups",
        ],
    }

    for intent, queries in intents.items():
        for query in queries[:MAX_BEHAVIORAL_PER_SEGMENT]:
            _add(rows, query, "behavioral", 5 if intent == "hiring" else 4, intent, source="behavioral")

    # Add one revenue-oriented query to keep problem coverage broad without duplication.
    _add(rows, "recurring revenue startups", "behavioral", 4, "revenue", source="behavioral")
    _add(rows, "revenue tracking startups", "behavioral", 4, "revenue", source="behavioral")
    _add(rows, "multi currency billing startups", "behavioral", 4, "billing", source="behavioral")
    _add(rows, "billing workflow startups", "behavioral", 4, "billing", source="behavioral")
    _add(rows, "payment processing startups", "behavioral", 4, "tools", source="behavioral")
    _add(rows, "revenue ops startups", "behavioral", 4, "revenue", source="behavioral")

    return rows


def _competitor_queries(context: StructuredContext) -> list[QueryItem]:
    rows: list[QueryItem] = []
    seen: set[str] = set()
    for name in context.known_companies:
        clean = _normalize_query(name)
        if clean in WELL_KNOWN_COMPETITORS and clean not in seen:
            _add(rows, f"{clean} competitors saas", "competitor", 5, "competitor")
            seen.add(clean)

    # Always keep focused competitor set available.
    for comp in ("stripe", "chargebee", "paddle"):
        if comp not in seen:
            _add(rows, f"{comp} competitors saas", "competitor", 5, "competitor")
            seen.add(comp)
    return rows


def _llm_queries(context: StructuredContext, cfg: QueryConfig) -> list[QueryItem]:
    rows: list[QueryItem] = []
    segments = expand_segments_with_llm(
        context,
        use_llm=bool(cfg.use_llm),
        max_segments=max(4, min(int(cfg.max_segments), 10)),
    )
    for seg in segments[:8]:
        seg_name = _normalize_phrase(seg.segment_name)
        if not seg_name:
            continue
        for kw in seg.keywords[:2]:
            k = _normalize_phrase(kw)
            if not k:
                continue
            # Keep LLM support short and problem-aware; treat as ICP-style support.
            _add(rows, f"{k} startups", "icp", 3, seg_name, source="llm")
    return rows


def _enforce_saas_ratio(selected: list[QueryItem]) -> list[QueryItem]:
    if not selected:
        return selected

    total = len(selected)
    max_saas = max(1, int(total * MAX_SAAS_RATIO))
    saas_count = 0
    adjusted: list[QueryItem] = []

    for item in selected:
        q = _normalize_query(item.query)
        has_saas = "saas" in q.split()

        if has_saas and saas_count >= max_saas:
            alt = _alt_without_saas(item.query)
            alt_wc = _word_count(alt)
            if alt and MIN_WORDS <= alt_wc <= MAX_WORDS and not _contains_banned(alt):
                item = QueryItem(query=alt, source=item.source, segment=item.segment, priority=item.priority, query_type=item.query_type)
                has_saas = False

        if has_saas:
            saas_count += 1
        adjusted.append(item)

    return adjusted


def _select_balanced(items: list[QueryItem], max_queries: int) -> list[QueryItem]:
    ordered = sorted(items, key=lambda x: (x.priority, -_word_count(x.query)), reverse=True)

    # 30% ICP, 25% industry, 30% behavioral, 10-15% competitor.
    targets = {
        "icp": max(3, int(max_queries * 0.30)),
        "industry": max(3, int(max_queries * 0.25)),
        "behavioral": max(3, int(max_queries * 0.30)),
        "competitor": max(2, int(max_queries * 0.12)),
    }

    picked: list[QueryItem] = []
    seen_exact: set[str] = set()
    region_counts: dict[str, int] = {}
    region_total = 0
    type_counts: dict[str, int] = {}

    # Pass 1: reserve slots by type (exact dedupe only).
    for qtype in ("icp", "industry", "behavioral", "competitor"):
        needed = targets.get(qtype, 0)
        if needed <= 0:
            continue
        for item in ordered:
            if len(picked) >= max_queries or needed <= 0:
                break
            if item.query_type != qtype:
                continue
            q = _normalize_query(item.query)
            if not q or _contains_banned(q):
                continue
            if q in seen_exact:
                continue
            region = _query_region(q)
            if region == "india" and region_total > 0:
                if region_counts.get("india", 0) >= max(2, int((region_total + 1) * 0.40)):
                    continue
            if type_counts.get(qtype, 0) >= targets.get(qtype, max_queries):
                continue
            seen_exact.add(q)
            picked.append(QueryItem(query=q, source=item.source, segment=item.segment, priority=item.priority, query_type=qtype))
            type_counts[qtype] = type_counts.get(qtype, 0) + 1
            if region:
                region_counts[region] = region_counts.get(region, 0) + 1
                region_total += 1
            needed -= 1

    # Pass 2: fill remaining slots with global near-dedupe.
    if len(picked) < max_queries:
        for item in ordered:
            if len(picked) >= max_queries:
                break
            q = _normalize_query(item.query)
            if not q or _contains_banned(q):
                continue
            if q in seen_exact:
                continue
            region = _query_region(q)
            if region == "india" and region_total > 0:
                if region_counts.get("india", 0) >= max(2, int((region_total + 1) * 0.40)):
                    continue
            if any(_near_duplicate(q, p.query) for p in picked):
                continue
            if type_counts.get(item.query_type, 0) >= targets.get(item.query_type, max_queries):
                continue
            seen_exact.add(q)
            picked.append(QueryItem(query=q, source=item.source, segment=item.segment, priority=item.priority, query_type=item.query_type))
            type_counts[item.query_type] = type_counts.get(item.query_type, 0) + 1
            if region:
                region_counts[region] = region_counts.get(region, 0) + 1
                region_total += 1

    return _enforce_saas_ratio(picked)


def generate_queries(
    markdown_path: str,
    *,
    mode: QueryMode = "hybrid",
    max_queries: int = 96,
    config: QueryConfig | None = None,
) -> list[dict[str, object]]:
    cfg = config or QueryConfig(mode=mode, max_queries=max_queries)
    context = parse_markdown_file(markdown_path)

    candidates: list[QueryItem] = []
    candidates.extend(_icp_queries(context))
    candidates.extend(_industry_region_queries(context))
    candidates.extend(_behavioral_queries(context))
    candidates.extend(_competitor_queries(context))

    if str(cfg.mode).lower() == "hybrid":
        candidates.extend(_llm_queries(context, cfg))

    # Keep the final query set curated and high precision.
    effective_max = max(40, min(int(cfg.max_queries), 50))
    selected = _select_balanced(candidates, effective_max)

    final: list[dict[str, object]] = []
    seen: set[str] = set()
    for item in selected:
        q = _normalize_query(item.query)
        if q in seen:
            continue
        if _word_count(q) > MAX_WORDS or _word_count(q) < MIN_WORDS:
            continue
        seen.add(q)
        final.append({"query": q, "type": item.query_type, "priority": int(item.priority)})
        if len(final) >= effective_max:
            break

    return final
