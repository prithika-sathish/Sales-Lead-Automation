from __future__ import annotations

import json
import os
import smtplib
import sys
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

# Adapter-level flag requested by user.
SEND_EMAILS = False

MAX_LEADS = 30
MIN_SCORE = 70
MAX_FOLLOWUPS = 2
FOLLOWUP_AFTER_DAYS = 3

ROOT = Path(__file__).resolve().parents[1]
SALESGPT_ROOT = ROOT / "Sales-Agent"
DEFAULT_INPUT = ROOT / "existing_pipeline_output.json"
FALLBACK_INPUT = ROOT / "output" / "full_pipeline_ranked_leads.json"
DEFAULT_MARKDOWN = ROOT / "sample_company.md"
GENERATED_EMAILS = ROOT / "generated_emails.json"
SENT_LOGS = ROOT / "sent_logs.json"
REPLIES_LOG = ROOT / "replies.json"

sys.path.insert(0, str(SALESGPT_ROOT))
try:
    from salesgpt.prompts import SALES_AGENT_TOOLS_PROMPT  # type: ignore
except Exception:
    SALES_AGENT_TOOLS_PROMPT = ""


@dataclass
class Lead:
    company_name: str
    domain: str
    why_it_matches: str
    score: float


@dataclass
class CompanyBrief:
    company_name: str
    product_description: str
    core_icp: str
    use_cases: str
    value_propositions: str


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _save_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def _ensure_input_file() -> Path:
    if DEFAULT_INPUT.exists():
        return DEFAULT_INPUT
    if FALLBACK_INPUT.exists():
        # Keep adapter input stable for future runs.
        DEFAULT_INPUT.write_text(FALLBACK_INPUT.read_text(encoding="utf-8"), encoding="utf-8")
        return DEFAULT_INPUT
    raise FileNotFoundError("No lead input found. Expected existing_pipeline_output.json or output/full_pipeline_ranked_leads.json")


def _load_company_brief(markdown_path: Path | None = None) -> CompanyBrief:
    path = markdown_path or DEFAULT_MARKDOWN
    if not path.exists():
        return CompanyBrief(company_name="", product_description="", core_icp="", use_cases="", value_propositions="")

    text = path.read_text(encoding="utf-8")

    def extract_section(header: str) -> list[str]:
        marker = f"## {header}"
        start = text.find(marker)
        if start < 0:
            return []
        start += len(marker)
        collected: list[str] = []
        for line in text[start:].splitlines():
            if line.startswith("## ") or line.startswith("---"):
                break
            cleaned = line.strip()
            if cleaned:
                collected.append(cleaned.lstrip("- ").strip())
        return collected

    def flatten(lines: list[str], *, limit: int | None = None) -> str:
        items = [line for line in lines if line and not line.lower().startswith("(note:")]
        if limit is not None:
            items = items[:limit]
        return "; ".join(items).strip()

    product_description = flatten(extract_section("PRODUCT_DESCRIPTION"), limit=1)
    core_icp = flatten(extract_section("CORE_ICP"), limit=3)
    use_cases = flatten(extract_section("USE_CASES"), limit=3)
    value_propositions = flatten(extract_section("VALUE_PROPOSITIONS"), limit=3)

    company_name = ""
    for line in text.splitlines():
        if line.strip().lower().startswith("# company:"):
            company_name = line.split(":", 1)[1].strip()
            break

    return CompanyBrief(
        company_name=company_name,
        product_description=product_description,
        core_icp=core_icp,
        use_cases=use_cases,
        value_propositions=value_propositions,
    )


def _compact_product_focus(product_description: str, company_name: str) -> str:
    text = str(product_description or "").strip().rstrip(".")
    lower = text.lower()
    prefix = f"{company_name.lower()} is a " if company_name else ""
    if prefix and lower.startswith(prefix):
        remainder = text[len(prefix):].strip()
        if " providing " in remainder:
            remainder = remainder.split(" providing ", 1)[1].strip()
        if ". it is " in remainder.lower():
            remainder = remainder.split(". It is", 1)[0].strip()
        return remainder
    if " providing " in lower:
        remainder = text.split(" providing ", 1)[1].strip()
        if ". it is " in remainder.lower():
            remainder = remainder.split(". It is", 1)[0].strip()
        return remainder
    return text


def _compact_claims(text: str) -> str:
    parts = [part.strip() for part in str(text or "").split(";") if part.strip()]
    if not parts:
        return ""
    normalized: list[str] = []
    for part in parts[:3]:
        cleaned = part.strip().rstrip(".")
        lowered = cleaned[0].lower() + cleaned[1:] if cleaned else cleaned
        lowered = lowered.replace("reduces ", "reduce ").replace("prevents ", "prevent ").replace("enables ", "enable ")
        normalized.append(lowered)
    if len(normalized) == 1:
        return normalized[0]
    if len(normalized) == 2:
        return f"{normalized[0]} and {normalized[1]}"
    return f"{', '.join(normalized[:-1])}, and {normalized[-1]}"


def _extract_domain(row: dict[str, Any]) -> str:
    domain = str(row.get("domain") or "").strip().lower()
    if domain:
        return domain
    website = str(row.get("website") or "").strip().lower()
    if not website:
        return ""
    website = website.replace("https://", "").replace("http://", "")
    return website.split("/")[0].strip()


def _to_lead(row: dict[str, Any]) -> Lead | None:
    name = str(row.get("company_name") or row.get("company") or "").strip()
    domain = _extract_domain(row)
    why = str(row.get("why_it_matches") or row.get("reason") or "").strip()
    score = float(row.get("final_score") or row.get("score") or 0)

    if not name or not domain:
        return None

    return Lead(company_name=name, domain=domain, why_it_matches=why, score=score)


def load_filtered_leads() -> list[Lead]:
    input_path = _ensure_input_file()
    rows = _load_json(input_path, default=[])

    leads: list[Lead] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        lead = _to_lead(row)
        if lead is None:
            continue
        if lead.score < MIN_SCORE:
            continue
        leads.append(lead)

    # Domain-level dedupe keeps strongest lead per domain.
    best_by_domain: dict[str, Lead] = {}
    for lead in sorted(leads, key=lambda x: x.score, reverse=True):
        if lead.domain not in best_by_domain:
            best_by_domain[lead.domain] = lead

    filtered = list(best_by_domain.values())[:MAX_LEADS]
    return filtered


def build_personalization_prompt(lead: Lead, brief: CompanyBrief) -> str:
    # SalesGPT-aligned short prompt for natural outreach.
    salesgpt_context = ""
    if SALES_AGENT_TOOLS_PROMPT:
        salesgpt_context = (
            "Use SalesGPT style constraints: short, conversational, no lists, clear CTA. "
            "Do not include tool syntax in the final email.\\n"
        )

    brief_context = ""
    if brief.company_name or brief.product_description:
        brief_context = (
            f"Sender company: {brief.company_name or 'the company in the markdown'}\\n"
            f"Product description: {brief.product_description}\\n"
            f"ICP: {brief.core_icp}\\n"
            f"Use cases: {brief.use_cases}\\n"
            f"Value props: {brief.value_propositions}\\n"
        )

    return (
        "Write a short outbound email (80-120 words).\\n"
        "Tone: natural, clear, no hype, one CTA.\\n"
        f"{salesgpt_context}"
        f"{brief_context}"
        f"Company: {lead.company_name}\\n"
        f"Domain: {lead.domain}\\n"
        f"Why relevant: {lead.why_it_matches}\\n"
        "Must mention the sender company name and why its billing/payments product matters for this prospect.\\n"
        "Avoid generic phrases and avoid bullet lists."
    )


def generate_email_copy(lead: Lead, brief: CompanyBrief) -> dict[str, str]:
    # Adapter-only generation path. If you later plug an LLM call here,
    # keep the same return shape.
    sender = brief.company_name or "our team"
    sender_focus = _compact_product_focus(brief.product_description, sender)
    sender_value = _compact_claims(brief.value_propositions) or "reduce billing complexity"
    subject = f"{sender}: idea for {lead.company_name} billing workflows"
    body = (
        f"Hi {lead.company_name} team,\\n\\n"
        f"I’m reaching out from {sender}, an API-first subscription billing platform focused on {sender_focus}. "
        f"We help teams like yours to {sender_value}.\\n\\n"
        f"I checked {lead.domain} and noticed signals around {lead.why_it_matches.lower()}. "
        "That usually means billing, pricing, or payment workflows are starting to get more complex as the product scales.\\n\\n"
        f"If helpful, I can share a short breakdown of how {sender} handles invoicing, recurring billing, and revenue operations for teams in your stage. "
        "Open to a quick 15-minute chat next week?\\n\\n"
        "Best,\\n"
        f"{sender} Sales Team"
    )
    return {"subject": subject, "body": body}


def _smtp_send(recipient: str, subject: str, body: str) -> tuple[bool, str]:
    sender_email = os.getenv("GMAIL_MAIL", "").strip()
    app_password = os.getenv("GMAIL_APP_PASSWORD", "").strip()

    if not sender_email or not app_password:
        return False, "Missing GMAIL_MAIL or GMAIL_APP_PASSWORD"

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender_email, app_password)
        server.send_message(msg)
        server.quit()
        return True, "sent"
    except Exception as exc:
        return False, str(exc)


def _load_replies_map() -> dict[str, Any]:
    rows = _load_json(REPLIES_LOG, default=[])
    mapped: dict[str, Any] = {}
    for row in rows:
        if isinstance(row, dict):
            domain = str(row.get("domain") or "").strip().lower()
            if domain:
                mapped[domain] = row
    return mapped


def _needs_followup(sent_row: dict[str, Any], replies_map: dict[str, Any]) -> bool:
    domain = str(sent_row.get("domain") or "").strip().lower()
    if not domain:
        return False
    if domain in replies_map:
        return False

    followup_count = int(sent_row.get("follow_up_count") or 0)
    if followup_count >= MAX_FOLLOWUPS:
        return False

    last_sent_at = str(sent_row.get("last_sent_at") or sent_row.get("sent_at") or "")
    if not last_sent_at:
        return False

    try:
        last_dt = datetime.fromisoformat(last_sent_at)
    except Exception:
        return False

    return datetime.now(tz=UTC) - last_dt >= timedelta(days=FOLLOWUP_AFTER_DAYS)


def _followup_email(lead: Lead, followup_num: int) -> dict[str, str]:
    subject = f"Following up: {lead.company_name} billing operations"
    body = (
        f"Hi {lead.company_name} team,\\n\\n"
        "Quick follow-up in case my previous note got buried. "
        "Happy to share a concise walkthrough specific to your subscription and payments workflows. "
        "If now is not the right time, I can follow up later.\\n\\n"
        "Best,\\n"
        "Sales Team"
    )
    return {"subject": subject, "body": body}


def run_adapter() -> None:
    """Initialize data files and run the email generation pipeline."""
    # Ensure all output files exist.
    if not GENERATED_EMAILS.exists():
        _save_json(GENERATED_EMAILS, [])
    if not SENT_LOGS.exists():
        _save_json(SENT_LOGS, [])
    if not REPLIES_LOG.exists():
        _save_json(REPLIES_LOG, [])
    
    leads = load_filtered_leads()
    brief = _load_company_brief()

    generated_rows: list[dict[str, Any]] = []
    sent_logs = _load_json(SENT_LOGS, default=[])
    if not isinstance(sent_logs, list):
        sent_logs = []

    replies_map = _load_replies_map()

    # Initial generation/send pass.
    for lead in leads:
        prompt = build_personalization_prompt(lead, brief)
        email = generate_email_copy(lead, brief)
        row = {
            "company_name": lead.company_name,
            "domain": lead.domain,
            "why_it_matches": lead.why_it_matches,
            "score": lead.score,
            "prompt": prompt,
            "subject": email["subject"],
            "body": email["body"],
            "sender_company_name": brief.company_name,
            "sender_product_description": brief.product_description,
            "sender_use_cases": brief.use_cases,
            "sender_value_propositions": brief.value_propositions,
            "generated_at": _now_iso(),
            "type": "initial",
        }
        generated_rows.append(row)

        recipient = f"contact@{lead.domain}"
        if SEND_EMAILS:
            ok, status = _smtp_send(recipient, email["subject"], email["body"])
        else:
            ok, status = False, "logged_only"

        sent_logs.append(
            {
                "company_name": lead.company_name,
                "domain": lead.domain,
                "recipient": recipient,
                "subject": email["subject"],
                "send_status": status,
                "send_success": bool(ok),
                "sent_at": _now_iso(),
                "last_sent_at": _now_iso(),
                "follow_up_count": 0,
            }
        )

    # Follow-up pass from existing logs.
    updated_logs: list[dict[str, Any]] = []
    for log_row in sent_logs:
        if not isinstance(log_row, dict):
            continue

        domain = str(log_row.get("domain") or "").strip().lower()
        matched = next((l for l in leads if l.domain == domain), None)
        if matched and _needs_followup(log_row, replies_map):
            followup_num = int(log_row.get("follow_up_count") or 0) + 1
            followup = _followup_email(matched, followup_num)
            recipient = str(log_row.get("recipient") or f"contact@{domain}")

            if SEND_EMAILS:
                ok, status = _smtp_send(recipient, followup["subject"], followup["body"])
            else:
                ok, status = False, "logged_only"

            generated_rows.append(
                {
                    "company_name": matched.company_name,
                    "domain": matched.domain,
                    "why_it_matches": matched.why_it_matches,
                    "score": matched.score,
                    "subject": followup["subject"],
                    "body": followup["body"],
                    "sender_company_name": brief.company_name,
                    "sender_product_description": brief.product_description,
                    "sender_use_cases": brief.use_cases,
                    "sender_value_propositions": brief.value_propositions,
                    "generated_at": _now_iso(),
                    "type": "follow_up",
                    "follow_up_number": followup_num,
                }
            )

            log_row["follow_up_count"] = followup_num
            log_row["last_sent_at"] = _now_iso()
            log_row["last_send_status"] = status
            log_row["last_send_success"] = bool(ok)

        updated_logs.append(log_row)

    _save_json(GENERATED_EMAILS, generated_rows)
    _save_json(SENT_LOGS, updated_logs)

    print(f"Prepared leads: {len(leads)}")
    print(f"Generated messages: {len(generated_rows)}")
    print(f"Saved: {GENERATED_EMAILS.name}, {SENT_LOGS.name}, {REPLIES_LOG.name}")
    print(f"SEND_EMAILS={SEND_EMAILS}")


if __name__ == "__main__":
    run_adapter()
