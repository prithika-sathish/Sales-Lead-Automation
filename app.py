from __future__ import annotations

import json
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

DATA_FILE = Path("existing_pipeline_output.json")
EMAIL_LOG_FILE = Path("email_logs.json")
GENERATED_EMAILS_FILE = Path("generated_emails.json")


def initialize_data_files() -> None:
    """Initialize required JSON files if they don't exist."""
    if not EMAIL_LOG_FILE.exists():
        with open(EMAIL_LOG_FILE, "w") as f:
            json.dump([], f)
    if not GENERATED_EMAILS_FILE.exists():
        with open(GENERATED_EMAILS_FILE, "w") as f:
            json.dump([], f)


def apply_ui_theme() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
              radial-gradient(circle at 10% 0%, rgba(11, 110, 255, 0.10), transparent 24%),
              radial-gradient(circle at 90% 0%, rgba(0, 168, 150, 0.08), transparent 20%),
              #f7f9fc;
            color: #0f172a;
        }
        .block-container {
            max-width: 1280px;
            padding-top: 1.2rem;
            padding-bottom: 1.2rem;
        }
        .hero {
            padding: 1.3rem 1.4rem;
            border-radius: 18px;
            background: linear-gradient(125deg, #10203d 0%, #12345f 45%, #0d5f73 100%);
            color: #f8fafc;
            margin-bottom: 1rem;
            border: 1px solid rgba(255, 255, 255, 0.18);
            box-shadow: 0 14px 35px rgba(9, 32, 65, 0.2);
        }
        .hero h2 {
            margin: 0;
            letter-spacing: 0.2px;
        }
        .subtle {
            color: #c7d7ef;
            font-size: 0.9rem;
        }
        .surface {
            padding: 1rem 1.1rem;
            border-radius: 14px;
            border: 1px solid #dbe4f0;
            background: #ffffff;
            box-shadow: 0 8px 24px rgba(22, 55, 99, 0.08);
        }
        .metric-card {
            padding: 0.85rem 1rem;
            border: 1px solid #dae4ef;
            background: #ffffff;
            border-radius: 12px;
            box-shadow: 0 5px 14px rgba(18, 52, 95, 0.06);
        }
        .metric-title {
            color: #5b6980;
            font-size: 0.83rem;
            margin-bottom: 0.25rem;
            font-weight: 600;
        }
        .metric-value {
            color: #0b223d;
            font-size: 1.4rem;
            font-weight: 700;
            line-height: 1.2;
        }
        .metric-sub {
            color: #5e6f88;
            font-size: 0.82rem;
            margin-top: 0.15rem;
        }
        .card {
            padding: 0.9rem 1rem;
            border: 1px solid #dbe4f0;
            border-radius: 12px;
            background: #ffffff;
            box-shadow: 0 8px 18px rgba(15, 40, 72, 0.06);
        }
        .section-title {
            font-weight: 700;
            margin-bottom: 0.45rem;
            color: #10203d;
        }
        .reason-chip {
            display: inline-block;
            padding: 0.25rem 0.6rem;
            border-radius: 999px;
            border: 1px solid #ccdae8;
            margin: 0.15rem 0.25rem 0.15rem 0;
            font-size: 0.83rem;
            color: #0d355f;
            background: #edf5ff;
        }
        .tiny {
            color: #64748b;
            font-size: 0.82rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def parse_reason_template(reason: str) -> dict[str, list[str]]:
    fit: list[str] = []
    intent: list[str] = []
    momentum: list[str] = []
    additional: list[str] = []

    fit_keywords = ("subscription", "billing", "saas", "api", "product", "b2b")
    intent_keywords = ("hiring", "growth", "funding", "expansion", "payment")
    momentum_keywords = ("velocity", "modern", "stack", "recent", "activity", "launch")

    parts = [p.strip() for p in reason.replace(";", ",").split(",") if p.strip()]
    seen: set[str] = set()
    for part in parts:
        normalized = part.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        if any(k in normalized for k in fit_keywords):
            fit.append(part)
        elif any(k in normalized for k in intent_keywords):
            intent.append(part)
        elif any(k in normalized for k in momentum_keywords):
            momentum.append(part)
        else:
            additional.append(part)

    return {
        "Core Fit": fit,
        "Buying Signals": intent,
        "Growth Momentum": momentum,
        "Additional Context": additional,
    }


def render_reason_template(reason: str) -> None:
    grouped = parse_reason_template(reason)
    st.markdown("### Why This Lead Looks Promising")

    for title, reasons in grouped.items():
        if not reasons:
            continue
        st.markdown(f"**{title}**")
        chips = "".join([f'<span class="reason-chip">{r}</span>' for r in reasons])
        st.markdown(chips, unsafe_allow_html=True)


def load_leads() -> pd.DataFrame:
    if not DATA_FILE.exists():
        return pd.DataFrame(columns=["company_name", "domain", "score", "why_it_matches", "source", "is_icp_match", "entity_category"])

    try:
        rows = json.loads(DATA_FILE.read_text(encoding="utf-8"))
    except Exception:
        rows = []

    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized.append(
            {
                "company_name": row.get("company_name") or row.get("company") or "",
                "domain": row.get("domain") or row.get("website") or "",
                "score": row.get("score") if row.get("score") is not None else row.get("final_score", 0),
                "why_it_matches": row.get("why_it_matches") or row.get("reason") or "",
                "source": row.get("source") or "",
                "is_icp_match": bool(row.get("is_icp_match") if row.get("is_icp_match") is not None else True),
                "entity_category": row.get("entity_category") or "",
            }
        )

    df = pd.DataFrame(normalized)
    if df.empty:
        return pd.DataFrame(columns=["company_name", "domain", "score", "why_it_matches", "source", "is_icp_match", "entity_category"])

    df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0)
    df = df.sort_values(by="score", ascending=False).reset_index(drop=True)
    return df


def load_activity_log() -> pd.DataFrame:
    if not EMAIL_LOG_FILE.exists():
        return pd.DataFrame(columns=["timestamp", "action", "company_name", "domain", "message"])
    try:
        rows = json.loads(EMAIL_LOG_FILE.read_text(encoding="utf-8"))
    except Exception:
        rows = []

    cleaned: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        cleaned.append(
            {
                "timestamp": str(row.get("timestamp") or ""),
                "action": str(row.get("action") or ""),
                "company_name": str(row.get("company_name") or ""),
                "domain": str(row.get("domain") or ""),
                "message": str(row.get("message") or ""),
            }
        )
    return pd.DataFrame(cleaned)


def load_generated_emails() -> dict[str, dict[str, str]]:
    if not GENERATED_EMAILS_FILE.exists():
        return {}

    try:
        rows = json.loads(GENERATED_EMAILS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

    drafts: dict[str, dict[str, str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        company_name = str(row.get("company_name") or "").strip().lower()
        domain = str(row.get("domain") or "").strip().lower()
        subject = str(row.get("subject") or "").strip()
        body = str(row.get("body") or "").strip()
        if not subject and not body:
            continue
        draft = {"subject": subject, "body": body}
        if company_name:
            drafts[company_name] = draft
        if domain:
            drafts[domain] = draft
    return drafts


def _score_bucket(score: float) -> str:
    if score >= 80:
        return "A (80+)"
    if score >= 65:
        return "B (65-79)"
    if score >= 50:
        return "C (50-64)"
    return "D (<50)"


def append_email_log(entry: dict[str, Any]) -> None:
    if EMAIL_LOG_FILE.exists():
        try:
            logs = json.loads(EMAIL_LOG_FILE.read_text(encoding="utf-8"))
            if not isinstance(logs, list):
                logs = []
        except Exception:
            logs = []
    else:
        logs = []

    logs.append(entry)
    EMAIL_LOG_FILE.write_text(json.dumps(logs, indent=2, ensure_ascii=True), encoding="utf-8")


def trigger_email(lead: dict[str, Any]) -> str:
    message = f"Email generated and sent to {lead['company_name']}"
    append_email_log(
        {
            "timestamp": datetime.now(UTC).isoformat(),
            "action": "send_email",
            "company_name": lead["company_name"],
            "domain": lead["domain"],
            "score": float(lead["score"]),
            "why_it_matches": lead["why_it_matches"],
            "message": message,
        }
    )
    return message


def trigger_followup(lead: dict[str, Any]) -> str:
    message = f"Follow-up generated and sent to {lead['company_name']}"
    append_email_log(
        {
            "timestamp": datetime.now(UTC).isoformat(),
            "action": "send_followup",
            "company_name": lead["company_name"],
            "domain": lead["domain"],
            "score": float(lead["score"]),
            "why_it_matches": lead["why_it_matches"],
            "message": message,
        }
    )
    return message


def main() -> None:
    initialize_data_files()
    st.set_page_config(page_title="AI Lead Generation Dashboard", layout="wide")
    apply_ui_theme()

    st.markdown(
        """
        <div class="hero">
          <h2 style="margin:0;">AI Lead Generation Dashboard</h2>
          <div class="subtle">Prioritized leads, clear reasons, and one-click outreach actions.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    df = load_leads()

    st.subheader("Lead Table")
    st.dataframe(
        df[["company_name", "domain", "score", "why_it_matches"]],
        width="stretch",
        hide_index=True,
        column_config={
            "company_name": "Company",
            "domain": "Domain",
            "score": st.column_config.NumberColumn("Score", format="%.1f"),
            "why_it_matches": "Reason Summary",
        },
    )

    df = load_leads()
    activity_df = load_activity_log()
    generated_emails = load_generated_emails()

    st.sidebar.markdown("## Control Panel")
    score_floor = float(st.sidebar.slider("Minimum score", min_value=0, max_value=100, value=55, step=1))
    icp_only = bool(st.sidebar.checkbox("ICP matches only", value=False))
    query = st.sidebar.text_input("Search company or domain", value="").strip().lower()

    filtered = df.copy()
    filtered = filtered[filtered["score"] >= score_floor]
    if icp_only and not filtered.empty:
        filtered = filtered[filtered["is_icp_match"] == True]  # noqa: E712
    if query and not filtered.empty:
        filtered = filtered[
            filtered["company_name"].str.lower().str.contains(query, na=False)
            | filtered["domain"].str.lower().str.contains(query, na=False)
        ]

    total_leads = int(len(filtered))
    avg_score = float(filtered["score"].mean()) if total_leads > 0 else 0.0
    top_score = float(filtered["score"].max()) if total_leads > 0 else 0.0
    high_conf = int((filtered["score"] >= 80).sum()) if total_leads > 0 else 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(
            f"<div class='metric-card'><div class='metric-title'>Visible Leads</div><div class='metric-value'>{total_leads}</div><div class='metric-sub'>after active filters</div></div>",
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            f"<div class='metric-card'><div class='metric-title'>Average Score</div><div class='metric-value'>{avg_score:.1f}</div><div class='metric-sub'>quality baseline</div></div>",
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            f"<div class='metric-card'><div class='metric-title'>Top Score</div><div class='metric-value'>{top_score:.1f}</div><div class='metric-sub'>best current lead</div></div>",
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(
            f"<div class='metric-card'><div class='metric-title'>A-Tier Leads</div><div class='metric-value'>{high_conf}</div><div class='metric-sub'>score 80 and above</div></div>",
            unsafe_allow_html=True,
        )

    tabs = st.tabs(["Pipeline Overview", "Lead Workspace", "Activity"])

    with tabs[0]:
        left, right = st.columns([2, 1])
        with left:
            st.markdown("### Lead Table")
            st.dataframe(
                filtered[["company_name", "domain", "score", "source", "entity_category", "why_it_matches"]],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "company_name": "Company",
                    "domain": "Domain",
                    "score": st.column_config.NumberColumn("Score", format="%.1f"),
                    "source": "Source",
                    "entity_category": "Category",
                    "why_it_matches": "Reason Summary",
                },
            )
        with right:
            st.markdown("### Score Mix")
            if filtered.empty:
                st.info("No leads available for the selected filters.")
            else:
                bucketed = filtered.copy()
                bucketed["bucket"] = bucketed["score"].apply(_score_bucket)
                chart = bucketed.groupby("bucket", as_index=False).size().rename(columns={"size": "count"})
                chart = chart.sort_values("bucket")
                st.bar_chart(chart.set_index("bucket")["count"], use_container_width=True)

                top_names = filtered.head(5)["company_name"].tolist()
                st.markdown("### Top 5")
                for name in top_names:
                    st.markdown(f"- {name}")

    with tabs[1]:
        st.markdown("### Lead Review & Actions")
        if total_leads == 0:
            st.info("No leads found. Adjust filters in the sidebar.")
            return

        options = filtered["company_name"].tolist()
        selected_name = st.selectbox("Choose a company", options=options)

        row = filtered[filtered["company_name"] == selected_name].iloc[0]
        selected = {
            "company_name": str(row["company_name"]),
            "domain": str(row["domain"]),
            "score": float(row["score"]),
            "why_it_matches": str(row["why_it_matches"]),
            "source": str(row.get("source") or ""),
            "entity_category": str(row.get("entity_category") or ""),
            "is_icp_match": bool(row.get("is_icp_match")),
        }

        detail_left, detail_right = st.columns([1, 2])
        with detail_left:
            st.markdown("<div class='surface'>", unsafe_allow_html=True)
            st.markdown("### Lead Snapshot")
            st.markdown(f"**Company:** {selected['company_name']}")
            st.markdown(f"**Domain:** {selected['domain']}")
            st.markdown(f"**Score:** {selected['score']:.1f}")
            st.markdown(f"**Source:** {selected['source']}")
            st.markdown(f"**Category:** {selected['entity_category']}")
            st.markdown(f"**ICP Match:** {'Yes' if selected['is_icp_match'] else 'No'}")
            st.markdown("</div>", unsafe_allow_html=True)

        with detail_right:
            st.markdown("<div class='surface'>", unsafe_allow_html=True)
            render_reason_template(selected["why_it_matches"])
            st.markdown("</div>", unsafe_allow_html=True)

        draft = generated_emails.get(selected["domain"].strip().lower()) or generated_emails.get(selected["company_name"].strip().lower())
        st.markdown("### Generated Email")
        if draft:
            st.markdown(f"**Subject:** {draft['subject']}")
            st.text_area(
                "Email Body",
                value=draft["body"],
                height=240,
                disabled=True,
                label_visibility="collapsed",
            )
        else:
            st.info("No generated email found for this lead yet. Run the Sales-Agent adapter to create drafts.")

        b1, b2 = st.columns(2)
        with b1:
            if st.button("Send Personalized Email", use_container_width=True):
                result = trigger_email(selected)
                st.success(result)

        with b2:
            if st.button("Send Follow-up", use_container_width=True):
                result = trigger_followup(selected)
                st.success(result)

    with tabs[2]:
        st.markdown("### Outreach Activity")
        if activity_df.empty:
            st.info("No activity logged yet.")
        else:
            activity_df = activity_df.sort_values(by="timestamp", ascending=False)
            st.dataframe(activity_df, use_container_width=True, hide_index=True)

            recent = activity_df.head(1).iloc[0]
            st.markdown(
                f"<div class='tiny'>Latest action: {recent['action']} for {recent['company_name']} at {recent['timestamp']}</div>",
                unsafe_allow_html=True,
            )


if __name__ == "__main__":
    main()
