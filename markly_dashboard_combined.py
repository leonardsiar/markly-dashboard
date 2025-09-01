# Mark.ly Pilot Dashboard — Combined Single File
# -------------------------------------------------
# This Streamlit app reads annotated AWS logs and produces a pilot dashboard
# with robust onboarding-session filtering (3-state), teacher funnel,
# retention, trust/override, power users, a student overview, and layered tabs.
#
# Key features:
# - Three session modes: Exclude onboarding (default), Only onboarding, Everything
# - Post-onboarding evaluation options (e.g., retention buckets, power users)
# - Denominator control: fixed 33 accounts vs observed unique teachers
# - Email→School mapping (extend as needed)
# - Student accounts tracked (even if IDs are gibberish)
# - Safer dataframe rendering for Streamlit deprecation of `use_container_width`
# - Layered tabs: Executive View, School Comparisons, Diagnostics (AI Trust & Students)
# - **Self-tests**: lightweight assertions you can enable via Streamlit secrets
#
# HOW TO USE
# 1) `pip install streamlit pandas altair openpyxl`
# 2) `streamlit run markly_dashboard_combined.py`
# 3) Upload your annotated Excel logs (e.g., /mnt/data/user_logs_annotated.xlsx)
# 4) Adjust sidebar controls.

from __future__ import annotations

import json
from datetime import timedelta
from typing import Dict, Any

import pandas as pd
import streamlit as st
import altair as alt

# -------------------------
# Configuration / Mappings
# -------------------------
# Extend this mapping based on your known teacher account list
EMAIL_TO_SCHOOL: Dict[str, str] = {
    # St Andrew
    "chong_chun_lian_esther@moe.edu.sg": "St Andrew",
    "marek_otreba@moe.edu.sg": "St Andrew",
    "wee_hui_ern_abigail@moe.edu.sg": "St Andrew",
    # Pei Hwa
    "fahmie_ali_abdat@moe.edu.sg": "Pei Hwa",
    "lim_airong_michelle@moe.edu.sg": "Pei Hwa",
    "tan_chor_yin_erin@moe.edu.sg": "Pei Hwa",
    "xu_mingjie_marcus@moe.edu.sg": "Pei Hwa",
    "koh_ting_suen_jewel@moe.edu.sg": "Pei Hwa",
    "su_yi_ying@moe.edu.sg": "Pei Hwa",
    # Northlight
    "yeo_xin_yi@moe.edu.sg": "Northlight",
    "justine_yoong_yuping@moe.edu.sg": "Northlight",
    "marcus_tan_lee_kiang@moe.edu.sg": "Northlight",
    "teong_ying_jun_fedora@moe.edu.sg": "Northlight",
    # Add remaining schools/emails here...
}

# Onboarding session windows by school (LOCAL time). Fill these with real windows.
# Example format per school:
#   {"date": "2025-08-15", "start": "09:00:00", "end": "11:30:00"}
SCHOOL_ONBOARDING: Dict[str, Dict[str, str]] = {
    # "St Andrew": {"date": "2025-08-15", "start": "09:00:00", "end": "11:30:00"},
    # "Pei Hwa": {"date": "2025-08-16", "start": "10:00:00", "end": "12:00:00"},
    # "Northlight": {"date": "2025-08-18", "start": "08:30:00", "end": "11:00:00"},
    # ... fill in the rest (7 schools total)
}

# Emails to exclude entirely from analysis (test users, etc.)
EXCLUDE_EMAILS = {"teacher1@moe.gov.sg", "teacher3@moe.gov.sg"}

# -------------------------
# Utility functions
# -------------------------

@st.cache_data(show_spinner=False)
def load_logs(file) -> pd.DataFrame:
    """Load annotated Excel logs. Expects at least columns:
    - '@timestamp' (UTC), '@message' (JSON-like string), 'event_name', 'email', 'user_type'
    Additional fields are tolerated.
    """
    df = pd.read_excel(file)
    # Normalize time
    if "@timestamp" in df.columns:
        df["@timestamp"] = pd.to_datetime(df["@timestamp"], errors="coerce", utc=True)
    # Normalize event and user_type
    if "event_name" in df.columns:
        df["event_name"] = df["event_name"].astype(str)
    if "user_type" in df.columns:
        df["user_type"] = df["user_type"].astype(str)
    if "email" in df.columns:
        df["email"] = df["email"].astype(str)
    else:
        df["email"] = None
    if "@message" not in df.columns:
        df["@message"] = "{}"
    return df


def parse_msg(raw: Any) -> Dict[str, Any]:
    """Parse the JSON-like '@message' column safely."""
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        return {}
    s = raw.strip()
    # Some logs store single quotes; try to coerce
    try:
        return json.loads(s)
    except Exception:
        try:
            s2 = s.replace("'", '"')
            return json.loads(s2)
        except Exception:
            return {}


def extract_generic_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Best-effort extraction of common IDs from message/details payloads.
    Adds: assessment_id, class_id, submission_id, student_id if present.
    """
    def get_from_both(row, key):
        msg = row.get("msg", {}) or {}
        det = row.get("details", {}) or {}
        if isinstance(msg, dict) and key in msg:
            return msg.get(key)
        if isinstance(det, dict) and key in det:
            return det.get(key)
        return None

    for k in ["assessment_id", "class_id", "submission_id", "student_id"]:
        df[k] = df.apply(lambda r: get_from_both(r, k), axis=1)
    return df


def show_df(df: pd.DataFrame):
    """Render dataframe using new width API when available, fallback otherwise."""
    try:
        st.dataframe(df, width="stretch")
    except TypeError:
        st.dataframe(df, use_container_width=True)


def apply_session_filter(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if mode == "Exclude onboarding sessions":
        return df[~df["is_onboarding"]]
    elif mode == "Include only onboarding sessions":
        return df[df["is_onboarding"]]
    else:
        return df


# -------------------------
# App UI
# -------------------------

st.set_page_config(page_title="Mark.ly Pilot Dashboard", layout="wide")
st.title("Mark.ly Pilot Dashboard")

# Sidebar inputs
st.sidebar.header("Inputs")
log_file = st.sidebar.file_uploader("Upload logs Excel (user_logs_annotated.xlsx)", type=["xlsx"])

total_accounts = st.sidebar.number_input("Total teacher accounts (target = 33)", min_value=1, value=33, step=1)
use_observed_denominator = st.sidebar.checkbox(
    "Use observed unique teachers as denominator",
    value=False,
    help=(
        "If checked, percentages are based only on teacher accounts that appear in the logs "
        "(observed), not the full expected 33."
    ),
)

# Timezone and optional school filter
tz_offset_hours = st.sidebar.number_input("Timezone offset from UTC (Singapore = +8)", value=8, step=1)
tz_delta = timedelta(hours=int(tz_offset_hours))

school_filter = st.sidebar.text_input("Filter by school name (contains, optional)", "")

st.sidebar.markdown("---")
session_filter_mode = st.sidebar.selectbox(
    "Onboarding session filter",
    options=[
        "Exclude onboarding sessions",
        "Include only onboarding sessions",
        "Include everything",
    ],
    index=0,
)

show_post_onboarding_only_power = st.sidebar.checkbox(
    "Power users calculated from post-onboarding only", value=True
)

if not log_file:
    st.info("Upload the **logs** file in the sidebar to begin.")
    st.stop()

# -------------------------
# Load + prepare data
# -------------------------

logs = load_logs(log_file)

# Remove excluded emails
logs = logs[~logs["email"].isin(EXCLUDE_EMAILS)].copy()

# Attach school mapping
logs["school"] = logs["email"].map(EMAIL_TO_SCHOOL).fillna("Unknown")

# Parse @message JSON
logs["msg"] = logs["@message"].apply(parse_msg)
logs["details"] = logs["msg"].apply(lambda d: d.get("details", {}) if isinstance(d, dict) else {})

# Extract common ids
logs = extract_generic_ids(logs)
logs["object_id"] = logs["msg"].apply(lambda d: d.get("object_id") if isinstance(d, dict) else None)
logs["details_id"] = logs["details"].apply(lambda d: d.get("id") if isinstance(d, dict) else None)
logs["error_message"] = logs["msg"].apply(lambda d: d.get("error_message") if isinstance(d, dict) else None)

# Local time features
logs["ts_local"] = logs["@timestamp"] + tz_delta
logs["date_local"] = logs["ts_local"].dt.date

# Optional global school filter (pre-tabs)
if school_filter.strip():
    logs = logs[logs["school"].astype(str).str.contains(school_filter, case=False, na=False)]

# First login date per teacher (local tz)
teacher_mask = logs["user_type"].str.lower() == "teacher"
first_login_date = (
    logs[teacher_mask & (logs["event_name"].str.lower() == "userlogin")]
    .groupby("email")["ts_local"]
    .min()
    .dt.date
)
logs["first_login_date"] = logs["email"].map(first_login_date)

# Onboarding membership

def in_school_session(row) -> bool:
    school = row["school"]
    sess = SCHOOL_ONBOARDING.get(school)
    if not sess:
        return False
    start_local = pd.to_datetime(f"{sess['date']} {sess['start']}")
    end_local = pd.to_datetime(f"{sess['date']} {sess['end']}")
    return (row["ts_local"] >= start_local) and (row["ts_local"] <= end_local)

logs["is_onboarding"] = logs.apply(in_school_session, axis=1)

# ============================
# Filters derived from data
# ============================

# Compute school list and date range (after session filter so onboarding logic applies)
preview_for_filters = apply_session_filter(logs, session_filter_mode).copy()
all_schools = sorted(preview_for_filters["school"].dropna().unique().tolist())
# Robust min/max for mixed-type date columns
_dl = pd.to_datetime(preview_for_filters["date_local"], errors="coerce")
_min_ts = _dl.min()
_max_ts = _dl.max()
_default_range = None if (pd.isna(_min_ts) or pd.isna(_max_ts)) else (_min_ts.date(), _max_ts.date())

# Sidebar: School multiselect + Date range
st.sidebar.markdown("### Data Filters")
selected_schools = st.sidebar.multiselect(
    "Schools", options=all_schools, default=all_schools,
    help="Select one or more schools to focus the analysis",
)

# Streamlit date_input requires a concrete default; fall back to last 30 days if unknown
try:
    if _default_range:
        date_range = st.sidebar.date_input("Date range (local)", value=_default_range)
    else:
        _today = pd.Timestamp.utcnow().tz_localize(None).date()
        date_range = st.sidebar.date_input("Date range (local)", value=(_today - timedelta(days=30), _today))
except Exception:
    _today = pd.Timestamp.utcnow().tz_localize(None).date()
    date_range = st.sidebar.date_input("Date range (local)", value=(_today - timedelta(days=30), _today))

# Helper to apply school + date filters consistently

def apply_data_filters(df: pd.DataFrame, date_range, selected_schools, session_filter_mode: str) -> pd.DataFrame:
    base = apply_session_filter(df, session_filter_mode)
    if selected_schools:
        base = base[base["school"].isin(selected_schools)]
    # date_range can be a single date or tuple from Streamlit
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
        if pd.notnull(start_d) and pd.notnull(end_d):
            base = base[(base["date_local"] >= start_d) & (base["date_local"] <= end_d)]
    return base

# ============================
# Tabs (Layer 1, Layer 3, Diagnostics)
# ============================

layer1_tab, schools_tab, diag_tab = st.tabs([
    "Executive View",  # Layer 1
    "School Comparisons",  # Layer 3
    "Diagnostics: AI Trust & Students",  # Rubric edits + Student activity
])

# ----------------------------
# Layer 1 – Executive View
# ----------------------------
with layer1_tab:
    st.subheader("Layer 1 – Executive View")
    view_logs = apply_data_filters(logs, date_range, selected_schools, session_filter_mode)
    teacher_logs = view_logs[view_logs["user_type"].str.lower() == "teacher"].copy()
    student_logs = view_logs[view_logs["user_type"].str.lower() == "student"].copy()

    # KPI Header
    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.metric("Schools (in view)", int(view_logs["school"].nunique()))
    with k2:
        denom = (teacher_logs["email"].nunique() if use_observed_denominator else total_accounts)
        st.metric("Teacher denominator", int(denom))
    with k3:
        logged_in_teachers = teacher_logs.loc[teacher_logs["event_name"].str.lower()=="userlogin", "email"].nunique()
        pct_login = round((logged_in_teachers / max(1, denom)) * 100, 1)
        st.metric("% Logged in", f"{pct_login}%", help="Post-onboarding by filter")
    with k4:
        graded_teachers = teacher_logs.loc[teacher_logs["event_name"].str.lower()=="gradesubmission", "email"].nunique()
        pct_graded = round((graded_teachers / max(1, denom)) * 100, 1)
        st.metric("% Graded", f"{pct_graded}%")
    with k5:
        fb = teacher_logs[teacher_logs["event_name"].str.lower().isin(["updatefeedback","deletefeedback","createfeedback"])].copy()
        trust_updates = int((fb["event_name"].str.lower()=="updatefeedback").sum())
        overrides = int(fb["event_name"].str.lower().isin(["deletefeedback","createfeedback"]).sum())
        trust_ratio = round(trust_updates/(trust_updates+overrides),3) if (trust_updates+overrides)>0 else None
        st.metric("Trust ratio", trust_ratio if trust_ratio is not None else "–")

    # Denominator explainer
    st.caption(
        "Percentages use the selected denominator: **observed unique teachers** (those appearing in logs) "
        "or the fixed target of 33."
    )

    # Funnel (teachers)
    st.markdown("### Adoption Funnel (Teachers)")
    # Exclude first-login day for stricter post-onboarding signal
    funnel_logs = teacher_logs[~(teacher_logs["date_local"] == teacher_logs["first_login_date"])].copy()

    emails_df = pd.DataFrame({"email": funnel_logs["email"].unique()})
    def flag_by_event(df: pd.DataFrame, ev: str) -> pd.Series:
        m = df["event_name"].str.lower() == ev
        return (
            df[m].groupby("email").size().reindex(emails_df["email"], fill_value=0).gt(0)
        )

    funnel_flags = emails_df.copy()
    for ev in ["userlogin","createclass","createassessment","gradesubmission"]:
        funnel_flags[ev] = flag_by_event(funnel_logs, ev).values
    funnel_flags["refined_any"] = flag_by_event(funnel_logs, "updatefeedback") | flag_by_event(funnel_logs, "updaterubric")

    def pct(n:int,d:int)->float:
        return round((n/max(d,1))*100,1)

    rows = []
    stages = [("Logged in","userlogin"),("Created class","createclass"),("Created assessment","createassessment"),("Graded submission","gradesubmission"),("Refined feedback","refined_any")]
    for label, col in stages:
        cnt = int(funnel_flags[col].sum())
        rows.append({"Metric":label,"Count":cnt,"% of denominator":pct(cnt, denom)})
    funnel_df = pd.DataFrame(rows)

    c1, c2 = st.columns(2)
    with c1:
        show_df(funnel_df)
    with c2:
        if not funnel_df.empty:
            st.altair_chart(
                alt.Chart(funnel_df).mark_bar().encode(
                    x=alt.X("Metric:N", sort=None), y="Count:Q", tooltip=["Metric","Count","% of denominator"]
                ), use_container_width=True
            )
        else:
            st.info("No teacher funnel data in the current filters.")

    # Retention buckets (teachers)
    st.markdown("### Retention Overview (Teachers)")
    post = teacher_logs[teacher_logs["date_local"] > teacher_logs["first_login_date"]].copy()
    ret_days = post.groupby("email")["date_local"].nunique()
    def bucket(n:int)->str:
        if n==0: return "0 days"
        if n==1: return "1 day"
        if 2<=n<=3: return "2–3 days"
        return "4+ days"
    ret_buckets = (
        ret_days.apply(bucket).value_counts().reindex(["0 days","1 day","2–3 days","4+ days"], fill_value=0).reset_index()
    )
    ret_buckets.columns = ["Retention Bucket","Number of Teachers"]

    c3, c4 = st.columns(2)
    with c3:
        show_df(ret_buckets)
    with c4:
        if not ret_buckets.empty:
            st.altair_chart(
                alt.Chart(ret_buckets).mark_bar().encode(
                    x=alt.X("Retention Bucket:N", sort=None), y="Number of Teachers:Q", tooltip=["Retention Bucket","Number of Teachers"]
                ), use_container_width=True
            )
        else:
            st.info("No retention data in the current filters.")

# ----------------------------
# Layer 3 – School Comparisons
# ----------------------------
with schools_tab:
    st.subheader("Layer 3 – School Comparisons")
    view_logs = apply_data_filters(logs, date_range, selected_schools, session_filter_mode)
    tlogs = view_logs[view_logs["user_type"].str.lower()=="teacher"].copy()
    tlogs = tlogs[~(tlogs["date_local"] == tlogs["first_login_date"])]  # post-onboarding signal

    # Funnel by school (percent of denominator per school)
    st.markdown("### Mini-Funnels per School")

    def per_school_percent(ev_name:str) -> pd.DataFrame:
        df = tlogs[tlogs["event_name"].str.lower()==ev_name].groupby(["school"])['email'].nunique().rename("count").reset_index()
        # denominators per school (observed teachers in view)
        denom_df = tlogs.groupby("school")["email"].nunique().rename("denom").reset_index()
        out = denom_df.merge(df, on="school", how="left").fillna({"count":0})
        out["percent"] = (out["count"]/out["denom"].replace(0,1))*100
        out["stage"] = ev_name
        return out

    stages = ["userlogin","createclass","createassessment","gradesubmission","updatefeedback","updaterubric"]
    parts = [per_school_percent(s) for s in stages]
    school_funnel = pd.concat(parts, ignore_index=True)

    if not school_funnel.empty:
        st.altair_chart(
            alt.Chart(school_funnel).mark_bar().encode(
                x=alt.X("stage:N", title="Stage", sort=["userlogin","createclass","createassessment","gradesubmission","updatefeedback","updaterubric"]),
                y=alt.Y("percent:Q", title="% of observed teachers"),
                column=alt.Column("school:N", title="School"),
                tooltip=["school","stage","percent"],
            ).resolve_scale(y='independent'),
            use_container_width=True,
        )
    else:
        st.info("No school funnel data in the current filters.")

    # Retention by school (stacked buckets)
    st.markdown("### Retention by School (stacked)")
    post = tlogs.copy()
    ret_days = post.groupby(["school","email"])['date_local'].nunique().reset_index(name="days")
    def bucket(n:int)->str:
        if n==0: return "0 days"
        if n==1: return "1 day"
        if 2<=n<=3: return "2–3 days"
        return "4+ days"
    ret_days["bucket"] = ret_days["days"].apply(bucket)
    ret_stack = ret_days.groupby(["school","bucket"]).size().reset_index(name="teachers")

    if not ret_stack.empty:
        st.altair_chart(
            alt.Chart(ret_stack).mark_bar().encode(
                x=alt.X("school:N", title="School"),
                y=alt.Y("teachers:Q", title="# Teachers"),
                color=alt.Color("bucket:N", title="Retention"),
                order=alt.Order('bucket', sort='ascending'),
                tooltip=["school","bucket","teachers"],
            ), use_container_width=True
        )
    else:
        st.info("No school retention data in the current filters.")

# ----------------------------
# Diagnostics – AI Trust & Students
# ----------------------------
with diag_tab:
    st.subheader("Diagnostics – AI Trust & Student Activity")
    view_logs = apply_data_filters(logs, date_range, selected_schools, session_filter_mode)

    # AI Trust (rubric/feedback edits)
    st.markdown("### AI Trust: Edits vs Overrides")
    tlogs = view_logs[view_logs["user_type"].str.lower()=="teacher"].copy()
    fb = tlogs[tlogs["event_name"].str.lower().isin(["updatefeedback","updaterubric","deletefeedback","createfeedback"])].copy()
    fb["kind"] = fb["event_name"].str.lower().map({
        "updatefeedback":"UpdateFeedback",
        "updaterubric":"UpdateRubric",
        "deletefeedback":"Override",
        "createfeedback":"Override",
    }).fillna("Other")
    trust_summary = fb.groupby("kind").size().reset_index(name="count")
    show_df(trust_summary)
    if not trust_summary.empty:
        st.altair_chart(
        alt.Chart(trust_summary).mark_bar().encode(x="kind:N", y="count:Q", tooltip=["kind","count"]),
        use_container_width=True
    )
    else:
        st.info("No AI trust edit/override events in the current filters.")

    # Student activity distribution
    st.markdown("### Student Activity Distribution")
    s_logs = view_logs[view_logs["user_type"].str.lower()=="student"].copy()
    # Distinct active days per student
    s_days = s_logs.groupby("email")["date_local"].nunique().reset_index(name="active_days")
    if not s_days.empty:
        st.altair_chart(
            alt.Chart(s_days).mark_bar().encode(
                x=alt.X("active_days:Q", bin=alt.Bin(maxbins=10), title="Active days per student"),
                y=alt.Y("count():Q", title="# Students"),
                tooltip=["count()","active_days"]
            ), use_container_width=True
        )
    else:
        st.info("No student activity in the current filter.")

    # Student logins over time (line)
    s_login = s_logs[s_logs["event_name"].str.lower()=="userlogin"].groupby("date_local").size().reset_index(name="logins")
    if not s_login.empty:
        st.altair_chart(
            alt.Chart(s_login).mark_line(point=True).encode(
                x=alt.X("date_local:T", title="Date"), y=alt.Y("logins:Q", title="Student logins"), tooltip=["date_local","logins"]
            ), use_container_width=True
        )
    else:
        st.info("No student logins in the current filter.")

# ----------------------------
# Lightweight self-tests (optional)
# ----------------------------

def _self_test():
    # parse_msg handles JSON and single-quoted JSON
    assert parse_msg('{"a": 1}')["a"] == 1
    assert parse_msg("{'a': 1}")["a"] == 1

    # apply_session_filter works for all modes
    _df = pd.DataFrame({"is_onboarding": [True, False, True]})
    assert len(apply_session_filter(_df, "Exclude onboarding sessions")) == 1
    assert len(apply_session_filter(_df, "Include only onboarding sessions")) == 2
    assert len(apply_session_filter(_df, "Include everything")) == 3

    # extract_generic_ids pulls from msg/details
    test = pd.DataFrame({
        "msg": [{"assessment_id": "A1"}, {}],
        "details": [{}, {"assessment_id": "A2"}],
    })
    out = extract_generic_ids(test.copy())
    assert set(out["assessment_id"].fillna("").unique()) == {"A1", "A2", ""}

# Enable with: add to .streamlit/secrets.toml -> RUN_SELF_TESTS = true
try:
    if bool(st.secrets.get("RUN_SELF_TESTS", False)):
        _self_test()
        st.sidebar.success("Self-tests passed ✅")
except Exception as _e:
    st.sidebar.warning(f"Self-tests failed: {_e}")
