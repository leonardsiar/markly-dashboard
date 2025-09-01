import streamlit as st
import pandas as pd
import numpy as np
import json
import altair as alt
from datetime import timedelta

st.set_page_config(page_title="Mark.ly Pilot Dashboard", layout="wide")

# ---------------------------------------------
# Hardcoded Email -> School mapping (lowercased emails)
# ---------------------------------------------
EMAIL_TO_SCHOOL = {
    "chong_chun_lian_esther@moe.edu.sg": "St Andrew",
    "marek_otreba@moe.edu.sg": "St Andrew",
    "wee_hui_ern_abigail@moe.edu.sg": "St Andrew",
    "fahmie_ali_abdat@moe.edu.sg": "Pei Hwa",
    "lim_airong_michelle@moe.edu.sg": "Pei Hwa",
    "tan_chor_yin_erin@moe.edu.sg": "Pei Hwa",
    "xu_mingjie_marcus@moe.edu.sg": "Pei Hwa",
    "koh_ting_suen_jewel@moe.edu.sg": "Pei Hwa",
    "su_yi_ying@moe.edu.sg": "Pei Hwa",
    "justine_yoong_yuping@moe.edu.sg": "Northlight",
    "marcus_tan_lee_kiang@moe.edu.sg": "Northlight",
    "teong_ying_jun_fedora@moe.edu.sg": "Northlight",
    "nor_hasni_yanti_hamim@moe.edu.sg": "Ngee Ann",
    "yeo_meow_ling_doreen@moe.edu.sg": "Ngee Ann",
    "nashita_allaudin@moe.edu.sg": "Ngee Ann",
    "tan_rou_ming@moe.edu.sg": "Ngee Ann",
    "tey_kelvin@moe.edu.sg": "Ngee Ann",
    "kiren_kaur_gill@moe.edu.sg": "Bartley",
    "lee_guo_sheng@moe.edu.sg": "Bartley",
    "wong_wun_hei_jonathan@moe.edu.sg": "Bartley",
    "wee_jing_yun@moe.edu.sg": "Bartley",
    "pek_chi_hiong_gary@moe.edu.sg": "Bartley",
    "tan_chee_keong_a@moe.edu.sg": "Bartley",
    "muhammad_bazlee_bakhtiar_afandi@moe.edu.sg": "Bartley",
    "kwan_ruiyun_kathleen@moe.edu.sg": "Bartley",
    "haryati_hajar_yusop@moe.edu.sg": "Anglican",
    "farahdilla_mohd_ariff@moe.edu.sg": "Anglican",
    "tham_kian_wen_carin@moe.edu.sg": "Anglican",
    "carmenwangjw@acsindep.edu.sg": "ACSI",
    "sheliathersy@acsindep.edu.sg": "ACSI",
    "karenng@acsindep.edu.sg": "ACSI",
    "hweehwee@acsindep.edu.sg": "ACSI",
    "ongkianjie@acsindep.edu.sg": "ACSI",
}

# ---------------------------------------------
# Helpers
# ---------------------------------------------
def load_logs(file):
    df = pd.read_excel(
        file,
        sheet_name="user_logs_annotated",
        usecols=["email", "user_type", "event_name", "@timestamp", "@message"],
    )
    df["email"] = df["email"].astype(str).str.strip().str.lower()
    df["user_type"] = df["user_type"].astype(str)
    df["event_name"] = df["event_name"].astype(str)
    df["@timestamp"] = pd.to_datetime(df["@timestamp"], errors="coerce")
    df = df.dropna(subset=["@timestamp", "email", "event_name"])
    df["event_lower"] = df["event_name"].str.lower()
    return df


def parse_msg(x):
    try:
        return json.loads(x) if pd.notna(x) else {}
    except Exception:
        return {}


def flatten(d, parent_key="", out=None):
    if out is None:
        out = {}
    if isinstance(d, dict):
        for k, v in d.items():
            key = f"{parent_key}.{k}" if parent_key else k
            flatten(v, key, out)
    elif isinstance(d, list):
        for i, v in enumerate(d):
            key = f"{parent_key}[{i}]"
            flatten(v, key, out)
    else:
        out[parent_key] = d
    return out


def extract_generic_ids(df):
    """Robust per-row generic extraction for assessment/submission ids from nested JSON."""
    assess_vals, subm_vals = [], []
    for _, row in df[["msg"]].iterrows():
        m = row["msg"]
        if not isinstance(m, dict):
            m = {}
        flat = flatten(m)
        cand_assess = [
            v
            for k, v in flat.items()
            if isinstance(k, str)
            and any(tok in k.lower() for tok in ["assessment", "assessment_id", "assessmentid"])
            and not any(tok in k.lower() for tok in ["assessment_name", "assessment_title", "assessmentdescription"])
        ]
        cand_subm = [
            v
            for k, v in flat.items()
            if isinstance(k, str)
            and any(tok in k.lower() for tok in ["submission", "submission_id", "submissionid"])
            and not any(tok in k.lower() for tok in ["submission_status", "submission_count"])
        ]
        if len(cand_subm) == 0 and isinstance(m, dict):
            det = m.get("details", {})
            if isinstance(det, dict):
                det_id = det.get("id")
                if det_id is not None:
                    cand_subm = [det_id]
        assess_vals.append(cand_assess[0] if cand_assess else None)
        subm_vals.append(cand_subm[0] if cand_subm else None)
    df["assessment_any"] = pd.Series(assess_vals, index=df.index)
    df["submission_any"] = pd.Series(subm_vals, index=df.index)
    return df


# ---------------------------------------------
# Sidebar inputs
# ---------------------------------------------
st.sidebar.header("Inputs")
log_file = st.sidebar.file_uploader("Upload logs Excel (user_logs_annotated.xlsx)", type=["xlsx"])

total_accounts = st.sidebar.number_input("Total accounts created", min_value=1, value=33, step=1)
tz_offset_hours = st.sidebar.number_input("Timezone offset from UTC (Singapore = +8)", value=8, step=1)
tz_delta = timedelta(hours=int(tz_offset_hours))

school_filter = st.sidebar.text_input("Filter by school name (contains, optional)", "")

st.sidebar.markdown("---")
exclude_onboarding_for_retention = st.sidebar.checkbox(
    "Use first login day as 'Onboarding' for retention (recommended)", value=True
)
show_post_onboarding_only_power = st.sidebar.checkbox(
    "Power users from post-onboarding only", value=True
)

if not log_file:
    st.title("Mark.ly Pilot Dashboard")
    st.info("Upload the **logs** file in the sidebar to begin.")
    st.stop()

# ---------------------------------------------
# Load + prepare data
# ---------------------------------------------
logs = load_logs(log_file)

# Teachers only
logs = logs[logs["user_type"].str.lower() == "teacher"].copy()

# Exclude specific emails from all analysis
exclude_emails = {"teacher1@moe.gov.sg", "teacher3@moe.gov.sg"}
logs = logs[~logs["email"].isin(exclude_emails)]

# Attach school via hardcoded mapping
logs["school"] = logs["email"].map(EMAIL_TO_SCHOOL).fillna("Unknown")

# Parse JSON payload
logs["msg"] = logs["@message"].apply(parse_msg)

# Extract common ids
logs = extract_generic_ids(logs)
logs["object_id"] = logs["msg"].apply(lambda d: d.get("object_id") if isinstance(d, dict) else None)
logs["details"] = logs["msg"].apply(lambda d: d.get("details", {}) if isinstance(d, dict) else {})
logs["details_id"] = logs["details"].apply(lambda d: d.get("id") if isinstance(d, dict) else None)
logs["error_message"] = logs["msg"].apply(lambda d: d.get("error_message") if isinstance(d, dict) else None)

# Time features
logs["ts_local"] = logs["@timestamp"] + tz_delta
logs["date_local"] = logs["ts_local"].dt.date

# Optional school filter
if school_filter.strip():
    logs = logs[logs["school"].astype(str).str.contains(school_filter, case=False, na=False)]

# Derive first login day per account (local tz)
first_login_date = logs[logs["event_lower"] == "userlogin"].groupby("email")["ts_local"].min().dt.date
logs["first_login_date"] = logs["email"].map(first_login_date)

st.title("Mark.ly Pilot Dashboard")

# ===== Adoption Funnel (includes onboarding) =====
st.header("Adoption Funnel (including onboarding)")
emails_df = pd.DataFrame({"email": logs["email"].unique()})
email_school_map = logs[["email", "school"]].drop_duplicates().set_index("email")["school"]
emails_df["school"] = emails_df["email"].map(email_school_map)

def flag_by_event(df, ev):
    return (
        df[df["event_lower"] == ev]
        .groupby("email")
        .size()
        .reindex(emails_df["email"], fill_value=0)
        .gt(0)
    )

funnel_flags = emails_df.copy()
for ev in ["userlogin", "createclass", "createassessment", "gradesubmission"]:
    funnel_flags[ev] = flag_by_event(logs, ev).values
# refined = updatefeedback or updaterubric
funnel_flags["refined_any"] = flag_by_event(logs, "updatefeedback") | flag_by_event(logs, "updaterubric")

def pct(n):
    return round(n / total_accounts * 100, 1)

rows = []
stages = [
    ("Logged in", "userlogin"),
    ("Created class", "createclass"),
    ("Created assessment", "createassessment"),
    ("Graded submission", "gradesubmission"),
    ("Refined feedback", "refined_any"),
]
for label, col in stages:
    cnt = int(funnel_flags[col].sum())
    rows.append({"Metric": label, "Count": cnt, "% of total": pct(cnt)})
funnel_df = pd.DataFrame(rows)

c1, c2 = st.columns([1, 1])
with c1:
    st.dataframe(funnel_df, use_container_width=True)
with c2:
    chart = (
        alt.Chart(funnel_df)
        .mark_bar()
        .encode(x=alt.X("Metric:N", sort=None), y="Count:Q", tooltip=["Metric", "Count", "% of total"])
    )
    st.altair_chart(chart, use_container_width=True)

# ===== Retention (post-onboarding only) =====
st.header("Retention (post-onboarding only)")
if exclude_onboarding_for_retention:
    post = logs[logs["date_local"] > logs["first_login_date"]].copy()
else:
    post = logs.copy()

ret_days = post.groupby("email")["date_local"].nunique().reindex(emails_df["email"], fill_value=0)

def bucket(n):
    if n == 0:
        return "1 day only"
    if 1 <= n <= 2:
        return "2–3 days"
    return "4+ days"

ret_buckets = (
    ret_days.apply(bucket)
    .value_counts()
    .reindex(["1 day only", "2–3 days", "4+ days"], fill_value=0)
    .reset_index()
)
ret_buckets.columns = ["Retention Bucket", "Number of Teachers"]

c1, c2 = st.columns([1, 1])
with c1:
    st.dataframe(ret_buckets, use_container_width=True)
with c2:
    st.altair_chart(
        alt.Chart(ret_buckets)
        .mark_bar()
        .encode(
            x=alt.X("Retention Bucket:N", sort=None),
            y="Number of Teachers:Q",
            tooltip=["Retention Bucket", "Number of Teachers"],
        ),
        use_container_width=True,
    )

# ===== Trust vs Override =====
st.header("Trust vs Override (feedback behaviour)")
fb = logs[logs["event_lower"].isin(["updatefeedback", "deletefeedback", "createfeedback"])].copy()
trust_updates = int((fb["event_lower"] == "updatefeedback").sum())
overrides = int(fb["event_lower"].isin(["deletefeedback", "createfeedback"]).sum())
trust_ratio = round(trust_updates / (trust_updates + overrides), 3) if (trust_updates + overrides) > 0 else None

trust_df = pd.DataFrame(
    {
        "Metric": ["UpdateFeedback (minor edits)", "Delete+Create (override)", "Trust ratio (updates / updates + Overrides)"],
        "Value": [trust_updates, overrides, trust_ratio],
    }
)
c1, c2 = st.columns([1, 1])
with c1:
    st.dataframe(trust_df, use_container_width=True)
with c2:
    st.altair_chart(
        alt.Chart(pd.DataFrame({"Type": ["UpdateFeedback", "Override"], "Count": [trust_updates, overrides]}))
        .mark_bar()
        .encode(x="Type:N", y="Count:Q", tooltip=["Type", "Count"]),
        use_container_width=True,
    )

# ===== Rubric updates (count + denominator + %) =====
st.header("Rubric Updates (count + denominator + %)")
assess_create = logs[logs["event_lower"] == "createassessment"].copy()
# unique assessments created (prefer object_id, fallback details_id, else generic assessment_any); keep NaN for safe unique counts
assess_create["assessment_key"] = assess_create["object_id"].combine_first(
    assess_create["details_id"]
).combine_first(assess_create["assessment_any"])

assess_create = assess_create[assess_create["assessment_key"].notna()]

rubric_updates = logs[logs["event_lower"] == "updaterubric"].copy()
rubric_updates["assessment_ref"] = rubric_updates["assessment_any"].combine_first(
    rubric_updates["object_id"]
).combine_first(rubric_updates["details_id"])

total_assessments = int(assess_create["assessment_key"].nunique())
assess_with_rubric_update = int(rubric_updates["assessment_ref"].dropna().nunique())
rubric_update_events = int(rubric_updates.shape[0])
rubric_rate = round(assess_with_rubric_update / total_assessments * 100, 1) if total_assessments > 0 else None

rubric_df = pd.DataFrame(
    {
        "Metric": [
            "UpdateRubric events (count)",
            "Assessments created (unique)",
            "Assessments with ≥1 rubric update (unique)",
            "Rubric update rate (%)",
        ],
        "Value": [rubric_update_events, total_assessments, assess_with_rubric_update, rubric_rate],
    }
)
c1, c2 = st.columns([1, 1])
with c1:
    st.dataframe(rubric_df, use_container_width=True)
with c2:
    st.altair_chart(
        alt.Chart(
            pd.DataFrame(
                {
                    "Category": ["Assessments", "With Rubric Update"],
                    "Count": [total_assessments, assess_with_rubric_update],
                }
            )
        )
        .mark_bar()
        .encode(x="Category:N", y="Count:Q", tooltip=["Category", "Count"]),
        use_container_width=True,
    )

# ===== Workflow Timing (proxy: Create → UpdateSubmission → UpdateFeedback) =====
st.header("Workflow Timing (proxy)")
upd_sub = logs[logs["event_lower"] == "updatesubmission"].copy()
upd_sub["assessment_ref"] = upd_sub["assessment_any"].combine_first(upd_sub["object_id"]).combine_first(upd_sub["details_id"])
upd_sub["submission_ref"] = upd_sub["submission_any"]

upd_fb = logs[logs["event_lower"] == "updatefeedback"].copy()
upd_fb["submission_ref"] = upd_fb["submission_any"]

t_create = assess_create.groupby("assessment_key")["@timestamp"].min()
t_first_updatesub = upd_sub.groupby("assessment_ref")["@timestamp"].min()

# Map submissions to assessments via UpdateSubmission
sub_to_assess = upd_sub.dropna(subset=["assessment_ref", "submission_ref"])[["submission_ref", "assessment_ref"]].drop_duplicates()
upd_fb_mapped = upd_fb.merge(sub_to_assess, on="submission_ref", how="left")
t_first_updatefb = upd_fb_mapped.dropna(subset=["assessment_ref"]).groupby("assessment_ref")["@timestamp"].min()

rows = []
for aid in t_create.index:
    t0 = t_create.get(aid, pd.NaT)
    t1 = t_first_updatesub.get(aid, pd.NaT)
    t2 = t_first_updatefb.get(aid, pd.NaT)
    if pd.isna(t0):
        continue
    d1 = (t1 - t0).total_seconds() / 60 if pd.notna(t1) else None  # Create → first processed submission
    d2 = (t2 - t1).total_seconds() / 60 if (pd.notna(t1) and pd.notna(t2)) else None  # First processed → first feedback edit
    rows.append(
        {"assessment_key": str(aid), "mins_create_to_updatesub": d1, "mins_updatesub_to_updatefb": d2}
    )

steps_proxy = pd.DataFrame(rows)

col1, col2 = st.columns([1, 1])
with col1:
    st.subheader("Per-assessment timings (minutes)")
    st.dataframe(steps_proxy, use_container_width=True)
with col2:
    # Summary stats
    def summarize_series(series):
        s = pd.to_numeric(series.dropna(), errors="coerce")
        if s.empty:
            return {"min": None, "median": None, "mean": None, "max": None, "n": 0}
        return {
            "min": round(float(s.min()), 1),
            "median": round(float(s.median()), 1),
            "mean": round(float(s.mean()), 1),
            "max": round(float(s.max()), 1),
            "n": int(s.shape[0]),
        }

    summary = (
        pd.DataFrame(
            {
                "Create→UpdateSubmission": summarize_series(steps_proxy["mins_create_to_updatesub"]),
                "UpdateSubmission→UpdateFeedback": summarize_series(steps_proxy["mins_updatesub_to_updatefb"]),
            }
        )
        .T.reset_index()
        .rename(columns={"index": "Step"})
    )
    st.subheader("Summary (minutes)")
    st.dataframe(summary, use_container_width=True)

# ===== OCR / Submission Batch Sizes (per assessment) =====
st.header("OCR / Submission Batch Sizes (per assessment)")
subs_per_assess = (
    sub_to_assess.groupby("assessment_ref")["submission_ref"]
    .nunique()
    .reset_index(name="unique_submissions")
    .sort_values("unique_submissions", ascending=False)
)

c1, c2 = st.columns([1, 1])
with c1:
    st.dataframe(subs_per_assess, use_container_width=True)
with c2:
    if not subs_per_assess.empty:
        hist = (
            alt.Chart(subs_per_assess)
            .mark_bar()
            .encode(
                x=alt.X("unique_submissions:Q", bin=alt.Bin(maxbins=20), title="Submissions per assessment"),
                y="count():Q",
                tooltip=[alt.Tooltip("unique_submissions:Q", title="Subs/assessment"), alt.Tooltip("count():Q", title="Count")],
            )
        )
        st.altair_chart(hist, use_container_width=True)

# ===== Power users =====
st.header("Power Users")
logs["day_local"] = logs["date_local"]
user_days = logs.groupby("email")["day_local"].nunique().rename("distinct_days")
graded_counts = logs.loc[logs["event_lower"] == "gradesubmission"].groupby("email").size().rename("graded_count")
refined_counts = logs[logs["event_lower"].isin(["updaterubric", "updatefeedback"])].groupby("email").size().rename("refined_count")
total_events = logs.groupby("email").size().rename("total_events")
user_school = logs.groupby("email")["school"].agg(lambda x: x.dropna().iloc[0] if (x.dropna().shape[0] > 0) else None)

per_account = (
    pd.concat([user_school, graded_counts, refined_counts, user_days, total_events], axis=1)
    .fillna(0)
    .reset_index()
)
per_account[["graded_count", "refined_count", "distinct_days", "total_events"]] = per_account[
    ["graded_count", "refined_count", "distinct_days", "total_events"]
].astype(int)

if show_post_onboarding_only_power:
    post_only = logs[logs["date_local"] > logs["first_login_date"]]
    post_days = post_only.groupby("email")["date_local"].nunique().rename("distinct_days_post")
    per_account = per_account.merge(post_days, on="email", how="left").fillna({"distinct_days_post": 0})
    per_account["distinct_days_eval"] = per_account["distinct_days_post"].astype(int)
else:
    per_account["distinct_days_eval"] = per_account["distinct_days"]

power = per_account.sort_values(
    ["graded_count", "refined_count", "distinct_days_eval", "total_events"], ascending=[False, False, False, False]
).head(10)
st.dataframe(
    power.rename(
        columns={
            "email": "Email",
            "school": "School",
            "graded_count": "Graded",
            "refined_count": "Refined",
            "distinct_days_eval": "Distinct days (eval)",
            "total_events": "Total events",
        }
    ),
    use_container_width=True,
)

# ===== Errors =====
# ===== st.header("Errors by Event Type")
# =====errors = logs[logs["error_message"].notna() & (logs["error_message"].astype(str).str.len() > 0)]
# =====err_counts = errors["event_lower"].value_counts().reset_index().rename(columns={"index": "event", "event_lower": "error_count"})

#======c1, c2 = st.columns([1, 1])
#======with c1:
#=======    st.dataframe(err_counts, use_container_width=True)
#=======with c2:
#=======    if not err_counts.empty:
#======        st.altair_chart(
#======            alt.Chart(err_counts).mark_bar().encode(x="event:N", y="error_count:Q", tooltip=["event", "error_count"]),
#======            use_container_width=True,
#======        )

st.caption(
    "Notes: Times are computed from UTC logs shifted by the selected timezone. "
    "Retention uses the first login day as onboarding (if enabled). "
    "Step-wise timings use a robust proxy (Create→UpdateSubmission→UpdateFeedback) based on available IDs. "
    "OCR batch size uses unique submissions per assessment via UpdateSubmission. "
    "School mapping is hardcoded in the script."
)
