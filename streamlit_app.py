
import streamlit as st
import pandas as pd
import numpy as np
import json
import pytz
from datetime import timedelta

import altair as alt  # Use Altair instead of Matplotlib

# ==============================
# Page config
# ==============================
st.set_page_config(page_title="Mark.ly Pilot Dashboard", layout="wide")

SGT = pytz.timezone("Asia/Singapore")

# ==============================
# Event mappings (product truth)
# ==============================
EVENTS_LOGIN = {"UserLogin"}
EVENTS_CREATE_CLASS = {"CreateClass"}                        # one-off setup
EVENTS_CREATE_ASSESSMENT = {"CreateAssessment"}              # repeatable activation-of-use
EVENTS_GRADE = {"GradeSubmission", "GradeSPG", "GradeOverall", "GradeLORMS"}
# Refinement: exclude UpdateRubric/UpdateQuestion
EVENTS_REFINE = {"UpdateFeedback", "UpdateSubmission", "CreateFeedback", "DeleteFeedback"}
# Ignore for user-activity purposes
EVENTS_IGNORE = {"ParseSubmission", "ParseAssessment", "ParseMarkingScheme", "OCRDocument"}

# Distinct "activity events" used for retention/zero-day
ACTIVITY_EVENTS = EVENTS_LOGIN | EVENTS_CREATE_CLASS | EVENTS_CREATE_ASSESSMENT | EVENTS_GRADE | EVENTS_REFINE

# ==============================
# Helpers
# ==============================
def extract_json(msg: str):
    if not isinstance(msg, str):
        return None
    i = msg.find("{"); j = msg.rfind("}")
    if i == -1 or j == -1 or j <= i:
        return None
    cand = msg[i:j+1]
    try:
        return json.loads(cand)
    except Exception:
        try:
            return json.loads(cand.replace("''", '"').replace('\\"', '"'))
        except Exception:
            return None

def load_logs(file) -> pd.DataFrame:
    # Supports Excel or CSV
    if getattr(file, "name", "").lower().endswith((".xlsx",".xls")):
        df_raw = pd.read_excel(file, sheet_name=0)
    else:
        df_raw = pd.read_csv(file)
    df = df_raw.copy()
    df.columns = [c.strip() for c in df.columns]
    # timestamps
    ts_col = "@timestamp" if "@timestamp" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
    if not ts_col:
        st.error("No '@timestamp' or 'timestamp' column found in logs.")
        return pd.DataFrame()
    df["ts_utc"] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    df["ts_sgt"] = df["ts_utc"].dt.tz_convert(SGT)

    # parse JSON payload
    parsed = df["@message"].apply(extract_json) if "@message" in df.columns else pd.Series([None]*len(df))
    def getd(obj,*ks):
        cur = obj
        for k in ks:
            if isinstance(cur, dict) and k in cur: cur = cur[k]
            else: return None
        return cur

    # event + user
    if "event_name" not in df.columns:
        df["event_name"] = None
    df["event_name"] = df["event_name"].fillna(parsed.apply(lambda d: getd(d, "event_name")))
    if "user_id" not in df.columns:
        df["user_id"] = None
    df["user_id_msg"] = parsed.apply(lambda d: getd(d, "user_id"))
    df["user_id"] = df["user_id"].fillna(df["user_id_msg"])

    # details we care about
    df["det"] = parsed.apply(lambda d: d.get("details") if isinstance(d, dict) else None)
    for key in ["id","assessment","answer","name","pdf_file","status","updated_at",
                "comments","comments_generated","annotation_colour","excerpt_start","excerpt_end",
                "rubric_type","score","answers_count","total_score"]:
        df[f"det_{key}"] = df["det"].apply(lambda d: d.get(key) if isinstance(d, dict) else None)

    # keep essential rows
    df = df[df["event_name"].notna() & df["user_id"].notna() & df["ts_sgt"].notna()].copy()
    # numeric excerpt bounds
    for col in ["det_excerpt_start","det_excerpt_end"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def load_roster(file) -> pd.DataFrame:
    # Excel or CSV
    if getattr(file, "name", "").lower().endswith((".xlsx",".xls")):
        r = pd.read_excel(file)
    else:
        r = pd.read_csv(file)
    r.columns = [c.strip().lower().replace(" ","_") for c in r.columns]
    needed = {"user_id","email","school"}
    if not needed.issubset(set(r.columns)):
        st.error(f"Roster missing required columns: {needed - set(r.columns)}")
        return pd.DataFrame()
    r["user_id"] = r["user_id"].astype(str)
    r["email"] = r["email"].astype(str).str.strip().str.lower()
    # normalise school labels
    repl = {
        "acsi":"ACSI", "anglican":"Anglican", "bartley":"Bartley",
        "ngee ann":"Ngee Ann","northlight":"Northlight","pei hwa":"Pei Hwa",
        "st andrew":"St Andrew","st. andrew":"St Andrew","st andrew's":"St Andrew","st. andrew's":"St Andrew"
    }
    r["school"] = r["school"].astype(str).str.strip().apply(lambda s: repl.get(s.lower(), s))
    if "display_name" not in r.columns:
        r["display_name"] = r["email"].str.split("@").str[0]
    return r[["user_id","email","school","display_name"]]

def default_onboarding_windows():
    rows = [
        {"school":"ACSI","start_sgt":"2025-08-12 14:30","end_sgt":"2025-08-12 15:30"},
        {"school":"Anglican","start_sgt":"2025-08-12 16:00","end_sgt":"2025-08-12 17:00"},
        {"school":"Ngee Ann","start_sgt":"2025-08-13 15:30","end_sgt":"2025-08-13 16:30"},
        {"school":"Bartley","start_sgt":"2025-08-13 15:30","end_sgt":"2025-08-13 16:30"},
        {"school":"Northlight","start_sgt":"2025-08-18 15:00","end_sgt":"2025-08-18 16:00"},
        {"school":"St Andrew","start_sgt":"2025-08-20 10:30","end_sgt":"2025-08-20 11:30"},
        {"school":"Pei Hwa","start_sgt":"2025-08-20 15:30","end_sgt":"2025-08-20 16:30"},
    ]
    return pd.DataFrame(rows)

def load_onboarding(file_or_df) -> pd.DataFrame:
    if file_or_df is None:
        return default_onboarding_windows()
    if hasattr(file_or_df, "name"):
        if file_or_df.name.lower().endswith((".xlsx",".xls")):
            o = pd.read_excel(file_or_df)
        else:
            o = pd.read_csv(file_or_df)
    else:
        o = file_or_df
    o.columns = [c.strip().lower() for c in o.columns]
    for c in ["school","start_sgt","end_sgt"]:
        if c not in o.columns:
            st.error("Onboarding windows need columns: school, start_sgt, end_sgt")
            return default_onboarding_windows()
    return o

def apply_onboarding_filter(df: pd.DataFrame, roster: pd.DataFrame, windows_df: pd.DataFrame, exclude=True) -> pd.DataFrame:
    if not exclude:
        return df.merge(roster[["user_id","school"]], on="user_id", how="left")
    df2 = df.merge(roster[["user_id","school"]], on="user_id", how="left")
    windows_df = windows_df.copy()
    windows_df["start_sgt"] = pd.to_datetime(windows_df["start_sgt"]).dt.tz_localize(SGT)
    windows_df["end_sgt"] = pd.to_datetime(windows_df["end_sgt"]).dt.tz_localize(SGT)
    mask = pd.Series(True, index=df2.index)
    for _, row in windows_df.iterrows():
        sch = row["school"]
        s = row["start_sgt"]; e = row["end_sgt"]
        m = (df2["school"]==sch) & (df2["ts_sgt"]>=s) & (df2["ts_sgt"]<=e)
        mask &= ~m
    return df2.loc[mask].copy()

def users_who_did(df: pd.DataFrame, events: set) -> set:
    return set(df.loc[df["event_name"].isin(events), "user_id"].astype(str).unique())

def compute_funnel(df: pd.DataFrame, cohort_ids: set) -> dict:
    s_login = users_who_did(df, EVENTS_LOGIN) & cohort_ids
    s_create_assess = users_who_did(df, EVENTS_CREATE_ASSESSMENT) & cohort_ids
    s_grade = users_who_did(df, EVENTS_GRADE) & cohort_ids
    s_refine = users_who_did(df, EVENTS_REFINE) & cohort_ids

    activated = s_login & s_create_assess
    graded_after_activation = activated & s_grade
    refined_after_grading = graded_after_activation & s_refine

    return {
        "cohort_size": len(cohort_ids),
        "logged_in": len(s_login),
        "created_assessment": len(s_create_assess),
        "graded_after_activation": len(graded_after_activation),
        "refined_after_grading": len(refined_after_grading)
    }

def compute_retention(df: pd.DataFrame, roster: pd.DataFrame) -> pd.DataFrame:
    df_a = df[df["event_name"].isin(ACTIVITY_EVENTS)].copy()
    df_a["date_sgt"] = df_a["ts_sgt"].dt.tz_convert(SGT).dt.date
    days = df_a.groupby("user_id")["date_sgt"].nunique()
    m = roster[["user_id","email","school","display_name"]].copy()
    m["active_days"] = m["user_id"].map(days).fillna(0).astype(int)
    def bucket(n):
        if n == 0: return "0 days"
        if n == 1: return "1 day"
        if n <= 3: return "2–3 days"
        return "4+ days"
    m["bucket"] = m["active_days"].apply(bucket)
    return m

def compute_trust_ratio(df: pd.DataFrame, window_minutes=5):
    # Trust: UpdateFeedback + UpdateSubmission
    TRUST_EVENTS = {"UpdateFeedback","UpdateSubmission"}
    DELETE = "DeleteFeedback"
    CREATE = "CreateFeedback"

    trust_counts = df[df["event_name"].isin(TRUST_EVENTS)].groupby("user_id")["event_name"].count().rename("trust_events")
    del_counts = df[df["event_name"]==DELETE].groupby("user_id")["event_name"].count().rename("delete_events")
    create_counts = df[df["event_name"]==CREATE].groupby("user_id")["event_name"].count().rename("create_events")

    # Replacement detection: Delete -> Create within window & overlapping excerpt
    repl = []
    grp = df[df["event_name"].isin({DELETE, CREATE})].sort_values("ts_sgt").groupby("user_id")
    for uid, g in grp:
        rows = g.reset_index(drop=True)
        for i, r in rows.iterrows():
            if r["event_name"] != DELETE:
                continue
            t0 = r["ts_sgt"]; s0, e0 = r["det_excerpt_start"], r["det_excerpt_end"]
            tmax = t0 + pd.Timedelta(minutes=window_minutes)
            cand = rows[(rows.index > i) & (rows["event_name"]==CREATE) & (rows["ts_sgt"]<=tmax)]
            for _, c in cand.iterrows():
                s1, e1 = c["det_excerpt_start"], c["det_excerpt_end"]
                if (pd.notna(s0) and pd.notna(e0) and pd.notna(s1) and pd.notna(e1)) and not (e0 < s1 or e1 < s0):
                    repl.append({"user_id": uid, "t_delete": t0, "t_create": c["ts_sgt"]})
                    break

    repl_df = pd.DataFrame(repl)
    repl_counts = repl_df.groupby("user_id").size().rename("replacement_events") if not repl_df.empty else pd.Series(dtype=int, name="replacement_events")

    all_users = pd.Index(df["user_id"].astype[str].unique())
    out = pd.DataFrame(index=all_users)
    out = out.join(trust_counts, how="left").join(del_counts, how="left").join(create_counts, how="left").join(repl_counts, how="left")
    out = out.fillna(0).astype(int)
    out["augment_events"] = (out["create_events"] - out["replacement_events"]).clip(lower=0)
    out["trust_num"] = out["trust_events"]
    out["trust_den"] = out["trust_events"] + out["delete_events"] + out["replacement_events"]
    out["trust_ratio_practical"] = np.where(out["trust_den"]>0, out["trust_num"]/out["trust_den"], np.nan)
    return out.reset_index(names="user_id")

def centred_funnel_chart_altair(funnel_counts: dict):
    # Build a centered horizontal bar chart using x/x2 encodings
    steps = ["Logged in","Created assessment","Graded","Refined"]
    values = [
        funnel_counts["logged_in"],
        funnel_counts["created_assessment"],
        funnel_counts["graded_after_activation"],
        funnel_counts["refined_after_grading"],
    ]
    if not values or max(values)==0:
        st.info("No funnel data to display yet.")
        return
    max_v = max(values)
    widths = [v/max_v for v in values]
    data = pd.DataFrame({
        "step": steps,
        "value": values,
        "width": widths,
        "left": [-w/2 for w in widths],
        "right": [w/2 for w in widths],
    })
    chart = alt.Chart(data).mark_bar().encode(
        x=alt.X("left:Q", title=None, scale=alt.Scale(domain=[-1, 1])),
        x2="right:Q",
        y=alt.Y("step:N", sort=steps, title=None),
        tooltip=[alt.Tooltip("step:N"), alt.Tooltip("value:Q")]
    ).properties(height=140)
    labels = alt.Chart(data).mark_text(align="left", dx=10).encode(
        x=alt.value(0),  # center line
        y=alt.Y("step:N", sort=steps),
        text="value:Q"
    )
    st.altair_chart(chart + labels, use_container_width=True)

# ==============================
# UI
# ==============================

st.title("Mark.ly Pilot Dashboard")
st.caption("Teacher-only, onboarding excluded by default. SGT timezone.")

with st.sidebar:
    st.header("Inputs")
    logs_file = st.file_uploader("Upload logs (Excel/CSV)", type=["xlsx","xls","csv"])
    roster_file = st.file_uploader("Upload teacher roster (Excel/CSV)", type=["xlsx","xls","csv"])
    onboard_file = st.file_uploader("Upload onboarding windows (optional)", type=["xlsx","xls","csv"])
    exclude_onboarding = st.checkbox("Exclude onboarding sessions", value=True)
    repl_window = st.number_input("Replacement detection window (minutes)", min_value=1, max_value=60, value=5, step=1)

ready = logs_file is not None and roster_file is not None

if not ready:
    st.info("Please upload the **logs** and the **teacher roster** to begin.")
    st.stop()

# Load data
df_logs = load_logs(logs_file)
roster = load_roster(roster_file)
onboard = load_onboarding(onboard_file)

if df_logs.empty or roster.empty:
    st.stop()

# Filter to teacher cohort
cohort_ids = set(roster["user_id"].astype(str).unique())
df_logs["user_id"] = df_logs["user_id"].astype(str)
df_logs = df_logs[df_logs["user_id"].isin(cohort_ids)].copy()

# Apply onboarding filter
df_logs = apply_onboarding_filter(df_logs, roster, onboard, exclude=exclude_onboarding)

# KPI header
colA, colB, colC, colD, colE = st.columns(5)
with colA:
    st.metric("Teacher cohort", len(cohort_ids))
with colB:
    active_any = df_logs[df_logs["event_name"].isin(ACTIVITY_EVENTS)]["user_id"].nunique()
    st.metric("Teachers with any activity", active_any)
with colC:
    s_login = users_who_did(df_logs, EVENTS_LOGIN)
    st.metric("Logged in", len(s_login))
with colD:
    s_create_class = users_who_did(df_logs, EVENTS_CREATE_CLASS)
    st.metric("Created class (one-off)", len(s_create_class))
with colE:
    s_create_assess = users_who_did(df_logs, EVENTS_CREATE_ASSESSMENT)
    st.metric("Created assessment", len(s_create_assess))

st.divider()

# Funnel (by user, centred)
st.subheader("Funnel (by teacher)")
funnel_counts = compute_funnel(df_logs, cohort_ids)
centred_funnel_chart_altair(funnel_counts)
st.caption("Stages: Logged in → Created assessment → Graded → Refined. Counts are unique teachers at each stage; bars are centered for visual clarity.")

st.divider()

# Retention
st.subheader("Retention (days active, SGT)")
ret = compute_retention(df_logs, roster)
bucket_counts = ret["bucket"].value_counts().reindex(["0 days","1 day","2–3 days","4+ days"]).fillna(0).astype(int)
ret_table = bucket_counts.reset_index()
ret_table.columns = ["Bucket","Teachers"]
st.dataframe(ret_table, use_container_width=True)
st.caption("Activity includes: Login, Create Class, Create Assessment, any Grade*, and refinement events (UpdateFeedback, UpdateSubmission, CreateFeedback, DeleteFeedback).")

# Zero-day teachers
zero_day = ret[ret["active_days"]==0][["display_name","email","school"]]
with st.expander("Zero-day teachers"):
    st.dataframe(zero_day, use_container_width=True)

st.divider()

# Trust ratio
st.subheader("Trust ratio (practical)")
trust = compute_trust_ratio(df_logs, window_minutes=int(repl_window))
trust_join = trust.merge(roster, on="user_id", how="left")
valid_trust = trust_join[trust_join["trust_den"]>0].copy()
overall_trust = float(valid_trust["trust_ratio_practical"].mean()) if not valid_trust.empty else float("nan")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Teachers with trust/distrust activity", len(valid_trust))
with col2:
    st.metric("Overall trust ratio (macro-avg)", "n/a" if np.isnan(overall_trust) else f"{overall_trust:.3f}")
with col3:
    st.metric("Replacement window (min)", int(repl_window))

st.dataframe(
    trust_join.sort_values("trust_ratio_practical", ascending=False)[
        ["display_name","email","school","trust_events","delete_events","replacement_events","augment_events","trust_ratio_practical"]
    ],
    use_container_width=True
)

st.divider()

# School comparison
st.subheader("School comparison")
login_users = users_who_did(df_logs, EVENTS_LOGIN)
create_users = users_who_did(df_logs, EVENTS_CREATE_ASSESSMENT)
grade_users = users_who_did(df_logs, EVENTS_GRADE)
refine_users = users_who_did(df_logs, EVENTS_REFINE)

school_df = roster.copy()
school_df["activated"] = school_df["user_id"].isin(login_users & create_users)
school_df["graded"] = school_df["user_id"].isin(grade_users)
school_df["refined"] = school_df["user_id"].isin(refine_users)
school_summary = school_df.groupby("school").agg(
    teachers=("user_id","nunique"),
    activated=("activated","sum"),
    graded=("graded","sum"),
    refined=("refined","sum"),
).reset_index()
st.dataframe(school_summary.sort_values("school"), use_container_width=True)

st.divider()

# Power users (by qualifying activity events)
st.subheader("Power users")
qual_events = df_logs[df_logs["event_name"].isin(ACTIVITY_EVENTS)].copy()
top_users = qual_events.groupby("user_id")["event_name"].count().sort_values(ascending=False).head(10).reset_index()
top_users = top_users.merge(roster, on="user_id", how="left")
top_users.rename(columns={"event_name":"activity_events"}, inplace=True)
st.dataframe(top_users[["display_name","email","school","activity_events"]], use_container_width=True)

# Daily usage (logins & grading)
st.subheader("Usage over time (daily)")
daily = df_logs.copy()
daily["date_sgt"] = daily["ts_sgt"].dt.tz_convert(SGT).dt.date
daily_login = daily[daily["event_name"].isin(EVENTS_LOGIN)].groupby("date_sgt")["event_name"].count().rename("logins")
daily_grading = daily[daily["event_name"].isin(EVENTS_GRADE)].groupby("date_sgt")["event_name"].count().rename("grading_events")
daily_usage = pd.concat([daily_login, daily_grading], axis=1).fillna(0).astype(int).reset_index().sort_values("date_sgt")

st.line_chart(daily_usage.set_index("date_sgt")[["logins"]])
st.line_chart(daily_usage.set_index("date_sgt")[["grading_events"]])
st.caption("Charts use daily aggregation in SGT.")
