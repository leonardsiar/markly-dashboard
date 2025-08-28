
import streamlit as st
import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt

st.set_page_config(page_title="Mark.ly Pilot Dashboard (Event Logs)", layout="wide")

st.title("📊 Mark.ly Pilot Dashboard — Event-Level Logs")
st.caption("Upload your event-level Excel logs (e.g., `user_logs_annotated.xlsx`). Analyse teacher adoption, funnel progression, feature validation, and true unique active days.")

# --- File uploader ---
uploaded = st.file_uploader("Upload Excel file (.xlsx)", type=["xlsx"])

# Verified milestone events (strictly from your dataset)
KNOWN_MILESTONES = [
    "UserLogin",
    "CreateClass",
    "CreateAssessment",
    "CreateMarkingScheme",
    "ParseAssessment",
    "ParseMarkingScheme",
    "OCRDocument",
    "ParseSubmission",
    "GradeSubmission",
    "CreateFeedback",
    "UpdateFeedback",
    "UpdateRubric",
    "UpdateQuestion",
    "UpdateSubmission",
    "DeleteClass",
    "DeleteFeedback",
]

def try_parse_message_payload(s):
    """If '@message' contains JSON, parse it to backfill missing fields."""
    if not isinstance(s, str):
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}

def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Rename common variants
    colmap = {}
    for c in df.columns:
        cl = c.strip().lower()
        if cl in {"timestamp", "@timestamp"}:
            colmap[c] = "@timestamp"
        elif cl in {"message", "@message"}:
            colmap[c] = "@message"
    if colmap:
        df = df.rename(columns=colmap)
    return df

def apply_sidebar_filters(df: pd.DataFrame):
    with st.sidebar:
        st.header("Filters")

        # User type filter (if present)
        if "user_type" in df.columns:
            types = sorted([str(x) for x in df["user_type"].dropna().unique().tolist()])
            default_types = [t for t in types if t.lower() == "teacher"] or types
            sel_types = st.multiselect("User type", types, default=default_types)
            df = df[df["user_type"].astype(str).isin(sel_types)]

        # School filter (if present)
        if "School" in df.columns:
            schools = sorted([s for s in df["School"].dropna().unique().tolist()])
            sel_schools = st.multiselect("School", schools, default=schools if schools else [])
            if sel_schools:
                df = df[df["School"].isin(sel_schools)]

        # Teacher filter
        emails = sorted(df["email"].dropna().unique().tolist())
        sel_emails = st.multiselect("Teacher (email)", emails, default=[])
        if sel_emails:
            df = df[df["email"].isin(sel_emails)]

        # Events filter
        present_events = sorted(df["event_name"].dropna().unique().tolist())
        milestone_defaults = [m for m in KNOWN_MILESTONES if m in present_events] or present_events
        sel_events = st.multiselect("Events", present_events, default=milestone_defaults)
        if sel_events:
            df = df[df["event_name"].isin(sel_events)]

    return df

if uploaded is None:
    st.info("Upload an event-level Excel file to begin. Expected columns include '@timestamp', 'email', 'event_name'. Optional: 'user_type' (filter to teachers), 'School'. If '@message' contains JSON, the app will try to backfill missing fields.")
else:
    # --- Load file ---
    try:
        df = pd.read_excel(uploaded)
    except Exception as e:
        st.error(f"Could not read Excel file: {e}")
        st.stop()

    # --- Normalise & backfill ---
    df = normalise_columns(df)

    if "@message" in df.columns:
        payload = df["@message"].apply(try_parse_message_payload)
        if "event_name" not in df.columns:
            df["event_name"] = payload.apply(lambda x: x.get("event_name"))
        if "email" not in df.columns:
            df["email"] = payload.apply(lambda x: x.get("email"))
        if "School" not in df.columns:
            df["School"] = payload.apply(lambda x: x.get("School"))

    # Validate required columns
    missing = [c for c in ["@timestamp", "event_name", "email"] if c not in df.columns]
    if missing:
        st.error(f"Missing required column(s): {', '.join(missing)}")
        st.stop()

    # Timestamps
    df["@timestamp"] = pd.to_datetime(df["@timestamp"], errors="coerce")
    df = df.dropna(subset=["@timestamp"])
    df["date"] = df["@timestamp"].dt.date

    # Apply filters in sidebar
    df = apply_sidebar_filters(df)

    if df.empty:
        st.warning("No data after filters. Adjust filters on the left.")
        st.stop()

    # --- Summary ---
    st.subheader("Summary")
    total_events = int(len(df))
    total_teachers = int(df["email"].nunique())
    events_by_teacher = df.groupby("email")["event_name"].count()
    active_teachers = int((events_by_teacher > 0).sum())
    median_events = int(events_by_teacher.median()) if not events_by_teacher.empty else 0
    mean_events = float(events_by_teacher.mean()) if not events_by_teacher.empty else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Events", total_events)
    c2.metric("Total Teachers", total_teachers)
    c3.metric("Active Teachers", active_teachers)
    c4.metric("Avg Events / Teacher", f"{mean_events:.1f}")
    c5.metric("Median Events / Teacher", f"{median_events}")

    # Unique active days
    days_per_teacher = df.groupby("email")["date"].nunique()
    repeat_count = int(days_per_teacher.ge(2).sum())
    pct_repeat = (repeat_count / total_teachers * 100.0) if total_teachers else 0.0

    c6, c7 = st.columns(2)
    with c6:
        st.metric("Teachers with ≥2 Active Days", f"{repeat_count} ({pct_repeat:.1f}%)")
    with c7:
        fig = plt.figure(figsize=(6,4))
        day_counts = days_per_teacher.value_counts().sort_index()
        plt.bar(day_counts.index, day_counts.values)
        plt.xlabel("Unique Active Days (per Teacher)")
        plt.ylabel("Teachers")
        plt.title("Distribution of Unique Active Days")
        st.pyplot(fig)

    st.divider()

    # --- Funnel ---
    st.subheader("Funnel Progression (Teachers Reaching Each Stage)")
    base = df["email"].nunique()
    milestones = [m for m in KNOWN_MILESTONES if m in df["event_name"].unique()]
    funnel_counts = []
    for m in milestones:
        n = int(df.loc[df["event_name"] == m, "email"].nunique())
        pct = round((n / base * 100.0), 1) if base else 0.0
        funnel_counts.append({"Milestone": m, "Teachers": n, "% of Teachers": pct})
    funnel_df = pd.DataFrame(funnel_counts)

    st.dataframe(funnel_df, use_container_width=True)

    fig2 = plt.figure(figsize=(8,5))
    plt.barh(funnel_df["Milestone"], funnel_df["Teachers"])
    plt.xlabel("Teachers")
    plt.title("Funnel: Teachers Reaching Each Milestone")
    st.pyplot(fig2)

    st.caption("Each milestone counts a teacher once if they performed that event at least once within the current filters.")

    st.divider()

    # --- Feature validation ---
    st.subheader("Feature Validation")
    if "School" in df.columns and df["School"].notna().any():
        heat = df.groupby(["School", "event_name"]).size().unstack(fill_value=0)
        st.dataframe(heat, use_container_width=True)

        st.markdown("**Stacked Events per School**")
        fig3 = plt.figure(figsize=(10,5))
        schools_order = heat.index.tolist()
        events_order = heat.columns.tolist()
        bottoms = np.zeros(len(schools_order))
        for ev in events_order:
            vals = heat[ev].values
            plt.bar(schools_order, vals, bottom=bottoms, label=ev)
            bottoms += vals
        plt.ylabel("Event Count")
        plt.xticks(rotation=45, ha="right")
        plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left", frameon=False)
        st.pyplot(fig3)
    else:
        counts = df["event_name"].value_counts().sort_values(ascending=False)
        st.dataframe(counts.to_frame("Event Count"))

        fig4 = plt.figure(figsize=(8,5))
        plt.bar(counts.index, counts.values)
        plt.xticks(rotation=45, ha="right")
        plt.ylabel("Event Count")
        plt.title("Overall Event Counts")
        st.pyplot(fig4)

    st.divider()

    # --- Teacher-level breakdown ---
    st.subheader("Teacher-Level Breakdown")
    pivot = df.groupby(["email", "event_name"]).size().unstack(fill_value=0).sort_index()
    if "School" in df.columns and df["School"].notna().any():
        school_map = df[["email","School"]].dropna().drop_duplicates().set_index("email")["School"].to_dict()
        pivot.insert(0, "School", pivot.index.map(lambda e: school_map.get(e, "")))
        pivot = pivot.reset_index().set_index(["School","email"])
    st.dataframe(pivot, use_container_width=True)

    st.divider()

    # --- Recency & trend ---
    st.subheader("Recency & Activity Over Time")
    cA, cB = st.columns(2)
    with cA:
        st.markdown("**Events per Day (Filtered Data)**")
        daily = df.groupby("date").size()
        fig5 = plt.figure(figsize=(6,4))
        plt.plot(daily.index.astype("datetime64[D]"), daily.values)
        plt.xlabel("Date")
        plt.ylabel("Events")
        plt.title("Daily Event Volume")
        st.pyplot(fig5)
    with cB:
        st.markdown("**Top Teachers by Event Count**")
        topn = events_by_teacher.sort_values(ascending=False).head(10)
        fig6 = plt.figure(figsize=(6,4))
        plt.barh(topn.index, topn.values)
        plt.xlabel("Events")
        plt.title("Top 10 Teachers by Events")
        st.pyplot(fig6)

    st.success("Dashboard ready. Use the left filters to slice by school, teacher, and event types.")
