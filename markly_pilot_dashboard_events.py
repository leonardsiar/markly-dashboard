import streamlit as st
import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt
import plotly.express as px  # Add at the top if not already imported

st.set_page_config(page_title="Mark.ly Pilot Dashboard (Event Logs)", layout="wide")

st.title("📊 Mark.ly Pilot Dashboard — Event-Level Logs")
st.caption(
    "Upload your event-level Excel logs (e.g., `user_logs_annotated.xlsx`). "
    "Analyse teacher adoption, funnel progression, feature validation, and true unique active days."
)

# --- File uploader ---
uploaded = st.file_uploader("Upload Excel file (.xlsx)", type=["xlsx"])

# Milestones strictly from your dataset
KNOWN_MILESTONES = [
    "UserLogin", "CreateClass", "CreateAssessment", "CreateMarkingScheme",
    "ParseAssessment", "ParseMarkingScheme", "OCRDocument",
    "ParseSubmission", "GradeSubmission",
    "CreateFeedback", "UpdateFeedback",
    "UpdateRubric", "UpdateQuestion", "UpdateSubmission",
    "DeleteClass", "DeleteFeedback",
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
    """Rename common variants to canonical names."""
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

# ---------- New filter UX: collect inputs first, apply on button press ----------
def get_filter_inputs(df: pd.DataFrame):
    with st.sidebar:
        st.header("Filters")

        # User type (default to teachers if present)
        if "user_type" in df.columns:
            types = sorted([str(x) for x in df["user_type"].dropna().unique().tolist()])
            default_types = [t for t in types if t.lower() == "teacher"] or types
            sel_types = st.multiselect("User type", types, default=default_types, key="flt_user_types")
        else:
            sel_types = None

        # School (robust empty handling)
        if "School" in df.columns:
            schools_series = df["School"].dropna().astype(str).str.strip()
            schools = sorted(s for s in schools_series.unique().tolist() if s)
            if not schools:
                st.info(
                    "No School values found. This usually means the uploaded file lacks a 'School' column, "
                    "the email→school mapping didn’t match any rows, or values are blank."
                )
                sel_schools = []
            else:
                sel_schools = st.multiselect("School", schools, default=schools, key="flt_schools")
        else:
            sel_schools = None

        # Teacher
        emails = sorted(df["email"].dropna().astype(str).unique().tolist())
        sel_emails = st.multiselect("Teacher (email)", emails, default=[], key="flt_emails")

        # Events
        present_events = sorted(df["event_name"].dropna().unique().tolist())
        milestone_defaults = [m for m in KNOWN_MILESTONES if m in present_events] or present_events
        sel_events = st.multiselect("Events", present_events, default=milestone_defaults, key="flt_events")

        # Buttons
        c1, c2 = st.columns(2)
        apply_clicked = c1.button("Apply filters", type="primary")
        reset_clicked = c2.button("Reset")

        with st.expander("Debug: counts by School", expanded=False):
            if "School" in df.columns:
                st.write(df["School"].value_counts(dropna=True))
            else:
                st.write("No 'School' column present.")

    # Handle reset
    if reset_clicked:
        for k in ("flt_user_types", "flt_schools", "flt_emails", "flt_events", "filters_applied"):
            if k in st.session_state:
                del st.session_state[k]

    # Mark filters applied
    if apply_clicked:
        st.session_state["filters_applied"] = True

    # Return raw selections (not yet applied)
    return {
        "user_types": sel_types,
        "schools": sel_schools,
        "emails": sel_emails,
        "events": sel_events,
        "apply": st.session_state.get("filters_applied", False),
    }

def apply_selected_filters(df: pd.DataFrame, sel: dict) -> pd.DataFrame:
    """Apply the selected filters to a copy of df only after Apply is pressed."""
    dff = df.copy()

    if not sel.get("apply", False):
        return dff  # return unfiltered until Apply is pressed

    if "user_type" in dff.columns and sel.get("user_types") is not None:
        dff = dff[dff["user_type"].astype(str).isin(sel["user_types"])]

    if "School" in dff.columns and sel.get("schools") is not None and len(sel["schools"]) > 0:
        dff = dff[dff["School"].isin(sel["schools"])]

    if sel.get("emails"):
        dff = dff[dff["email"].isin(sel["emails"])]

    if sel.get("events"):
        dff = dff[dff["event_name"].isin(sel["events"])]

    return dff
# -------------------------------------------------------------------------------

if uploaded is None:
    st.info(
        "Upload an event-level Excel file to begin. Expected columns: '@timestamp', 'email', 'event_name'. "
        "Optional: 'user_type', 'School'. If '@message' contains JSON, the app will try to backfill fields."
    )
else:
    # --- Load file ---
    try:
        df = pd.read_excel(uploaded)
    except Exception as e:
        st.error(f"Could not read Excel file: {e}")
        st.stop()

    # --- Normalise & backfill ---
    df = normalise_columns(df)

    # If a 'school' column exists in a different case, normalise it to 'School'
    school_like_cols = [c for c in df.columns if c.strip().lower() == "school"]
    if school_like_cols and "School" not in df.columns:
        df.rename(columns={school_like_cols[0]: "School"}, inplace=True)

    if "@message" in df.columns:
        payload = df["@message"].apply(try_parse_message_payload)
        if "event_name" not in df.columns:
            df["event_name"] = payload.apply(lambda x: x.get("event_name"))
        if "email" not in df.columns:
            df["email"] = payload.apply(lambda x: x.get("email"))
        if "School" not in df.columns:
            df["School"] = payload.apply(lambda x: x.get("School"))

    # Clean empty strings in School
    if "School" in df.columns:
        df["School"] = df["School"].astype(str).str.strip()
        df.loc[df["School"].eq("") | df["School"].str.lower().eq("nan"), "School"] = np.nan

    # --- Email → School mapping (after backfill, before filters) ---
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

    # Primary: exact email mapping
    email_lc = df["email"].astype(str).str.strip().str.lower()
    mapped_school = email_lc.map(EMAIL_TO_SCHOOL)

    # Secondary: simple domain heuristic (optional)
    domain_map = {
        "acsindep.edu.sg": "ACSI",
        # add more domains here if helpful
    }
    email_domain = email_lc.str.split("@").str[-1]
    domain_school = email_domain.map(domain_map)

    # Combine preference: mapped_school > existing School > domain_school
    if "School" not in df.columns:
        df["School"] = np.nan
    df["School"] = np.where(mapped_school.notna(), mapped_school, df["School"])
    df["School"] = np.where(df["School"].isna() & domain_school.notna(), domain_school, df["School"])

    # Only keep rows where email is in EMAIL_TO_SCHOOL
    valid_emails = set(EMAIL_TO_SCHOOL.keys())
    df["email"] = df["email"].astype(str).str.strip().str.lower()
    df = df[df["email"].isin(valid_emails)]

    # Validate required columns
    missing = [c for c in ["@timestamp", "event_name", "email"] if c not in df.columns]
    if missing:
        st.error(f"Missing required column(s): {', '.join(missing)}")
        st.stop()

    # Timestamps
    df["@timestamp"] = pd.to_datetime(df["@timestamp"], errors="coerce")
    df = df.dropna(subset=["@timestamp"])
    df["date"] = df["@timestamp"].dt.date

    # Sidebar filters (after mapping): collect, then apply on click
    selections = get_filter_inputs(df)
    df_filtered = apply_selected_filters(df, selections)

    if not selections.get("apply", False):
        st.warning("Adjust filters in the sidebar and click **Apply filters** to update the dashboard.")
        working_df = df   # show overall until applied
    else:
        working_df = df_filtered

    if working_df.empty:
        st.warning("No data after applied filters. Adjust filters on the left and click **Apply filters**.")
        st.stop()

    # ========= Summary =========
    st.subheader("Summary")
    total_events = int(len(working_df))
    total_teachers = int(working_df["email"].nunique())
    events_by_teacher = working_df.groupby("email")["event_name"].count()
    active_teachers = int((events_by_teacher > 0).sum())
    median_events = int(events_by_teacher.median()) if not events_by_teacher.empty else 0
    mean_events = float(events_by_teacher.mean()) if not events_by_teacher.empty else 0.0

    # Unique active days
    days_per_teacher = working_df.groupby("email")["date"].nunique()
    repeat_count = int(days_per_teacher.ge(2).sum())
    pct_repeat = (repeat_count / total_teachers * 100.0) if total_teachers else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Events", total_events)
    c2.metric("Total Teachers", total_teachers)
    c3.metric("Active Teachers", active_teachers)
    c4.metric("Avg Events / Teacher", f"{mean_events:.1f}")
    c5.metric("Median Events / Teacher", f"{median_events}")

    c6, c7 = st.columns(2)
    with c6:
        st.metric("Teachers with ≥2 Active Days", f"{repeat_count} ({pct_repeat:.1f}%)")
    with c7:
        # Prepare data for Plotly
        plot_df = pd.DataFrame({
            "email": days_per_teacher.index,
            "active_days": days_per_teacher.values
        })
        fig = px.bar(
            plot_df,
            x="active_days",
            y="email",
            orientation="h",
            hover_data=["email", "active_days"],
            labels={"active_days": "Unique Active Days", "email": "Teacher Email"},
            title="Distribution of Unique Active Days"
        )
        st.plotly_chart(fig, use_container_width=True)

    # Export: Summary table
    summary_df = pd.DataFrame({
        "Metric": [
            "Total Events", "Total Teachers", "Active Teachers",
            "Avg Events per Teacher", "Median Events per Teacher",
            "Teachers with ≥2 Active Days", "% Teachers with ≥2 Active Days"
        ],
        "Value": [
            total_events, total_teachers, active_teachers,
            round(mean_events, 1), median_events,
            repeat_count, round(pct_repeat, 1)
        ]
    })
    st.download_button(
        "⬇️ Download Summary (CSV)",
        data=summary_df.to_csv(index=False).encode("utf-8"),
        file_name="summary.csv",
        mime="text/csv",
    )

    st.divider()

    # ========= Funnel =========
    st.subheader("Funnel Progression (Teachers Reaching Each Stage)")
    base = working_df["email"].nunique()
    milestones = [m for m in KNOWN_MILESTONES if m in working_df["event_name"].unique()]
    funnel_counts = []
    for m in milestones:
        n = int(working_df.loc[working_df["event_name"] == m, "email"].nunique())
        pct = round((n / base * 100.0), 1) if base else 0.0
        funnel_counts.append({"Milestone": m, "Teachers": n, "% of Teachers": pct})
    funnel_df = pd.DataFrame(funnel_counts)

    st.dataframe(funnel_df, width="stretch")

    fig2 = plt.figure(figsize=(8,5))
    plt.barh(funnel_df["Milestone"], funnel_df["Teachers"])
    plt.xlabel("Teachers")
    plt.title("Funnel: Teachers Reaching Each Milestone")
    st.pyplot(fig2)

    st.download_button(
        "⬇️ Download Funnel (CSV)",
        data=funnel_df.to_csv(index=False).encode("utf-8"),
        file_name="funnel.csv",
        mime="text/csv",
    )

    st.divider()

    # ========= Feature validation =========
    st.subheader("Feature Validation")
    if "School" in working_df.columns and working_df["School"].notna().any():
        heat = working_df.groupby(["School", "event_name"]).size().unstack(fill_value=0)
        st.dataframe(heat, width="stretch")

        st.download_button(
            "⬇️ Download Feature Validation (CSV)",
            data=heat.to_csv().encode("utf-8"),
            file_name="feature_validation_by_school.csv",
            mime="text/csv",
        )

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
        counts = working_df["event_name"].value_counts().sort_values(ascending=False)
        st.dataframe(counts.to_frame("Event Count"), width="stretch")

        st.download_button(
            "⬇️ Download Event Counts (CSV)",
            data=counts.to_csv().encode("utf-8"),
            file_name="event_counts_overall.csv",
            mime="text/csv",
        )

        fig4 = plt.figure(figsize=(8,5))
        plt.bar(counts.index, counts.values)
        plt.xticks(rotation=45, ha="right")
        plt.ylabel("Event Count")
        plt.title("Overall Event Counts")
        st.pyplot(fig4)

    st.divider()

    # ========= Teacher-level breakdown =========
    st.subheader("Teacher-Level Breakdown")
    pivot = (
        working_df
        .assign(email=working_df["email"].astype(str), event_name=working_df["event_name"].astype(str))
        .groupby(["email", "event_name"])
        .size()
        .unstack(fill_value=0)
        .sort_index()
    )
    if "School" in working_df.columns and working_df["School"].notna().any():
        school_map = working_df[["email","School"]].dropna().drop_duplicates().set_index("email")["School"].to_dict()
        pivot.insert(0, "School", pivot.index.map(lambda e: school_map.get(e, "")))
        pivot = pivot.reset_index().set_index(["School","email"])
    st.dataframe(pivot, width="stretch")

    st.download_button(
        "⬇️ Download Teacher Breakdown (CSV)",
        data=pivot.to_csv().encode("utf-8"),
        file_name="teacher_breakdown.csv",
        mime="text/csv",
    )

    st.divider()

    # ========= Recency & trend =========
    st.subheader("Recency & Activity Over Time")
    cA, cB = st.columns(2)
    with cA:
        st.markdown("**Events per Day (Filtered Data)**")
        daily = working_df.groupby("date").size().rename("events")
        x_dates = pd.to_datetime(daily.index)  # safe datetime conversion for plotting
        fig5 = plt.figure(figsize=(6,4))
        plt.plot(x_dates, daily.values)
        plt.xlabel("Date")
        plt.ylabel("Events")
        plt.title("Daily Event Volume")
        st.pyplot(fig5)
        st.download_button(
            "⬇️ Download Daily Event Volume (CSV)",
            data=daily.to_csv().encode("utf-8"),
            file_name="daily_event_volume.csv",
            mime="text/csv",
        )
    with cB:
        st.markdown("**Top Teachers by Event Count**")
        topn = events_by_teacher.sort_values(ascending=False).head(10).rename("events")
        fig6 = plt.figure(figsize=(6,4))
        plt.barh(topn.index, topn.values)
        plt.xlabel("Events")
        plt.title("Top 10 Teachers by Events")
        st.pyplot(fig6)
        st.download_button(
            "⬇️ Download Top Teachers (CSV)",
            data=topn.to_csv().encode("utf-8"),
            file_name="top_teachers.csv",
            mime="text/csv",
        )

    st.success("Dashboard ready. Use the left filters, click **Apply filters**, then export the exact slice you need.")