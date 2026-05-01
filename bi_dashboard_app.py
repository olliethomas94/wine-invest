#!/usr/bin/env python3
"""
bi_dashboard_app.py

Clean Streamlit dashboard for the wine trading system.

Uses:
- action_queue for trading opportunities
- price_history_daily for BI + BBX price trends

Run:
    cd /Users/olliethomas/Documents/wine-invest
    source venv/bin/activate
    streamlit run bi_dashboard_app.py
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path("/Users/olliethomas/Documents/wine-invest")
DB_PATH = PROJECT_ROOT / "db" / "wine.db"

st.set_page_config(
    page_title="Fine Wine Trading Dashboard",
    page_icon="🍷",
    layout="wide",
)


# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------

@st.cache_data(ttl=30)
def table_exists(table_name: str) -> bool:
    if not DB_PATH.exists():
        return False
    conn = sqlite3.connect(DB_PATH)
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


@st.cache_data(ttl=30)
def load_table(table_name: str) -> pd.DataFrame:
    if not DB_PATH.exists() or not table_exists(table_name):
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    try:
        return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    finally:
        conn.close()


@st.cache_data(ttl=30)
def list_tables() -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    try:
        return pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
            conn,
        )
    finally:
        conn.close()


@st.cache_data(ttl=30)
def load_action_queue() -> pd.DataFrame:
    df = load_table("action_queue")
    if df.empty:
        return df
    if "opportunity_score" in df.columns:
        df = df.sort_values("opportunity_score", ascending=False)
    return df


def display_name(row: pd.Series) -> str:
    wine_name = str(row.get("wine_name", "") or "").strip()
    producer = str(row.get("producer", "") or "").strip()
    region = str(row.get("region", "") or "").strip()
    vintage = str(row.get("vintage", "") or "").strip()
    wine_key = str(row.get("wine_key", "") or "").strip()

    if wine_name and wine_name.lower() != "nan":
        base = wine_name
    elif producer and producer.lower() != "nan":
        base = producer
    else:
        base = wine_key

    bits = [base]
    if vintage and vintage.lower() != "nan":
        bits.append(vintage)
    if region and region.lower() != "nan":
        bits.append(f"({region})")

    return " ".join(bits)


def canonical_wine_key(row: pd.Series) -> str:
    """
    Shared wine identity for overlaying BI and BBX.
    Prefer LWIN + vintage. Do NOT include source.
    Avoid bottle/pack where possible so BI and BBX can overlay.
    """
    lwin = str(row.get("lwin", "") or "").strip()
    vintage = str(row.get("vintage", "") or "").strip()
    wine_key = str(row.get("wine_key", "") or "").strip()

    if lwin and vintage and lwin.lower() != "nan" and vintage.lower() != "nan":
        return f"{lwin}|{vintage}"

    # Fallback: if wine_key looks like lwin|vintage|pack, strip to first 2 parts.
    parts = wine_key.split("|")
    if len(parts) >= 2 and parts[0] and parts[1]:
        return f"{parts[0]}|{parts[1]}"

    return wine_key


@st.cache_data(ttl=30)
def load_price_history() -> pd.DataFrame:
    """
    Loads price_history_daily first.
    This is the new table containing both BI and BBX snapshots.
    """
    if table_exists("price_trends_combined"):
        df = load_table("price_trends_combined")
    elif table_exists("price_history_daily"):
        df = load_table("price_history_daily")
    elif table_exists("wine_price_features_daily"):
        df = load_table("wine_price_features_daily")
    else:
        return pd.DataFrame()

    if df.empty:
        return df

    # Standard columns.
    if "snapshot_date" not in df.columns and "date" in df.columns:
        df["snapshot_date"] = df["date"]
    if "source" not in df.columns:
        df["source"] = "unknown"
    if "wine_key" not in df.columns:
        df["wine_key"] = "unknown"
    if "price_per_6" not in df.columns:
        df["price_per_6"] = None

    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")
    df["source"] = df["source"].fillna("unknown").astype(str).str.lower()
    df["wine_key"] = df["wine_key"].fillna("unknown").astype(str)
    df["price_per_6"] = pd.to_numeric(df["price_per_6"], errors="coerce")

    # Ensure text fields exist.
    for col in ["wine_name", "producer", "region", "appellation", "lwin", "vintage", "bottle_size", "url"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str)

    df = df.dropna(subset=["snapshot_date", "price_per_6"])
    df["canonical_key"] = df.apply(canonical_wine_key, axis=1)
    df["display_name"] = df.apply(display_name, axis=1)
    return df


# -----------------------------------------------------------------------------
# Formatting helpers
# -----------------------------------------------------------------------------

def clickable_link(url: object, label: str) -> str:
    if url is None or pd.isna(url):
        return ""
    url = str(url).strip()
    if not url:
        return ""
    return f'<a href="{url}" target="_blank">{label}</a>'


def pct_change_from_first(series: pd.Series) -> float | None:
    series = pd.to_numeric(series, errors="coerce").dropna()
    if len(series) < 2:
        return None
    first = series.iloc[0]
    last = series.iloc[-1]
    if first == 0:
        return None
    return (last - first) / first * 100


# -----------------------------------------------------------------------------
# App
# -----------------------------------------------------------------------------

st.title("🍷 Fine Wine Trading Dashboard")
st.caption("Action queue + BI/BBX trend intelligence")

if not DB_PATH.exists():
    st.error(f"Database not found: {DB_PATH}")
    st.stop()

queue_df = load_action_queue()
history_df = load_price_history()

tab1, tab2, tab3 = st.tabs(["🎯 Action Queue", "📈 Price Trends", "🛠 Diagnostics"])


# -----------------------------------------------------------------------------
# Action Queue
# -----------------------------------------------------------------------------

with tab1:
    st.subheader("🎯 Action Queue")

    if queue_df.empty:
        st.warning("No action_queue rows found. Run build_action_queue.py first.")
    else:
        min_score = st.slider("Minimum opportunity score", 0, 100, 0)
        min_realistic = st.slider("Minimum realistic net spread %", -50, 100, 0)
        max_rows = st.slider("Rows to show", 10, 300, 100, 10)

        df = queue_df.copy()
        if "opportunity_score" in df.columns:
            df = df[df["opportunity_score"].fillna(0) >= min_score]
        if "realistic_net_spread_pct" in df.columns:
            df = df[df["realistic_net_spread_pct"].fillna(-999) >= min_realistic]

        df = df.head(max_rows)

        c1, c2, c3 = st.columns(3)
        c1.metric("Queue rows", len(queue_df))
        c2.metric("Filtered rows", len(df))
        c3.metric(
            "Best score",
            f"{df['opportunity_score'].max():.1f}"
            if "opportunity_score" in df.columns and not df.empty
            else "—",
        )

        if not df.empty:
            display = df.copy()
            display["BBX"] = display["bbx_url"].apply(lambda x: clickable_link(x, "BBX")) if "bbx_url" in display.columns else ""
            display["BI"] = display["bi_url"].apply(lambda x: clickable_link(x, "BI")) if "bi_url" in display.columns else ""

            cols = [
                "signal_grade",
                "status",
                "opportunity_score",
                "realistic_net_spread_pct",
                "spread_pct",
                "bbx_price",
                "bi_price",
                "realistic_exit_price",
                "match_quality_score",
                "liquidity_score",
                "lwin",
                "vintage",
                "wine_name",
                "warning_flags",
                "BBX",
                "BI",
            ]
            cols = [c for c in cols if c in display.columns]
            st.write(display[cols].to_html(escape=False, index=False), unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# Price Trends
# -----------------------------------------------------------------------------

with tab2:
    st.subheader("📈 Price Trends")

    if history_df.empty:
        st.warning("No price history found. Run: python scripts/build_price_history_daily.py --bi --bbx")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Rows", len(history_df))
        c2.metric("Sources", history_df["source"].nunique())
        c3.metric("Wines", history_df["wine_key"].nunique())
        c4.metric(
            "Date range",
            f"{history_df['snapshot_date'].min().date()} → {history_df['snapshot_date'].max().date()}",
        )

        st.write("### Source counts")
        source_counts = history_df.groupby("source").size().reset_index(name="rows")
        st.dataframe(source_counts, use_container_width=True)

        sources = sorted(history_df["source"].dropna().unique().tolist())
        selected_sources = st.multiselect("Sources", sources, default=sources)

        dfh = history_df[history_df["source"].isin(selected_sources)].copy()

        search = st.text_input("Search wine / producer / region", "")
        if search.strip():
            s = search.strip().lower()
            mask = dfh["wine_key"].astype(str).str.lower().str.contains(s, na=False)
            for col in ["display_name", "wine_name", "producer", "region", "appellation", "lwin", "vintage"]:
                if col in dfh.columns:
                    mask = mask | dfh[col].astype(str).str.lower().str.contains(s, na=False)
            dfh = dfh[mask]

        st.write("### Top risers / fallers from available history")
        lookback_days = st.selectbox("Lookback", [7, 30, 90], index=1)

        latest_date = dfh["snapshot_date"].max()
        cutoff = latest_date - pd.Timedelta(days=lookback_days)
        window = dfh[dfh["snapshot_date"] >= cutoff].copy()

        mover_rows = []
        for (source, wine_key), g in window.groupby(["source", "wine_key"], dropna=False):
            g = g.sort_values("snapshot_date")
            change = pct_change_from_first(g["price_per_6"])
            if change is None:
                continue
            latest = g.iloc[-1]
            mover_rows.append(
                {
                    "source": source,
                    "wine_key": wine_key,
                    "wine_name": latest.get("wine_name", ""),
                    "producer": latest.get("producer", ""),
                    "region": latest.get("region", ""),
                    "latest_price_per_6": latest.get("price_per_6"),
                    f"change_{lookback_days}d_pct": change,
                    "observations": len(g),
                }
            )

        movers = pd.DataFrame(mover_rows)
        if movers.empty:
            st.info("No riser/faller data yet. You need at least two snapshots on different dates for price changes.")
        else:
            metric = f"change_{lookback_days}d_pct"
            left, right = st.columns(2)
            with left:
                st.write("#### Risers")
                st.dataframe(movers.sort_values(metric, ascending=False).head(25), use_container_width=True)
            with right:
                st.write("#### Fallers")
                st.dataframe(movers.sort_values(metric, ascending=True).head(25), use_container_width=True)

        st.write("### Wine chart")

        # One dropdown entry per wine, with BI and BBX overlaid in the chart.
        wine_lookup = (
            dfh.sort_values(["display_name", "snapshot_date"])
            .drop_duplicates(subset=["canonical_key"], keep="last")
            .copy()
        )
        wine_lookup["selector"] = wine_lookup["display_name"] + " — " + wine_lookup["canonical_key"].astype(str)
        wine_options = sorted(wine_lookup["selector"].dropna().astype(str).unique().tolist())

        if wine_options:
            selected_label = st.selectbox("Select wine", wine_options)
            selected_key = selected_label.split(" — ")[-1]
            wine_hist = dfh[dfh["canonical_key"].astype(str) == selected_key].copy()

            chart_df = wine_hist.pivot_table(
                index="snapshot_date",
                columns="source",
                values="price_per_6",
                aggfunc="median",
            ).sort_index()

            st.line_chart(chart_df, use_container_width=True)

            latest_prices = (
                wine_hist.sort_values("snapshot_date")
                .groupby("source", as_index=False)
                .tail(1)[["source", "snapshot_date", "price_per_6", "wine_name", "producer", "region", "url"]]
                .sort_values("source")
            )

            st.write("#### Latest prices")
            st.dataframe(latest_prices, use_container_width=True)

            st.write("#### Full history")
            st.dataframe(wine_hist.sort_values(["source", "snapshot_date"]), use_container_width=True)
        else:
            st.info("No wines match the current filters.")

        st.write("### Region / producer / appellation movement")

        segment_choice = st.selectbox("Segment by", ["region", "producer", "appellation"], index=0)
        segment_col = segment_choice if segment_choice in dfh.columns else None

        if segment_col:
            segment_base = dfh.copy()
            segment_base[segment_col] = segment_base[segment_col].replace("nan", "").fillna("")
            segment_base = segment_base[segment_base[segment_col].astype(str).str.strip() != ""]

            if segment_base.empty:
                st.info(f"No usable {segment_col} values found in price_history_daily. Check the Diagnostics tab preview.")
            else:
                latest_date = segment_base["snapshot_date"].max()
                cutoff = latest_date - pd.Timedelta(days=lookback_days)
                segment_window = segment_base[segment_base["snapshot_date"] >= cutoff].copy()

                rows = []
                for (source, segment_value), g in segment_window.groupby(["source", segment_col], dropna=False):
                    wine_changes = []
                    for _, wg in g.groupby("wine_key"):
                        change = pct_change_from_first(wg.sort_values("snapshot_date")["price_per_6"])
                        if change is not None:
                            wine_changes.append(change)

                    rows.append(
                        {
                            "source": source,
                            segment_col: segment_value,
                            "wines": g["wine_key"].nunique(),
                            "observations": len(g),
                            "median_price": g["price_per_6"].median(),
                            f"median_change_{lookback_days}d_pct": pd.Series(wine_changes).median() if wine_changes else None,
                        }
                    )

                segment = pd.DataFrame(rows)
                if segment.empty:
                    st.info("No segment data available yet.")
                else:
                    min_wines = st.slider("Minimum wines in segment", 1, 50, 5, key="segment_min_wines")
                    segment = segment[segment["wines"] >= min_wines]
                    metric_col = f"median_change_{lookback_days}d_pct"

                    left, right = st.columns(2)
                    with left:
                        st.write("#### Largest / most active segments")
                        st.dataframe(segment.sort_values("wines", ascending=False).head(50), use_container_width=True)
                    with right:
                        st.write(f"#### Strongest segments, {lookback_days}d")
                        if metric_col in segment.columns:
                            st.dataframe(segment.sort_values(metric_col, ascending=False).head(50), use_container_width=True)
                        else:
                            st.dataframe(segment.head(50), use_container_width=True)


# -----------------------------------------------------------------------------
# Diagnostics
# -----------------------------------------------------------------------------

with tab3:
    st.subheader("🛠 Diagnostics")

    st.write("Database:", str(DB_PATH))
    st.write("Tables:")
    st.dataframe(list_tables(), use_container_width=True)

    st.write("### price_history_daily preview")
    raw_hist = load_table("price_history_daily")
    st.write("Rows:", len(raw_hist))
    if not raw_hist.empty:
        st.dataframe(raw_hist.head(20), use_container_width=True)
        if "source" in raw_hist.columns:
            st.dataframe(raw_hist.groupby("source").size().reset_index(name="rows"), use_container_width=True)

    st.write("### action_queue preview")
    st.write("Rows:", len(queue_df))
    if not queue_df.empty:
        st.dataframe(queue_df.head(20), use_container_width=True)
