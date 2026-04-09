import sqlite3

def get_last_snapshot_date(db_path="wine.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT MAX(snapshot_date) FROM snapshots")
    result = cursor.fetchone()
    
    conn.close()
    
    return result[0]

import sqlite3
import pandas as pd
import streamlit as st
import altair as alt

st.set_page_config(page_title="BBX Dashboard", layout="wide")

DB_PATH = "db/wine.db"


@st.cache_data(ttl=300)
def run_query(query, params=None):
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn, params=params or ())


@st.cache_data(ttl=300)
def get_latest_snapshot_id():
    df = run_query("SELECT MAX(id) AS snapshot_id FROM snapshots")
    return int(df.iloc[0]["snapshot_id"])


@st.cache_data(ttl=300)
def get_enriched_opportunities(snapshot_id):
    return run_query("""
        SELECT *
        FROM v_market_stats_enriched
        WHERE snapshot_id = ?
    """, (snapshot_id,))


@st.cache_data(ttl=300)
def get_history(lwin_code, vintage, bottle_size):
    return run_query("""
        SELECT
            s.created_at,
            e.lowest_price,
            e.median_price,
            e.highest_price,
            e.listing_count
        FROM v_market_stats_enriched e
        JOIN snapshots s ON e.snapshot_id = s.id
        WHERE e.lwin_code = ?
          AND COALESCE(e.vintage, '') = COALESCE(?, '')
          AND COALESCE(e.bottle_size, '') = COALESCE(?, '')
        ORDER BY s.id
    """, (lwin_code, vintage, bottle_size))


@st.cache_data(ttl=300)
def get_latest_distribution(snapshot_id, lwin_code, vintage, bottle_size):
    return run_query("""
        SELECT
            description,
            page_url,
            price_ex_tax
        FROM listings
        WHERE snapshot_id = ?
          AND lwin_code = ?
          AND COALESCE(vintage, '') = COALESCE(?, '')
          AND COALESCE(bottle_size, '') = COALESCE(?, '')
          AND price_ex_tax IS NOT NULL
        ORDER BY price_ex_tax
    """, (snapshot_id, lwin_code, vintage, bottle_size))


st.title("BBX Wine Market Dashboard")
st.caption("BBX opportunities with Liv-ex LWIN enrichment and price history")

latest_snapshot_id = get_latest_snapshot_id()
df = get_enriched_opportunities(latest_snapshot_id)

st.sidebar.header("Filters")

min_listings = st.sidebar.slider("Minimum listings", 1, 10, 3)
min_discount = st.sidebar.slider("Minimum discount %", 0.0, 100.0, 10.0, 1.0)
require_lwin = st.sidebar.checkbox("Require LWIN", value=True)
exclude_odd = st.sidebar.checkbox("Exclude odd formats", value=True)

country_options = sorted([x for x in df["country"].dropna().unique().tolist()])
region_options = sorted([x for x in df["region"].dropna().unique().tolist()])
sub_region_options = sorted([x for x in df["sub_region"].dropna().unique().tolist()])
colour_options = sorted([x for x in df["colour"].dropna().unique().tolist()])
type_options = sorted([x for x in df["type"].dropna().unique().tolist()])

selected_countries = st.sidebar.multiselect("Country", country_options)
selected_regions = st.sidebar.multiselect("Region", region_options)
selected_sub_regions = st.sidebar.multiselect("Sub-region", sub_region_options)
selected_colours = st.sidebar.multiselect("Colour", colour_options)
selected_types = st.sidebar.multiselect("Type", type_options)

filtered = df.copy()
filtered = filtered[filtered["listing_count"] >= min_listings]
filtered = filtered[filtered["discount_pct"].fillna(0) >= min_discount]

if require_lwin:
    filtered = filtered[filtered["lwin_code"].notna()]

if exclude_odd:
    filtered = filtered[
        ~filtered["bottle_size"].fillna("").str.contains("Assortment|Case of 1 s", case=False, regex=True)
    ]

if selected_countries:
    filtered = filtered[filtered["country"].isin(selected_countries)]

if selected_regions:
    filtered = filtered[filtered["region"].isin(selected_regions)]

if selected_sub_regions:
    filtered = filtered[filtered["sub_region"].isin(selected_sub_regions)]

if selected_colours:
    filtered = filtered[filtered["colour"].isin(selected_colours)]

if selected_types:
    filtered = filtered[filtered["type"].isin(selected_types)]

filtered = filtered.sort_values("opportunity_score", ascending=False).reset_index(drop=True)

st.subheader("Opportunity screen")

show = filtered[[
    "display_name",
    "producer_name",
    "country",
    "region",
    "sub_region",
    "vintage",
    "bottle_size",
    "listing_count",
    "lowest_price",
    "median_price",
    "discount_pct",
    "opportunity_score"
]].copy()

st.dataframe(show, use_container_width=True, hide_index=True)

if filtered.empty:
    st.stop()

labels = [
    f"{row['display_name'] or row['property']} | {row['vintage'] or 'NV'} | {row['bottle_size']}"
    for _, row in filtered.iterrows()
]

selected_label = st.selectbox("Select wine", labels)
selected = filtered.iloc[labels.index(selected_label)]

st.subheader("Selected wine")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Wine", selected["display_name"] if pd.notna(selected["display_name"]) else selected["property"])
c2.metric("Country / Region", f"{selected['country'] or ''} / {selected['region'] or ''}")
c3.metric("Lowest / Median", f"£{selected['lowest_price']:,.0f} / £{selected['median_price']:,.0f}")
c4.metric("Discount", f"{selected['discount_pct']:.2f}%")

history = get_history(selected["lwin_code"], selected["vintage"], selected["bottle_size"])

if not history.empty:
    history["created_at"] = pd.to_datetime(history["created_at"])

    chart = (
        alt.Chart(history)
        .transform_fold(
            ["lowest_price", "median_price", "highest_price"],
            as_=["series", "price"]
        )
        .mark_line(point=True)
        .encode(
            x=alt.X("created_at:T", title="Snapshot date"),
            y=alt.Y("price:Q", title="Price ex tax (£)"),
            color="series:N",
            tooltip=["created_at:T", "series:N", alt.Tooltip("price:Q", format=",.0f")]
        )
        .properties(height=320)
    )

    st.subheader("Price over time")
    st.altair_chart(chart, use_container_width=True)

    listing_chart = (
        alt.Chart(history)
        .mark_bar()
        .encode(
            x=alt.X("created_at:T", title="Snapshot date"),
            y=alt.Y("listing_count:Q", title="Listing count"),
            tooltip=["created_at:T", "listing_count:Q"]
        )
        .properties(height=180)
    )

    st.subheader("Listing count over time")
    st.altair_chart(listing_chart, use_container_width=True)

distribution = get_latest_distribution(
    latest_snapshot_id,
    selected["lwin_code"],
    selected["vintage"],
    selected["bottle_size"]
)

if not distribution.empty:
    distribution = distribution.reset_index(drop=True)
    distribution["listing_number"] = distribution.index + 1

    dist_chart = (
        alt.Chart(distribution)
        .mark_circle(size=100)
        .encode(
            x=alt.X("listing_number:Q", title="Listing ordered by price"),
            y=alt.Y("price_ex_tax:Q", title="Price ex tax (£)"),
            tooltip=["description:N", alt.Tooltip("price_ex_tax:Q", format=",.0f"), "page_url:N"]
        )
        .properties(height=300)
    )

    st.subheader("Current snapshot price distribution")
    st.altair_chart(dist_chart, use_container_width=True)

    st.dataframe(distribution, use_container_width=True, hide_index=True)
