#!/usr/bin/env python3
"""
build_price_history_daily.py

Snapshots BI and/or BBX latest prices into a clean long-format history table:

    price_history_daily

This is used by the Streamlit dashboard for:
- BI risers/fallers
- BBX risers/fallers
- BI vs BBX price trends over time
- producer/region movement

Run:
    python scripts/build_price_history_daily.py --bi
    python scripts/build_price_history_daily.py --bbx
    python scripts/build_price_history_daily.py --bi --bbx

If you only want today's BI history:
    python scripts/build_price_history_daily.py --bi
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "db" / "wine.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
HISTORY_TABLE = "price_history_daily"


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def connect() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def get_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    if not table_exists(conn, table_name):
        return []
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]


def first_existing(cols: list[str], candidates: list[str]) -> str | None:
    lower_map = {c.lower(): c for c in cols}
    for c in candidates:
        if c in cols:
            return c
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


def ensure_history_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
            snapshot_date TEXT,
            source TEXT,
            wine_key TEXT,
            lwin TEXT,
            vintage TEXT,
            bottle_size TEXT,
            wine_name TEXT,
            producer TEXT,
            region TEXT,
            appellation TEXT,
            price_per_6 REAL,
            url TEXT,
            raw_price REAL,
            raw_pack REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    existing = set(get_columns(conn, HISTORY_TABLE))
    required = {
        "snapshot_date": "TEXT",
        "source": "TEXT",
        "wine_key": "TEXT",
        "lwin": "TEXT",
        "vintage": "TEXT",
        "bottle_size": "TEXT",
        "wine_name": "TEXT",
        "producer": "TEXT",
        "region": "TEXT",
        "appellation": "TEXT",
        "price_per_6": "REAL",
        "url": "TEXT",
        "raw_price": "REAL",
        "raw_pack": "REAL",
        "created_at": "TEXT",
    }

    for col, typ in required.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE {HISTORY_TABLE} ADD COLUMN {col} {typ}")

    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{HISTORY_TABLE}_source_key_date "
        f"ON {HISTORY_TABLE}(source, wine_key, snapshot_date)"
    )
    conn.commit()


def make_wine_key(df: pd.DataFrame) -> pd.Series:
    lwin = df.get("lwin", pd.Series([""] * len(df))).fillna("").astype(str)
    vintage = df.get("vintage", pd.Series([""] * len(df))).fillna("").astype(str)
    bottle_size = df.get("bottle_size", pd.Series([""] * len(df))).fillna("").astype(str)

    key = lwin + "|" + vintage + "|" + bottle_size
    fallback = df.get("wine_name", pd.Series(["unknown"] * len(df))).fillna("unknown").astype(str)
    key = key.where(lwin.str.len() > 0, fallback)
    return key


def delete_existing_snapshot(conn: sqlite3.Connection, snapshot_date: str, source: str) -> None:
    conn.execute(
        f"DELETE FROM {HISTORY_TABLE} WHERE snapshot_date=? AND source=?",
        (snapshot_date, source),
    )
    conn.commit()


# -----------------------------------------------------------------------------
# BI snapshot
# -----------------------------------------------------------------------------

def snapshot_bi(conn: sqlite3.Connection, snapshot_date: str) -> int:
    table = "bi_normalized_latest"
    if not table_exists(conn, table):
        print(f"Missing table: {table}")
        return 0

    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    if df.empty:
        return 0

    cols = list(df.columns)

    out = pd.DataFrame(index=df.index)
    out["snapshot_date"] = pd.Series(snapshot_date, index=df.index)
    out["source"] = pd.Series("bi", index=df.index)
    out["lwin"] = df[first_existing(cols, ["lwin", "code"])] if first_existing(cols, ["lwin", "code"]) else ""
    out["vintage"] = df[first_existing(cols, ["vintage"])] if first_existing(cols, ["vintage"]) else ""
    out["bottle_size"] = df[first_existing(cols, ["pack"])] if first_existing(cols, ["pack"]) else ""
    out["wine_name"] = df[first_existing(cols, ["bi_wine_name", "wine_name", "name"])] if first_existing(cols, ["bi_wine_name", "wine_name", "name"]) else ""
    out["producer"] = df[first_existing(cols, ["bi_producer", "producer"])] if first_existing(cols, ["bi_producer", "producer"]) else ""
    out["region"] = df[first_existing(cols, ["bi_region", "region"])] if first_existing(cols, ["bi_region", "region"]) else ""
    out["appellation"] = df[first_existing(cols, ["bi_appellation", "appellation"])] if first_existing(cols, ["bi_appellation", "appellation"]) else ""
    out["url"] = df[first_existing(cols, ["bi_url", "url"])] if first_existing(cols, ["bi_url", "url"]) else ""

    # BI price: safest normalisation is offer_case_price / pack * 6.
    case_col = first_existing(cols, ["offer_case_price", "offer_per_6", "price_per_6"])
    pack_col = first_existing(cols, ["pack"])

    case_price = pd.to_numeric(df[case_col], errors="coerce") if case_col else pd.Series([None] * len(df))
    pack = pd.to_numeric(df[pack_col], errors="coerce") if pack_col else pd.Series([6] * len(df))
    pack = pack.replace(0, 6).fillna(6)

    if case_col == "offer_per_6" or case_col == "price_per_6":
        out["price_per_6"] = case_price
    else:
        out["price_per_6"] = case_price / pack * 6

    out["raw_price"] = case_price
    out["raw_pack"] = pack
    out["wine_key"] = make_wine_key(out)

    out = out.dropna(subset=["price_per_6"])
    out = out[out["price_per_6"] > 0]
    out = out[out["wine_key"] != "unknown"]

    delete_existing_snapshot(conn, snapshot_date, "bi")
    out.to_sql(HISTORY_TABLE, conn, if_exists="append", index=False)
    conn.commit()
    return len(out)


# -----------------------------------------------------------------------------
# BBX snapshot
# -----------------------------------------------------------------------------

def snapshot_bbx(conn: sqlite3.Connection, snapshot_date: str) -> int:
    table = "bbx_normalized_latest"
    if not table_exists(conn, table):
        print(f"Missing table: {table}")
        return 0

    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    if df.empty:
        return 0

    cols = list(df.columns)

    lwin_col = first_existing(cols, ["lwin", "lwin_code", "code"])
    vintage_col = first_existing(cols, ["vintage"])
    bottle_col = first_existing(cols, ["bottle_size", "pack", "case_size", "format"])
    name_col = first_existing(cols, ["bbx_description", "wine_name", "property", "name", "description"])
    producer_col = first_existing(cols, ["producer", "bbx_producer"])
    region_col = first_existing(cols, ["region", "bbx_region"])
    appellation_col = first_existing(cols, ["appellation", "bbx_appellation"])
    url_col = first_existing(cols, ["bbx_url", "url", "link"])
    price_col = first_existing(cols, [
        "bbx_lowest_per_6",
        "lowest_per_6",
        "price_per_6",
        "bbx_price_per_6",
        "price",
    ])

    if price_col is None:
        raise RuntimeError(f"Could not identify BBX price column. Columns: {cols}")

    out = pd.DataFrame(index=df.index)
    out["snapshot_date"] = pd.Series(snapshot_date, index=df.index)
    out["source"] = pd.Series("bbx", index=df.index)
    out["lwin"] = df[lwin_col] if lwin_col else ""
    out["vintage"] = df[vintage_col] if vintage_col else ""
    out["bottle_size"] = df[bottle_col] if bottle_col else ""
    out["wine_name"] = df[name_col] if name_col else ""
    out["producer"] = df[producer_col] if producer_col else ""
    out["region"] = df[region_col] if region_col else ""
    out["appellation"] = df[appellation_col] if appellation_col else ""
    out["url"] = df[url_col] if url_col else ""
    out["price_per_6"] = pd.to_numeric(df[price_col], errors="coerce")
    out["raw_price"] = out["price_per_6"]
    out["raw_pack"] = 6
    out["wine_key"] = make_wine_key(out)

    out = out.dropna(subset=["price_per_6"])
    out = out[out["price_per_6"] > 0]
    out = out[out["wine_key"] != "unknown"]

    delete_existing_snapshot(conn, snapshot_date, "bbx")
    out.to_sql(HISTORY_TABLE, conn, if_exists="append", index=False)
    conn.commit()
    return len(out)


# -----------------------------------------------------------------------------
# Feature columns for dashboard rankings
# -----------------------------------------------------------------------------

def rebuild_trend_features(conn: sqlite3.Connection) -> int:
    df = pd.read_sql_query(f"SELECT * FROM {HISTORY_TABLE}", conn)
    if df.empty:
        return 0

    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")
    df["price_per_6"] = pd.to_numeric(df["price_per_6"], errors="coerce")
    df = df.dropna(subset=["snapshot_date", "price_per_6"])
    df = df.sort_values(["source", "wine_key", "snapshot_date"])

    frames = []
    for (_, _), g in df.groupby(["source", "wine_key"], dropna=False):
        g = g.sort_values("snapshot_date").copy()
        g = g.set_index("snapshot_date")
        price = g["price_per_6"]

        g["price_7d_median"] = price.rolling("7D", min_periods=1).median()
        g["price_30d_median"] = price.rolling("30D", min_periods=1).median()
        g["price_90d_median"] = price.rolling("90D", min_periods=1).median()
        g["price_180d_median"] = price.rolling("180D", min_periods=1).median()

        reset = g.reset_index()
        for days in [7, 30, 90]:
            vals = []
            for _, row in reset.iterrows():
                current_date = row["snapshot_date"]
                current_price = row["price_per_6"]
                prior = reset[reset["snapshot_date"] <= current_date - pd.Timedelta(days=days)]
                if prior.empty:
                    vals.append(None)
                else:
                    prior_price = prior.iloc[-1]["price_per_6"]
                    vals.append((current_price - prior_price) / prior_price * 100 if prior_price else None)
            reset[f"price_change_{days}d_pct"] = vals

        reset["discount_to_90d_median_pct"] = (
            (reset["price_90d_median"] - reset["price_per_6"]) / reset["price_90d_median"] * 100
        )
        reset["discount_to_180d_median_pct"] = (
            (reset["price_180d_median"] - reset["price_per_6"]) / reset["price_180d_median"] * 100
        )

        reset = reset.set_index("snapshot_date")
        reset["volatility_30d"] = price.rolling("30D", min_periods=2).std()
        reset["volatility_90d"] = price.rolling("90D", min_periods=2).std()
        reset["liquidity_30d_avg"] = pd.Series(1, index=price.index).rolling("30D", min_periods=1).sum()
        frames.append(reset.reset_index())

    features = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if features.empty:
        return 0

    features["snapshot_date"] = pd.to_datetime(features["snapshot_date"]).dt.strftime("%Y-%m-%d")
    features.to_sql("price_history_features_daily", conn, if_exists="replace", index=False)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_price_history_features_source_key_date "
        "ON price_history_features_daily(source, wine_key, snapshot_date)"
    )
    conn.commit()
    return len(features)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=date.today().isoformat())
    parser.add_argument("--bi", action="store_true", help="Snapshot BI latest prices")
    parser.add_argument("--bbx", action="store_true", help="Snapshot BBX latest prices")
    args = parser.parse_args()

    if not args.bi and not args.bbx:
        args.bi = True

    with connect() as conn:
        ensure_history_table(conn)

        bi_count = snapshot_bi(conn, args.snapshot_date) if args.bi else 0
        bbx_count = snapshot_bbx(conn, args.snapshot_date) if args.bbx else 0
        feature_count = 0

    print("\nPrice history build complete")
    print("=" * 72)
    print(f"Snapshot date : {args.snapshot_date}")
    print(f"BI rows       : {bi_count:,}")
    print(f"BBX rows      : {bbx_count:,}")
    print(f"Feature rows  : {feature_count:,}")
    print(f"History table : {HISTORY_TABLE}")
    print("Feature table : price_history_features_daily")


if __name__ == "__main__":
    main()
