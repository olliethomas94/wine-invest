#!/usr/bin/env python3
"""
build_price_trends_combined.py

Combines:
- BBX historical price features from wine_price_features_daily
- BI daily snapshots from price_history_daily

into:
- price_trends_combined

Key fixes:
- Normalises LWIN values so 1015362 and 1015362.0 become the same key
- Creates canonical_key = lwin|vintage
- Deduplicates repeated BBX rows per source/date/canonical_key using median price
- Keeps a readable display name

Run:
    python scripts/build_price_trends_combined.py
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path("/Users/olliethomas/Documents/wine-invest")
DB_PATH = PROJECT_ROOT / "db" / "wine.db"
OUT_TABLE = "price_trends_combined"


def clean_lwin(x) -> str:
    if x is None or pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"\.0$", "", s)
    return s


def clean_vintage(x) -> str:
    if x is None or pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"\.0$", "", s)
    return s


def parse_bbx_wine_key(wine_key: object) -> dict:
    """
    Current BBX historical key usually looks like:
        property|lwin|vintage|case_format

    Example:
        Chateau Talbot|1015362.0|2022|Case of 6 Bottles
    """
    parts = str(wine_key).split("|")

    if len(parts) >= 3:
        property_name = parts[0].strip()
        lwin = clean_lwin(parts[1])
        vintage = clean_vintage(parts[2])
        bottle_size = parts[3].strip() if len(parts) >= 4 else ""
        canonical_key = f"{lwin}|{vintage}" if lwin and vintage else str(wine_key)
        display_name = f"{property_name} {vintage}".strip()
        return {
            "canonical_key": canonical_key,
            "lwin": lwin,
            "vintage": vintage,
            "bottle_size": bottle_size,
            "wine_name": display_name,
            "producer": "",
            "region": "",
            "appellation": "",
            "url": "",
        }

    return {
        "canonical_key": str(wine_key),
        "lwin": "",
        "vintage": "",
        "bottle_size": "",
        "wine_name": str(wine_key),
        "producer": "",
        "region": "",
        "appellation": "",
        "url": "",
    }


def parse_bi_row(row: pd.Series) -> dict:
    lwin = clean_lwin(row.get("lwin", ""))
    vintage = clean_vintage(row.get("vintage", ""))

    # Fallback from BI wine_key, usually lwin|vintage|pack
    if not lwin or not vintage:
        parts = str(row.get("wine_key", "")).split("|")
        if len(parts) >= 2:
            lwin = clean_lwin(parts[0])
            vintage = clean_vintage(parts[1])

    canonical_key = f"{lwin}|{vintage}" if lwin and vintage else str(row.get("wine_key", ""))

    return {
        "canonical_key": canonical_key,
        "lwin": lwin,
        "vintage": vintage,
        "bottle_size": str(row.get("bottle_size", "") or ""),
        "wine_name": str(row.get("wine_name", "") or ""),
        "producer": str(row.get("producer", "") or ""),
        "region": str(row.get("region", "") or ""),
        "appellation": str(row.get("appellation", "") or ""),
        "url": str(row.get("url", "") or ""),
    }


def load_bbx(conn: sqlite3.Connection) -> pd.DataFrame:
    bbx = pd.read_sql_query(
        """
        SELECT
            snapshot_date,
            source,
            wine_key,
            price_per_6
        FROM wine_price_features_daily
        WHERE source = 'bbx'
        """,
        conn,
    )

    if bbx.empty:
        return bbx

    parsed = bbx["wine_key"].apply(parse_bbx_wine_key).apply(pd.Series)
    out = pd.concat([bbx, parsed], axis=1)
    out["source"] = "bbx"
    out["price_per_6"] = pd.to_numeric(out["price_per_6"], errors="coerce")
    out["snapshot_date"] = pd.to_datetime(out["snapshot_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return out


def load_bi(conn: sqlite3.Connection) -> pd.DataFrame:
    bi = pd.read_sql_query(
        """
        SELECT
            snapshot_date,
            source,
            wine_key,
            lwin,
            vintage,
            bottle_size,
            wine_name,
            producer,
            region,
            appellation,
            price_per_6,
            url
        FROM price_history_daily
        WHERE source = 'bi'
        """,
        conn,
    )

    if bi.empty:
        return bi

    parsed = bi.apply(parse_bi_row, axis=1).apply(pd.Series)

    # Drop duplicate parsed columns from original BI before concat.
    base = bi[["snapshot_date", "source", "wine_key", "price_per_6"]].copy()
    out = pd.concat([base, parsed], axis=1)
    out["source"] = "bi"
    out["price_per_6"] = pd.to_numeric(out["price_per_6"], errors="coerce")
    out["snapshot_date"] = pd.to_datetime(out["snapshot_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return out


def main() -> None:
    conn = sqlite3.connect(DB_PATH)

    bbx = load_bbx(conn)
    bi = load_bi(conn)

    combined = pd.concat([bbx, bi], ignore_index=True)

    combined = combined.dropna(subset=["snapshot_date", "source", "canonical_key", "price_per_6"])
    combined = combined[combined["canonical_key"].astype(str).str.strip() != ""]
    combined = combined[combined["price_per_6"] > 0]

    # Deduplicate variants such as 1015362 and 1015362.0.
    # Median keeps this robust if there are duplicate same-day rows.
    agg_cols = {
        "price_per_6": "median",
        "wine_key": "first",
        "lwin": "first",
        "vintage": "first",
        "bottle_size": "first",
        "wine_name": "first",
        "producer": "first",
        "region": "first",
        "appellation": "first",
        "url": "first",
    }

    combined = (
        combined.groupby(["snapshot_date", "source", "canonical_key"], as_index=False)
        .agg(agg_cols)
        .sort_values(["canonical_key", "source", "snapshot_date"])
    )

    combined.to_sql(OUT_TABLE, conn, if_exists="replace", index=False)
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{OUT_TABLE}_canonical_source_date "
        f"ON {OUT_TABLE}(canonical_key, source, snapshot_date)"
    )
    conn.commit()
    conn.close()

    print(f"Combined trend rows: {len(combined):,}")
    print("\nRows by source:")
    print(combined.groupby("source").size().to_string())
    print("\nDate ranges:")
    print(combined.groupby("source")["snapshot_date"].agg(["min", "max"]).to_string())
    print("\nExample overlapping keys:")
    overlap = combined.groupby("canonical_key")["source"].nunique()
    print(f"Keys with both BI and BBX: {(overlap >= 2).sum():,}")


if __name__ == "__main__":
    main()
