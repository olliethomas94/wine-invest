#!/usr/bin/env python3
"""
build_action_queue.py

Build a practical fine wine trading action queue.

This version is designed for your current schema:
- wine_price_features_daily is a LONG historical/features table:
    snapshot_date, source, wine_key, price_per_6, price_30d_median, etc.
- The action queue must therefore start from the BI-vs-BBX comparison table,
  not directly from wine_price_features_daily.

The script will:
1. Auto-detect the best comparison table containing BBX and BI prices
2. Calculate spread correctly as BI price - BBX price
3. Enrich rows with historical BBX/BI features where possible
4. Apply hard filters, realistic exit-price logic, liquidity/staleness gates
5. Write:
   - action_queue
   - action_queue_rejected
   - optional debug CSVs

Run:
    python scripts/build_action_queue.py --include-rejected-csv

If auto-detection picks the wrong table, specify it explicitly:
    python scripts/build_action_queue.py --comparison-table bi_best_offer_comparison
"""

from __future__ import annotations

import argparse
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "db" / "wine.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

ACTION_QUEUE_TABLE = "action_queue"
REJECTED_SIGNALS_TABLE = "action_queue_rejected"


@dataclass(frozen=True)
class Thresholds:
    min_bbx_price: float = 300.0
    max_spread_pct: float = 60.0
    max_bi_to_bbx_ratio: float = 1.60
    min_realistic_net_spread_pct: float = 8.0
    strong_realistic_net_spread_pct: float = 15.0
    stale_reject_days: int = 45
    stale_review_days: int = 21
    stale_review_spread_pct: float = 25.0
    min_liquidity_score: float = 20.0
    weak_liquidity_score: float = 40.0
    high_volatility_score: float = 80.0
    min_match_quality_general: float = 70.0
    min_match_quality_burgundy: float = 90.0
    suspicious_large_spread_pct: float = 40.0


THRESHOLDS = Thresholds()


# -----------------------------------------------------------------------------
# SQLite helpers
# -----------------------------------------------------------------------------

def connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone() is not None


def list_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    return [r[0] for r in rows]


def get_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    if not table_exists(conn, table_name):
        return []
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]


def first_existing_column(columns: Iterable[str], candidates: Iterable[str]) -> Optional[str]:
    colset = set(columns)
    lower_map = {c.lower(): c for c in columns}

    for c in candidates:
        if c in colset:
            return c
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


def read_table(conn: sqlite3.Connection, table_name: str) -> pd.DataFrame:
    return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)


# -----------------------------------------------------------------------------
# Comparison table discovery
# -----------------------------------------------------------------------------

def score_comparison_table(conn: sqlite3.Connection, table_name: str) -> int:
    cols = get_columns(conn, table_name)
    low = [c.lower() for c in cols]
    joined = " ".join(low)

    score = 0

    # Strong hints from names.
    if "comparison" in table_name.lower():
        score += 30
    if "bi" in table_name.lower():
        score += 20
    if "bbx" in table_name.lower():
        score += 20
    if "offer" in table_name.lower():
        score += 10

    # Strong hints from columns.
    if any("bbx" in c for c in low):
        score += 30
    if any("bi" in c for c in low):
        score += 30
    if any("spread" in c for c in low):
        score += 20
    if any("lwin" in c for c in low):
        score += 15
    if any("vintage" in c for c in low):
        score += 10
    if any("offer_per_6" in c for c in low):
        score += 20
    if "price_per_6" in joined:
        score += 5

    # Penalise pure historical/features tables.
    if table_name == "wine_price_features_daily":
        score -= 100
    if "source" in low and "price_per_6" in low and not any("bbx" in c for c in low):
        score -= 50

    return score


def auto_detect_comparison_table(conn: sqlite3.Connection) -> str:
    tables = list_tables(conn)
    scored = [(score_comparison_table(conn, t), t, get_columns(conn, t)) for t in tables]
    scored = sorted(scored, reverse=True, key=lambda x: x[0])

    if not scored or scored[0][0] <= 0:
        raise RuntimeError(
            "Could not auto-detect a BI-vs-BBX comparison table. "
            "Run: sqlite3 db/wine.db \".tables\" and pass --comparison-table TABLE_NAME"
        )

    return scored[0][1]


# -----------------------------------------------------------------------------
# Column resolution
# -----------------------------------------------------------------------------

def resolve_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    cols = list(df.columns)

    return {
        "lwin": first_existing_column(cols, ["lwin", "LWIN", "lwin_code", "bbx_lwin", "bi_lwin"]),
        "vintage": first_existing_column(cols, ["vintage", "Vintage", "bbx_vintage", "bi_vintage"]),
        "wine_key": first_existing_column(cols, ["wine_key", "key", "normalized_key", "product_key"]),
        "wine_name": first_existing_column(cols, ["wine_name", "name", "product_name", "property", "bbx_description", "bbx_name", "bi_name", "bbx_wine_name", "bi_wine_name"]),
        "producer": first_existing_column(cols, ["producer", "producer_name", "bbx_producer", "bi_producer"]),
        "region": first_existing_column(cols, ["region", "appellation", "bbx_region", "bi_region"]),
        "bottle_size": first_existing_column(cols, ["bottle_size", "format", "case_format", "pack_size", "case_size"]),

        # Main price columns. Add more here if your comparison table has different names.
        "bbx_price": first_existing_column(cols, [
            "bbx_price",
            "bbx_price_per_6",
            "bbx_offer_per_6",
            "bbx_best_offer_per_6",
            "bbx_best_offer",
            "bbx_market_price",
            "bbx_price_per_case",
            "bbx_lowest_price",
            "bbx_lowest_per_6",
            "bbx_normalized_price",
        ]),
        "bi_price": first_existing_column(cols, [
            "bi_price",
            "bi_price_per_6",
            "bi_offer_per_6",
            "offer_per_6",
            "bi_best_offer_per_6",
            "bi_best_offer",
            "best_bi_offer_per_6",
            "bi_market_price",
            "bi_price_per_case",
            "bi_normalized_price",
        ]),

        "spread": first_existing_column(cols, ["spread", "gross_spread", "price_spread", "spread_value", "spread_abs_per_6"]),
        "spread_pct": first_existing_column(cols, ["spread_pct", "gross_spread_pct", "spread_percent"]),
        "net_spread_pct": first_existing_column(cols, ["net_spread_pct", "net_spread_after_fees_pct"]),

        "fair_value": first_existing_column(cols, ["fair_value", "fair_value_price", "rolling_median", "market_median", "price_90d_median"]),
        "recent_market_median": first_existing_column(cols, ["recent_market_median", "market_30d_median", "rolling_30d_median", "price_30d_median"]),
        "liquidity_score": first_existing_column(cols, ["liquidity_score", "liquidity", "market_liquidity_score"]),
        "volatility_score": first_existing_column(cols, ["volatility_score", "volatility", "price_volatility_score"]),
        "persistence_score": first_existing_column(cols, ["persistence_score", "spread_persistence_score", "persistence"]),
        "fair_value_discount_pct": first_existing_column(cols, ["fair_value_discount_pct", "discount_to_fair_value_pct", "discount_to_90d_median_pct"]),
        "match_quality_score": first_existing_column(cols, ["match_quality_score", "match_score", "name_match_score", "match_confidence"]),
        "bi_price_age_days": first_existing_column(cols, ["bi_price_age_days", "bi_unchanged_days", "bi_price_unchanged_days"]),
        "bi_url": first_existing_column(cols, ["bi_url", "bi_link"]),
        "bbx_url": first_existing_column(cols, ["bbx_url", "bbx_link"]),
        "snapshot_date": first_existing_column(cols, ["snapshot_date", "date", "as_of_date", "run_date"]),
    }


def value(row: pd.Series, col: Optional[str], default: Any = None) -> Any:
    if col is None or col not in row.index:
        return default
    v = row[col]
    if pd.isna(v):
        return default
    return v


def num(row: pd.Series, col: Optional[str], default: float = 0.0) -> float:
    v = value(row, col, default)
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


# -----------------------------------------------------------------------------
# Historical feature enrichment
# -----------------------------------------------------------------------------

def normalise_key_part(x: Any) -> str:
    if x is None or pd.isna(x):
        return ""
    s = str(x).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def make_join_key(df: pd.DataFrame, c: Dict[str, Optional[str]]) -> pd.Series:
    if c.get("wine_key") and c["wine_key"] in df.columns:
        return df[c["wine_key"]].map(normalise_key_part)

    parts = []
    for colname in ["lwin", "vintage", "bottle_size"]:
        col = c.get(colname)
        if col and col in df.columns:
            parts.append(df[col].map(normalise_key_part))

    if parts:
        out = parts[0]
        for p in parts[1:]:
            out = out + "|" + p
        return out

    return pd.Series([""] * len(df), index=df.index)


def enrich_with_history_features(conn: sqlite3.Connection, df: pd.DataFrame) -> pd.DataFrame:
    """Attach BBX/BI historical features from wine_price_features_daily where possible."""
    if not table_exists(conn, "wine_price_features_daily"):
        return df

    features = pd.read_sql_query("SELECT * FROM wine_price_features_daily", conn)
    if features.empty or "source" not in features.columns:
        return df

    # Use latest row per source/wine_key.
    if "snapshot_date" in features.columns:
        features = features.sort_values("snapshot_date")

    if "wine_key" not in features.columns:
        return df

    latest = features.drop_duplicates(subset=["source", "wine_key"], keep="last").copy()
    latest["join_key"] = latest["wine_key"].map(normalise_key_part)

    out = df.copy()
    c = resolve_columns(out)
    out["join_key"] = make_join_key(out, c)

    keep_cols = [
        "join_key",
        "price_30d_median",
        "price_90d_median",
        "price_180d_median",
        "discount_to_90d_median_pct",
        "discount_to_180d_median_pct",
        "volatility_30d",
        "volatility_90d",
        "liquidity_30d_avg",
    ]
    keep_cols = [x for x in keep_cols if x in latest.columns]

    for source in ["bbx", "bi"]:
        src = latest[latest["source"].astype(str).str.lower() == source][keep_cols].copy()
        if src.empty:
            continue
        rename = {col: f"{source}_{col}" for col in src.columns if col != "join_key"}
        src = src.rename(columns=rename)
        out = out.merge(src, on="join_key", how="left")

    out = out.drop(columns=["join_key"], errors="ignore")

    # Fill generic features from BBX history first, because the buy leg is BBX.
    if "bbx_price_90d_median" in out.columns and "fair_value" not in out.columns:
        out["fair_value"] = out["bbx_price_90d_median"]
    if "bbx_price_30d_median" in out.columns and "recent_market_median" not in out.columns:
        out["recent_market_median"] = out["bbx_price_30d_median"]
    if "bbx_discount_to_90d_median_pct" in out.columns and "fair_value_discount_pct" not in out.columns:
        out["fair_value_discount_pct"] = out["bbx_discount_to_90d_median_pct"]
    if "bbx_volatility_30d" in out.columns and "volatility_score" not in out.columns:
        # Convert raw volatility into a rough 0-100 risk score.
        out["volatility_score"] = out["bbx_volatility_30d"].fillna(0).clip(lower=0, upper=100)
    if "bbx_liquidity_30d_avg" in out.columns and "liquidity_score" not in out.columns:
        # Scale observed avg count to a rough score. This is deliberately lenient:
        # 1 observation/month ~= 25, 2 ~= 50, 4+ ~= 100.
        out["liquidity_score"] = (out["bbx_liquidity_30d_avg"].fillna(0) * 25).clip(lower=0, upper=100)

    return out


# -----------------------------------------------------------------------------
# Signal calculations
# -----------------------------------------------------------------------------

def is_burgundy(row: pd.Series, c: Dict[str, Optional[str]]) -> bool:
    region = str(value(row, c["region"], "")).lower()
    name = str(value(row, c["wine_name"], "")).lower()
    return any(term in region or term in name for term in [
        "burgundy", "bourgogne", "cote d'or", "côte d'or", "cote de nuits", "côte de nuits",
        "cote de beaune", "côte de beaune", "gevrey", "vosne", "chambolle", "meursault",
        "puligny", "chassagne", "nuits-saint-georges", "romanee", "romanée",
    ])


def calculate_prices(row: pd.Series, c: Dict[str, Optional[str]]) -> Dict[str, float]:
    bbx_price = num(row, c["bbx_price"])
    bi_price = num(row, c["bi_price"])

    spread = bi_price - bbx_price
    spread_pct = spread / bbx_price * 100.0 if bbx_price > 0 else 0.0

    if c["net_spread_pct"]:
        net_spread_pct = num(row, c["net_spread_pct"])
    else:
        # Rough all-in haircut until replaced by exact fee model.
        net_spread_pct = spread_pct - 7.0

    fair_value = num(row, c["fair_value"], default=0.0)
    recent_market_median = num(row, c["recent_market_median"], default=0.0)

    candidates = []
    if bi_price > 0:
        candidates.append(bi_price * 0.97)
    if fair_value > 0:
        candidates.append(fair_value * 1.05)
    if recent_market_median > 0:
        candidates.append(recent_market_median * 1.05)

    realistic_exit_price = min(candidates) if candidates else 0.0

    estimated_buy_cost_pct = 2.5
    estimated_sell_cost_pct = 5.0
    estimated_slippage_pct = 2.0
    total_cost = bbx_price * ((estimated_buy_cost_pct + estimated_sell_cost_pct + estimated_slippage_pct) / 100.0)

    realistic_net_spread = realistic_exit_price - bbx_price - total_cost
    realistic_net_spread_pct = realistic_net_spread / bbx_price * 100.0 if bbx_price > 0 else 0.0

    return {
        "bbx_price": bbx_price,
        "bi_price": bi_price,
        "spread": spread,
        "spread_pct": spread_pct,
        "net_spread_pct": net_spread_pct,
        "fair_value": fair_value,
        "recent_market_median": recent_market_median,
        "realistic_exit_price": realistic_exit_price,
        "realistic_net_spread": realistic_net_spread,
        "realistic_net_spread_pct": realistic_net_spread_pct,
    }


def infer_match_quality(row: pd.Series, c: Dict[str, Optional[str]]) -> float:
    existing = num(row, c["match_quality_score"], default=-1.0)
    if existing >= 0:
        return existing

    score = 0.0
    if value(row, c["lwin"]):
        score += 30
    if value(row, c["vintage"]):
        score += 20
    if value(row, c["bottle_size"]):
        score += 15
    if value(row, c["producer"]):
        score += 10
    if value(row, c["wine_name"]):
        score += 10
    if value(row, c["region"]):
        score += 5

    return min(score, 90.0)


def infer_liquidity_score(row: pd.Series, c: Dict[str, Optional[str]]) -> float:
    existing = num(row, c["liquidity_score"], default=-1.0)
    if existing >= 0:
        return existing
    return 50.0


def has_liquidity_data(row: pd.Series, c: Dict[str, Optional[str]]) -> bool:
    col = c.get("liquidity_score")
    if col is None or col not in row.index:
        return False
    return not pd.isna(row[col])


def infer_volatility_score(row: pd.Series, c: Dict[str, Optional[str]]) -> float:
    existing = num(row, c["volatility_score"], default=-1.0)
    if existing >= 0:
        return existing
    return 50.0


def infer_persistence_score(row: pd.Series, c: Dict[str, Optional[str]]) -> float:
    existing = num(row, c["persistence_score"], default=-1.0)
    if existing >= 0:
        return existing
    return 50.0


def infer_fair_value_discount_pct(row: pd.Series, c: Dict[str, Optional[str]], prices: Dict[str, float]) -> float:
    existing = num(row, c["fair_value_discount_pct"], default=-999.0)
    if existing != -999.0:
        return existing
    fair_value = prices["fair_value"]
    bbx_price = prices["bbx_price"]
    if fair_value > 0 and bbx_price > 0:
        return (fair_value - bbx_price) / fair_value * 100.0
    return 0.0


def evaluate_signal(row: pd.Series, c: Dict[str, Optional[str]], t: Thresholds = THRESHOLDS) -> Dict[str, Any]:
    prices = calculate_prices(row, c)
    reasons: List[str] = []
    warnings: List[str] = []

    bbx_price = prices["bbx_price"]
    bi_price = prices["bi_price"]
    spread_pct = prices["spread_pct"]
    net_spread_pct = prices["net_spread_pct"]
    realistic_net_spread_pct = prices["realistic_net_spread_pct"]

    burgundy = is_burgundy(row, c)
    match_quality_score = infer_match_quality(row, c)
    liquidity_score = infer_liquidity_score(row, c)
    volatility_score = infer_volatility_score(row, c)
    persistence_score = infer_persistence_score(row, c)
    fair_value_discount_pct = infer_fair_value_discount_pct(row, c, prices)
    bi_price_age_days = num(row, c["bi_price_age_days"], default=0.0)

    if bbx_price <= 0:
        reasons.append("missing_or_invalid_bbx_price")
    if bi_price <= 0:
        reasons.append("missing_or_invalid_bi_price")
    if bbx_price > 0 and bbx_price < t.min_bbx_price:
        reasons.append("bbx_price_below_minimum")
    if spread_pct <= 0:
        reasons.append("non_positive_spread")
    if spread_pct > t.max_spread_pct:
        reasons.append("spread_pct_above_sanity_limit")
    if bbx_price > 0 and bi_price > bbx_price * t.max_bi_to_bbx_ratio:
        reasons.append("bi_price_more_than_1_6x_bbx")
    if net_spread_pct < t.min_realistic_net_spread_pct:
        warnings.append("raw_net_spread_below_target")

    if realistic_net_spread_pct < t.min_realistic_net_spread_pct:
        reasons.append("realistic_net_spread_below_minimum")

    if burgundy and match_quality_score < t.min_match_quality_burgundy:
        reasons.append("burgundy_match_quality_too_low")
    elif match_quality_score < t.min_match_quality_general:
        reasons.append("match_quality_too_low")

    if bi_price_age_days > t.stale_reject_days:
        reasons.append("bi_price_stale_over_45_days")
    elif bi_price_age_days > t.stale_review_days and spread_pct > t.stale_review_spread_pct:
        warnings.append("bi_price_potentially_stale")

    if has_liquidity_data(row, c) and liquidity_score < t.min_liquidity_score:
        warnings.append("liquidity_score_low")
    elif not has_liquidity_data(row, c):
        warnings.append("liquidity_data_missing")
    if volatility_score > t.high_volatility_score and liquidity_score < t.weak_liquidity_score:
        warnings.append("high_volatility_low_liquidity")

    if spread_pct > t.suspicious_large_spread_pct:
        warnings.append("large_spread_manual_check")
    if burgundy:
        warnings.append("burgundy_manual_cuvee_check")

    if reasons:
        status = "rejected"
    elif warnings:
        status = "review"
    else:
        status = "accepted"

    spread_component = min(max(realistic_net_spread_pct, 0), 30) / 30 * 30
    match_component = min(max(match_quality_score, 0), 100) / 100 * 25
    liquidity_component = min(max(liquidity_score, 0), 100) / 100 * 20
    fair_value_component = min(max(fair_value_discount_pct, 0), 25) / 25 * 15
    persistence_component = min(max(persistence_score, 0), 100) / 100 * 10

    opportunity_score = spread_component + match_component + liquidity_component + fair_value_component + persistence_component

    if burgundy and match_quality_score < 95:
        opportunity_score -= 20
    if bi_price_age_days > t.stale_review_days:
        opportunity_score -= 25
    if spread_pct > t.suspicious_large_spread_pct:
        opportunity_score -= 15
    if liquidity_score < t.weak_liquidity_score:
        opportunity_score -= 20

    opportunity_score = max(0.0, min(100.0, opportunity_score))

    if status == "rejected":
        signal_grade = "D"
    elif opportunity_score >= 80 and realistic_net_spread_pct >= t.strong_realistic_net_spread_pct:
        signal_grade = "A"
    elif opportunity_score >= 65:
        signal_grade = "B"
    else:
        signal_grade = "C"

    return {
        "status": status,
        "signal_grade": signal_grade,
        "rejection_reasons": ",".join(reasons),
        "warning_flags": ",".join(warnings),
        "is_burgundy": int(burgundy),
        "match_quality_score": round(match_quality_score, 2),
        "liquidity_score": round(liquidity_score, 2),
        "volatility_score": round(volatility_score, 2),
        "persistence_score": round(persistence_score, 2),
        "fair_value_discount_pct": round(fair_value_discount_pct, 2),
        "bi_price_age_days": round(bi_price_age_days, 2),
        "opportunity_score": round(opportunity_score, 2),
        **{k: round(v, 2) for k, v in prices.items()},
    }


# -----------------------------------------------------------------------------
# Output
# -----------------------------------------------------------------------------

def build_output_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    c = resolve_columns(df)

    if not c["bbx_price"] or not c["bi_price"]:
        raise RuntimeError(
            "Could not identify BBX and BI price columns in the comparison table.\n"
            f"Available columns: {list(df.columns)}\n"
            "Add your real column names to resolve_columns() under bbx_price / bi_price."
        )

    records: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for _, row in df.iterrows():
        evaluation = evaluate_signal(row, c)
        record = {
            "created_at": now,
            "snapshot_date": value(row, c["snapshot_date"], None),
            "lwin": value(row, c["lwin"], None),
            "vintage": value(row, c["vintage"], None),
            "wine_key": value(row, c["wine_key"], None),
            "wine_name": value(row, c["wine_name"], None),
            "producer": value(row, c["producer"], None),
            "region": value(row, c["region"], None),
            "bottle_size": value(row, c["bottle_size"], None),
            "bbx_url": value(row, c["bbx_url"], None),
            "bi_url": value(row, c["bi_url"], None),
            **evaluation,
        }
        records.append(record)

    out = pd.DataFrame(records)

    queue_first = {"accepted": 0, "review": 1, "rejected": 2}
    out["_status_sort"] = out["status"].map(queue_first).fillna(9)
    out = out.sort_values(
        by=["_status_sort", "signal_grade", "opportunity_score", "realistic_net_spread_pct"],
        ascending=[True, True, False, False],
    ).drop(columns=["_status_sort"])

    return out


def write_tables(conn: sqlite3.Connection, out: pd.DataFrame) -> None:
    queue = out[out["status"].isin(["accepted", "review"])].copy()
    rejected = out[out["status"] == "rejected"].copy()

    if len(queue):
        queue = queue.sort_values(
            by=["signal_grade", "opportunity_score", "realistic_net_spread_pct"],
            ascending=[True, False, False],
        )

    if len(rejected):
        rejected = rejected.sort_values(
            by=["opportunity_score", "spread_pct"],
            ascending=[False, False],
        )

    queue.to_sql(ACTION_QUEUE_TABLE, conn, if_exists="replace", index=False)
    rejected.to_sql(REJECTED_SIGNALS_TABLE, conn, if_exists="replace", index=False)

    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{ACTION_QUEUE_TABLE}_lwin_vintage ON {ACTION_QUEUE_TABLE}(lwin, vintage)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{ACTION_QUEUE_TABLE}_grade ON {ACTION_QUEUE_TABLE}(signal_grade)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{ACTION_QUEUE_TABLE}_score ON {ACTION_QUEUE_TABLE}(opportunity_score)")
    conn.commit()


def print_summary(out: pd.DataFrame, comparison_table: str, columns: Dict[str, Optional[str]]) -> None:
    print("\nAction queue build complete")
    print("=" * 72)
    print(f"Comparison table: {comparison_table}")
    print(f"BBX price column: {columns['bbx_price']}")
    print(f"BI price column : {columns['bi_price']}")
    print(f"Total evaluated : {len(out):,}")

    status_counts = out["status"].value_counts(dropna=False).to_dict()
    for status in ["accepted", "review", "rejected"]:
        print(f"{status.title():<10}: {status_counts.get(status, 0):,}")

    print("\nGrades in queue:")
    queue = out[out["status"].isin(["accepted", "review"])]
    if len(queue):
        print(queue["signal_grade"].value_counts().sort_index().to_string())
    else:
        print("No accepted/review signals")

    print("\nTop rejection reasons:")
    rejected = out[out["status"] == "rejected"]
    if len(rejected):
        reason_counts: Dict[str, int] = {}
        for reasons in rejected["rejection_reasons"].fillna(""):
            for reason in str(reasons).split(","):
                reason = reason.strip()
                if reason:
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
        for reason, count in sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)[:15]:
            print(f"{reason:<40} {count:,}")
    else:
        print("No rejected signals")

    print("\nTop 20 queue candidates:")
    display_cols = [
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
        "bi_price_age_days",
        "lwin",
        "vintage",
        "wine_key",
        "wine_name",
        "warning_flags",
    ]
    display_cols = [c for c in display_cols if c in queue.columns]
    if len(queue):
        print(queue[display_cols].head(20).to_string(index=False))
    else:
        print("No queue candidates")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build fine wine trading action queue")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to SQLite database")
    parser.add_argument("--comparison-table", default=None, help="BI-vs-BBX comparison table to use")
    parser.add_argument("--include-rejected-csv", action="store_true", help="Export debug CSVs")
    parser.add_argument("--csv-dir", default=str(PROJECT_ROOT / "data"), help="CSV export directory")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    with connect(db_path) as conn:
        comparison_table = args.comparison_table or auto_detect_comparison_table(conn)
        if not table_exists(conn, comparison_table):
            raise RuntimeError(f"Comparison table not found: {comparison_table}")

        df = read_table(conn, comparison_table)
        df = enrich_with_history_features(conn, df)
        columns = resolve_columns(df)
        out = build_output_dataframe(df)
        write_tables(conn, out)

    if args.include_rejected_csv:
        csv_dir = Path(args.csv_dir)
        csv_dir.mkdir(parents=True, exist_ok=True)
        out[out["status"] == "rejected"].to_csv(csv_dir / "action_queue_rejected_debug.csv", index=False)
        out[out["status"].isin(["accepted", "review"])].to_csv(csv_dir / "action_queue_debug.csv", index=False)

    print_summary(out, comparison_table, columns)


if __name__ == "__main__":
    main()
