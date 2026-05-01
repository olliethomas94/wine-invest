import sqlite3
import pandas as pd

DB_PATH = "db/wine.db"

conn = sqlite3.connect(DB_PATH)

bbx = pd.read_sql("SELECT * FROM bbx_normalized_latest", conn)
bi = pd.read_sql("SELECT * FROM bi_normalized_latest", conn)

print("BBX columns:", bbx.columns.tolist())
print("BI columns:", bi.columns.tolist())

bbx_work = pd.DataFrame({
    "property": bbx["property"],
    "bbx_description": bbx["bbx_description"],
    "lwin_code": bbx["lwin_code"].astype(str),
    "vintage": bbx["vintage"].astype(str),
    "bottle_size": bbx["bottle_size"].astype(str),
    "bbx_lowest_per_6": pd.to_numeric(bbx["bbx_lowest_per_6"], errors="coerce"),
    "bbx_url": bbx["bbx_url"],
})

bi_work = pd.DataFrame({
    "bi_name": bi["bi_wine_name"],
    "lwin_code": bi["lwin"].astype(str),
    "vintage": bi["vintage"].astype(str),
    "bottle_size": "Case of 6 Bottles",
    "best_bi_offer_per_6": pd.to_numeric(bi["offer_per_6"], errors="coerce"),
    "bi_url": bi["bi_url"],
})

bbx_work["join_key"] = (
    bbx_work["lwin_code"].fillna("")
    + "|"
    + bbx_work["vintage"].fillna("")
    + "|"
    + bbx_work["bottle_size"].fillna("")
)

bi_work["join_key"] = (
    bi_work["lwin_code"].fillna("")
    + "|"
    + bi_work["vintage"].fillna("")
    + "|"
    + bi_work["bottle_size"].fillna("")
)

bi_best = (
    bi_work.dropna(subset=["best_bi_offer_per_6"])
    .sort_values("best_bi_offer_per_6", ascending=True)
    .drop_duplicates(subset=["join_key"], keep="first")
)

df = bbx_work.merge(
    bi_best,
    on="join_key",
    how="inner",
    suffixes=("", "_bi")
)

df = df.dropna(subset=["bbx_lowest_per_6", "best_bi_offer_per_6"])

# Correct direction:
# BUY BBX, exit near BI
df["spread_abs_per_6"] = df["best_bi_offer_per_6"] - df["bbx_lowest_per_6"]
df["spread_pct"] = df["spread_abs_per_6"] / df["bbx_lowest_per_6"] * 100

# Keep only where BI is higher than BBX
df = df[df["best_bi_offer_per_6"] > df["bbx_lowest_per_6"]].copy()

df["match_confidence"] = "high"

out = pd.DataFrame({
    "property": df["property"],
    "bbx_description": df["bbx_description"],
    "lwin_code": df["lwin_code"],
    "vintage": df["vintage"],
    "bottle_size": df["bottle_size"],
    "bbx_lowest_per_6": df["bbx_lowest_per_6"],
    "best_bi_offer_per_6": df["best_bi_offer_per_6"],
    "spread_abs_per_6": df["spread_abs_per_6"],
    "spread_pct": df["spread_pct"],
    "match_confidence": df["match_confidence"],
    "bbx_url": df["bbx_url"],
    "bi_url": df["bi_url"],
})

bad = out[out["bbx_lowest_per_6"] >= out["best_bi_offer_per_6"]]

if not bad.empty:
    raise SystemExit("Bad direction rows detected. Refusing to write table.")

out = out.sort_values("spread_pct", ascending=False)

out.to_sql(
    "bi_bbx_best_offer_comparison",
    conn,
    if_exists="replace",
    index=False
)

conn.close()

print(f"Built BI/BBX comparison rows: {len(out)}")
print("Positive spread = BI price higher than BBX price.")
print("Trade direction = BUY BBX, exit near BI.")