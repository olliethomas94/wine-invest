import pandas as pd
import sqlite3
from datetime import datetime
import json
import glob

DB_PATH = "db/wine.db"
DATA_DIR = "data"

def clean_text(value):
    if pd.isna(value):
        return None
    return str(value).strip()

def clean_vintage(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text if text else None

def clean_price(value):
    if pd.isna(value):
        return None
    try:
        return float(value)
    except Exception:
        return None

def main():
    csv_files = sorted(glob.glob(f"{DATA_DIR}/*.csv"))
    if not csv_files:
        raise FileNotFoundError("No CSV files found in data/")

    latest_file = csv_files[-1]
    print(f"Using file: {latest_file}")

    df = pd.read_csv(latest_file)

    required_cols = [
        "Property",
        "Vintage",
        "Description",
        "PriceExTax",
        "BottleSize",
        "PageURL",
        "LwinCode",
        "BBRProductCode",
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO snapshots (created_at, source_file) VALUES (?, ?)",
        (datetime.utcnow().isoformat(), latest_file)
    )
    snapshot_id = cur.lastrowid

    for _, row in df.iterrows():
        raw_payload = {
            col: None if pd.isna(row.get(col)) else str(row.get(col))
            for col in df.columns
        }

        cur.execute(
            """
            INSERT INTO listings (
                snapshot_id,
                property,
                vintage,
                description,
                price_ex_tax,
                bottle_size,
                page_url,
                lwin_code,
                bbr_product_code,
                raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                clean_text(row.get("Property")),
                clean_vintage(row.get("Vintage")),
                clean_text(row.get("Description")),
                clean_price(row.get("PriceExTax")),
                clean_text(row.get("BottleSize")),
                clean_text(row.get("PageURL")),
                clean_text(row.get("LwinCode")),
                clean_text(row.get("BBRProductCode")),
                json.dumps(raw_payload),
            )
        )

    conn.commit()
    conn.close()

    print(f"Imported snapshot {snapshot_id} with {len(df)} rows")

if __name__ == "__main__":
    main()
