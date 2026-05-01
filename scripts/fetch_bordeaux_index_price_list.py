import os
import json
import sqlite3
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "db" / "wine.db"

BASE_URL = "https://api.biwine.com/v2"

EMAIL = os.getenv("BI_USERNAME")
PASSWORD = os.getenv("BI_PASSWORD")
SALES_LEDGER = os.getenv("BI_SALES_LEDGER")
PURCHASE_LEDGER = os.getenv("BI_PURCHASE_LEDGER")


def login():
    response = requests.post(
        f"{BASE_URL}/user/login",
        json={
            "email": EMAIL,
            "password": PASSWORD,
            "salesLedger": SALES_LEDGER,
            "purchaseLedger": PURCHASE_LEDGER,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["token"]


def fetch_price_list(token):
    response = requests.get(
        f"{BASE_URL}/market/bi-price-list",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def ensure_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bordeaux_index_price_list_snapshots (
            id INTEGER PRIMARY KEY,
            snapshot_date DATE,
            row_count INTEGER,
            status TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bordeaux_index_price_list (
            id INTEGER PRIMARY KEY,
            snapshot_id INTEGER,
            code TEXT,
            name TEXT,
            vintage INTEGER,
            grower TEXT,
            colour TEXT,
            region TEXT,
            appellation TEXT,
            currency TEXT,
            tax TEXT,
            pack INTEGER,
            size REAL,
            lwin TEXT,
            bordeaux_code TEXT,
            product_type TEXT,
            url TEXT,
            offer_price REAL,
            offer_cs INTEGER,
            offer_btl INTEGER,
            raw_json TEXT
        )
    """)


def create_snapshot(conn):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO bordeaux_index_price_list_snapshots (
            snapshot_date,
            row_count,
            status
        )
        VALUES (date('now'), 0, 'running')
    """)
    return cur.lastrowid


def insert_rows(conn, snapshot_id, rows):
    data = []

    for r in rows:
        data.append((
            snapshot_id,
            r.get("code"),
            r.get("name"),
            int(r["vintage"]) if r.get("vintage") else None,
            r.get("grower"),
            r.get("colour"),
            r.get("region"),
            r.get("appellation"),
            r.get("currency"),
            r.get("tax"),
            r.get("pack"),
            r.get("size"),
            str(r.get("lwin")) if r.get("lwin") is not None else None,
            str(r.get("bordeauxCode")) if r.get("bordeauxCode") is not None else None,
            r.get("productType"),
            r.get("url"),
            r.get("offer_price"),
            r.get("offer_cs"),
            r.get("offer_btl"),
            json.dumps(r),
        ))

    conn.executemany("""
        INSERT INTO bordeaux_index_price_list (
            snapshot_id,
            code,
            name,
            vintage,
            grower,
            colour,
            region,
            appellation,
            currency,
            tax,
            pack,
            size,
            lwin,
            bordeaux_code,
            product_type,
            url,
            offer_price,
            offer_cs,
            offer_btl,
            raw_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data)


def main():
    print("Logging in...")
    token = login()

    print("Fetching BI price list...")
    rows = fetch_price_list(token)

    print(f"Rows fetched: {len(rows)}")

    if not rows:
        raise RuntimeError("BI price list returned zero rows")

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA busy_timeout = 60000")

    ensure_tables(conn)

    snapshot_id = create_snapshot(conn)
    insert_rows(conn, snapshot_id, rows)

    conn.execute("""
        UPDATE bordeaux_index_price_list_snapshots
        SET row_count = ?, status = 'success'
        WHERE id = ?
    """, (len(rows), snapshot_id))

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_bi_price_list_snapshot_lwin_vintage
        ON bordeaux_index_price_list(snapshot_id, lwin, vintage)
    """)

    conn.commit()
    conn.close()

    print(f"Done. Inserted {len(rows)} BI price list rows.")


if __name__ == "__main__":
    main()