import sqlite3
from pathlib import Path

import pandas as pd

DB_PATH = "db/wine.db"
XLSX_PATH = "/Users/olliethomas/Downloads/LWINdatabase.xlsx"
SHEET_NAME = "LWINdatabase"


def clean_text(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(".0"):
        text = text[:-2]
    return text


def main():
    if not Path(XLSX_PATH).exists():
        raise FileNotFoundError(XLSX_PATH)

    df = pd.read_excel(XLSX_PATH, sheet_name=SHEET_NAME)

    expected = [
        "LWIN",
        "DISPLAY_NAME",
        "PRODUCER_TITLE",
        "PRODUCER_NAME",
        "WINE",
        "COUNTRY",
        "REGION",
        "SUB_REGION",
        "COLOUR",
        "TYPE",
        "SUB_TYPE",
        "DESIGNATION",
        "CLASSIFICATION",
        "STATUS",
    ]

    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.executescript("""
    CREATE TABLE IF NOT EXISTS wine_master (
        lwin_code TEXT PRIMARY KEY,
        status TEXT,
        display_name TEXT,
        producer_title TEXT,
        producer_name TEXT,
        wine_name TEXT,
        country TEXT,
        region TEXT,
        sub_region TEXT,
        colour TEXT,
        type TEXT,
        sub_type TEXT,
        designation TEXT,
        classification TEXT
    );
    """)

    rows = []
    for _, row in df.iterrows():
        lwin = clean_text(row.get("LWIN"))
        if not lwin:
            continue

        rows.append((
            lwin,
            clean_text(row.get("STATUS")),
            clean_text(row.get("DISPLAY_NAME")),
            clean_text(row.get("PRODUCER_TITLE")),
            clean_text(row.get("PRODUCER_NAME")),
            clean_text(row.get("WINE")),
            clean_text(row.get("COUNTRY")),
            clean_text(row.get("REGION")),
            clean_text(row.get("SUB_REGION")),
            clean_text(row.get("COLOUR")),
            clean_text(row.get("TYPE")),
            clean_text(row.get("SUB_TYPE")),
            clean_text(row.get("DESIGNATION")),
            clean_text(row.get("CLASSIFICATION")),
        ))

    cur.executemany("""
    INSERT INTO wine_master (
        lwin_code,
        status,
        display_name,
        producer_title,
        producer_name,
        wine_name,
        country,
        region,
        sub_region,
        colour,
        type,
        sub_type,
        designation,
        classification
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(lwin_code) DO UPDATE SET
        status = excluded.status,
        display_name = excluded.display_name,
        producer_title = excluded.producer_title,
        producer_name = excluded.producer_name,
        wine_name = excluded.wine_name,
        country = excluded.country,
        region = excluded.region,
        sub_region = excluded.sub_region,
        colour = excluded.colour,
        type = excluded.type,
        sub_type = excluded.sub_type,
        designation = excluded.designation,
        classification = excluded.classification
    """, rows)

    conn.commit()

    count = cur.execute("SELECT COUNT(*) FROM wine_master").fetchone()[0]
    conn.close()

    print(f"Imported/updated rows: {len(rows)}")
    print(f"wine_master total rows: {count}")


if __name__ == "__main__":
    main()
