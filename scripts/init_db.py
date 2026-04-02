import sqlite3
from pathlib import Path

Path("db").mkdir(exist_ok=True)

conn = sqlite3.connect("db/wine.db")

conn.executescript("""
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS listings (
    id INTEGER PRIMARY KEY,
    snapshot_id INTEGER,
    wine TEXT,
    vintage TEXT,
    price REAL,
    raw_json TEXT
);
""")

conn.close()
print("DB ready")

