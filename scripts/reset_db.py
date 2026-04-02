import sqlite3
from pathlib import Path

Path("db").mkdir(exist_ok=True)

conn = sqlite3.connect("db/wine.db")

conn.executescript("""
DROP TABLE IF EXISTS listings;
DROP TABLE IF EXISTS snapshots;

CREATE TABLE snapshots (
    id INTEGER PRIMARY KEY,
    created_at TEXT NOT NULL,
    source_file TEXT
);

CREATE TABLE listings (
    id INTEGER PRIMARY KEY,
    snapshot_id INTEGER NOT NULL,
    property TEXT,
    vintage TEXT,
    description TEXT,
    price_ex_tax REAL,
    bottle_size TEXT,
    page_url TEXT,
    lwin_code TEXT,
    bbr_product_code TEXT,
    raw_json TEXT,
    FOREIGN KEY (snapshot_id) REFERENCES snapshots(id)
);

CREATE INDEX idx_listings_snapshot_id ON listings(snapshot_id);
CREATE INDEX idx_listings_lwin_code ON listings(lwin_code);
CREATE INDEX idx_listings_property_vintage ON listings(property, vintage);
""")

conn.close()
print("Database reset and rebuilt")

