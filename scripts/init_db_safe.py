import sqlite3
from pathlib import Path

Path("db").mkdir(exist_ok=True)

conn = sqlite3.connect("db/wine.db")

conn.executescript("""
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY,
    created_at TEXT NOT NULL,
    source_file TEXT
);

CREATE TABLE IF NOT EXISTS listings (
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

CREATE INDEX IF NOT EXISTS idx_listings_snapshot_id ON listings(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_listings_lwin_code ON listings(lwin_code);
CREATE INDEX IF NOT EXISTS idx_listings_property_vintage ON listings(property, vintage);

CREATE TABLE IF NOT EXISTS market_stats (
    id INTEGER PRIMARY KEY,
    snapshot_id INTEGER NOT NULL,
    property TEXT,
    vintage TEXT,
    listing_count INTEGER NOT NULL,
    lowest_price REAL,
    median_price REAL,
    avg_price REAL,
    highest_price REAL,
    price_range REAL,
    FOREIGN KEY (snapshot_id) REFERENCES snapshots(id)
);

CREATE INDEX IF NOT EXISTS idx_market_stats_snapshot
ON market_stats(snapshot_id);

CREATE INDEX IF NOT EXISTS idx_market_stats_property_vintage
ON market_stats(property, vintage);
""")

conn.close()
print("Safe DB init complete")
