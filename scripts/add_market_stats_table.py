import sqlite3

conn = sqlite3.connect("db/wine.db")

conn.executescript("""
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
print("market_stats table ready")
