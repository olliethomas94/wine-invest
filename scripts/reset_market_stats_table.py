import sqlite3

conn = sqlite3.connect("db/wine.db")

conn.executescript("""
DROP TABLE IF EXISTS market_stats;

CREATE TABLE market_stats (
    id INTEGER PRIMARY KEY,
    snapshot_id INTEGER NOT NULL,
    property TEXT,
    lwin_code TEXT,
    vintage TEXT,
    bottle_size TEXT,
    listing_count INTEGER NOT NULL,
    lowest_price REAL,
    median_price REAL,
    avg_price REAL,
    highest_price REAL,
    price_range REAL,
    FOREIGN KEY (snapshot_id) REFERENCES snapshots(id)
);

CREATE INDEX idx_market_stats_snapshot
ON market_stats(snapshot_id);

CREATE INDEX idx_market_stats_grouping
ON market_stats(lwin_code, vintage, bottle_size);
""")

conn.close()
print("market_stats table rebuilt")
