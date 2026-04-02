import sqlite3

conn = sqlite3.connect("db/wine.db")

sql = """
DELETE FROM market_stats
WHERE snapshot_id = (SELECT MAX(id) FROM snapshots);

INSERT INTO market_stats (
    snapshot_id,
    property,
    vintage,
    listing_count,
    lowest_price,
    median_price,
    avg_price,
    highest_price,
    price_range
)
WITH latest_snapshot AS (
    SELECT MAX(id) AS snapshot_id
    FROM snapshots
),
base AS (
    SELECT
        l.snapshot_id,
        l.property,
        l.vintage,
        l.price_ex_tax
    FROM listings l
    JOIN latest_snapshot s
      ON l.snapshot_id = s.snapshot_id
    WHERE l.price_ex_tax IS NOT NULL
),
ordered AS (
    SELECT
        snapshot_id,
        property,
        vintage,
        price_ex_tax,
        ROW_NUMBER() OVER (
            PARTITION BY snapshot_id, property, vintage
            ORDER BY price_ex_tax
        ) AS rn,
        COUNT(*) OVER (
            PARTITION BY snapshot_id, property, vintage
        ) AS cnt
    FROM base
),
medians AS (
    SELECT
        snapshot_id,
        property,
        vintage,
        AVG(price_ex_tax) AS median_price
    FROM ordered
    WHERE rn IN ((cnt + 1) / 2, (cnt + 2) / 2)
    GROUP BY snapshot_id, property, vintage
),
aggregates AS (
    SELECT
        snapshot_id,
        property,
        vintage,
        COUNT(*) AS listing_count,
        MIN(price_ex_tax) AS lowest_price,
        AVG(price_ex_tax) AS avg_price,
        MAX(price_ex_tax) AS highest_price
    FROM base
    GROUP BY snapshot_id, property, vintage
)
SELECT
    a.snapshot_id,
    a.property,
    a.vintage,
    a.listing_count,
    a.lowest_price,
    m.median_price,
    a.avg_price,
    a.highest_price,
    a.highest_price - a.lowest_price AS price_range
FROM aggregates a
JOIN medians m
  ON a.snapshot_id = m.snapshot_id
 AND a.property = m.property
 AND (
      (a.vintage = m.vintage)
      OR (a.vintage IS NULL AND m.vintage IS NULL)
 )
"""

conn.executescript(sql)
conn.close()

print("market_stats built for latest snapshot")
