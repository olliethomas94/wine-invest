import sqlite3

conn = sqlite3.connect("db/wine.db")

sql = """
DELETE FROM market_stats
WHERE snapshot_id = (SELECT MAX(id) FROM snapshots);

INSERT INTO market_stats (
    snapshot_id,
    property,
    lwin_code,
    vintage,
    bottle_size,
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
        l.lwin_code,
        l.vintage,
        l.bottle_size,
        l.price_ex_tax
    FROM listings l
    JOIN latest_snapshot s
      ON l.snapshot_id = s.snapshot_id
    WHERE l.price_ex_tax IS NOT NULL
),
grouped AS (
    SELECT
        snapshot_id,
        COALESCE(lwin_code, '__NO_LWIN__') AS lwin_group,
        COALESCE(vintage, '__NO_VINTAGE__') AS vintage_group,
        COALESCE(bottle_size, '__NO_BOTTLESIZE__') AS bottle_group,
        property,
        lwin_code,
        vintage,
        bottle_size,
        price_ex_tax
    FROM base
),
ordered AS (
    SELECT
        snapshot_id,
        lwin_group,
        vintage_group,
        bottle_group,
        property,
        lwin_code,
        vintage,
        bottle_size,
        price_ex_tax,
        ROW_NUMBER() OVER (
            PARTITION BY snapshot_id, lwin_group, vintage_group, bottle_group
            ORDER BY price_ex_tax
        ) AS rn,
        COUNT(*) OVER (
            PARTITION BY snapshot_id, lwin_group, vintage_group, bottle_group
        ) AS cnt
    FROM grouped
),
medians AS (
    SELECT
        snapshot_id,
        lwin_group,
        vintage_group,
        bottle_group,
        AVG(price_ex_tax) AS median_price
    FROM ordered
    WHERE rn IN ((cnt + 1) / 2, (cnt + 2) / 2)
    GROUP BY snapshot_id, lwin_group, vintage_group, bottle_group
),
aggregates AS (
    SELECT
        snapshot_id,
        lwin_group,
        vintage_group,
        bottle_group,
        MIN(property) AS property,
        MIN(lwin_code) AS lwin_code,
        MIN(vintage) AS vintage,
        MIN(bottle_size) AS bottle_size,
        COUNT(*) AS listing_count,
        MIN(price_ex_tax) AS lowest_price,
        AVG(price_ex_tax) AS avg_price,
        MAX(price_ex_tax) AS highest_price
    FROM grouped
    GROUP BY snapshot_id, lwin_group, vintage_group, bottle_group
)
SELECT
    a.snapshot_id,
    a.property,
    a.lwin_code,
    a.vintage,
    a.bottle_size,
    a.listing_count,
    a.lowest_price,
    m.median_price,
    a.avg_price,
    a.highest_price,
    a.highest_price - a.lowest_price AS price_range
FROM aggregates a
JOIN medians m
  ON a.snapshot_id = m.snapshot_id
 AND a.lwin_group = m.lwin_group
 AND a.vintage_group = m.vintage_group
 AND a.bottle_group = m.bottle_group;
"""

conn.executescript(sql)
conn.close()

print("market_stats built for latest snapshot")
