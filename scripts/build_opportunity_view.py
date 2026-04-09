import sqlite3

conn = sqlite3.connect("db/wine.db")

conn.executescript("""
DROP VIEW IF EXISTS v_opportunities;

CREATE VIEW v_opportunities AS
SELECT
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
    price_range,
    ROUND(
        CASE
            WHEN median_price > 0 THEN ((median_price - lowest_price) / median_price) * 100
            ELSE NULL
        END
    , 2) AS discount_pct,
    ROUND(
        (
            CASE
                WHEN median_price > 0 THEN ((median_price - lowest_price) / median_price) * 100
                ELSE 0
            END
        ) * 0.6
        +
        (
            CASE
                WHEN listing_count >= 5 THEN 20
                WHEN listing_count >= 3 THEN 12
                WHEN listing_count >= 2 THEN 6
                ELSE 0
            END
        ) * 0.25
        +
        (
            CASE
                WHEN median_price > 0 THEN (price_range / median_price) * 100
                ELSE 0
            END
        ) * 0.15
    , 2) AS opportunity_score
FROM market_stats;
""")

conn.close()
print("Opportunity view built")