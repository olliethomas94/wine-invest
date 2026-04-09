import sqlite3

conn = sqlite3.connect("db/wine.db")

conn.executescript("""
DROP VIEW IF EXISTS v_market_stats_enriched;

CREATE VIEW v_market_stats_enriched AS
SELECT
    m.snapshot_id,
    m.property,
    m.lwin_code,
    m.vintage,
    m.bottle_size,
    m.listing_count,
    m.lowest_price,
    m.median_price,
    m.avg_price,
    m.highest_price,
    m.price_range,
    w.status,
    w.display_name,
    w.producer_name,
    w.wine_name,
    w.country,
    w.region,
    w.sub_region,
    w.colour,
    w.type,
    w.sub_type,
    w.designation,
    w.classification,
    ROUND(
        CASE
            WHEN m.median_price > 0 THEN ((m.median_price - m.lowest_price) / m.median_price) * 100
            ELSE NULL
        END
    , 2) AS discount_pct,
    ROUND(
        (
            CASE
                WHEN m.median_price > 0 THEN ((m.median_price - m.lowest_price) / m.median_price) * 100
                ELSE 0
            END
        ) * 0.6
        +
        (
            CASE
                WHEN m.listing_count >= 5 THEN 20
                WHEN m.listing_count >= 3 THEN 12
                WHEN m.listing_count >= 2 THEN 6
                ELSE 0
            END
        ) * 0.25
        +
        (
            CASE
                WHEN m.median_price > 0 THEN (m.price_range / m.median_price) * 100
                ELSE 0
            END
        ) * 0.15
    , 2) AS opportunity_score
FROM market_stats m
LEFT JOIN wine_master w
    ON m.lwin_code = w.lwin_code;
""")

conn.close()
print("Enriched view built")
