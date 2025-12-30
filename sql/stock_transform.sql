INSERT INTO stock_data_processed (
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    daily_return
)
SELECT DISTINCT
    symbol,
    date,
    FIRST_VALUE(open) OVER w AS open,
    MAX(high) OVER w AS high,
    MIN(low) OVER w AS low,
    LAST_VALUE(close) OVER w AS close,
    (AVG(volume) OVER w)::BIGINT AS volume,
    (LAST_VALUE(close) OVER w - FIRST_VALUE(open) OVER w) / FIRST_VALUE(open) OVER w AS daily_return
FROM (
    SELECT
        symbol,
        DATE(ingestion_time) AS date,
        open,
        high,
        low,
        close,
        volume,
        ingestion_time
    FROM stock_data
) t
WINDOW w AS (
    PARTITION BY symbol, date
    ORDER BY ingestion_time
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
ON CONFLICT (symbol, date) DO NOTHING;