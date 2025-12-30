CREATE TABLE IF NOT EXISTS stock_data_processed (
    symbol          VARCHAR(10) NOT NULL,
    date            DATE NOT NULL,
    open            NUMERIC NOT NULL,
    high            NUMERIC NOT NULL,
    low             NUMERIC NOT NULL,
    close           NUMERIC NOT NULL,
    volume          BIGINT,
    daily_return    NUMERIC,
    PRIMARY KEY (symbol, date)
);