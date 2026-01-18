CREATE TABLE cse_prices (
    ticker TEXT NOT NULL,
    trade_date DATE NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    adj_close NUMERIC,
    volume BIGINT,
    PRIMARY KEY (ticker, trade_date)
);

CREATE INDEX idx_cse_prices_date ON cse_prices (trade_date);

CREATE TABLE exchange_rates (
    rate_date DATE NOT NULL,
    base_currency TEXT NOT NULL,
    quote_currency TEXT NOT NULL,
    rate NUMERIC,
    PRIMARY KEY (rate_date, base_currency, quote_currency)
);

CREATE INDEX idx_fx_rates_date ON exchange_rates (rate_date);

CREATE TABLE ingest_state (
    source_name TEXT NOT NULL PRIMARY KEY,
    last_date DATE
);