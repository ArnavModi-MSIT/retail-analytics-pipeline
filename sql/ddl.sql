-- Star schema for Retail Analytics Data Pipeline
-- Grain of fact_sales: one row per Invoice line item (Invoice + StockCode)
-- Scope: valid, non-return, non-null-CustomerID transactions only.
--        Returns and quarantined rows are staged separately (parquet), not loaded here.

-- =========================
-- DIMENSION: dim_date
-- =========================
CREATE TABLE dim_date (
    date_key        INTEGER PRIMARY KEY,        -- YYYYMMDD
    full_date       DATE NOT NULL UNIQUE,
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    day             SMALLINT NOT NULL,
    day_of_week     SMALLINT NOT NULL,           -- 1=Monday .. 7=Sunday
    day_name        VARCHAR(10) NOT NULL,
    is_weekend      BOOLEAN NOT NULL
);

-- =========================
-- DIMENSION: dim_product
-- =========================
CREATE TABLE dim_product (
    product_key     SERIAL PRIMARY KEY,
    stock_code      VARCHAR(20) NOT NULL UNIQUE,
    description     VARCHAR(255),
    created_at      TIMESTAMP NOT NULL DEFAULT now()
);

-- =========================
-- DIMENSION: dim_customer
-- =========================
CREATE TABLE dim_customer (
    customer_key    SERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL UNIQUE,     -- source CustomerID, guaranteed non-null (Option A)
    created_at      TIMESTAMP NOT NULL DEFAULT now()
);

-- =========================
-- DIMENSION: dim_country
-- =========================
CREATE TABLE dim_country (
    country_key     SERIAL PRIMARY KEY,
    country_name    VARCHAR(100) NOT NULL UNIQUE
);

-- =========================
-- FACT: fact_sales
-- Grain: one row per (Invoice, StockCode) line item
-- =========================
CREATE TABLE fact_sales (
    fact_sales_key  BIGSERIAL PRIMARY KEY,
    invoice_no      VARCHAR(20) NOT NULL,
    product_key     INTEGER NOT NULL REFERENCES dim_product(product_key),
    customer_key    INTEGER NOT NULL REFERENCES dim_customer(customer_key),
    date_key        INTEGER NOT NULL REFERENCES dim_date(date_key),
    country_key     INTEGER NOT NULL REFERENCES dim_country(country_key),
    invoice_datetime TIMESTAMP NOT NULL,          -- full timestamp, kept alongside date_key for intraday analysis
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(10, 2) NOT NULL,
    revenue         NUMERIC(12, 2) NOT NULL,      -- quantity * unit_price, precomputed at load time
    source          VARCHAR(10) NOT NULL,          -- 'csv' (historical bulk) or 'api' (daily incremental)
    loaded_at       TIMESTAMP NOT NULL DEFAULT now()
);

-- =========================
-- INDEXES
-- =========================
CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_country ON fact_sales(country_key);
CREATE INDEX idx_fact_sales_invoice ON fact_sales(invoice_no);
