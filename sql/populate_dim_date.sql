-- Populate dim_date once at setup.
-- Covers 2009-01-01 to 2030-12-31: historical CSV range (2009-2011) plus
-- present/future dates needed by daily API ingestion (fetched_at = today).

INSERT INTO dim_date (date_key, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
    d AS full_date,
    EXTRACT(YEAR FROM d)::SMALLINT AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT AS quarter,
    EXTRACT(MONTH FROM d)::SMALLINT AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(DAY FROM d)::SMALLINT AS day,
    EXTRACT(ISODOW FROM d)::SMALLINT AS day_of_week,  -- 1=Monday .. 7=Sunday
    TO_CHAR(d, 'Day') AS day_name,
    EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend
FROM generate_series('2009-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (date_key) DO NOTHING;
