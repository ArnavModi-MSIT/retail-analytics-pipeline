-- Run once against your existing database (already ran ddl.sql before this column was added).
-- New fresh installs get this column directly from ddl.sql — this file is just for your current DB.

ALTER TABLE fact_sales ADD COLUMN source VARCHAR(10) NOT NULL DEFAULT 'csv';
ALTER TABLE fact_sales ALTER COLUMN source DROP DEFAULT;  -- default was only to backfill existing rows
