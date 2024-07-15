-- models/bronze/dates.sql
-- Transformations:
    -- No transformations were made as the Bronze layer functions as a Raw/Landing layer of the ingestion step.

-- The line below saves the dbt model externally as parquet. There are also other options. Check: https://github.com/duckdb/dbt-duckdb?tab=readme-ov-file#writing-to-external-files
-- {{ config(materialized='external', location='/workspace/external_ingestion/bronze_parquet_output/dates.parquet') }}

select
    "DateID" as date_id,
    "Date" as date,
    "Week" as week,
    "Month" as month,
    "Quarter" as quarter,
    "Year" as year
    -- "extracted_at",                   -- Does not exist in the CSV file
    -- current_timestamp as inserted_at  -- Overwrite with current timestamp (Does not exist in the CSV file)
from {{ source('bronze','dates') }}        -- Here, 'data' comes from the 'name:' tag in the sources.yml