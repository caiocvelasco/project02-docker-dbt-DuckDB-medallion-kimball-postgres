-- models/bronze/dates.sql
-- Transformations:
    -- Renaming columns
    -- Updating inserted_at

select
    "DateID" as date_id,
    "Date" as date,
    "Week" as week,
    "Month" as month,
    "Quarter" as quarter,
    "Year" as year
    -- "extracted_at",                   -- Does not exist in the CSV file
    -- current_timestamp as inserted_at  -- Overwrite with current timestamp (Does not exist in the CSV file)
from {{ source('data', 'dates') }}  -- References the CSV files in a folder external to this dbt project, as defined in "/workspace/dbt_1_ingestion/models/sources/internal.yaml"