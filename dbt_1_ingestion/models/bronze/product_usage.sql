-- models/bronze/product_usage.sql
-- Transformations:
    -- Renaming columns
    -- Updating inserted_at

select
    "UsageID" as usage_id,
    "CustomerID" as customer_id,
    "DateID" as date_id,
    "ProductID" as product_id,
    "NumLogins" as num_logins,
    "Amount" as amount
    -- "extracted_at",                   -- Does not exist in the CSV file
    -- current_timestamp as inserted_at  -- Overwrite with current timestamp (Does not exist in the CSV file)
from {{ source('data', 'product_usage') }}  -- References the CSV files in a folder external to this dbt project, as defined in "/workspace/dbt_1_ingestion/models/sources/internal.yaml"
