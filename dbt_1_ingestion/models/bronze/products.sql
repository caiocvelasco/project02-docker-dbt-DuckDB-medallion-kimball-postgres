-- models/bronze/products.sql
-- Transformations:
    -- Renaming columns
    -- Updating inserted_at

select
    "ProductID" as product_id,
    "ProductName" as product_name,
    "Category" as category,
    "Price" as price
    -- "extracted_at",                   -- Does not exist in the CSV file
    -- current_timestamp as inserted_at  -- Overwrite with current timestamp (Does not exist in the CSV file)
from {{ source('data', 'products') }}  -- References the CSV files in a folder external to this dbt project, as defined in "/workspace/dbt_1_ingestion/models/sources/internal.yaml"
