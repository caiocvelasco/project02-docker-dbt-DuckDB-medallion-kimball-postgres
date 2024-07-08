-- models/bronze/customers.sql
-- Transformations:
    -- Renaming columns
    -- Updating inserted_at

select
    "CustomerID" as customer_id,
    "Name" as name,
    "Age" as age,
    "Gender" as gender,
    "SignupDate" as signup_date
    -- "extracted_at",                   -- Does not exist in the CSV file
    -- current_timestamp as inserted_at  -- Overwrite with current timestamp (Does not exist in the CSV file)
from {{ source('data','customers') }}    -- References the CSV files in a folder external to this dbt project, as defined in "/workspace/dbt_1_ingestion/models/sources/internal.yaml"