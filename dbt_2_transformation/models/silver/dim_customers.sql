-- models/silver/dim_customers.sql
-- Transformations:
    -- Ensuring a date format to signup_date

select
    customer_id,
    name,
    age,
    gender,
    cast(signup_date as date) as signup_date,  -- Convert to date format
    -- extracted_at,
    current_timestamp as inserted_at  -- -- Overwrite with current timestamp
from {{ source('bronze_parquet_output', 'customers') }}  -- References the bronze.customers table
