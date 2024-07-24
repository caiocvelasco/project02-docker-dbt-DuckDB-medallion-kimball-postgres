-- models/bronze/customers.sql
-- Transformations:
    -- No transformations were made as the Bronze layer functions as a Raw/Landing layer of the ingestion step.

select
    "CustomerID" as customer_id,
    "Name" as name,
    "Age" as age,
    "Gender" as gender,
    "SignupDate" as signup_date
    -- "extracted_at",                   -- Does not exist in the CSV file
    -- current_timestamp as inserted_at  -- Overwrite with current timestamp (Does not exist in the CSV file)
from {{ source('bronze','customers') }}   -- Here, 'data' comes from the 'name:' tag in the sources.yml