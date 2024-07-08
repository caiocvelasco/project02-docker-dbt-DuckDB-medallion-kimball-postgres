-- models/bronze/subscriptions.sql
-- Transformations:
    -- Renaming columns
    -- Updating inserted_at

select
    "SubscriptionID" as subscription_id,
    "CustomerID" as customer_id,
    "StartDate" as start_date,
    "EndDate" as end_date,
    "Type" as type,
    "Status" as status
    -- "extracted_at",                   -- Does not exist in the CSV file
    -- current_timestamp as inserted_at  -- Overwrite with current timestamp (Does not exist in the CSV file)
from {{ source('data', 'subscriptions') }}  -- References the CSV files in a folder external to this dbt project, as defined in "/workspace/dbt_1_ingestion/models/sources/internal.yaml"
