-- models/bronze/support_interactions.sql
-- Transformations:
    -- Renaming columns
    -- Updating inserted_at

select
    "InteractionID" as interaction_id,
    "CustomerID" as customer_id,
    "DateID" as date_id,
    "IssueType" as issue_type,
    "ResolutionTime" as resolution_time
    -- "extracted_at",                   -- Does not exist in the CSV file
    -- current_timestamp as inserted_at  -- Overwrite with current timestamp (Does not exist in the CSV file)
from {{ source('data', 'support_interactions') }}  -- References the CSV files in a folder external to this dbt project, as defined in "/workspace/dbt_1_ingestion/models/sources/internal.yaml"
