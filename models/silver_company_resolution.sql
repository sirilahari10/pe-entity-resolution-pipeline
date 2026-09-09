```sql
-- models/silver_company_resolution.sql
-- Normalizes messy company names to create a unified golden record

WITH salesforce_raw AS (
    SELECT 
        account_id AS source_id,
        'Salesforce' AS source_system,
        account_name AS raw_name,
        ARR,
        industry
    FROM {{ ref('bronze_salesforce_accounts') }}
),

pitchbook_raw AS (
    SELECT 
        company_id AS source_id,
        'PitchBook' AS source_system,
        company_name AS raw_name,
        NULL AS ARR,
        vertical AS industry
    FROM {{ ref('bronze_pitchbook_market_data') }}
),

combined_sources AS (
    SELECT * FROM salesforce_raw
    UNION ALL
    SELECT * FROM pitchbook_raw
),

-- Standardize strings: lowercase, remove all punctuation, strip corporate suffixes
normalized_names AS (
    SELECT 
        *,
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(LOWER(raw_name), '[^a-z0-9 ]', '', 'g'), 
                '\b(llc|inc|corp|corporation|ltd|co|holdings)\b', 
                '', 
                'g'
            )
        ) AS normalized_merge_key
    FROM combined_sources
)

-- Generate the Golden Record using MD5 surrogate keys
SELECT 
    MD5(normalized_merge_key) AS golden_company_id,
    MAX(raw_name) AS canonical_name,
    MAX(ARR) AS latest_arr,
    ARRAY_AGG(source_system) AS systems_present_in
FROM normalized_names
GROUP BY normalized_merge_key
