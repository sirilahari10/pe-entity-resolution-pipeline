"""
pipeline_runner.py
Simulates a Databricks/Prefect orchestration job running a data pipeline.
Generates messy mock PE data, executes the SQL transformation, and outputs Canonical Records.
"""
import duckdb
import pandas as pd

def simulate_pipeline():
    print("🚀 Starting PE Entity Resolution Pipeline...")
    
    # 1. Initialize local DuckDB (simulating Databricks Lakehouse engine)
    con = duckdb.connect(':memory:')
    
    # 2. Mock Bronze Data (The Messy Reality of PE Systems)
    salesforce_data = pd.DataFrame({
        'account_id': ['SF-001', 'SF-002', 'SF-003'],
        'account_name': ['TechNova Solutions LLC', 'Acme Corporation, Inc.', 'DataFlow Analytics'],
        'ARR': [5000000, 12000000, 850000],
        'industry': ['SaaS', 'Manufacturing', 'Data Infrastructure']
    })
    
    pitchbook_data = pd.DataFrame({
        'company_id': ['PB-991', 'PB-992', 'PB-993'],
        'company_name': ['TechNova Solutions', 'Acme Corp', 'DataFlow Analytics Holdings'],
        'vertical': ['Enterprise Software', 'Industrials', 'B2B Tech']
    })
    
    con.register('bronze_salesforce_accounts', salesforce_data)
    con.register('bronze_pitchbook_market_data', pitchbook_data)
    
    print("✅ Bronze Layer: Ingested messy Salesforce and PitchBook data.")

    # 3. Execute the Silver Layer Resolution 
    # (Uses DuckDB specific syntax like LIST() and 'g' regex flags for local execution)
    silver_query = """
    WITH combined AS (
        SELECT account_name AS raw_name, ARR, industry, 'Salesforce' as source FROM bronze_salesforce_accounts
        UNION ALL
        SELECT company_name AS raw_name, NULL AS ARR, vertical AS industry, 'Pitchbook' as source FROM bronze_pitchbook_market_data
    ),
    normalized AS (
        SELECT 
            raw_name,
            ARR,
            industry,
            source,
            TRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(LOWER(raw_name), '[^a-z0-9 ]', '', 'g'), 
                    '\\b(llc|inc|corp|corporation|ltd|co|holdings)\\b', 
                    '', 
                    'g'
                )
            ) AS merge_key
        FROM combined
    )
    SELECT 
        MD5(merge_key) AS golden_company_id,
        MAX(raw_name) AS canonical_name,
        MAX(ARR) AS latest_arr,
        LIST(source) AS found_in_systems
    FROM normalized
    GROUP BY merge_key;
    """
    
    print("⏳ Silver Layer: Running Entity Resolution & String Normalization...")
    golden_records = con.execute(silver_query).df()
    
    # 4. Expose the Semantic Layer
    print("\n🏆 Gold Layer: Resolved Canonical Records Ready for AI Agents:")
    print(golden_records)

if __name__ == "__main__":
    simulate_pipeline()





