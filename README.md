# Private Equity Entity Resolution (dbt + Python)

In private equity, the foundation of every firm-wide dashboard and AI agent is a single, governed source of truth. When pipeline and market data spans across CRMs (Salesforce) and market intelligence platforms (PitchBook, SourceScrub), entity resolution becomes the most critical "mechanical work" to get right.

This Proof of Work demonstrates a modern Medallion architecture (Bronze ➔ Silver ➔ Gold) simulating a Databricks/dbt ecosystem to solve the entity resolution problem.

## The Architecture
1. **Bronze (Raw Ingestion):** Ingests messy, unformatted company records from mock Salesforce and PitchBook sources.
2. **Silver (Entity Resolution):** A dbt-style SQL transformation that applies string normalization, strips corporate suffixes (LLC, Inc, Corp), and uses MD5 hashing to generate a deterministic surrogate key.
3. **Gold (Semantic Layer):** Exposes a clean, deduplicated canonical `dim_company` table ready to be queried by an AI agent or downstream ML models.

## Try it locally
This pipeline is fully self-contained using `duckdb` to simulate the Databricks SQL engine. 
```bash
pip install duckdb pandas tabulate
python pipeline_runner.py
