# Insurance Premium Data Pipeline

A data engineering portfolio project using synthetic insurance customers,
policies, and premium payments. The goal is to turn source exports into reliable
reporting tables and show received premiums by month, product, region, and customer
segment in Power BI.

## Pipeline

Python source files → Databricks bronze → silver → gold → Power BI

Source generation, Bronze ingestion, Silver validation, and Spark SQL Gold
reporting are implemented. The demo passes pipeline verification and Gold
reconciliation in Databricks. The Power BI report is planned.

## Data and processing

The default demo starts with 500 customers, 1,000 policies, and 10,000 payments.
Later batches contain corrections, duplicates, late payments, and invalid rows.
A separate fixture has 14 small payment batches for checking these scenarios.
All data is fictional.

Bronze stores raw row payloads, file status, and provenance in Delta tables.
Original files remain in the volume. Silver validates values and relationships,
retains rejected rows with reasons, and keeps the latest accepted payment per ID.
Gold joins the accepted records into monthly premium totals and payment counts
by product, region, and customer segment.

The verified demo contains 10,200 current payments totaling EUR 1,377,893.03.
Bronze and Silver replays preserve stored records and audit timestamps. Gold
refreshes its reporting table from the current Silver state.

## Local development

The generator and local tests use Python 3.10+ and the standard library.

```powershell
python -m insurance_demo.fixtures
python -m insurance_demo.generate
python -m unittest discover -s tests
```

The fixture files are in `data/fixtures/`; the larger demo is generated in
`data/generated/`. Generated demo files and ZIP archives are excluded from Git.

## Databricks notebooks

- `00_inspect_sources.py`: source archive inspection and extraction.
- `01_ingest_bronze.py`: raw delivery ingestion and replay handling.
- `02_transform_silver.py`: reference validation and current payment state.
- `03_verify_pipeline.py`: final-state and identical-replay verification.
- `04_build_gold.py`: monthly reporting with Spark SQL.

The notebooks use Databricks Free Edition, a Unity Catalog volume, and Delta
tables. Documentation covers [source storage](docs/databricks_upload.md),
[Bronze ingestion](docs/bronze_ingestion.md), [Silver processing](docs/silver_processing.md),
and [Gold reporting](docs/gold_reporting.md).
