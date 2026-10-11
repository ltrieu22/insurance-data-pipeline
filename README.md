# Insurance Premium Data Pipeline

A data engineering portfolio project using synthetic insurance customers,
policies, and premium payments. The goal is to turn source exports into reliable
reporting tables and show received premiums by month, product, region, and customer
segment in Power BI.

## Pipeline

Python source files → Databricks bronze → silver → gold → Power BI

Source generation, file inspection, and bronze ingestion are implemented.
Silver validation, payment-version handling, gold tables, and the Power BI
dashboard are next.

## Data and bronze ingestion

The default demo starts with 500 customers, 1,000 policies, and 10,000 payments.
Later batches contain corrections, duplicates, late payments, and invalid rows.
A separate fixture has 14 small payment batches for checking these scenarios.
All data is fictional.

Bronze stores original file bytes, raw row payloads, and run history in Delta
tables. Invalid values and duplicate source rows remain available, together with
their source path, batch, checksum, and record number. Identical reruns add no
raw file or row records. Fixture and demo tables stay separate.

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

- `00_inspect_sources.py`: checks and extracts the uploaded source archive.
- `01_ingest_bronze.py`: captures raw deliveries and checks safe reruns.

The notebooks uses Databricks Free Edition and Unity Catalog volume. Setup and
verification details are in the [upload notes](docs/databricks_upload.md) and
[bronze notes](docs/bronze_ingestion.md).
