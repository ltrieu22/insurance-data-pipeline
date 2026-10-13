# Insurance Premium Data Pipeline

A data engineering portfolio project using synthetic insurance customers,
policies, and premium payments. The goal is to turn source exports into reliable
reporting tables and show received premiums by month, product, region, and customer
segment in Power BI.

## Pipeline

Python source files → Databricks bronze → silver → gold → Power BI

Source generation, file inspection, bronze ingestion, silver validation and a
Spark SQL gold notebook are implemented. Local checks pass. Execution of the new
gold notebook in Databricks,Power BI next.

## Data and bronze ingestion

The default demo starts with 500 customers, 1,000 policies, and 10,000 payments.
Later batches contain corrections, duplicates, late payments, and invalid rows.
A separate fixture has 14 small payment batches for checking these scenarios.
All data is fictional.

Bronze stores raw row payloads and file status in Delta tables. Original files in the volume. Invalid values and duplicate source rows retain their
source path, batch, checksum and record number. Silver validates relationships,
retains rejections, and keeps the latest accepted payment per ID. Identical reruns
preserve stored results. New `simple` tables keep the earlier tables intact.

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
- `02_transform_silver.py`: validates references and computes current payments.
- `03_verify_pipeline.py`: checks final results and replays; see the
  [silver notes](docs/silver_processing.md) for scope and changed fixture expectations.
- `04_build_gold.py`: joins current Silver records into one monthly reporting
  table. [gold notes](docs/gold_reporting.md) for fields and checks.

The notebooks uses Databricks Free Edition and Unity Catalog volume. Setup and
verification details are in the [upload notes](docs/databricks_upload.md) and
[bronze notes](docs/bronze_ingestion.md).
