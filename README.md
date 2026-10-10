# Insurance Premium Data Project

A small project to practise Python, SQL, and Databricks using fake insurance data.
The plan is to turn customer, policy, and payment files into reporting tables
and build a Power BI dashboard.

So far, there's a data generator, tests, and a notebook for checking the files
in Databricks. Bronze, silver, and gold tables are next.

## Run it locally

Use Python 3.10 or newer. No extra packages needed.

```powershell
python -m insurance_demo.fixtures
python -m insurance_demo.generate
python -m unittest discover -s tests
```

The first command creates small test batches in `data/fixtures/`.
The second creates a larger demo in `data/generated/` with 500 customers,
1,000 policies, and 10,000 starting payments. Generated demo files and ZIPs stay
out of Git.

## Databricks

Follow [the upload guide](docs/databricks_upload.md) to upload the data and run
`databricks/00_inspect_sources.py`. Start with the small test batches.
