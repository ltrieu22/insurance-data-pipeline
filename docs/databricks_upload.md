# Upload and inspect the source exports

Start with the small fixture. Source inspection uses Databricks Free Edition
and its included storage. No Azure subscription or credentials are required.

## 1. Prepare the local bundle

From the project directory, run:

```powershell
python -m insurance_demo.fixtures
```

This creates `data/fixtures/fixture_sources.zip`, containing the reference data,
14 payment batches, manifest, and expected results.

## 2. Create a managed volume and upload the ZIP

1. Open the Databricks workspace. In the sidebar, choose **New**, then
   **Add or upload data**, then **Upload files to a volume**.
2. Choose an existing catalog and schema that you can write to. Their names depend
   on your workspace; use the names actually shown there.
3. Choose **Create volume** if necessary. Create a managed volume named
   `insurance_sources`, using the workspace's default storage.
4. Upload `fixture_sources.zip` to the root of that volume, not a nested directory.
5. Copy the volume path. It has this structure:
   `/Volumes/<catalog>/<schema>/insurance_sources`.

Use the file upload option to keep the raw files in the volume. Tables will be
created in later notebooks. See the Databricks upload documentation:
[Work with files in Unity Catalog volumes](https://docs.databricks.com/aws/en/volumes/volume-files).

## 3. Import the starter notebook

Import the local `databricks/00_inspect_sources.py` file into your Databricks
workspace as a Python notebook. In a workspace folder, use **Import** from its menu
and select the file. Attach the notebook to the available serverless compute.

Run the first Python cell to create the widgets, then set:

- `volume_root`: the exact volume path copied above.
- `dataset`: `fixture`.

Run the remaining cells. The notebook checks paths, checksums, and row counts,
then extracts the files into the volume's `fixture/` directory. Identical reruns
leave existing files unchanged. Changed files raise an error.

The previews should show one customer, one policy, and one EUR 100.00 payment.
Values remain strings for validation in the later pipeline.

## 4. Use the larger demo later

Run `python -m insurance_demo.generate` locally and upload
`data/generated/demo_sources.zip` to the same volume root. Set the notebook's
`dataset` widget to `demo`; extraction uses a separate `demo/` directory. The
manifest lists three payment batches.

Databricks Free Edition includes serverless compute and default storage, with
usage quotas. See [Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition).
Keep the fixture and demo in separate transformation schemas or explicitly rebuild
state before switching datasets, since their immutable reference baselines differ.

Next: [bronze ingestion](bronze_ingestion.md), silver validation and version
handling, then gold tables for Power BI.

## Updating an existing setup

For changes limited to comments, formatting, or printed output, update
`00_inspect_sources.py` in Databricks by copying the changed cells or reimporting
the notebook. Keep the existing volume and uploaded ZIP. If the notebook is
reimported, set its `volume_root` and `dataset` widgets again before running it.

Uploaded source files only need to change when the generated data changes.
Use a new local output directory and keep changed deliveries separate from
existing sources.
