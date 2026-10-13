# Source files and Databricks storage

The pipeline uses synthetic source exports stored in a Unity Catalog volume in
Databricks Free Edition. Each dataset has a ZIP archive containing reference
files, payment batches, a manifest, and verification expectations.

## Source bundles

| Dataset | Local archive | Contents |
| --- | --- | --- |
| `fixture` | `data/fixtures/fixture_sources.zip` | One customer, one policy, and 14 small payment batches |
| `demo` | `data/generated/demo_sources.zip` | 500 customers, 1,000 policies, and three payment batches |

Source generation uses `insurance_demo.fixtures` and `insurance_demo.generate`.
Generated demo files and ZIP archives are excluded from Git. The manifest records
source paths, SHA-256 checksums, row counts, batch IDs, and declared batch times.
`expected_outcomes.json` contains acceptance cases and is separate from pipeline
input.

## Volume layout

The project volume is `/Volumes/insurance_project/default/insurance_sources`:

```text
insurance_sources/
    fixture_sources.zip
    demo_sources.zip
    fixture/
    demo/
```

Each extracted dataset directory contains `manifest.json`,
`expected_outcomes.json`, `reference/`, and `batches/`.

| Parameter | Meaning |
| --- | --- |
| `volume_root` | Volume directory: `/Volumes/<catalog>/<schema>/<volume>` |
| `dataset` | Dataset name: `fixture` or `demo` |

Notebook `00_inspect_sources.py` reads `{volume_root}/{dataset}_sources.zip` and
extracts it into `{volume_root}/{dataset}/`. The volume parameter identifies the
directory; the archive filename is derived from the dataset name.

## Inspection and extraction

Notebook 00 validates archive paths, manifest identity, source checksums, and row
counts before extraction. Duplicate archive paths and paths outside the target
directory are rejected. Existing files with identical contents remain unchanged;
different contents at an existing path cause an error.

The notebook previews customers, policies, and the first payment batch using
Spark. Preview columns are read as strings. The fixture preview contains one
customer, one policy, and one EUR 100.00 payment.

Bronze reads the extracted dataset directory. Its Delta tables use the catalog
and schema from `volume_root`, with separate names for fixture and demo data.
[Bronze ingestion](bronze_ingestion.md) describes the file and row tables.
