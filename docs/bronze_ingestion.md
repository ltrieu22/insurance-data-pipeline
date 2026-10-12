# Bronze ingestion

Import `databricks/01_ingest_bronze.py` as a Python notebook. Keep
`volume_root = /Volumes/insurance_project/default/insurance_sources` and
`dataset = fixture`, then run it. Reuse the extracted sources; no regeneration,
upload, or table reset is needed. Run one execution at a time.

## Small retained scope

Bronze reads the customer CSV, policy JSON array, and payment CSV deliveries in
`manifest.json`. It checks file structure, required reporting columns, checksums,
and declared row counts. It does not clean business values or deduplicate rows.
Additional columns remain in the raw payload and are ignored by Silver.

Two new Delta tables use the prefix `insurance_fixture_simple_bronze_` in the
volume's existing catalog/schema:

| Suffix | Contents |
| --- | --- |
| `files` | Source path/hash, batch ID/time, parsed row count, file status, errors |
| `rows` | Original field values as JSON, source path/hash, batch ID/time, record number, first ingestion time |

The record number is one-based: CSV data records after the header or objects in
the JSON array. It is not a physical line number. Strings, blanks, whitespace,
JSON nulls, unused fields, and duplicate source positions survive ingestion.
Exact file bytes stay in the volume; Bronze no longer stores a second byte copy.

## Reruns and failures

An insert-only Delta merge uses `(batch_id, entity)` for files and adds
`source_row_number` for rows. Identical reruns add no records and preserve first
ingestion times. Changed bytes, source path, declared batch time, parsed count,
or file status/errors for a stored delivery are blocked. Corrections require a
new batch ID. Run the same inputs again if a write is interrupted.

Parseable rows from a failed file remain in Bronze. Unparseable files produce a
failed file record with no invented rows; their bytes remain at the source path.
Silver skips payment deliveries with failed files. Missing or unreadable files
stop preparation before any ingestion writes. Operational exceptions appear in
the notebook output; there is no persistent run-history table.

Only the fields used by Silver are required. Policy start/end dates are preserved
as raw fields but are no longer required or validated for this reporting scope.
Repeated JSON field names and malformed CSV records still fail structural checks.
`expected_outcomes.json` is never read by processing.

## Fixture acceptance

A first run into the **new** tables should report:

```text
Bronze: 17 raw rows, 17 newly stored; failed files: 1.
```

Expect 16 files: two reference files and 14 payment files. The 17 raw rows are one
customer, one policy, and 15 payments. Only `fixture_011_missing_column` fails,
with `MISSING_REQUIRED_COLUMN`. `fixture_012_empty` is ready with zero rows.
A second identical run reports `0 newly stored`.

[The separate verification notebook](silver_processing.md#verification) compares
stored payloads and file identities with the sources and checks complete stored
records before/after replay, including timestamps. Local tests check the parser
and delivery identity; PySpark and Delta still require Databricks execution.

The demo uses `dataset = demo` and `insurance_demo_simple_bronze_*` tables.
All earlier `insurance_<dataset>_bronze_*` tables remain untouched. Byte-copy
storage, schema-warning history, fingerprints of manifest declarations, and
operational run history are deliberately outside the reduced implementation.
