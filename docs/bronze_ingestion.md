# Bronze ingestion

Notebook `01_ingest_bronze.py` captures raw customer, policy, and payment
deliveries from the extracted source directory. The `volume_root` and `dataset`
parameters identify the [source storage](databricks_upload.md) and table namespace.

## Source handling

Bronze reads the customer CSV, policy JSON array, and payment CSV files listed in
`manifest.json`. Parsing runs in Python on the driver for this small synthetic
dataset. File checks cover structure, required reporting columns, checksums, and
declared row counts. Business validation belongs to Silver.

Raw payloads preserve source values, whitespace, blanks, JSON nulls, additional
fields, and duplicate source rows. Policy start and end dates remain raw fields
outside the reporting model. Repeated JSON field names and malformed CSV records
fail structural checks. `expected_outcomes.json` is used only for verification.

## Tables

The Delta table prefix is
`<catalog>.<schema>.insurance_<dataset>_simple_bronze_`:

| Suffix | Contents |
| --- | --- |
| `files` | Source path and checksum, batch ID and time, parsed row count, file status, and errors |
| `rows` | Raw JSON payload, source path and checksum, batch ID and time, record number, and first ingestion time |

Record numbers are one-based CSV data records after the header or objects in the
JSON array. They identify source positions rather than physical lines. Original
file bytes remain in the volume.

## Reruns and failures

Insert-only Delta merges use `(batch_id, entity)` for files and
`(batch_id, entity, source_row_number)` for rows. Identical reruns add no records
and preserve ingestion timestamps.

A batch ID identifies an immutable delivery. Changes to its source bytes, path,
declared batch time, parsed count, or file status and errors block ingestion.
Corrections have a new batch ID. An interrupted write can be retried with the same
inputs. The processing model supports one ingestion execution at a time.

Files have `READY` or `FAILED` status. Parseable rows from a failed file remain in
Bronze. An unparseable file has a failed file record and no parsed rows. Silver
skips failed payment deliveries. Missing or unreadable source files stop
preparation before ingestion writes.

Operational exceptions appear in notebook output. Persistent run history and
schema-warning history are outside the current scope.

## Fixture results

The fixture contains 16 files: two reference files and 14 payment files. Its
17 raw rows are one customer, one policy, and 15 payments. The first load reports:

```text
Bronze: 17 raw rows, 17 newly stored; failed files: 1.
```

Only `fixture_011_missing_column` fails, with `MISSING_REQUIRED_COLUMN`.
`fixture_012_empty` is ready with zero rows. An identical replay reports
`0 newly stored`.

[Notebook 03](silver_processing.md#verification) compares stored payloads and
file identities with the sources and checks complete records before and after
replay, including timestamps. Local tests cover parsing and delivery identity;
Databricks verification covers Spark and Delta execution.
