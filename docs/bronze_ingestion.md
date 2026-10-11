# Bronze ingestion

The source inspection notebook has already succeeded with the fixture at
`/Volumes/insurance_project/default/insurance_sources`. The existing ZIP and
extracted files can be reused without regenerating or uploading the sources.

Import `databricks/01_ingest_bronze.py` as a Python notebook. Run the helper and
widget cells, keep `dataset = fixture` and the existing volume path, then run
the last cell on serverless compute. The notebook processes the baseline and
all payment deliveries listed in the manifest. Run one ingestion at a time.

Three managed Delta tables are created in the volume's existing catalog/schema:

- `insurance_project.default.insurance_fixture_bronze_files`: exact file bytes,
  checksums, declared batch time, structural failures, and schema warnings.
- `insurance_project.default.insurance_fixture_bronze_rows`: parsed raw payloads
  with source path, checksum, entity, batch, and one-based record number.
- `insurance_project.default.insurance_fixture_bronze_runs`: each execution's
  status, timestamps, input row count, newly stored row count, and failures.

The record number counts CSV data records after the header or objects in the JSON
array. It is not a physical line number; a quoted CSV field can span lines.
`raw_payload` preserves strings, blanks, whitespace, and JSON nulls. The file
table preserves the original encoding, header, quoting, and file formatting.

Identical rows at different source positions remain separate bronze rows.
Identical reruns add no file or row records and preserve the first ingestion
time and run ID. Run history gains one entry. This uses an insert-only
[Delta MERGE](https://docs.databricks.com/aws/en/delta/merge).
Changing a stored delivery's contents, file path, checksum declaration, row-count
declaration, or batch time fails before the merges. A corrected delivery needs
a new batch ID. If a write is interrupted, rerun the same immutable inputs;
existing records are retained and missing records can be inserted.

File contract failures stay visible in the file table. Parseable rows from those
files are retained for audit, but a later silver notebook must skip the failed
batch. Unparseable files retain their original bytes without manufacturing rows.
Missing or unreadable files stop ingestion and produce a failed run entry.
Other independent deliveries can be captured together with a failed file.
Business validation and payment-version conflicts belong to silver.
`expected_outcomes.json` is never read by this notebook.

## Fixture checks

On a fresh fixture run, the summary should report:

```text
COMPLETED_WITH_FILE_FAILURES: 17 raw rows, 17 newly stored.
```

There should be 16 file records: one customer file, one policy file, and 14 payment
files. The 17 raw rows are one customer, one policy, and 15 payment records.
Only `fixture_011_missing_column` should have file status `FAILED`, with
`MISSING_REQUIRED_COLUMN`. The header-only `fixture_012_empty` is `READY` with
zero records. Payment-version conflicts are retained as raw data at this stage.

The notebook compares stored payloads with parsed inputs, then repeats both merges
and checks that file records, row records, and first-ingestion metadata stay
unchanged. Running the last cell again should report `0 newly stored`.
The table previews should retain `12.345`, `-10.00`, the two copies in
`fixture_002_duplicate`, and the padded strings in
`fixture_013_normalized_duplicate`.

Local tests cover parsing and provenance; Delta writes and replay checks need an
actual Databricks run. Bronze does not calculate the EUR 230.00 acceptance total.
That check follows silver validation and version handling.

JSON objects with repeated field names fail the file contract, retaining the
original bytes. Choosing one of those values would lose source information.

The larger demo uses the same notebook with `dataset = demo`, after its own
inspection run. Its tables use `insurance_demo_bronze_*` names, keeping all
processing state separate from the fixture.
