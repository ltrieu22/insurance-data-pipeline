# Silver validation and payment versions

Import `databricks/02_transform_silver.py` as a Python notebook. It reads the
existing Bronze tables and uses the extracted manifest to order deliveries.
It does not regenerate or upload source data. All records are synthetic.

## Design

Customers are validated first, then policies against accepted customers, then
payments against accepted policies. Surrounding whitespace is trimmed while ID
case and leading zeros are preserved. Categories remain case sensitive. Dates
use `YYYY-MM-DD` and timestamps use `YYYY-MM-DDTHH:MM:SSZ` in this version.
Payment date and timestamp checks use the declared batch time.

Amounts are checked before conversion to `DECIMAL(12,2)`: malformed values,
excess decimal places, overflow, nonpositive amounts, and currencies other than
EUR are rejected. Invalid corrections cannot replace accepted payments.

For each dataset, Silver creates these managed Delta tables in the existing
catalog/schema, using the prefix `insurance_fixture_silver_` or
`insurance_demo_silver_`:

| Suffix | Contents |
| --- | --- |
| `customers` | Accepted, deduplicated customers with Bronze provenance |
| `policies` | Accepted, deduplicated policies with Bronze provenance |
| `baseline` | Reference delivery fingerprint and load status |
| `payment_versions` | One immutable record per accepted payment ID/version |
| `rejections` | Original payload, provenance, reason codes, and rejection kind |
| `runs` | One operational record per baseline or payment attempt |

The `payments` **view** selects the newest version per payment ID from
`payment_versions`. It is the current accepted Silver payment state.

The entire reference baseline is checked before either reference table is
written. Identical normalized duplicates collapse; conflicting IDs fail the
baseline. Its fingerprint is locked before writing references, and its status
becomes `READY` after both writes. Payments require a ready baseline. An
interrupted reference load can resume with the same delivery; changing an
accepted or partially loaded baseline requires an explicit rebuild.

Payment rows are normalized and validated before any version comparison.
Every valid incoming version is checked against both other incoming versions
and all stored versions. A conflicting payload fails the batch before payment
publication, including otherwise valid accompanying records.

A successful batch inserts its new, unambiguous versions with one insert-only
[Delta MERGE](https://docs.databricks.com/aws/en/delta/merge). The view therefore
reflects the new versions without a second current-table write. Older valid
versions are also retained, including a stale version first encountered after
its correction; they cannot replace newer data. Identical versions add nothing.
Late receipts are allowed, and absent incremental records never delete payments.

Run one execution at a time. Validation deliberately collects this small sample
on the driver; it is not a design for large datasets or concurrent writers.
Rejection and run-history writes are separate from payment publication. If an
operational error interrupts a run, rerun the same immutable deliveries. An
`ERROR` or missing run record alone cannot prove whether a payment commit occurred.
Expected file and version failures use status `FAILED` and commit no payments.

## Counts and audit

`input_row_count` counts raw source rows. `rejected_row_count` counts each
row-validation rejection once, regardless of how many reasons it has.
`duplicate_row_count` counts extra identical normalized rows within that input.
Batch conflicts have rejection kind `BATCH_CONFLICT` and separate run error
codes; they are excluded from the row-validation rejection count.

After removing invalid rows and repeated versions, `superseded_version_count`
counts versions older than another incoming version of the same payment.
Each newest candidate is classified once as inserted, updated, stale, or
unchanged compared with the accepted current payment. Those are candidate
counts, not raw-row counts. `new_version_count` counts previously unseen
versions retained by a successful batch, including older valid evidence.
On failed batches the payment change counts are zero. Replays may add run
records but preserve reference, version, and rejection records and their first
processing metadata.

## Verify in Databricks, one dataset at a time

1. Rerun the latest Bronze notebook with `dataset = fixture` to confirm its
   cleaned version and replay checks against the existing data. Expect 17 raw
   rows, 0 newly stored, and only `fixture_011_missing_column` failing the file
   contract. No source rebuild is needed.
2. Run the Silver notebook with the existing volume path,
   `dataset = fixture`, and `mode = verify` (the defaults). Use empty Silver
   payment state for the first run; the notebook never drops or resets tables.
3. Expect `Verified fixture: full sequence and replays.` and 3 current payments,
   EUR 230.00 total, August EUR 50.00, and September EUR 180.00.
   Batches 009, 010, and 011 must fail. Verification compares version records
   before and after failures and continues with the next independent batch.
4. After the fixture passes, select `dataset = demo`, `mode = verify` and run
   the final cell. Reuse its existing Bronze data. Expect 10,200 current
   payments and 14 rejected update rows, with no failed demo batches.

Verification asserts each batch's outcome, immediately replays the initial
delivery, and finally replays all successful deliveries and the reference
baseline. Accepted versions, reference data, rejections, and first-processing
metadata must remain unchanged. `expected_outcomes.json` is read only by this
verification function, to assert results produced by the pipeline.

On an already completed dataset, verification reports `completed state and
replays`; it cannot recreate the original intermediate states. If a run was
interrupted halfway, use `mode = process` to resume, then verify. Keep the first
successful full-sequence output as evidence. Process mode never reads the oracle
and lists failed deliveries while continuing independent payment batches.

Local tests exercise the same validation, planning, and delivery checks,
including the complete fixture and default demo. They do **not** execute PySpark
or Delta; the notebook run is required to verify those operations. Gold and
Power BI remain the next milestone.
