# Silver validation and current payments

Import `databricks/02_transform_silver.py` as a Python notebook, after running
Bronze. Use the existing volume and `dataset = fixture`. There is no `mode`
widget: processing and acceptance verification are separate notebooks.
All records and domain rules are synthetic project assumptions.

## Processing flow

1. Read the complete Bronze delivery sequence in manifest order.
2. Validate customers, then policies against accepted customers.
3. Validate each payment batch against accepted policies. Keep an in-memory
   dictionary containing the latest accepted row for each payment ID.
4. Retain rejected rows with their original payload, source location, and reasons.
5. Merge the final current payments into one Delta table.

Every run recomputes the small sample from its complete source sequence. This is
an intentional tradeoff: it avoids a batch ledger, baseline recovery protocol,
and accepted-version history. Keep all deliveries in the manifest, in their
original order; append new deliveries rather than removing or reordering them.
It is not a scalable incremental processing design. Python validation collects
this sample on the driver. Run one execution at a time.

## Fields and validation

| Entity | Business fields retained in Silver |
| --- | --- |
| Customers | `customer_id`, `region`, `customer_segment` |
| Policies | `policy_id`, `customer_id`, `product_code` |
| Payments | `payment_id`, `policy_id`, `payment_date`, `amount`, `currency`, `source_updated_at` |

Trim surrounding whitespace. Preserve ID case and leading zeros. Required values
must be nonblank strings. Regions, segments, and fictional products use the
existing case-sensitive source vocabulary. Only accepted parents satisfy links;
an unknown customer or policy is rejected rather than lost through a join.
Policy start/end dates remain available in Bronze but are not validated or used
in this reporting model.

Dates use `YYYY-MM-DD`; update timestamps use `YYYY-MM-DDTHH:MM:SSZ`. Neither may
exceed the declared batch time, and an update's UTC date cannot precede its payment
date. These checks use delivery metadata, not today's date.

Money uses Python `Decimal` and Delta `DECIMAL(12,2)`. Require positive EUR amounts
within the decimal range and at most two fractional digits. Reject excess
precision, malformed amounts, and overflow before conversion; never silently
round or use floating-point totals.

## Duplicates and corrections

Identical normalized reference IDs collapse to one row. Different retained
business values for the same reference ID fail reference publication. Existing
accepted reference records are compared with the incoming baseline before
merging; changing the accepted baseline requires review and an approved rebuild.
There is no baseline marker or recovery state machine.

For valid payments, compare the business payload: policy, payment date, decimal
amount, and currency. In each batch:

- Identical `(payment_id, source_updated_at)` versions collapse.
- Different payloads for the same version within the batch, or against the
  current version reached so far in the sequence, fail the whole payment batch.
  Keep conflict rows with `CONFLICTING_PAYMENT_VERSION`; apply no accompanying
  payment changes from that batch.
- Otherwise, take the newest valid version per payment ID. Insert an unseen ID
  or replace its current row only with a newer timestamp.
- Equal versions are no-ops. Older versions are ignored, including different
  payloads for versions older than the current one. No historical conflict check
  is claimed.
- Reject invalid corrections without replacing accepted rows. Other valid rows
  can continue when there is no batch conflict or file failure.

Late receipts are allowed. Missing rows in an incremental file never mean
cancellation or deletion. Corrections may change amount, policy, or payment date
if the new row passes validation; the payment ID stays the same.

Recomputing from the complete sequence preserves the original batch decisions
on rerun even after later corrections. One final payment merge inserts new IDs
and updates only newer timestamps; equal timestamps preserve existing metadata.
Reference and rejection merges are insert-only. These separate table writes are
not one transaction. An operational failure can be retried with the same sources;
there is no automated repair of edited tables or changed source history.

## Tables and counts

The new prefix is `insurance_fixture_simple_silver_` (or
`insurance_demo_simple_silver_`) in the volume's catalog/schema:

| Suffix | Contents |
| --- | --- |
| `customers` | Accepted customers and source provenance |
| `policies` | Accepted policy/customer/product relationships and provenance |
| `payments` | One current accepted row per payment ID, with provenance |
| `rejections` | Original payload, provenance, and sorted reason codes |

There is no versions table, current-payment view, baseline marker, or run table.
Earlier `insurance_<dataset>_silver_*` tables and views are left intact.

The notebook displays status/error codes, input row count, and rejected row count
per payment batch. Rejected counts include each invalid or conflicting source row once,
even with several reasons. An accompanying valid row blocked by a batch conflict
stays in Bronze but is not counted as rejected. File failures have no row-level
Silver rejection. Rejections use `(batch_id, entity, source_row_number)` so replay
does not duplicate them. Duplicate, stale, superseded, inserted, and updated
counters are deferred; the summaries are not persistent operational history.

## Verification

Import `01_ingest_bronze.py`, `02_transform_silver.py`, and
`03_verify_pipeline.py` into the **same Databricks workspace folder**. Run notebook
03 with its fixture defaults. Its two `%run` cells execute Bronze and Silver;
its remaining cells check final results and replay both processing functions.
Databricks documents this inline execution in
[notebook workflows](https://docs.databricks.com/aws/en/notebooks/notebook-workflows).
The existing volume is the default. The notebook creates only the new `simple`
tables; it does not reset or migrate the earlier tables.

Expected fixture outcome:

- 3 current payments, EUR 230.00 total.
- August EUR 50.00; September EUR 180.00, grouped by **payment date**.
- Only batches 009 and 011 fail. Batch 009 blocks its accompanying new payment.
- Batch 010 succeeds as a stale no-op; its historical conflict check was removed.
- Four stored rejection rows: three invalid rows and one current-version conflict.
- Replay preserves all six tables, including first ingestion/processing metadata.

Success prints `Verified fixture: final state and identical replays.` This does
not claim to verify each intermediate state in Databricks. Local tests separately
check intermediate totals, failed-batch isolation, validation, and replays using
the same Python planning functions; they do not execute PySpark or Delta.

The uploaded `expected_outcomes.json` still describes the original source fixture.
Only notebook 03 adapts its expectations for batch 010 and the conflict-row count.
It reads the oracle to assert produced results, never to build pipeline output.
The generator, checked-in source files, and uploaded bundles remain unchanged.

The default demo passes locally with 500 customers, 1,000 policies, 10,200 current
payments, and 14 rejected update rows. Actual execution of the **simplified**
notebooks in Databricks is pending for both datasets. Earlier successful
Databricks outputs verify the previous implementation only.

After the fixture is verified, the demo can use the same notebook: change
`$dataset="fixture"` to `$dataset="demo"` in both `%run` cells before running it.
No regeneration or upload is needed for the existing extracted demo sources.

Gold and Power BI wait until this milestone is verified in Databricks. Gold will
sum each current payment once and group months by payment date. Refunds, reversals,
invoices, overdue balances, policy amendments, historical reference dimensions,
Azure deployment, and notebook conversion remain outside this milestone.
