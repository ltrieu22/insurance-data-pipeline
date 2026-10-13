# Silver validation and current payments

Notebook `02_transform_silver.py` validates Bronze records, retains rejections,
and stores the latest accepted payment per ID. Processing and acceptance
verification are separate notebooks. All records and domain rules are synthetic
project assumptions.

## Processing model

Silver reads the complete Bronze delivery sequence in manifest order. Customers
are validated first, followed by policies against accepted customers. Payment
batches are then validated against accepted policies. An in-memory dictionary
holds the latest accepted payment per ID, and the final state is merged into
Delta tables.

Each execution recomputes the sequence from its complete history. The manifest
history is ordered and append-only. This design avoids a batch ledger and
accepted-version history, but driver-side Python validation limits scalability.
The processing model supports one execution at a time.

## Fields and validation

| Entity | Business fields retained in Silver |
| --- | --- |
| Customers | `customer_id`, `region`, `customer_segment` |
| Policies | `policy_id`, `customer_id`, `product_code` |
| Payments | `payment_id`, `policy_id`, `payment_date`, `amount`, `currency`, `source_updated_at` |

Surrounding whitespace is trimmed. IDs preserve case and leading zeros. Required
values are nonblank strings. Regions, segments, and fictional products use the
case-sensitive source vocabulary. Only accepted parents satisfy relationships;
unknown customers and policies produce rejected rows.

Dates use `YYYY-MM-DD`; update timestamps use `YYYY-MM-DDTHH:MM:SSZ`. Neither may
exceed the declared batch time, and an update's UTC date cannot precede its
payment date. Validation uses delivery metadata rather than the current date.

Amounts use Python `Decimal` and Delta `DECIMAL(12,2)`. Valid amounts are positive,
denominated in EUR, within the decimal range, and have at most two fractional
digits. Excess precision, malformed amounts, and overflow are rejected before
conversion. Policy start and end dates remain in Bronze, outside this model.

## Duplicates and corrections

Identical normalized reference IDs collapse to one row. Different retained
business values for the same reference ID fail reference publication. A change
to the accepted reference baseline stops processing.

Payment payload comparisons use policy, payment date, decimal amount, and
currency. For valid rows:

- Identical `(payment_id, source_updated_at)` versions collapse.
- Different payloads for the same version within a batch, or against the current
  version reached in the sequence, fail the whole payment batch. Conflict rows
  carry `CONFLICTING_PAYMENT_VERSION`, and no payment changes from that batch
  are applied.
- The newest valid version per ID becomes the candidate. An unseen ID is
  inserted; an existing ID is updated only by a newer timestamp.
- Equal versions are no-ops. Older versions are ignored, including different
  payloads for versions older than the current one.
- Invalid corrections are rejected without replacing accepted rows. Other valid
  rows continue when the batch has no version conflict or file failure.

Late receipts are allowed. Missing rows in an incremental file do not imply
deletion. Valid corrections may change amount, policy, or payment date while
retaining the payment ID.

Identical reruns preserve payment state and audit metadata. Reference and
rejection merges are insert-only; payment merges update only newer timestamps.
Separate table writes are not one transaction. An interrupted execution can be
retried with the same sources; edited tables and changed source history have no
automated repair.

## Tables and batch summaries

The Delta table prefix is
`<catalog>.<schema>.insurance_<dataset>_simple_silver_`:

| Suffix | Contents |
| --- | --- |
| `customers` | Accepted customers and source provenance |
| `policies` | Accepted policy, customer, and product relationships with provenance |
| `payments` | One current accepted row per payment ID, with provenance |
| `rejections` | Original payload, provenance, and reason codes |

Batch summaries contain status, error codes, input row count, and rejected row
count. Each invalid or conflicting source row counts once, even with several
reasons. A valid row blocked by a batch conflict remains in Bronze and is not
counted as rejected. File failures have no row-level Silver rejection.

Rejections use `(batch_id, entity, source_row_number)` so replay does not duplicate
them. Batch summaries are displayed rather than stored as operational history.
Historical-version checks and detailed processing counters are outside this scope.

## Verification

Notebook `03_verify_pipeline.py` executes Bronze and Silver through relative
`%run` references, checks final results against the selected dataset's acceptance
cases, and compares all six tables before and after replay. Checks cover payment
totals, monthly amounts, batch outcomes, rejection reasons, key uniqueness,
relationships, and unchanged audit metadata.

Local tests also cover intermediate batch states and failed-batch isolation.
They exercise the Python planning functions; Spark and Delta execution are
verified in Databricks. `expected_outcomes.json` is verification input only.

The fixture oracle includes a historical-conflict scenario outside Silver's
current-version scope. Notebook 03 treats it as a stale no-op and includes
current-version conflict rows in rejected-row counts.

| Dataset | Customers | Policies | Current payments | Rejected rows | Total EUR |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fixture | 1 | 1 | 3 | 4 | 230.00 |
| Default demo | 500 | 1,000 | 10,200 | 14 | 1,377,893.03 |

The fixture groups to August EUR 50.00 and September EUR 180.00 by payment date.
Batches 009 and 011 fail; batch 009 blocks its accompanying new payment.
Batch 010 succeeds as a stale no-op. Identical replay preserves stored records
and timestamps.

The default demo passed final-state and identical-replay verification in
Databricks on 2026-10-13. Both datasets pass local tests.

[Gold reporting](gold_reporting.md) aggregates the accepted Silver records.
The reporting model covers positive received EUR payments. Refunds, invoices,
overdue balances, policy amendments, and historical reference dimensions are
outside its scope.
