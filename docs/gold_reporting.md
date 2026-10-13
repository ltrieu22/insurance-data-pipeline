# Gold monthly reporting

Import `databricks/04_build_gold.py` as a Python notebook. Run it after Silver for
the selected dataset, using the same `volume_root` and `dataset` widgets. Existing
extracted sources and Silver tables are reused. Gold does not run Bronze or Silver.

The default volume path is `/Volumes/insurance_project/default/insurance_sources`
and the default dataset is `fixture`. Use `demo` for the larger reporting sample
once its Silver tables are ready.

## One reporting table

The notebook joins the current Silver payments to policies, then customers, and
groups them with Spark SQL. It creates:

`<catalog>.<schema>.insurance_<dataset>_simple_gold_monthly_premiums`

Grain: one row per payment month, region, customer segment, and product code.

| Column | Meaning |
| --- | --- |
| `payment_month` | First day of the payment's month, stored as a date |
| `region` | Customer region from the accepted reference baseline |
| `customer_segment` | Customer segment from the accepted reference baseline |
| `product_code` | Product from the payment's accepted policy |
| `payment_count` | Number of current accepted payments in the group |
| `total_premium_eur` | Sum of their current accepted amounts in EUR |

Payments are already validated and unique in Silver. Gold uses those records
directly; rejected rows and superseded source versions do not enter this table.
The accepted policy/customer keys must be unique and their relationships complete.
The notebook reconciles the sum of Gold payment counts and premium amounts with
Silver to check that the joins preserved the overall count and amount.

Monthly reporting uses `payment_date`, not ingestion or source update time.
`TRUNC(payment_date, 'MONTH')` produces a date suitable for chronological sorting
and Power BI. `SUM(amount)` retains Spark's decimal arithmetic; no float conversion
or rounding is added.

## Refresh behavior

`CREATE OR REPLACE TABLE ... USING DELTA AS SELECT ...` refreshes this Gold table
from the complete current Silver state on every run. This is a small full refresh,
not an incremental Gold merge. Newer valid corrections and late receipts are
therefore reflected in all affected groups, including removal of an old group
when its last payment moves to another month or classification.

Identical inputs produce the same reporting rows, counts, and amounts. Delta may
record another table version. Bronze, Silver, earlier tables, and source files are
not modified. Run after Silver finishes, with one pipeline execution at a time.

Gold reads the last accepted Silver state. Failed payment batches leave that state
unchanged; Gold does not independently read or reinterpret batch failure history.

## Fixture check

With the complete fixture processed in Silver, expect exactly these two rows:

| payment_month | region | customer_segment | product_code | payment_count | total_premium_eur |
| --- | --- | --- | --- | ---: | ---: |
| 2026-08-01 | Uusimaa | STANDARD | LIFE_BASIC | 1 | 50.00 |
| 2026-09-01 | Uusimaa | STANDARD | LIFE_BASIC | 2 | 180.00 |

The reconciliation must show 3 payments and EUR 230.00 on both sides, followed by
`Gold payment count and premium total match Silver.` Run the notebook again and
confirm the same two rows and totals. For the default demo, Gold payment counts
must sum to 10,200 and its amount must match the current demo Silver amount.

Local tests execute the reporting and reconciliation queries in SQLite, with
small adapters for Spark's month truncation and exact decimal sums. They cover the
fixture, all grouping dimensions, corrections, empty input, and unmatched links.
They do not execute Spark or Delta. Databricks execution of notebook 04 remains
pending until its actual output has been checked.

## Power BI scope

This table supports received premiums and payment counts by month, product,
region, and segment. In Power BI, sum `total_premium_eur` and `payment_count`;
counting Gold rows counts reporting groups rather than payments. Unique customer
or policy counts and payment-level drill-through are outside this monthly table.

Power BI setup is deferred. A manual CSV export or a Databricks connection can
consume the same Gold result later; neither is configured by this notebook.
