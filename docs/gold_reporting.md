# Gold monthly reporting

Notebook `04_build_gold.py` joins current accepted Silver payments, policies,
and customers into one monthly reporting table using Spark SQL. The catalog and
schema come from `volume_root`; `dataset` selects the fixture or demo table family.
Gold reads a completed Silver state and runs sequentially with the pipeline.

## Reporting table

The Delta table is
`<catalog>.<schema>.insurance_<dataset>_simple_gold_monthly_premiums`.

Grain: one row per payment month, region, customer segment, and product code.

| Column | Meaning |
| --- | --- |
| `payment_month` | First day of the payment's month, stored as a date |
| `region` | Customer region from the accepted reference baseline |
| `customer_segment` | Customer segment from the accepted reference baseline |
| `product_code` | Product from the payment's accepted policy |
| `payment_count` | Number of current accepted payments in the group |
| `total_premium_eur` | Sum of their current accepted amounts in EUR |

Silver provides unique accepted payments and complete policy/customer
relationships. Rejected rows and superseded source versions do not enter Gold.
Monthly reporting uses `payment_date`; `TRUNC(payment_date, 'MONTH')` produces
the month date, and `SUM(amount)` retains Spark's decimal arithmetic.

## Refresh and reconciliation

`CREATE OR REPLACE TABLE ... USING DELTA AS SELECT ...` refreshes the reporting
table from the complete current Silver state. Accepted corrections and late
receipts are reflected in all affected groups. An old group disappears when its
last payment moves to another month or classification.

Identical inputs produce the same reporting rows, counts, and amounts. Delta may
record another table version. The refresh changes only the Gold reporting table.
Failed payment batches leave the accepted Silver state unchanged.

The notebook compares Silver's payment count and amount with the sums of Gold's
`payment_count` and `total_premium_eur`. A difference fails reconciliation.

## Verification results

The fixture produces two reporting rows:

| payment_month | region | customer_segment | product_code | payment_count | total_premium_eur |
| --- | --- | --- | --- | ---: | ---: |
| 2026-08-01 | Uusimaa | STANDARD | LIFE_BASIC | 1 | 50.00 |
| 2026-09-01 | Uusimaa | STANDARD | LIFE_BASIC | 2 | 180.00 |

The default demo passed Gold reconciliation in Databricks on 2026-10-13:

| Metric | Silver | Gold |
| --- | ---: | ---: |
| Payments | 10,200 | 10,200 |
| Total EUR | 1,377,893.03 | 1,377,893.03 |

The default demo has 144 reporting groups across nine months. The notebook's
displayed preview is limited to 20 rows.

Local tests execute the reporting and reconciliation queries in SQLite with
adapters for month truncation and exact decimal sums. They cover the fixture,
grouping dimensions, corrections, empty input, and unmatched relationships.
Spark and Delta behavior are covered by Databricks execution.

## Power BI model

The table supports received premiums and payment counts by month, product,
region, and segment. Premium totals aggregate with `SUM(total_premium_eur)`;
payment counts aggregate with `SUM(payment_count)`. A count of Gold rows measures
reporting groups. Unique customer or policy counts and payment-level drill-through
are outside this monthly model.

Power BI Desktop imports the demo Gold table through a Databricks SQL warehouse
as `Monthly premiums`. The saved project is
[`powerbi/insurance_premiums.pbip`](../powerbi/insurance_premiums.pbip).
Its two report pages provide an overview and deeper analysis; current
[screenshots and captions](../README.md#power-bi-report) document their appearance.

The numeric measures are:

```dax
Total premium EUR = SUM('Monthly premiums'[total_premium_eur])
Total payments = SUM('Monthly premiums'[payment_count])
Average payment EUR = DIVIDE([Total premium EUR], [Total payments])
```

Average payment is weighted by payment count in the current filter context,
including matrix totals. Each scatter point represents a monthly Gold reporting
group across all four grain dimensions, not an individual payment.

The separate Import check confirmed 144 reporting groups, nine months, 10,200
payments, and EUR 1,377,893.03. The weighted average payment is EUR 135.09.
September is partial: 160 accepted payments dated 2 September 2026 total
EUR 21,854.23; the final declared delivery cutoff is 4 September 2026, 00:00 UTC.
This coverage does not support interpreting September as a full-month decline.

Cross-page slicer synchronization and final interaction tests remain unconfirmed.
Power BI Service publication and scheduled refresh are outside the implemented
scope. The connection uses Import mode.
