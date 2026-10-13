# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: monthly received premiums
# MAGIC Current accepted payments are grouped by payment month, region, segment,
# MAGIC and product. Each execution refreshes the Gold reporting table from Silver.

# COMMAND ----------

from pathlib import PurePosixPath


MONTHLY_PREMIUMS_SQL = """
    SELECT
        TRUNC(payments.payment_date, 'MONTH') AS payment_month,
        customers.region,
        customers.customer_segment,
        policies.product_code,
        COUNT(*) AS payment_count,
        SUM(payments.amount) AS total_premium_eur
    FROM {silver}_payments AS payments
    INNER JOIN {silver}_policies AS policies
        ON payments.policy_id = policies.policy_id
    INNER JOIN {silver}_customers AS customers
        ON policies.customer_id = customers.customer_id
    GROUP BY
        TRUNC(payments.payment_date, 'MONTH'),
        customers.region,
        customers.customer_segment,
        policies.product_code
"""

RECONCILIATION_SQL = """
    SELECT
        (SELECT COUNT(*) FROM {silver}_payments) AS silver_payment_count,
        (SELECT COALESCE(SUM(payment_count), 0) FROM {gold}) AS gold_payment_count,
        (SELECT COALESCE(SUM(amount), 0) FROM {silver}_payments) AS silver_total_eur,
        (SELECT COALESCE(SUM(total_premium_eur), 0) FROM {gold}) AS gold_total_eur
"""


def build_gold(spark, volume_root, dataset):
    root = PurePosixPath(volume_root)
    if root.parts[:2] != ("/", "Volumes") or len(root.parts) != 5 or dataset not in ("fixture", "demo"):
        raise ValueError("Use /Volumes/<catalog>/<schema>/<volume> and fixture or demo.")
    namespace = ".".join("`" + part.replace("`", "``") + "`" for part in root.parts[2:4])
    silver = f"{namespace}.insurance_{dataset}_simple_silver"
    gold = f"{namespace}.insurance_{dataset}_simple_gold_monthly_premiums"
    query = MONTHLY_PREMIUMS_SQL.format(silver=silver)
    spark.sql(f"CREATE OR REPLACE TABLE {gold} USING DELTA AS {query}")
    return gold, silver


# COMMAND ----------
# MAGIC %md
# MAGIC ## Dataset parameters
# MAGIC The volume path determines the catalog and schema. `dataset` selects fixture or demo.

# COMMAND ----------

if "dbutils" in globals():
    dbutils.widgets.text("volume_root", "/Volumes/insurance_project/default/insurance_sources")
    dbutils.widgets.dropdown("dataset", "fixture", ["fixture", "demo"])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Monthly aggregation
# MAGIC Spark SQL performs the joins and aggregation. The month is the first day
# MAGIC of the payment's month, stored as a date. Amounts retain decimal precision.

# COMMAND ----------

if "dbutils" in globals():
    gold_table, gold_silver_prefix = build_gold(
        spark, dbutils.widgets.get("volume_root").strip(), dbutils.widgets.get("dataset")
    )
    print(f"Gold table: {gold_table}")
    display(spark.sql(f"""
        SELECT * FROM {gold_table}
        ORDER BY payment_month, region, customer_segment, product_code
        LIMIT 20
    """))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver reconciliation
# MAGIC Gold and Silver are compared by overall payment count and amount.
# MAGIC Only four summary values are returned to Python; payment rows stay in Spark.

# COMMAND ----------

if "dbutils" in globals():
    gold_reconciliation = spark.sql(RECONCILIATION_SQL.format(
        silver=gold_silver_prefix, gold=gold_table
    ))
    display(gold_reconciliation)
    gold_totals = gold_reconciliation.first()
    if (
        gold_totals["silver_payment_count"] != gold_totals["gold_payment_count"]
        or gold_totals["silver_total_eur"] != gold_totals["gold_total_eur"]
    ):
        raise AssertionError("Gold totals differ from Silver; check the accepted relationships.")
    print("Gold payment count and premium total match Silver.")
