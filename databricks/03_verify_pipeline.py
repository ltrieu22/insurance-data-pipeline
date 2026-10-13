# Databricks notebook source
# MAGIC %md
# MAGIC # Acceptance: final state and identical replays
# MAGIC Verifies Bronze and Silver results and identical replays.
# MAGIC Local tests cover intermediate batch states.

# COMMAND ----------

# MAGIC %run ./01_ingest_bronze $dataset="fixture"

# COMMAND ----------

# MAGIC %run ./02_transform_silver $dataset="fixture"

# COMMAND ----------

import json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path


def reduced_expectations(oracle, dataset):
    expected = oracle["after_batches"]
    if dataset == "fixture":
        # The source oracle includes historical conflict checks outside the current scope.
        expected["fixture_010_conflict_history"].update(
            status="SUCCESS", expected_reason_codes=[]
        )
        # Rejected-row counts include current-version conflict rows.
        expected["fixture_009_conflict_current"]["rejected_row_count_in_this_batch"] = 1
    return expected


def payment_totals(records):
    by_month = defaultdict(lambda: Decimal("0.00"))
    for row in records:
        by_month[row["payment_date"].strftime("%Y-%m")] += row["amount"]
    return {
        "current_payment_count": len(records),
        "total_amount_eur": format(sum(by_month.values(), Decimal("0.00")), ".2f"),
        "amount_by_month_eur": {month: format(amount, ".2f") for month, amount in sorted(by_month.items())},
    }


def assert_batch_summaries(results, expected):
    if {result["batch_id"] for result in results} != set(expected):
        raise AssertionError("Processed batches differ from the acceptance cases.")
    for result in results:
        target = expected[result["batch_id"]]
        if result["status"] != target["status"]:
            raise AssertionError(f"Unexpected status: {result}")
        if result["rejected_row_count"] != target["rejected_row_count_in_this_batch"]:
            raise AssertionError(f"Unexpected rejected row count: {result}")


def pipeline_snapshot(spark, bronze, silver):
    tables = {
        f"{bronze}_files": ("batch_id", "entity"),
        f"{bronze}_rows": ("batch_id", "entity", "source_row_number"),
        f"{silver}_customers": ("customer_id",),
        f"{silver}_policies": ("policy_id",),
        f"{silver}_payments": ("payment_id",),
        f"{silver}_rejections": ("batch_id", "entity", "source_row_number"),
    }
    return {table: spark.table(table).orderBy(*keys).collect() for table, keys in tables.items()}


def assert_bronze_state(spark, prefix, volume_root, dataset):
    files, rows = prepare_sources(Path(volume_root) / dataset, dataset)
    for suffix, incoming, fields in (
        ("files", files, tuple(FILE_KEYS) + FILE_IDENTITY),
        ("rows", rows, ("batch_id", "entity", "source_row_number", "raw_payload", "source_sha256")),
    ):
        stored = collect_records(spark, f"{prefix}_{suffix}")
        expected = sorted(tuple(row[field] for field in fields) for row in incoming)
        actual = sorted(tuple(row[field] for field in fields) for row in stored)
        if actual != expected:
            raise AssertionError(f"Stored Bronze {suffix} differ from the delivered sources.")


def assert_final_state(spark, prefix, dataset, expected, results, oracle):
    records = collect_records(spark, f"{prefix}_payments")
    final = list(expected.values())[-1]
    for key, actual in payment_totals(records).items():
        if actual != final[key]:
            raise AssertionError(f"{key}: actual {actual!r}, expected {final[key]!r}")
    for entity in ("customers", "policies", "payments"):
        identifiers = [row[FIELDS[entity][0]] for row in collect_records(spark, f"{prefix}_{entity}")]
        if len(identifiers) != len(set(identifiers)):
            raise AssertionError(f"Duplicate accepted {entity} IDs.")
    customers = {row["customer_id"] for row in collect_records(spark, f"{prefix}_customers")}
    policies = collect_records(spark, f"{prefix}_policies")
    if any(row["customer_id"] not in customers for row in policies):
        raise AssertionError("Accepted policy has no accepted customer.")
    policy_ids = {policy["policy_id"] for policy in policies}
    if any(row["policy_id"] not in policy_ids for row in records):
        raise AssertionError("Accepted payment has no accepted policy.")
    expected_counts = {"fixture": (1, 1), "demo": (500, 1000)}
    if (len(customers), len(policies)) != expected_counts[dataset]:
        raise AssertionError("Unexpected reference counts.")
    rejections = collect_records(spark, f"{prefix}_rejections")
    if len(rejections) != sum(result["rejected_row_count"] for result in results):
        raise AssertionError("Stored rejection count differs from the batch summaries.")
    for result in results:
        reasons = set(result["error_codes"])
        for row in rejections:
            if row["batch_id"] == result["batch_id"]:
                reasons.update(row["reason_codes"])
        target = expected[result["batch_id"]]
        if "expected_reason_codes" in target and reasons != set(target["expected_reason_codes"]):
            raise AssertionError(f"Unexpected reasons for {result['batch_id']}: {sorted(reasons)}")
    if dataset == "demo":
        actual = {json.loads(row["raw_payload"])["payment_id"]: row["reason_codes"] for row in rejections}
        target = {
            row["payment_id"]: sorted(row["expected_reason_codes"])
            for row in oracle["intentional_invalid_rows_in_updates"]
        }
        if actual != target:
            raise AssertionError("Demo rejection reasons differ from the acceptance cases.")


# COMMAND ----------

if "dbutils" in globals():
    verification_root = dbutils.widgets.get("volume_root").strip()
    verification_dataset = dbutils.widgets.get("dataset")
    oracle = json.loads((Path(verification_root) / verification_dataset / "expected_outcomes.json").read_bytes())
    expected = reduced_expectations(oracle, verification_dataset)
    assert_batch_summaries(silver_results, expected)
    assert_bronze_state(spark, bronze_prefix, verification_root, verification_dataset)
    assert_final_state(spark, silver_prefix, verification_dataset, expected, silver_results, oracle)
    before_replay = pipeline_snapshot(spark, bronze_prefix, silver_prefix)
    _, replay_bronze = ingest_bronze(spark, verification_root, verification_dataset)
    _, replay_silver = process_silver(spark, verification_root, verification_dataset)
    assert_batch_summaries(replay_silver, expected)
    if replay_bronze["new_rows"]:
        raise AssertionError("Replay added raw rows.")
    if before_replay != pipeline_snapshot(spark, bronze_prefix, silver_prefix):
        raise AssertionError("Replay changed stored records or their first processing metadata.")
    print(f"Verified {verification_dataset}: final state and identical replays.")
