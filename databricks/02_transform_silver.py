# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: validate references and payment versions
# MAGIC Reuse existing Bronze tables.
# MAGIC Validation runs on the driver for this small sample. Delta stores the results.

# COMMAND ----------

import hashlib
import json
import re
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path, PurePosixPath
from uuid import uuid4


FIELDS = {
    "customers": ("customer_id", "region", "customer_segment"),
    "policies": ("policy_id", "customer_id", "product_code", "start_date", "end_date"),
    "payments": (
        "payment_id", "policy_id", "payment_date", "amount", "currency", "source_updated_at"
    ),
}
CATEGORIES = {
    "region": {"Uusimaa", "North Ostrobothnia", "Pirkanmaa", "Other"},
    "customer_segment": {"STANDARD", "PRIORITY"},
    "product_code": {"LIFE_BASIC", "LIFE_PLUS"},
}
PROVENANCE = (
    "dataset", "entity", "batch_id", "batch_as_of_utc", "source_file", "source_sha256",
    "source_row_number", "raw_payload", "ingested_at", "ingestion_run_id",
)
COUNT_FIELDS = (
    "input_row_count", "rejected_row_count", "duplicate_row_count", "superseded_version_count",
    "stale_version_count", "unchanged_version_count", "inserted_count", "updated_count",
    "new_version_count",
)


def utc(value):
    """Spark returns UTC timestamps without timezone information on the driver."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_date(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise ValueError("Use YYYY-MM-DD.")
    return date.fromisoformat(value)


def parse_timestamp(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", value
    ):
        raise ValueError("Use YYYY-MM-DDTHH:MM:SSZ.")
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def parse_amount(value):
    if value is None or value == "":
        return None, []
    if not isinstance(value, str) or not re.fullmatch(r"[+-]?[0-9]+(?:\.[0-9]{1,2})?", value):
        return None, ["INVALID_AMOUNT_FORMAT"]
    amount = Decimal(value)
    reasons = []
    if amount.copy_abs() > Decimal("9999999999.99"):
        reasons.append("INVALID_AMOUNT_FORMAT")
    if amount <= 0:
        reasons.append("NON_POSITIVE_AMOUNT")
    if reasons:
        return None, reasons
    return amount.quantize(Decimal("0.01")), []


def normalized_payload(raw, entity):
    payload = json.loads(raw["raw_payload"])
    result = {}
    for field in FIELDS[entity]:
        value = payload.get(field)
        result[field] = value.strip() if isinstance(value, str) else value
    if entity == "policies" and result["end_date"] == "":
        result["end_date"] = None
    return result


def validate_row(raw, entity, parents):
    values = normalized_payload(raw, entity)
    reasons = set()
    for field, value in values.items():
        if field == "end_date" and value is None:
            continue
        if value is None or value == "":
            reasons.add("MISSING_REQUIRED_VALUE")
        elif not isinstance(value, str):
            reasons.add("INVALID_VALUE_TYPE")

    for field, allowed in CATEGORIES.items():
        value = values.get(field)
        if isinstance(value, str) and value and value not in allowed:
            reasons.add("INVALID_CATEGORY")

    for field in ("start_date", "end_date", "payment_date", "source_updated_at"):
        value = values.get(field)
        if value is None or value == "":
            continue
        try:
            if field == "source_updated_at":
                values[field] = parse_timestamp(value)
            else:
                values[field] = parse_date(value)
        except ValueError:
            values[field] = None
            reasons.add("INVALID_DATE_OR_TIMESTAMP")

    if entity == "policies":
        start, end = values["start_date"], values["end_date"]
        if isinstance(start, date) and isinstance(end, date) and end < start:
            reasons.add("INVALID_POLICY_DATE_RANGE")
        parent = values["customer_id"]
        if isinstance(parent, str) and parent and parent not in parents:
            reasons.add("UNKNOWN_CUSTOMER")

    if entity == "payments":
        values["amount"], amount_reasons = parse_amount(values["amount"])
        reasons.update(amount_reasons)
        currency = values["currency"]
        if isinstance(currency, str) and currency and currency != "EUR":
            reasons.add("UNSUPPORTED_CURRENCY")
        parent = values["policy_id"]
        if isinstance(parent, str) and parent and parent not in parents:
            reasons.add("UNKNOWN_POLICY")
        received, updated = values["payment_date"], values["source_updated_at"]
        cutoff = utc(raw["batch_as_of_utc"])
        if (
            (isinstance(received, date) and received > cutoff.date())
            or (isinstance(updated, datetime) and updated > cutoff)
        ):
            reasons.add("FUTURE_PAYMENT_OR_UPDATE")
        if isinstance(received, date) and isinstance(updated, datetime) and updated.date() < received:
            reasons.add("UPDATE_BEFORE_PAYMENT")
    return values, sorted(reasons)


def audit_row(raw, reasons, kind="ROW_VALIDATION"):
    return {**{key: raw[key] for key in PROVENANCE}, "reason_codes": reasons, "rejection_kind": kind}


def new_plan(rows):
    plan = dict.fromkeys(COUNT_FIELDS, 0)
    plan.update(
        status="SUCCESS", error_codes=[], error_detail="", rejections=[],
        input_row_count=len(rows),
    )
    return plan


def finish_plan(plan):
    plan["rejected_row_count"] = sum(
        row["rejection_kind"] == "ROW_VALIDATION" for row in plan["rejections"]
    )
    if plan["error_codes"]:
        plan["status"] = "FAILED"
    elif plan["rejected_row_count"]:
        plan["status"] = "SUCCESS_WITH_REJECTIONS"
    return plan


def reference_plan(rows):
    plan = new_plan(rows)
    customer_ids = set()
    for entity in ("customers", "policies"):
        id_field = FIELDS[entity][0]
        seen, conflicts, valid = {}, set(), {}
        entity_rows = [row for row in rows if row["entity"] == entity]
        for raw in entity_rows:
            normalized = normalized_payload(raw, entity)
            identifier = normalized[id_field]
            if isinstance(identifier, str) and identifier:
                if identifier in seen and normalized != seen[identifier]:
                    conflicts.add(identifier)
                elif identifier in seen:
                    plan["duplicate_row_count"] += 1
                seen[identifier] = normalized
            values, reasons = validate_row(raw, entity, customer_ids)
            if reasons:
                plan["rejections"].append(audit_row(raw, reasons))
            else:
                valid.setdefault(identifier, {**values, **{key: raw[key] for key in PROVENANCE}})
        if conflicts:
            plan["error_codes"].append("CONFLICTING_REFERENCE_ID")
            for raw in entity_rows:
                identifier = normalized_payload(raw, entity)[id_field]
                if isinstance(identifier, str) and identifier in conflicts:
                    plan["rejections"].append(audit_row(raw, ["CONFLICTING_REFERENCE_ID"], "BATCH_CONFLICT"))
            for identifier in conflicts:
                valid.pop(identifier, None)
        if entity == "customers":
            customer_ids = set(valid)
        plan[entity] = list(valid.values())
    plan["error_codes"] = sorted(set(plan["error_codes"]))
    return finish_plan(plan)


def payment_payload(row):
    return tuple(row[field] for field in ("policy_id", "payment_date", "amount", "currency"))


def current_payments(versions):
    current = {}
    for row in versions:
        identifier = row["payment_id"]
        if identifier not in current or row["source_updated_at"] > current[identifier]["source_updated_at"]:
            current[identifier] = row
    return current


def payment_plan(rows, policies, history):
    plan = new_plan(rows)
    plan["versions"] = []
    known = {(row["payment_id"], row["source_updated_at"]): row for row in history}
    incoming, source_rows, conflicts = {}, defaultdict(list), set()
    for raw in rows:
        values, reasons = validate_row(raw, "payments", policies)
        if reasons:
            plan["rejections"].append(audit_row(raw, reasons))
            continue
        key = (values["payment_id"], values["source_updated_at"])
        source_rows[key].append(raw)
        if key in incoming:
            if payment_payload(values) != payment_payload(incoming[key]):
                conflicts.add(key)
            else:
                plan["duplicate_row_count"] += 1
        else:
            incoming[key] = {**values, **{field: raw[field] for field in PROVENANCE}}
        if key in known and payment_payload(values) != payment_payload(known[key]):
            conflicts.add(key)

    # A conflict blocks every payment change in this batch.
    if conflicts:
        plan["error_codes"] = ["CONFLICTING_PAYMENT_VERSION"]
        for key in sorted(conflicts):
            for raw in source_rows[key]:
                plan["rejections"].append(audit_row(raw, plan["error_codes"], "BATCH_CONFLICT"))
        return finish_plan(plan)

    candidates = current_payments(incoming.values())
    current = current_payments(history)
    plan["superseded_version_count"] = len(incoming) - len(candidates)
    for identifier, candidate in candidates.items():
        previous = current.get(identifier)
        if previous is None:
            plan["inserted_count"] += 1
        elif candidate["source_updated_at"] > previous["source_updated_at"]:
            plan["updated_count"] += 1
        elif candidate["source_updated_at"] < previous["source_updated_at"]:
            plan["stale_version_count"] += 1
        else:
            plan["unchanged_version_count"] += 1
    plan["versions"] = [row for key, row in incoming.items() if key not in known]
    plan["new_version_count"] = len(plan["versions"])
    return finish_plan(plan)


# COMMAND ----------

COMMON_SCHEMA = """dataset STRING, entity STRING, batch_id STRING, batch_as_of_utc TIMESTAMP,
    source_file STRING, source_sha256 STRING, source_row_number BIGINT, raw_payload STRING,
    ingested_at TIMESTAMP, ingestion_run_id STRING"""
ROW_SCHEMA = COMMON_SCHEMA + ", processed_at TIMESTAMP, silver_run_id STRING"
COUNT_SCHEMA = ", ".join(f"{field} BIGINT" for field in COUNT_FIELDS)
SCHEMAS = {
    "customers": f"customer_id STRING, region STRING, customer_segment STRING, {ROW_SCHEMA}",
    "policies": f"""policy_id STRING, customer_id STRING, product_code STRING,
        start_date DATE, end_date DATE, {ROW_SCHEMA}""",
    "payment_versions": f"""payment_id STRING, policy_id STRING, payment_date DATE,
        amount DECIMAL(12,2), currency STRING, source_updated_at TIMESTAMP, {ROW_SCHEMA}""",
    "rejections": f"""{COMMON_SCHEMA}, reason_codes ARRAY<STRING>, rejection_kind STRING,
        processed_at TIMESTAMP, silver_run_id STRING""",
    "baseline": """dataset STRING, batch_id STRING, fingerprint STRING, status STRING,
        processed_at TIMESTAMP, silver_run_id STRING""",
    "runs": f"""silver_run_id STRING, dataset STRING, entity STRING, batch_id STRING,
        started_at TIMESTAMP, completed_at TIMESTAMP, status STRING, error_codes ARRAY<STRING>,
        error_detail STRING, {COUNT_SCHEMA}""",
}


def merge_insert(spark, table, records, keys, schema):
    if not records:
        return
    spark.createDataFrame(records, schema).createOrReplaceTempView("insurance_silver_incoming")
    condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
    spark.sql(f"""
        MERGE INTO {table} AS target USING insurance_silver_incoming AS source
        ON {condition}
        WHEN NOT MATCHED THEN INSERT *
    """)


def collect_records(spark, table):
    records = [row.asDict(recursive=True) for row in spark.table(table).collect()]
    for row in records:
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = utc(value)
    return records


def open_silver(spark, volume_root, dataset):
    root = PurePosixPath(volume_root)
    if root.parts[:2] != ("/", "Volumes") or len(root.parts) != 5 or dataset not in ("fixture", "demo"):
        raise ValueError("Use /Volumes/<catalog>/<schema>/<volume> and fixture or demo.")
    namespace = ".".join("`" + part.replace("`", "``") + "`" for part in root.parts[2:4])
    prefix = f"{namespace}.insurance_{dataset}_silver"
    bronze = f"{namespace}.insurance_{dataset}_bronze"
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    manifest = json.loads((Path(volume_root) / dataset / "manifest.json").read_bytes())
    if (
        manifest.get("schema_version") != 1
        or manifest.get("dataset") != f"insurance_premium_{dataset}"
        or manifest.get("synthetic") is not True
    ):
        raise ValueError("Expected the selected version 1 synthetic source manifest.")
    ids = [manifest["baseline"]["batch_id"]] + [batch["batch_id"] for batch in manifest["batches"]]
    if len(ids) != len(set(ids)):
        raise ValueError("Manifest batch IDs must be unique.")
    for suffix, schema in SCHEMAS.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {prefix}_{suffix} ({schema}) USING DELTA")
    columns = ", ".join(FIELDS["payments"] + PROVENANCE + ("processed_at", "silver_run_id"))
    spark.sql(f"""
        CREATE OR REPLACE VIEW {prefix}_payments AS
        SELECT {columns} FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY payment_id ORDER BY source_updated_at DESC
            ) AS version_rank FROM {prefix}_payment_versions
        ) WHERE version_rank = 1
    """)
    return prefix, bronze, manifest


def delivery_errors(files, rows, entities, batch, dataset):
    errors = set()
    if len(files) != len(entities) or {file["entity"] for file in files} != set(entities):
        errors.add("MISSING_OR_AMBIGUOUS_FILE")
    cutoff = parse_timestamp(batch["batch_as_of_utc"])
    for file in files:
        if file["entity"] not in entities:
            continue
        description = batch["files"][file["entity"]] if "files" in batch else batch["file"]
        if file["file_status"] != "READY":
            errors.update(file["error_codes"] or ["FILE_NOT_READY"])
        if (
            file["dataset"] != dataset or file["batch_id"] != batch["batch_id"]
            or utc(file["batch_as_of_utc"]) != cutoff
            or file["source_sha256"] != description["sha256"]
            or (file["file_status"] == "READY" and file["input_row_count"] != description["row_count"])
            or not file["source_file"].replace("\\", "/").endswith("/" + description["path"])
            or hashlib.sha256(bytes(file["source_content"])).hexdigest() != file["source_sha256"]
        ):
            errors.add("BRONZE_DELIVERY_MISMATCH")
        matching = [row for row in rows if row["entity"] == file["entity"]]
        positions = {row["source_row_number"] for row in matching}
        if len(matching) != file["input_row_count"] or positions != set(range(1, len(matching) + 1)):
            errors.add("INCOMPLETE_BRONZE_ROWS")
        for row in matching:
            for field in ("dataset", "batch_id", "batch_as_of_utc", "source_file", "source_sha256"):
                if row[field] != file[field]:
                    errors.add("BRONZE_DELIVERY_MISMATCH")
    if any(row["entity"] not in entities for row in rows):
        errors.add("BRONZE_DELIVERY_MISMATCH")
    return sorted(errors)


def check_baseline_identity(files, stored, batch_id):
    fingerprint = hashlib.sha256(json.dumps(sorted(
        (file["entity"], file["delivery_fingerprint"]) for file in files
    )).encode("utf-8")).hexdigest()
    if stored and (
        len(stored) != 1 or stored[0]["batch_id"] != batch_id
        or stored[0]["fingerprint"] != fingerprint
    ):
        return fingerprint, ["IMMUTABLE_BASELINE_CHANGED"]
    return fingerprint, []


def process_batch(spark, prefix, bronze, dataset, batch, baseline=False):
    from pyspark.sql import functions as F

    run_id, started = str(uuid4()), datetime.now(timezone.utc)
    plan = new_plan([])
    try:
        selected_files = spark.table(f"{bronze}_files").where(F.col("batch_id") == batch["batch_id"])
        selected_rows = spark.table(f"{bronze}_rows").where(F.col("batch_id") == batch["batch_id"])
        files = [row.asDict() for row in selected_files.collect()]
        rows = [row.asDict() for row in selected_rows.orderBy("entity", "source_row_number").collect()]
        for record in files + rows:
            record["batch_as_of_utc"] = utc(record["batch_as_of_utc"])
        plan["input_row_count"] = len(rows)
        plan["error_codes"] = delivery_errors(
            files, rows, ("customers", "policies") if baseline else ("payments",), batch, dataset
        )
        if plan["error_codes"]:
            return finish_plan(plan)
        if baseline:
            stored = collect_records(spark, f"{prefix}_baseline")
            fingerprint, plan["error_codes"] = check_baseline_identity(files, stored, batch["batch_id"])
            if plan["error_codes"]:
                return finish_plan(plan)
            plan = reference_plan(rows)
            if plan["status"] != "FAILED":
                marker = {
                    "dataset": dataset, "batch_id": batch["batch_id"], "fingerprint": fingerprint,
                    "status": "LOADING", "processed_at": started, "silver_run_id": run_id,
                }
                # Lock identity first so an interrupted load can only resume the same baseline.
                merge_insert(spark, f"{prefix}_baseline", [marker], ["dataset"], SCHEMAS["baseline"])
                for entity in ("customers", "policies"):
                    before = spark.table(f"{prefix}_{entity}").count()
                    records = [{**row, "processed_at": started, "silver_run_id": run_id} for row in plan[entity]]
                    merge_insert(spark, f"{prefix}_{entity}", records, [FIELDS[entity][0]], SCHEMAS[entity])
                    plan["inserted_count"] += spark.table(f"{prefix}_{entity}").count() - before
                spark.sql(f"UPDATE {prefix}_baseline SET status = 'READY' WHERE status = 'LOADING'")
        else:
            marker = collect_records(spark, f"{prefix}_baseline")
            if len(marker) != 1 or marker[0]["status"] != "READY":
                plan["error_codes"] = ["BASELINE_NOT_READY"]
                return finish_plan(plan)
            policies = {row.policy_id for row in spark.table(f"{prefix}_policies").select("policy_id").collect()}
            history = collect_records(spark, f"{prefix}_payment_versions")
            plan = payment_plan(rows, policies, history)
        rejections = [{**row, "processed_at": started, "silver_run_id": run_id} for row in plan["rejections"]]
        merge_insert(spark, f"{prefix}_rejections", rejections,
                     ["dataset", "batch_id", "entity", "source_row_number", "rejection_kind"], SCHEMAS["rejections"])
        if not baseline and plan["status"] != "FAILED":
            versions = [{**row, "processed_at": started, "silver_run_id": run_id} for row in plan["versions"]]
            # One insert-only Delta transaction is the payment publication boundary.
            merge_insert(spark, f"{prefix}_payment_versions", versions,
                         ["payment_id", "source_updated_at"], SCHEMAS["payment_versions"])
        return plan
    except Exception as error:
        plan["status"] = "ERROR"
        plan["error_detail"] = str(error)
        raise
    finally:
        run = {
            "silver_run_id": run_id, "dataset": dataset, "entity": "reference" if baseline else "payments",
            "batch_id": batch["batch_id"], "started_at": started, "completed_at": datetime.now(timezone.utc),
            **{key: plan[key] for key in ("status", "error_codes", "error_detail") + COUNT_FIELDS},
        }
        spark.createDataFrame([run], SCHEMAS["runs"]).write.mode("append").saveAsTable(f"{prefix}_runs")


def process_sequence(spark, prefix, bronze, dataset, manifest):
    baseline = process_batch(spark, prefix, bronze, dataset, manifest["baseline"], baseline=True)
    if baseline["status"] == "FAILED":
        raise ValueError(f"Reference baseline failed: {baseline['error_codes']}")
    results = []
    for batch in manifest["batches"]:
        result = process_batch(spark, prefix, bronze, dataset, batch)
        results.append(batch_summary(batch, result))
    return results


def batch_summary(batch, result):
    return {"batch_id": batch["batch_id"], **{key: result[key] for key in ("status", "error_codes") + COUNT_FIELDS}}


# COMMAND ----------

def payment_totals(records):
    by_month = defaultdict(lambda: Decimal("0.00"))
    for row in records:
        by_month[row["payment_date"].strftime("%Y-%m")] += row["amount"]
    return {
        "current_payment_count": len(records),
        "total_amount_eur": format(sum(by_month.values(), Decimal("0.00")), ".2f"),
        "amount_by_month_eur": {month: format(amount, ".2f") for month, amount in sorted(by_month.items())},
    }


def assert_outcome(result, records, expected):
    for key, actual in payment_totals(records).items():
        if actual != expected[key]:
            raise AssertionError(f"{key}: actual {actual!r}, expected {expected[key]!r}")
    if result["status"] != expected["status"]:
        raise AssertionError(f"Unexpected batch status: {result['status']}")
    if result["rejected_row_count"] != expected["rejected_row_count_in_this_batch"]:
        raise AssertionError(f"Unexpected rejected row count: {result['rejected_row_count']}")
    reasons = set(result["error_codes"])
    for rejection in result["rejections"]:
        reasons.update(rejection["reason_codes"])
    if "expected_reason_codes" in expected and reasons != set(expected["expected_reason_codes"]):
        raise AssertionError(f"Unexpected reasons: {sorted(reasons)}")


def table_snapshot(spark, prefix, suffix):
    keys = {
        "customers": ("customer_id",),
        "policies": ("policy_id",),
        "baseline": ("dataset",),
        "payment_versions": ("payment_id", "source_updated_at"),
        "rejections": ("batch_id", "entity", "source_row_number", "rejection_kind"),
    }[suffix]
    records = collect_records(spark, f"{prefix}_{suffix}")
    return sorted(records, key=lambda row: tuple(row[key] for key in keys))


def verify_sequence(spark, prefix, bronze, dataset, manifest, volume_root):
    """The oracle is read only here, to assert independently produced results."""
    oracle = json.loads((Path(volume_root) / dataset / "expected_outcomes.json").read_bytes())
    expected = oracle["after_batches"]
    fresh = not spark.table(f"{prefix}_payment_versions").take(1)
    baseline = process_batch(spark, prefix, bronze, dataset, manifest["baseline"], baseline=True)
    if baseline["status"] == "FAILED":
        raise AssertionError(f"Reference baseline failed: {baseline['error_codes']}")
    final = expected[manifest["batches"][-1]["batch_id"]]
    final_totals = {
        key: final[key]
        for key in ("current_payment_count", "total_amount_eur", "amount_by_month_eur")
    }
    if not fresh and payment_totals(collect_records(spark, f"{prefix}_payments")) != final_totals:
        raise ValueError(
            "Verification needs empty Silver payment state or a completed sequence. "
            "Resume with mode=process, then verify."
        )
    results = []
    for index, batch in enumerate(manifest["batches"]):
        before = table_snapshot(spark, prefix, "payment_versions")
        result = process_batch(spark, prefix, bronze, dataset, batch)
        after = table_snapshot(spark, prefix, "payment_versions")
        if result["status"] == "FAILED" and before != after:
            raise AssertionError(f"Failed batch changed payment state: {batch['batch_id']}")
        target = expected[batch["batch_id"]].copy()
        if not fresh:
            target.update(final_totals)
        assert_outcome(result, collect_records(spark, f"{prefix}_payments"), target)
        if dataset == "demo" and batch["batch_id"] == "demo_002_updates":
            actual_reasons = {
                json.loads(row["raw_payload"])["payment_id"]: row["reason_codes"]
                for row in result["rejections"] if row["rejection_kind"] == "ROW_VALIDATION"
            }
            expected_reasons = {
                note["payment_id"]: sorted(note["expected_reason_codes"])
                for note in oracle["intentional_invalid_rows_in_updates"]
            }
            if actual_reasons != expected_reasons:
                raise AssertionError("Demo rejection reasons differ from the oracle.")
        results.append(batch_summary(batch, result))
        if fresh and index == 0:
            replay = process_batch(spark, prefix, bronze, dataset, batch)
            assert_outcome(replay, collect_records(spark, f"{prefix}_payments"), expected[batch["batch_id"]])
            if after != table_snapshot(spark, prefix, "payment_versions"):
                raise AssertionError("Initial replay changed accepted versions.")
    before = {}
    for suffix in ("customers", "policies", "baseline", "payment_versions", "rejections"):
        before[suffix] = table_snapshot(spark, prefix, suffix)
    baseline_replay = process_batch(spark, prefix, bronze, dataset, manifest["baseline"], baseline=True)
    if baseline_replay["status"] == "FAILED" or baseline_replay["inserted_count"]:
        raise AssertionError("Baseline replay failed or added accepted references.")
    for batch in manifest["batches"]:
        if expected[batch["batch_id"]]["status"] != "FAILED":
            replay = process_batch(spark, prefix, bronze, dataset, batch)
            target = expected[batch["batch_id"]].copy()
            target.update(final_totals)
            assert_outcome(replay, collect_records(spark, f"{prefix}_payments"), target)
    for suffix, records in before.items():
        if records != table_snapshot(spark, prefix, suffix):
            raise AssertionError(f"Successful replay changed {suffix}.")
    return results, "full sequence and replays" if fresh else "completed state and replays"


# COMMAND ----------

if "dbutils" in globals():
    dbutils.widgets.text("volume_root", "/Volumes/insurance_project/default/insurance_sources")
    dbutils.widgets.dropdown("dataset", "fixture", ["fixture", "demo"])
    dbutils.widgets.dropdown("mode", "verify", ["verify", "process"])

# COMMAND ----------

if "dbutils" in globals():
    volume_root = dbutils.widgets.get("volume_root").strip()
    dataset = dbutils.widgets.get("dataset")
    prefix, bronze, manifest = open_silver(spark, volume_root, dataset)
    if dbutils.widgets.get("mode") == "verify":
        results, verification = verify_sequence(spark, prefix, bronze, dataset, manifest, volume_root)
        print(f"Verified {dataset}: {verification}.")
    else:
        results = process_sequence(spark, prefix, bronze, dataset, manifest)
    summary_schema = "batch_id STRING, status STRING, error_codes ARRAY<STRING>, " + COUNT_SCHEMA
    display(spark.createDataFrame(results, summary_schema))
    display(spark.sql(f"SELECT COUNT(*) AS current_payments, SUM(amount) AS amount_eur FROM {prefix}_payments"))
    display(spark.sql(f"""SELECT DATE_FORMAT(payment_date, 'yyyy-MM') AS month, SUM(amount) AS amount_eur
        FROM {prefix}_payments GROUP BY 1 ORDER BY 1"""))
