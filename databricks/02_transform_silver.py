# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: clean references and current payments
# MAGIC Validation uses Python on the driver for this small synthetic sample.
# MAGIC Run one execution at a time. Acceptance checks are in `03_verify_pipeline`.

# COMMAND ----------

import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path, PurePosixPath


FIELDS = {
    "customers": ("customer_id", "region", "customer_segment"),
    "policies": ("policy_id", "customer_id", "product_code"),
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
    "entity", "batch_id", "batch_as_of_utc", "source_file", "source_sha256",
    "source_row_number", "raw_payload", "ingested_at",
)
SILVER_COMMON_SCHEMA = """entity STRING, batch_id STRING, batch_as_of_utc TIMESTAMP,
    source_file STRING, source_sha256 STRING, source_row_number BIGINT,
    raw_payload STRING, ingested_at TIMESTAMP, processed_at TIMESTAMP"""
SCHEMAS = {
    "customers": f"customer_id STRING, region STRING, customer_segment STRING, {SILVER_COMMON_SCHEMA}",
    "policies": f"policy_id STRING, customer_id STRING, product_code STRING, {SILVER_COMMON_SCHEMA}",
    "payments": f"""payment_id STRING, policy_id STRING, payment_date DATE,
        amount DECIMAL(12,2), currency STRING, source_updated_at TIMESTAMP, {SILVER_COMMON_SCHEMA}""",
    "rejections": f"{SILVER_COMMON_SCHEMA}, reason_codes ARRAY<STRING>",
}
SUMMARY_SCHEMA = """batch_id STRING, status STRING, error_codes ARRAY<STRING>,
    input_row_count BIGINT, rejected_row_count BIGINT"""


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
    return result


def validate_row(raw, entity, parents):
    values = normalized_payload(raw, entity)
    reasons = set()
    for field, value in values.items():
        if value is None or value == "":
            reasons.add("MISSING_REQUIRED_VALUE")
        elif not isinstance(value, str):
            reasons.add("INVALID_VALUE_TYPE")

    for field, allowed in CATEGORIES.items():
        value = values.get(field)
        if isinstance(value, str) and value and value not in allowed:
            reasons.add("INVALID_CATEGORY")

    for field in ("payment_date", "source_updated_at"):
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


def audit_row(raw, reasons):
    return {**{key: raw[key] for key in PROVENANCE}, "reason_codes": sorted(reasons)}


def new_plan():
    return {"status": "SUCCESS", "error_codes": [], "rejections": [], "changes": []}


def finish_plan(plan):
    if plan["error_codes"]:
        plan["status"] = "FAILED"
    elif plan["rejections"]:
        plan["status"] = "SUCCESS_WITH_REJECTIONS"
    return plan


def reference_plan(rows):
    plan = new_plan()
    customers = set()
    for entity in ("customers", "policies"):
        id_field = FIELDS[entity][0]
        entity_rows = [row for row in rows if row["entity"] == entity]
        seen, conflicts, accepted = {}, set(), {}
        for raw in entity_rows:
            values = normalized_payload(raw, entity)
            identifier = values[id_field]
            if isinstance(identifier, str) and identifier:
                if identifier in seen and values != seen[identifier]:
                    conflicts.add(identifier)
                seen[identifier] = values
        for raw in entity_rows:
            values, reasons = validate_row(raw, entity, customers)
            identifier = values[id_field]
            if isinstance(identifier, str) and identifier in conflicts:
                reasons.append("CONFLICTING_REFERENCE_ID")
            if reasons:
                plan["rejections"].append(audit_row(raw, reasons))
            else:
                accepted.setdefault(identifier, {**values, **{key: raw[key] for key in PROVENANCE}})
        if conflicts:
            plan["error_codes"] = ["CONFLICTING_REFERENCE_ID"]
        plan[entity] = list(accepted.values())
        if entity == "customers":
            customers = set(accepted)
    return finish_plan(plan)


def payment_payload(row):
    return tuple(row[field] for field in ("policy_id", "payment_date", "amount", "currency"))


def payment_plan(rows, policies, current):
    plan = new_plan()
    stored = {row["payment_id"]: row for row in current}
    versions, conflicts = {}, set()
    for raw in rows:
        values, reasons = validate_row(raw, "payments", policies)
        if reasons:
            plan["rejections"].append(audit_row(raw, reasons))
            continue
        key = (values["payment_id"], values["source_updated_at"])
        previous = stored.get(values["payment_id"])
        if key in versions and payment_payload(values) != payment_payload(versions[key]):
            conflicts.add(key)
        if previous and values["source_updated_at"] == previous["source_updated_at"]:
            if payment_payload(values) != payment_payload(previous):
                conflicts.add(key)
        versions.setdefault(key, {**values, **{field: raw[field] for field in PROVENANCE}})

    # Check the whole batch before publishing any payment changes.
    if conflicts:
        plan["error_codes"] = ["CONFLICTING_PAYMENT_VERSION"]
        for raw in rows:
            values, reasons = validate_row(raw, "payments", policies)
            if not reasons and (values["payment_id"], values["source_updated_at"]) in conflicts:
                plan["rejections"].append(audit_row(raw, plan["error_codes"]))
        return finish_plan(plan)

    candidates = {}
    for row in versions.values():
        identifier = row["payment_id"]
        if identifier not in candidates or row["source_updated_at"] > candidates[identifier]["source_updated_at"]:
            candidates[identifier] = row
    for identifier, candidate in candidates.items():
        previous = stored.get(identifier)
        if previous is None or candidate["source_updated_at"] > previous["source_updated_at"]:
            plan["changes"].append(candidate)
    return finish_plan(plan)


# COMMAND ----------


def collect_records(spark, table):
    records = [row.asDict(recursive=True) for row in spark.table(table).collect()]
    for row in records:
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = utc(value)
    return records


def merge_records(spark, table, records, keys, schema, payments=False):
    if not records:
        return
    processed = datetime.now(timezone.utc)
    records = [{**row, "processed_at": processed} for row in records]
    spark.createDataFrame(records, schema).createOrReplaceTempView("insurance_silver_incoming")
    condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
    update = ""
    if payments:
        update = "WHEN MATCHED AND source.source_updated_at > target.source_updated_at THEN UPDATE SET *"
    spark.sql(f"""
        MERGE INTO {table} AS target USING insurance_silver_incoming AS source
        ON {condition}
        {update}
        WHEN NOT MATCHED THEN INSERT *
    """)


def delivery_errors(files, rows, entities, batch):
    errors = set()
    if len(files) != len(entities) or {file["entity"] for file in files} != set(entities):
        errors.add("MISSING_OR_AMBIGUOUS_FILE")
    for file in files:
        if file["entity"] not in entities:
            continue
        description = batch["files"][file["entity"]] if "files" in batch else batch["file"]
        if file["file_status"] != "READY":
            errors.update(file["error_codes"] or ["FILE_NOT_READY"])
        if (
            file["source_sha256"] != description["sha256"]
            or utc(file["batch_as_of_utc"]) != parse_timestamp(batch["batch_as_of_utc"])
            or not file["source_file"].replace("\\", "/").endswith("/" + description["path"])
            or (file["file_status"] == "READY" and file["input_row_count"] != description["row_count"])
        ):
            errors.add("BRONZE_DELIVERY_MISMATCH")
        matching = [row for row in rows if row["entity"] == file["entity"]]
        positions = [row["source_row_number"] for row in matching]
        if len(matching) != file["input_row_count"] or sorted(positions) != list(range(1, len(matching) + 1)):
            errors.add("INCOMPLETE_BRONZE_ROWS")
        for row in matching:
            if any(row[field] != file[field] for field in (
                "batch_id", "batch_as_of_utc", "source_file", "source_sha256"
            )):
                errors.add("BRONZE_DELIVERY_MISMATCH")
    if any(row["entity"] not in entities for row in rows):
        errors.add("BRONZE_DELIVERY_MISMATCH")
    return sorted(errors)


def check_reference_data(incoming, stored, entity):
    """Freeze the accepted reference data without a separate load-state table."""
    fields = FIELDS[entity] + ("batch_id", "source_sha256")
    expected = {tuple(row[field] for field in fields) for row in incoming}
    actual = {tuple(row[field] for field in fields) for row in stored}
    if stored and actual != expected:
        raise ValueError(f"Accepted {entity} baseline changed; review before rebuilding.")


def batch_summary(batch, rows, plan):
    return {
        "batch_id": batch["batch_id"], "status": plan["status"], "error_codes": plan["error_codes"],
        "input_row_count": len(rows), "rejected_row_count": len(plan["rejections"]),
    }


def payment_sequence(files, rows, batches, policies):
    # Recompute the complete sequence so reruns need no batch ledger or version table.
    current, rejections, results = {}, [], []
    for batch in batches:
        batch_files = [file for file in files if file["batch_id"] == batch["batch_id"]]
        batch_rows = [row for row in rows if row["batch_id"] == batch["batch_id"]]
        errors = delivery_errors(batch_files, batch_rows, ("payments",), batch)
        if errors:
            plan = new_plan()
            plan["error_codes"] = errors
            finish_plan(plan)
        else:
            plan = payment_plan(batch_rows, policies, list(current.values()))
        rejections.extend(plan["rejections"])
        if plan["status"] != "FAILED":
            for change in plan["changes"]:
                current[change["payment_id"]] = change
        results.append(batch_summary(batch, batch_rows, plan))
    return list(current.values()), rejections, results


def process_silver(spark, volume_root, dataset):
    root = PurePosixPath(volume_root)
    if root.parts[:2] != ("/", "Volumes") or len(root.parts) != 5 or dataset not in ("fixture", "demo"):
        raise ValueError("Use /Volumes/<catalog>/<schema>/<volume> and fixture or demo.")
    namespace = ".".join("`" + part.replace("`", "``") + "`" for part in root.parts[2:4])
    prefix = f"{namespace}.insurance_{dataset}_simple_silver"
    bronze = f"{namespace}.insurance_{dataset}_simple_bronze"
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    manifest = json.loads((Path(volume_root) / dataset / "manifest.json").read_bytes())
    if manifest.get("dataset") != f"insurance_premium_{dataset}" or manifest.get("synthetic") is not True:
        raise ValueError("Manifest does not match the selected synthetic dataset.")
    files = collect_records(spark, f"{bronze}_files")
    rows = collect_records(spark, f"{bronze}_rows")
    rows.sort(key=lambda row: (row["batch_id"], row["entity"], row["source_row_number"]))
    for suffix, schema in SCHEMAS.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {prefix}_{suffix} ({schema}) USING DELTA")

    baseline = manifest["baseline"]
    reference_files = [file for file in files if file["batch_id"] == baseline["batch_id"]]
    reference_rows = [row for row in rows if row["batch_id"] == baseline["batch_id"]]
    errors = delivery_errors(reference_files, reference_rows, ("customers", "policies"), baseline)
    if errors:
        raise ValueError(f"Reference files failed: {errors}")
    plan = reference_plan(reference_rows)
    rejection_keys = ["batch_id", "entity", "source_row_number"]
    merge_records(spark, f"{prefix}_rejections", plan["rejections"], rejection_keys, SCHEMAS["rejections"])
    if plan["status"] == "FAILED":
        raise ValueError(f"Reference validation failed: {plan['error_codes']}")
    for entity in ("customers", "policies"):
        check_reference_data(plan[entity], collect_records(spark, f"{prefix}_{entity}"), entity)
    for entity in ("customers", "policies"):
        merge_records(spark, f"{prefix}_{entity}", plan[entity], [FIELDS[entity][0]], SCHEMAS[entity])
    policies = {row["policy_id"] for row in plan["policies"]}

    current, rejections, results = payment_sequence(files, rows, manifest["batches"], policies)
    merge_records(spark, f"{prefix}_rejections", rejections, rejection_keys, SCHEMAS["rejections"])
    merge_records(spark, f"{prefix}_payments", current, ["payment_id"], SCHEMAS["payments"], payments=True)
    return prefix, results


# COMMAND ----------

if "dbutils" in globals():
    dbutils.widgets.text("volume_root", "/Volumes/insurance_project/default/insurance_sources")
    dbutils.widgets.dropdown("dataset", "fixture", ["fixture", "demo"])

# COMMAND ----------

if "dbutils" in globals():
    silver_prefix, silver_results = process_silver(
        spark, dbutils.widgets.get("volume_root").strip(), dbutils.widgets.get("dataset")
    )
    display(spark.createDataFrame(silver_results, SUMMARY_SCHEMA))
    display(spark.sql(f"SELECT COUNT(*) AS current_payments, SUM(amount) AS amount_eur FROM {silver_prefix}_payments"))
