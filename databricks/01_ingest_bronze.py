# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: raw deliveries
# MAGIC Captures raw rows, file status, and provenance from extracted sources.
# MAGIC Identical replays preserve stored records. Ingestion supports one execution at a time.

# COMMAND ----------

import csv
import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath


REQUIRED_COLUMNS = {
    "customers": {"customer_id", "region", "customer_segment"},
    "policies": {"policy_id", "customer_id", "product_code"},
    "payments": {
        "payment_id", "policy_id", "payment_date", "amount", "currency", "source_updated_at"
    },
}
FILE_KEYS = ["batch_id", "entity"]
FILE_IDENTITY = (
    "batch_as_of_utc", "source_file", "source_sha256", "input_row_count",
    "file_status", "error_codes",
)
BRONZE_COMMON_SCHEMA = """entity STRING, batch_id STRING, batch_as_of_utc TIMESTAMP,
    source_file STRING, source_sha256 STRING, ingested_at TIMESTAMP"""
FILE_SCHEMA = BRONZE_COMMON_SCHEMA + """, input_row_count BIGINT,
    file_status STRING, error_codes ARRAY<STRING>, error_detail STRING"""
RAW_SCHEMA = BRONZE_COMMON_SCHEMA + ", source_row_number BIGINT, raw_payload STRING"


def unique_json_object(pairs):
    record = {}
    for name, value in pairs:
        if name in record:
            raise ValueError(f"Repeated JSON field name: {name}")
        record[name] = value
    return record


def parse_records(content, entity):
    """Parse structure while preserving source values, including whitespace."""
    text = content.decode("utf-8")
    if entity == "policies":
        records = json.loads(text, object_pairs_hook=unique_json_object)
        if not isinstance(records, list) or any(not isinstance(row, dict) for row in records):
            raise ValueError("Policies must be a JSON array of objects.")
        columns = set().union(*(row.keys() for row in records)) if records else set()
    else:
        reader = csv.reader(io.StringIO(text, newline=""), strict=True)
        header = next(reader, [])
        if not header or any(not name.strip() for name in header) or len(header) != len(set(header)):
            raise ValueError("CSV needs a header with unique, nonblank column names.")
        columns, records = set(header), []
        for values in reader:
            if len(values) != len(header):
                raise ValueError("CSV row width differs from its header.")
            records.append(dict(zip(header, values)))
    return records, columns


def prepare_sources(sources_root, dataset):
    """Read deliveries in manifest order; expected outcomes are not pipeline input."""
    manifest = json.loads(
        (sources_root / "manifest.json").read_bytes(), object_pairs_hook=unique_json_object
    )
    if (
        manifest.get("schema_version") != 1
        or manifest.get("synthetic") is not True
        or manifest.get("dataset") != f"insurance_premium_{dataset}"
    ):
        raise ValueError("Expected the selected version 1 synthetic source manifest.")
    baseline = manifest["baseline"]
    deliveries = [(baseline, entity, baseline["files"][entity]) for entity in ("customers", "policies")]
    deliveries += [(batch, "payments", batch["file"]) for batch in manifest["batches"]]
    batch_ids = [baseline["batch_id"]] + [batch["batch_id"] for batch in manifest["batches"]]
    if any(not isinstance(value, str) or not value.strip() for value in batch_ids):
        raise ValueError("Batch IDs must be nonblank strings.")
    if len(batch_ids) != len(set(batch_ids)):
        raise ValueError("Batch IDs must be unique within the manifest.")

    files, rows = [], []
    for batch, entity, description in deliveries:
        as_of = datetime.strptime(
            batch["batch_as_of_utc"], "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=timezone.utc)
        relative = PurePosixPath(description["path"])
        if relative.is_absolute() or ".." in relative.parts or "\\" in description["path"]:
            raise ValueError(f"Unsafe source path: {relative}")
        path = (sources_root / str(relative)).resolve()
        if not path.is_relative_to(sources_root.resolve()):
            raise ValueError(f"Source path escapes its directory: {relative}")
        content = path.read_bytes()
        checksum = hashlib.sha256(content).hexdigest()
        errors, details = [], []
        if checksum != description["sha256"]:
            errors.append("CHECKSUM_MISMATCH")
        try:
            records, columns = parse_records(content, entity)
        except (ValueError, UnicodeError, csv.Error) as error:
            records, columns = [], set()
            errors.append("UNPARSEABLE_FILE")
            details.append(str(error))
        else:
            missing = REQUIRED_COLUMNS[entity] - columns
            # An empty JSON array has no header to check.
            if missing and not (entity == "policies" and not records):
                errors.append("MISSING_REQUIRED_COLUMN")
                details.append("Missing columns: " + ", ".join(sorted(missing)))
            if len(records) != description["row_count"]:
                errors.append("ROW_COUNT_MISMATCH")

        provenance = {
            "entity": entity, "batch_id": batch["batch_id"], "batch_as_of_utc": as_of,
            "source_file": str(path), "source_sha256": checksum,
        }
        files.append({
            **provenance, "input_row_count": len(records),
            "file_status": "FAILED" if errors else "READY",
            "error_codes": errors, "error_detail": "; ".join(details),
        })
        for number, record in enumerate(records, start=1):
            rows.append({
                **provenance, "source_row_number": number,
                "raw_payload": json.dumps(record, ensure_ascii=False, sort_keys=True),
            })
    return files, rows


def check_immutable_files(files, stored):
    previous = {(file["batch_id"], file["entity"]): file for file in stored}
    for file in files:
        key = (file["batch_id"], file["entity"])
        if key in previous and any(file[field] != previous[key][field] for field in FILE_IDENTITY):
            raise ValueError(f"Changed immutable delivery: {key}. Use a new batch ID.")


def merge_raw(spark, table, incoming, keys):
    incoming.createOrReplaceTempView("insurance_bronze_incoming")
    condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
    spark.sql(f"""
        MERGE INTO {table} AS target USING insurance_bronze_incoming AS source
        ON {condition}
        WHEN NOT MATCHED THEN INSERT *
    """)


def ingest_bronze(spark, volume_root, dataset):
    root = PurePosixPath(volume_root)
    if root.parts[:2] != ("/", "Volumes") or len(root.parts) != 5 or dataset not in ("fixture", "demo"):
        raise ValueError("Use /Volumes/<catalog>/<schema>/<volume> and fixture or demo.")
    namespace = ".".join("`" + part.replace("`", "``") + "`" for part in root.parts[2:4])
    prefix = f"{namespace}.insurance_{dataset}_simple_bronze"
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    files, rows = prepare_sources(Path(volume_root) / dataset, dataset)
    for suffix, schema in (("files", FILE_SCHEMA), ("rows", RAW_SCHEMA)):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {prefix}_{suffix} ({schema}) USING DELTA")
    stored = [row.asDict() for row in spark.table(f"{prefix}_files").collect()]
    for file in stored:
        file["batch_as_of_utc"] = file["batch_as_of_utc"].replace(tzinfo=timezone.utc)
    check_immutable_files(files, stored)
    before = spark.table(f"{prefix}_rows").count()
    started = datetime.now(timezone.utc)
    for record in files + rows:
        record["ingested_at"] = started
    merge_raw(spark, f"{prefix}_files", spark.createDataFrame(files, FILE_SCHEMA), FILE_KEYS)
    merge_raw(spark, f"{prefix}_rows", spark.createDataFrame(rows, RAW_SCHEMA), FILE_KEYS + ["source_row_number"])
    inserted = spark.table(f"{prefix}_rows").count() - before
    return prefix, {"input_rows": len(rows), "new_rows": inserted,
                    "failed_files": sum(file["file_status"] == "FAILED" for file in files)}


# COMMAND ----------

if "dbutils" in globals():
    dbutils.widgets.text("volume_root", "/Volumes/insurance_project/default/insurance_sources")
    dbutils.widgets.dropdown("dataset", "fixture", ["fixture", "demo"])

# COMMAND ----------

if "dbutils" in globals():
    bronze_prefix, bronze_summary = ingest_bronze(
        spark, dbutils.widgets.get("volume_root").strip(), dbutils.widgets.get("dataset")
    )
    print(f"Bronze: {bronze_summary['input_rows']} raw rows, "
          f"{bronze_summary['new_rows']} newly stored; failed files: {bronze_summary['failed_files']}.")
    display(spark.table(f"{bronze_prefix}_files").select(
        "entity", "batch_id", "input_row_count", "file_status", "error_codes"
    ).orderBy("entity", "batch_id"))
