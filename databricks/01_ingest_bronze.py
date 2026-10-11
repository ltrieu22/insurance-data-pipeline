# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: retain source deliveries
# MAGIC Reuse the files extracted by `00_inspect_sources`. No business values are
# MAGIC trimmed, cast, validated, or deduplicated here. Run one ingestion at a time.

# COMMAND ----------

import csv
import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from uuid import uuid4


REQUIRED_COLUMNS = {
    "customers": {"customer_id", "region", "customer_segment"},
    "policies": {"policy_id", "customer_id", "product_code", "start_date"},
    "payments": {
        "payment_id", "policy_id", "payment_date", "amount", "currency", "source_updated_at"
    },
}
KNOWN_COLUMNS = {**REQUIRED_COLUMNS, "policies": REQUIRED_COLUMNS["policies"] | {"end_date"}}


def unique_json_object(pairs):
    """Reject repeated JSON field names instead of silently choosing one value."""
    record = {}
    for name, value in pairs:
        if name in record:
            raise ValueError(f"Repeated JSON field name: {name}")
        record[name] = value
    return record


def parse_records(content: bytes, entity: str) -> tuple[list[dict], set[str]]:
    """Parse file structure without normalizing or validating business values."""
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
        columns = set(header)
        records = []
        for values in reader:
            if len(values) != len(header):
                raise ValueError("CSV row width differs from its header.")
            records.append(dict(zip(header, values)))
    return records, columns


def prepare_sources(sources_root: Path, dataset: str) -> tuple[list[dict], list[dict]]:
    """Build raw file and row records from the manifest, never the expected results."""
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
        errors, details, warnings = [], [], []
        if checksum != description["sha256"]:
            errors.append("CHECKSUM_MISMATCH")
        try:
            records, columns = parse_records(content, entity)
        except (ValueError, UnicodeError, csv.Error) as error:
            records, columns = [], set()
            errors.append("UNPARSEABLE_FILE")
            details.append(str(error))
        if "UNPARSEABLE_FILE" not in errors:
            missing = REQUIRED_COLUMNS[entity] - columns
            # An empty JSON array has no header; its schema is checked when rows arrive.
            if missing and not (entity == "policies" and not records):
                errors.append("MISSING_REQUIRED_COLUMN")
                details.append("Missing columns: " + ", ".join(sorted(missing)))
            extra = columns - KNOWN_COLUMNS[entity]
            if extra:
                warnings.append("Additional columns: " + ", ".join(sorted(extra)))
            if len(records) != description["row_count"]:
                errors.append("ROW_COUNT_MISMATCH")

        provenance = {
            "dataset": dataset, "entity": entity, "batch_id": batch["batch_id"],
            "batch_as_of_utc": as_of, "source_file": str(path), "source_sha256": checksum,
        }
        identity = [
            batch["batch_as_of_utc"], str(path), checksum,
            description["sha256"], description["row_count"],
        ]
        fingerprint = hashlib.sha256(json.dumps(identity).encode("utf-8")).hexdigest()
        files.append({
            **provenance, "delivery_fingerprint": fingerprint, "source_content": content,
            "input_row_count": len(records), "file_status": "FAILED" if errors else "READY",
            "error_codes": errors, "error_detail": "; ".join(details), "schema_warnings": warnings,
        })
        for number, record in enumerate(records, start=1):
            rows.append({
                **provenance, "source_row_number": number,
                "raw_payload": json.dumps(record, ensure_ascii=False, sort_keys=True),
            })
    return files, rows


def merge_raw(spark, table, incoming, keys):
    incoming.createOrReplaceTempView("insurance_bronze_incoming")
    condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
    spark.sql(f"""
        MERGE INTO {table} AS target USING insurance_bronze_incoming AS source
        ON {condition}
        WHEN NOT MATCHED THEN INSERT *
    """)


def verify_bronze(spark, prefix, files, rows, incoming_files, incoming_rows, keys):
    """Check stored source contents and replay writes with fresh run metadata."""
    from pyspark.sql import functions as F

    delivered = incoming_files.select(*keys)
    stored = spark.table(f"{prefix}_rows").join(delivered, keys, "left_semi").collect()
    expected = {
        (row["batch_id"], row["entity"], row["source_row_number"]): row["raw_payload"]
        for row in rows
    }
    actual = {
        (row.batch_id, row.entity, row.source_row_number): row.raw_payload for row in stored
    }
    if actual != expected or len(stored) != len(expected):
        raise ValueError("Stored bronze rows differ from the source records.")

    stored_files = spark.table(f"{prefix}_files").orderBy(*keys).collect()
    expected_files = {
        (file["batch_id"], file["entity"]): file["source_content"] for file in files
    }
    actual_files = {}
    for file in stored_files:
        key = (file.batch_id, file.entity)
        if key in expected_files:
            if key in actual_files:
                raise ValueError(f"Repeated bronze file record: {key}")
            actual_files[key] = bytes(file.source_content)
    if actual_files != expected_files:
        raise ValueError("Stored bronze file bytes differ from the sources.")

    # Existing content and first-ingestion metadata must survive a new run ID/time.
    row_keys = keys + ["source_row_number"]
    stored_rows = spark.table(f"{prefix}_rows").orderBy(*row_keys).collect()
    replay_id = str(uuid4())
    replay_files = incoming_files.withColumn("ingested_at", F.current_timestamp()).withColumn(
        "ingestion_run_id", F.lit(replay_id)
    )
    replay_rows = incoming_rows.withColumn("ingested_at", F.current_timestamp()).withColumn(
        "ingestion_run_id", F.lit(replay_id)
    )
    merge_raw(spark, f"{prefix}_files", replay_files, keys)
    merge_raw(spark, f"{prefix}_rows", replay_rows, row_keys)
    replayed_rows = spark.table(f"{prefix}_rows").orderBy(*row_keys).collect()
    replayed_files = spark.table(f"{prefix}_files").orderBy(*keys).collect()
    if stored_rows != replayed_rows or stored_files != replayed_files:
        raise ValueError("Bronze changed during an identical replay.")


def ingest_bronze(spark, volume_root, dataset):
    from pyspark.sql import functions as F

    root = PurePosixPath(volume_root)
    if root.parts[:2] != ("/", "Volumes") or len(root.parts) != 5 or dataset not in ("fixture", "demo"):
        raise ValueError("Use /Volumes/<catalog>/<schema>/<volume> and fixture or demo.")
    catalog, schema = root.parts[2:4]
    namespace = ".".join("`" + name.replace("`", "``") + "`" for name in (catalog, schema))
    prefix = f"{namespace}.insurance_{dataset}_bronze"
    common = """dataset STRING, entity STRING, batch_id STRING, batch_as_of_utc TIMESTAMP,
        source_file STRING, source_sha256 STRING"""
    metadata = "ingested_at TIMESTAMP, ingestion_run_id STRING"
    file_schema = common + """, delivery_fingerprint STRING, source_content BINARY,
        input_row_count BIGINT, file_status STRING, error_codes ARRAY<STRING>,
        error_detail STRING, schema_warnings ARRAY<STRING>, """ + metadata
    row_schema = common + ", source_row_number BIGINT, raw_payload STRING, " + metadata
    run_schema = """ingestion_run_id STRING, dataset STRING, started_at TIMESTAMP,
        completed_at TIMESTAMP, status STRING, input_row_count BIGINT,
        inserted_row_count BIGINT, failed_file_count BIGINT, error_detail STRING"""
    for suffix, ddl in (("files", file_schema), ("rows", row_schema), ("runs", run_schema)):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {prefix}_{suffix} ({ddl}) USING DELTA")

    run_id, started = str(uuid4()), datetime.now(timezone.utc)
    files, rows = [], []
    before = spark.table(f"{prefix}_rows").count()
    status, error_detail = "FAILED", ""
    try:
        files, rows = prepare_sources(Path(volume_root) / dataset, dataset)
        for record in files + rows:
            record.update(ingested_at=started, ingestion_run_id=run_id)
        incoming_files = spark.createDataFrame(files, file_schema)
        incoming_rows = spark.createDataFrame(rows, row_schema)
        keys = ["dataset", "batch_id", "entity"]
        existing_files = spark.table(f"{prefix}_files")
        changed = incoming_files.alias("new").join(existing_files.alias("old"), keys).where(
            F.col("new.delivery_fingerprint") != F.col("old.delivery_fingerprint")
        ).select(*keys).limit(1).collect()
        if changed:
            raise ValueError(f"Changed immutable delivery: {changed[0].asDict()}. Use a new batch ID.")

        merge_raw(spark, f"{prefix}_files", incoming_files, keys)
        merge_raw(spark, f"{prefix}_rows", incoming_rows, keys + ["source_row_number"])

        verify_bronze(spark, prefix, files, rows, incoming_files, incoming_rows, keys)
        if any(file["file_status"] == "FAILED" for file in files):
            status = "COMPLETED_WITH_FILE_FAILURES"
        else:
            status = "SUCCESS"
    except Exception as error:
        error_detail = str(error)
        raise
    finally:
        run = {
            "ingestion_run_id": run_id, "dataset": dataset, "started_at": started,
            "completed_at": datetime.now(timezone.utc), "status": status,
            "input_row_count": len(rows),
            "inserted_row_count": spark.table(f"{prefix}_rows").count() - before,
            "failed_file_count": sum(file["file_status"] == "FAILED" for file in files),
            "error_detail": error_detail,
        }
        spark.createDataFrame([run], run_schema).write.mode("append").saveAsTable(f"{prefix}_runs")
    return prefix, run


# COMMAND ----------

# Runtime cells are guarded so the file parser can also be tested without Databricks.
if "dbutils" in globals():
    dbutils.widgets.text("volume_root", "/Volumes/insurance_project/default/insurance_sources")
    dbutils.widgets.dropdown("dataset", "fixture", ["fixture", "demo"])

# COMMAND ----------

if "dbutils" in globals():
    table_prefix, run = ingest_bronze(
        spark, dbutils.widgets.get("volume_root").strip(), dbutils.widgets.get("dataset")
    )
    print(f"{run['status']}: {run['input_row_count']} raw rows, {run['inserted_row_count']} newly stored.")
    display(spark.table(f"{table_prefix}_files").select(
        "entity", "batch_id", "input_row_count", "file_status", "error_codes", "schema_warnings"
    ).orderBy("entity", "batch_id"))
    display(spark.table(f"{table_prefix}_rows").select(
        "entity", "batch_id", "source_row_number", "raw_payload"
    ).orderBy("entity", "batch_id", "source_row_number").limit(20))
