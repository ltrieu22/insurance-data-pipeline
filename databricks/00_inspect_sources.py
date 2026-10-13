# Databricks notebook source
# MAGIC %md
# MAGIC # Source file inspection
# MAGIC Checks and extracts source archives in a Unity Catalog volume.

# COMMAND ----------

dbutils.widgets.text("volume_root", "", "Unity Catalog volume path")
dbutils.widgets.dropdown("dataset", "fixture", ["fixture", "demo"])

# COMMAND ----------

import csv
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
from zipfile import ZipFile

volume_root = Path(dbutils.widgets.get("volume_root").strip())
dataset = dbutils.widgets.get("dataset")
if not str(volume_root).startswith("/Volumes/") or len(volume_root.parts) < 5:
    raise ValueError("Enter an existing /Volumes/<catalog>/<schema>/<volume> path.")

archive_path = volume_root / f"{dataset}_sources.zip"
sources_root = volume_root / dataset
with ZipFile(archive_path) as archive:
    names = archive.namelist()
    if len(names) != len(set(names)):
        raise ValueError("The archive contains duplicate file paths.")
    contents = {}
    for name in names:
        relative = PurePosixPath(name)
        if relative.is_absolute() or ".." in relative.parts or "\\" in name:
            raise ValueError(f"Unsafe archive path: {name}")
        target = (sources_root / name).resolve()
        if not target.is_relative_to(sources_root.resolve()):
            raise ValueError(f"Archive path escapes its destination: {name}")
        contents[name] = archive.read(name)

manifest = json.loads(contents["manifest.json"])
if manifest.get("synthetic") is not True or manifest.get("schema_version") != 1:
    raise ValueError("Expected a version 1 synthetic source manifest.")
if manifest.get("dataset") != f"insurance_premium_{dataset}":
    raise ValueError("The selected dataset does not match the uploaded archive.")

# Check all source files before writing to the volume.
descriptions = list(manifest["baseline"]["files"].values())
descriptions += [batch["file"] for batch in manifest["batches"]]
for description in descriptions:
    path = description["path"]
    content = contents[path]
    if hashlib.sha256(content).hexdigest() != description["sha256"]:
        raise ValueError(f"Checksum mismatch: {path}")
    if path.endswith(".json"):
        rows = json.loads(content)
    else:
        rows = list(csv.DictReader(io.StringIO(content.decode("utf-8"))))
    if len(rows) != description["row_count"]:
        raise ValueError(f"Row count mismatch: {path}")

# Check for changed files before extracting any of them.
for name, content in contents.items():
    target = sources_root / name
    if target.exists() and (not target.is_file() or target.read_bytes() != content):
        raise FileExistsError(f"Refusing to replace different source contents: {target}")
for name, content in contents.items():
    target = sources_root / name
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        with target.open("xb") as output:
            output.write(content)

print(f"Sources: {sources_root}")

# COMMAND ----------

# Keep raw values as strings until the validation stage.
customers_path = sources_root / manifest["baseline"]["files"]["customers"]["path"]
policies_path = sources_root / manifest["baseline"]["files"]["policies"]["path"]
payments_path = sources_root / manifest["batches"][0]["file"]["path"]

customers = spark.read.option("header", True).schema(
    "customer_id STRING, region STRING, customer_segment STRING"
).csv(str(customers_path))
policies = spark.read.option("multiLine", True).schema(
    "policy_id STRING, customer_id STRING, product_code STRING, "
    "start_date STRING, end_date STRING"
).json(str(policies_path))
payments = spark.read.option("header", True).schema(
    "payment_id STRING, policy_id STRING, payment_date STRING, "
    "amount STRING, currency STRING, source_updated_at STRING"
).csv(str(payments_path))

display(customers.limit(5))
display(policies.limit(5))
display(payments.limit(5))
