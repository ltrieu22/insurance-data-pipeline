"""Write source files, manifests, and ZIP archives."""

import csv
import hashlib
import io
import json
from pathlib import Path
from zipfile import ZIP_STORED, ZipFile, ZipInfo


CUSTOMER_COLUMNS = ("customer_id", "region", "customer_segment")
PAYMENT_COLUMNS = (
    "payment_id", "policy_id", "payment_date", "amount", "currency", "source_updated_at"
)


def csv_bytes(rows: list[dict], columns: tuple[str, ...]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def file_description(path: str, content: bytes, row_count: int) -> dict:
    return {
        "path": path,
        "row_count": row_count,
        "sha256": hashlib.sha256(content).hexdigest(),
    }


def reference_exports(customers: list[dict], policies: list[dict]) -> tuple[dict, dict]:
    files = {
        "reference/customers.csv": csv_bytes(customers, CUSTOMER_COLUMNS),
        "reference/policies.json": json_bytes(policies),
    }
    baseline = {
        "batch_id": "reference_v1",
        "batch_as_of_utc": "2026-09-02T00:00:00Z",
        "files": {
            "customers": file_description(
                "reference/customers.csv", files["reference/customers.csv"], len(customers)
            ),
            "policies": file_description(
                "reference/policies.json", files["reference/policies.json"], len(policies)
            ),
        },
    }
    return files, baseline


def add_payment_batch(
    files: dict[str, bytes],
    batches: list[dict],
    batch_id: str,
    as_of: str,
    rows: list[dict],
    description: str,
    columns: tuple[str, ...] = PAYMENT_COLUMNS,
) -> None:
    path = f"batches/{batch_id}/payments.csv"
    files[path] = csv_bytes(rows, columns)
    batches.append({
        "batch_id": batch_id,
        "batch_as_of_utc": as_of,
        "description": description,
        "file": file_description(path, files[path], len(rows)),
    })


def add_archive(files: dict[str, bytes], name: str) -> None:
    """Use fixed ZIP metadata so repeated runs produce identical archives."""
    buffer = io.BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_STORED) as archive:
        for path, content in sorted(files.items()):
            entry = ZipInfo(path, date_time=(2026, 1, 1, 0, 0, 0))
            entry.create_system = 3
            entry.external_attr = 0o644 << 16
            archive.writestr(entry, content)
    files[name] = buffer.getvalue()


def write_dataset(destination: Path, files: dict[str, bytes]) -> int:
    """Check for conflicts before writing; leave identical files unchanged."""
    if destination.exists() and not destination.is_dir():
        raise FileExistsError(f"Output must be a directory: {destination}")
    for relative_path, content in files.items():
        target = destination / relative_path
        parent = target.parent
        while parent != destination:
            if parent.exists() and not parent.is_dir():
                raise FileExistsError(f"Expected a directory: {parent}")
            parent = parent.parent
        if target.exists() and (not target.is_file() or target.read_bytes() != content):
            raise FileExistsError(
                f"Refusing to change existing delivery: {target}. "
                "Use another --output directory for different data."
            )
    destination.mkdir(parents=True, exist_ok=True)
    created = 0
    for relative_path, content in files.items():
        target = destination / relative_path
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("xb") as output:
                output.write(content)
            created += 1
    return created
