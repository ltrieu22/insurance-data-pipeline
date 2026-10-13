"""Check reporting SQL locally; Spark/Delta execution is verified in Databricks."""

import json
import runpy
import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from insurance_demo.fixtures import build_fixture
from insurance_demo.source_files import write_dataset


ROOT = Path(__file__).resolve().parents[1]
GOLD = runpy.run_path(str(ROOT / "databricks" / "04_build_gold.py"))


def truncate_month(value, unit):
    if unit != "MONTH":
        raise ValueError("The reporting query must truncate to a month.")
    return date.fromisoformat(value).replace(day=1).isoformat()


class ExactSum:
    """Let SQLite sum decimal strings without converting money to floats."""

    def __init__(self):
        self.total = Decimal("0.00")
        self.integers_only = True

    def step(self, value):
        if value is not None:
            self.total += Decimal(str(value))
            self.integers_only = self.integers_only and isinstance(value, int)

    def finalize(self):
        return int(self.total) if self.integers_only else format(self.total, ".2f")


class GoldReportingTests(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.addCleanup(self.connection.close)
        self.connection.row_factory = sqlite3.Row
        self.connection.create_function("TRUNC", 2, truncate_month)
        self.connection.create_aggregate("SUM", 1, ExactSum)
        self.connection.executescript("""
            CREATE TABLE silver_customers (
                customer_id TEXT, region TEXT, customer_segment TEXT
            );
            CREATE TABLE silver_policies (
                policy_id TEXT, customer_id TEXT, product_code TEXT
            );
            CREATE TABLE silver_payments (
                payment_id TEXT, policy_id TEXT, payment_date TEXT,
                amount TEXT, source_updated_at TEXT
            );
        """)

    def load_rows(self, customers, policies, payments):
        self.connection.executemany(
            "INSERT INTO silver_customers VALUES (?, ?, ?)", customers
        )
        self.connection.executemany(
            "INSERT INTO silver_policies VALUES (?, ?, ?)", policies
        )
        self.connection.executemany(
            "INSERT INTO silver_payments VALUES (?, ?, ?, ?, ?)", payments
        )

    def reporting_rows(self):
        query = GOLD["MONTHLY_PREMIUMS_SQL"].format(silver="silver")
        records = self.connection.execute(query).fetchall()
        return sorted(
            tuple(row)[:-1] + (Decimal(row["total_premium_eur"]),) for row in records
        )

    def reconciliation(self):
        query = GOLD["MONTHLY_PREMIUMS_SQL"].format(silver="silver")
        self.connection.execute(f"CREATE TEMP VIEW gold AS {query}")
        return self.connection.execute(
            GOLD["RECONCILIATION_SQL"].format(silver="silver", gold="gold")
        ).fetchone()

    def test_fixture_uses_current_payments_and_payment_month(self):
        bronze = runpy.run_path(str(ROOT / "databricks" / "01_ingest_bronze.py"))
        silver = runpy.run_path(str(ROOT / "databricks" / "02_transform_silver.py"))
        exported = build_fixture()
        manifest = json.loads(exported["manifest.json"])
        with tempfile.TemporaryDirectory() as temporary:
            sources = Path(temporary).resolve()
            write_dataset(sources, exported)
            files, rows = bronze["prepare_sources"](sources, "fixture")
        for row in rows:
            row["ingested_at"] = datetime(2026, 10, 1, tzinfo=timezone.utc)
        references = silver["reference_plan"]([row for row in rows if row["entity"] != "payments"])
        policies = {row["policy_id"] for row in references["policies"]}
        payments, _, _ = silver["payment_sequence"](files, rows, manifest["batches"], policies)
        self.load_rows(
            [(row["customer_id"], row["region"], row["customer_segment"]) for row in references["customers"]],
            [(row["policy_id"], row["customer_id"], row["product_code"]) for row in references["policies"]],
            [(row["payment_id"], row["policy_id"], str(row["payment_date"]), str(row["amount"]),
              row["source_updated_at"].isoformat()) for row in payments],
        )
        self.assertEqual(self.reporting_rows(), [
            ("2026-08-01", "Uusimaa", "STANDARD", "LIFE_BASIC", 1, Decimal("50.00")),
            ("2026-09-01", "Uusimaa", "STANDARD", "LIFE_BASIC", 2, Decimal("180.00")),
        ])
        totals = self.reconciliation()
        self.assertEqual(totals["silver_payment_count"], totals["gold_payment_count"])
        self.assertEqual(totals["silver_total_eur"], totals["gold_total_eur"])

    def test_all_dimensions_and_years_form_separate_groups_with_exact_amounts(self):
        self.load_rows(
            [("C1", "Uusimaa", "STANDARD"), ("C2", "Other", "STANDARD"),
             ("C3", "Uusimaa", "PRIORITY")],
            [("P1", "C1", "LIFE_BASIC"), ("P2", "C1", "LIFE_PLUS"),
             ("P3", "C2", "LIFE_BASIC"), ("P4", "C3", "LIFE_BASIC")],
            [("PM1", "P1", "2026-08-15", "30.10", "2026-09-01T12:00:00Z"),
             ("PM2", "P1", "2026-08-31", "20.20", "2026-09-01T12:00:00Z"),
             ("PM3", "P1", "2026-09-01", "120.00", "2026-09-01T12:00:00Z"),
             ("PM4", "P2", "2026-08-01", "10.00", "2026-09-01T12:00:00Z"),
             ("PM5", "P3", "2026-08-01", "10.00", "2026-09-01T12:00:00Z"),
             ("PM6", "P4", "2026-08-01", "10.00", "2026-09-01T12:00:00Z"),
             ("PM7", "P1", "2025-08-01", "10.00", "2026-09-01T12:00:00Z")],
        )
        self.assertEqual(self.reporting_rows(), [
            ("2025-08-01", "Uusimaa", "STANDARD", "LIFE_BASIC", 1, Decimal("10.00")),
            ("2026-08-01", "Other", "STANDARD", "LIFE_BASIC", 1, Decimal("10.00")),
            ("2026-08-01", "Uusimaa", "PRIORITY", "LIFE_BASIC", 1, Decimal("10.00")),
            ("2026-08-01", "Uusimaa", "STANDARD", "LIFE_BASIC", 2, Decimal("50.30")),
            ("2026-08-01", "Uusimaa", "STANDARD", "LIFE_PLUS", 1, Decimal("10.00")),
            ("2026-09-01", "Uusimaa", "STANDARD", "LIFE_BASIC", 1, Decimal("120.00")),
        ])

    def test_changed_current_payment_moves_out_of_its_old_group(self):
        self.load_rows(
            [("C1", "Uusimaa", "STANDARD")], [("P1", "C1", "LIFE_BASIC")],
            [("PM1", "P1", "2026-08-15", "100.00", "2026-09-01T12:00:00Z")],
        )
        self.assertEqual(self.reporting_rows()[0][0], "2026-08-01")
        self.connection.execute("""
            UPDATE silver_payments SET payment_date = '2026-09-01', amount = '120.00'
            WHERE payment_id = 'PM1'
        """)
        self.assertEqual(self.reporting_rows(), [
            ("2026-09-01", "Uusimaa", "STANDARD", "LIFE_BASIC", 1, Decimal("120.00"))
        ])

    def test_empty_input_has_no_groups_and_reconciles_to_zero(self):
        self.assertEqual(self.reporting_rows(), [])
        self.assertEqual(tuple(self.reconciliation()), (0, 0, 0, 0))

    def test_reconciliation_exposes_an_unmatched_payment(self):
        self.load_rows([], [], [("PM1", "P_MISSING", "2026-08-15", "50.00", "2026-09-01T12:00:00Z")])
        totals = self.reconciliation()
        self.assertNotEqual(totals["silver_payment_count"], totals["gold_payment_count"])
        self.assertNotEqual(totals["silver_total_eur"], totals["gold_total_eur"])


if __name__ == "__main__":
    unittest.main()
