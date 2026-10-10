import csv
import hashlib
import io
import json
import random
import tempfile
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from zipfile import ZipFile

from insurance_demo.fixtures import build_fixture
from insurance_demo.generate import DemoConfig, build_demo
from insurance_demo.source_files import write_dataset


def csv_rows(content: bytes) -> list[dict]:
    return list(csv.DictReader(io.StringIO(content.decode("utf-8"))))


class SourceGenerationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = DemoConfig(seed=42, customers=12, policies=20, payments=150)
        cls.files = build_demo(cls.config)
        cls.manifest = json.loads(cls.files["manifest.json"])
        cls.expected = json.loads(cls.files["expected_outcomes.json"])

    def test_same_seed_produces_identical_files_including_archive(self) -> None:
        self.assertEqual(self.files, build_demo(self.config))

    def test_different_seed_changes_data(self) -> None:
        different = build_demo(DemoConfig(seed=43, customers=12, policies=20, payments=150))
        self.assertNotEqual(self.files["reference/customers.csv"], different["reference/customers.csv"])
        initial_path = self.manifest["batches"][0]["file"]["path"]
        self.assertNotEqual(self.files[initial_path], different[initial_path])

    def test_generation_does_not_change_global_random_state(self) -> None:
        state = random.getstate()
        build_demo(self.config)
        self.assertEqual(state, random.getstate())

    def test_manifest_checksums_and_row_counts_match_both_datasets(self) -> None:
        for files in (self.files, build_fixture()):
            manifest = json.loads(files["manifest.json"])
            self.assertTrue(manifest["synthetic"])
            descriptors = list(manifest["baseline"]["files"].values())
            descriptors += [batch["file"] for batch in manifest["batches"]]
            for description in descriptors:
                with self.subTest(dataset=manifest["dataset"], path=description["path"]):
                    content = files[description["path"]]
                    self.assertEqual(hashlib.sha256(content).hexdigest(), description["sha256"])
                    rows = json.loads(content) if description["path"].endswith(".json") else csv_rows(content)
                    self.assertEqual(len(rows), description["row_count"])

    def test_baseline_has_unique_keys_and_valid_relationships(self) -> None:
        customers = csv_rows(self.files["reference/customers.csv"])
        policies = json.loads(self.files["reference/policies.json"])
        customer_ids = {row["customer_id"] for row in customers}
        self.assertEqual(len(customers), 12)
        self.assertEqual(len(customer_ids), 12)
        self.assertEqual(len(policies), 20)
        self.assertEqual(len({row["policy_id"] for row in policies}), 20)
        for customer in customers:
            self.assertIn(customer["region"], {"Uusimaa", "North Ostrobothnia", "Pirkanmaa", "Other"})
            self.assertIn(customer["customer_segment"], {"STANDARD", "PRIORITY"})
        for policy in policies:
            self.assertIn(policy["customer_id"], customer_ids)
            self.assertIn(policy["product_code"], {"LIFE_BASIC", "LIFE_PLUS"})
            start = date.fromisoformat(policy["start_date"])
            if policy["end_date"] is not None:
                self.assertGreaterEqual(date.fromisoformat(policy["end_date"]), start)

    def test_clean_payment_rows_meet_the_source_contract(self) -> None:
        policy_ids = {row["policy_id"] for row in json.loads(self.files["reference/policies.json"])}
        bad_ids = {row["payment_id"] for row in self.expected["intentional_invalid_rows_in_updates"]}
        initial = self.manifest["batches"][0]
        initial_rows = csv_rows(self.files[initial["file"]["path"]])
        self.assertEqual(len({row["payment_id"] for row in initial_rows}), self.config.payments)
        for batch in self.manifest["batches"]:
            cutoff = datetime.fromisoformat(batch["batch_as_of_utc"].replace("Z", "+00:00"))
            for row in csv_rows(self.files[batch["file"]["path"]]):
                if row["payment_id"] in bad_ids:
                    self.assertEqual(batch["batch_id"], "demo_002_updates")
                    continue
                self.assertTrue(all(row.values()))
                self.assertIn(row["policy_id"], policy_ids)
                self.assertEqual(row["currency"], "EUR")
                amount = Decimal(row["amount"])
                self.assertTrue(amount.is_finite())
                self.assertGreater(amount, 0)
                self.assertLess(amount, Decimal("10000000000"))
                self.assertGreaterEqual(amount.as_tuple().exponent, -2)
                received = date.fromisoformat(row["payment_date"])
                updated = datetime.fromisoformat(row["source_updated_at"].replace("Z", "+00:00"))
                self.assertLessEqual(received, cutoff.date())
                self.assertLessEqual(updated, cutoff)
                self.assertGreaterEqual(updated.date(), received)

    def test_demo_expectations_follow_the_planned_changes(self) -> None:
        initial = csv_rows(self.files[self.manifest["batches"][0]["file"]["path"]])
        updates = csv_rows(self.files[self.manifest["batches"][1]["file"]["path"]])
        initial_ids = {row["payment_id"] for row in initial}
        new = {
            row["payment_id"]: row for row in updates
            if row["payment_id"].startswith("PM") and row["payment_id"] not in initial_ids
        }
        initial_total = sum((Decimal(row["amount"]) for row in initial), Decimal("0.00"))
        # Each of the first 100 payments increases by EUR 25, plus new payments.
        final_total = initial_total + Decimal("25.00") * 100
        final_total += sum((Decimal(row["amount"]) for row in new.values()), Decimal("0.00"))
        expected = self.expected["after_batches"]
        self.assertEqual(expected["demo_001_initial"]["total_amount_eur"], f"{initial_total:.2f}")
        self.assertEqual(expected["demo_002_updates"]["total_amount_eur"], f"{final_total:.2f}")
        self.assertEqual(expected["demo_002_updates"]["current_payment_count"], 153)
        self.assertEqual(expected["demo_002_updates"]["rejected_row_count_in_this_batch"], 14)
        self.assertEqual(len([row for row in updates if not row["payment_id"].startswith("PM")]), 14)
        self.assertEqual(
            expected["demo_002_updates"]["total_amount_eur"],
            expected["demo_003_stale_and_duplicates"]["total_amount_eur"],
        )
        for state in expected.values():
            monthly_total = sum(map(Decimal, state["amount_by_month_eur"].values()), Decimal("0.00"))
            self.assertEqual(monthly_total, Decimal(state["total_amount_eur"]))

    def test_smallest_supported_dataset(self) -> None:
        files = build_demo(DemoConfig(customers=1, policies=1, payments=1))
        expected = json.loads(files["expected_outcomes.json"])
        self.assertEqual(expected["after_batches"]["demo_001_initial"]["current_payment_count"], 1)
        self.assertEqual(expected["after_batches"]["demo_002_updates"]["current_payment_count"], 2)

    def test_invalid_sizes_are_rejected(self) -> None:
        for field in ("customers", "policies", "payments"):
            for value in (0, -1):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(ValueError):
                        DemoConfig(**{field: value})

    def test_archives_contain_the_exact_source_files(self) -> None:
        for files, archive_name in ((self.files, "demo_sources.zip"), (build_fixture(), "fixture_sources.zip")):
            with ZipFile(io.BytesIO(files[archive_name])) as archive:
                self.assertEqual(set(archive.namelist()), set(files) - {archive_name})
                for path in archive.namelist():
                    self.assertNotIn("..", Path(path).parts)
                    self.assertFalse(Path(path).is_absolute())
                    self.assertEqual(archive.read(path), files[path])

    def test_identical_regeneration_does_not_rewrite_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "delivery"
            self.assertEqual(write_dataset(output, self.files), len(self.files))
            modification_times = {path: (output / path).stat().st_mtime_ns for path in self.files}
            self.assertEqual(write_dataset(output, self.files), 0)
            self.assertEqual(modification_times, {
                path: (output / path).stat().st_mtime_ns for path in self.files
            })

    def test_changed_delivery_fails_before_any_files_are_written(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            original = {"existing.csv": b"original\n"}
            write_dataset(output, original)
            # The new file appears first deliberately: preflight must detect
            # the later conflict before creating anything.
            changed = {"new.csv": b"new\n", "existing.csv": b"changed\n"}
            with self.assertRaises(FileExistsError):
                write_dataset(output, changed)
            self.assertFalse((output / "new.csv").exists())
            self.assertEqual((output / "existing.csv").read_bytes(), b"original\n")

    def test_non_directory_parent_is_detected_before_writing(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            (output / "blocked").write_text("keep me", encoding="utf-8")
            with self.assertRaises(FileExistsError):
                write_dataset(output, {"new.csv": b"new\n", "blocked/input.csv": b"data\n"})
            self.assertFalse((output / "new.csv").exists())
            self.assertEqual((output / "blocked").read_text(encoding="utf-8"), "keep me")


class FixtureTests(unittest.TestCase):
    def test_fixture_covers_the_agreed_scenarios_with_hand_written_totals(self) -> None:
        files = build_fixture()
        manifest = json.loads(files["manifest.json"])
        expected = json.loads(files["expected_outcomes.json"])["after_batches"]
        self.assertEqual(len(manifest["batches"]), 14)
        self.assertEqual([expected[batch["batch_id"]]["total_amount_eur"] for batch in manifest["batches"]], [
            "100.00", "100.00", "120.00", "120.00", "120.00", "120.00",
            "170.00", "170.00", "170.00", "170.00", "170.00", "170.00", "170.00", "230.00",
        ])
        failed = {batch_id for batch_id, state in expected.items() if state["status"] == "FAILED"}
        self.assertEqual(failed, {
            "fixture_009_conflict_current", "fixture_010_conflict_history", "fixture_011_missing_column"
        })
        late_state = expected["fixture_007_late"]
        self.assertEqual(late_state["amount_by_month_eur"], {"2026-08": "50.00", "2026-09": "120.00"})
        for state in expected.values():
            self.assertEqual(
                sum(map(Decimal, state["amount_by_month_eur"].values()), Decimal("0.00")),
                Decimal(state["total_amount_eur"]),
            )

    def test_fixture_preserves_intentional_bad_files_and_rows(self) -> None:
        files = build_fixture()
        precision = csv_rows(files["batches/fixture_006_precision/payments.csv"])
        self.assertEqual(precision[0]["amount"], "12.345")
        missing = csv_rows(files["batches/fixture_011_missing_column/payments.csv"])
        self.assertNotIn("payment_id", missing[0])
        self.assertEqual(csv_rows(files["batches/fixture_012_empty/payments.csv"]), [])
        conflict = csv_rows(files["batches/fixture_009_conflict_current/payments.csv"])
        self.assertEqual({row["payment_id"] for row in conflict}, {"PM000001", "PM000003"})
        normalized = csv_rows(files["batches/fixture_013_normalized_duplicate/payments.csv"])[0]
        self.assertEqual(normalized["payment_id"], " PM000001 ")
        self.assertEqual(normalized["amount"], " 120.0 ")

    def test_checked_in_fixture_matches_its_definition(self) -> None:
        root = Path(__file__).resolve().parents[1] / "data" / "fixtures"
        for path, content in build_fixture().items():
            if path.endswith(".zip"):
                continue  # The archive is reproducible and need not be versioned.
            with self.subTest(path=path):
                self.assertEqual((root / path).read_bytes(), content)


if __name__ == "__main__":
    unittest.main()
