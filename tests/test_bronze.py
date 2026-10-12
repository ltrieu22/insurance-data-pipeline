import hashlib
import json
import runpy
import tempfile
import unittest
from pathlib import Path

from insurance_demo.fixtures import build_fixture
from insurance_demo.generate import DemoConfig, build_demo
from insurance_demo.source_files import write_dataset


# Load the notebook's Python helpers; its Databricks runtime cells stay inactive.
ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = runpy.run_path(str(ROOT / "databricks" / "01_ingest_bronze.py"))
prepare_sources = NOTEBOOK["prepare_sources"]
parse_records = NOTEBOOK["parse_records"]


class BronzeSourceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.source_files = build_fixture()
        write_dataset(self.root, self.source_files)

    def change_source(self, path, content):
        """Change a test delivery and update its declared checksum."""
        (self.root / path).write_bytes(content)
        manifest = json.loads((self.root / "manifest.json").read_bytes())
        descriptions = list(manifest["baseline"]["files"].values())
        descriptions += [batch["file"] for batch in manifest["batches"]]
        for description in descriptions:
            if description["path"] == path:
                description["sha256"] = hashlib.sha256(content).hexdigest()
        (self.root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    def test_all_fixture_deliveries_retain_paths_hashes_and_raw_values(self):
        files, rows = prepare_sources(self.root, "fixture")
        self.assertEqual(len(files), 16)
        self.assertEqual(len(rows), 17)
        self.assertEqual(sum(row["entity"] == "payments" for row in rows), 15)
        for file in files:
            relative = Path(file["source_file"]).relative_to(self.root).as_posix()
            self.assertEqual(file["source_sha256"], hashlib.sha256(self.source_files[relative]).hexdigest())
            self.assertNotIn("source_content", file)
        policy = next(row for row in rows if row["entity"] == "policies")
        self.assertIsNone(json.loads(policy["raw_payload"])["end_date"])

    def test_bad_values_whitespace_and_duplicate_row_positions_survive(self):
        _, rows = prepare_sources(self.root, "fixture")
        batches = {}
        for row in rows:
            batches.setdefault(row["batch_id"], []).append(row)
        self.assertEqual(json.loads(batches["fixture_006_precision"][0]["raw_payload"])["amount"], "12.345")
        self.assertEqual(
            json.loads(batches["fixture_008_invalid_correction"][0]["raw_payload"])["amount"], "-10.00"
        )
        padded = json.loads(batches["fixture_013_normalized_duplicate"][0]["raw_payload"])
        self.assertEqual(padded["payment_id"], " PM000001 ")
        self.assertEqual(padded["amount"], " 120.0 ")
        duplicates = batches["fixture_002_duplicate"]
        self.assertEqual([row["source_row_number"] for row in duplicates], [1, 2])
        self.assertEqual(duplicates[0]["raw_payload"], duplicates[1]["raw_payload"])

    def test_only_the_missing_column_file_fails_bronze_contract_checks(self):
        files, rows = prepare_sources(self.root, "fixture")
        failed = [file for file in files if file["file_status"] == "FAILED"]
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0]["batch_id"], "fixture_011_missing_column")
        self.assertEqual(failed[0]["error_codes"], ["MISSING_REQUIRED_COLUMN"])
        self.assertIn("payment_id", failed[0]["error_detail"])
        self.assertTrue(any(row["batch_id"] == failed[0]["batch_id"] for row in rows))
        empty = next(file for file in files if file["batch_id"] == "fixture_012_empty")
        self.assertEqual(empty["file_status"], "READY")
        self.assertEqual(empty["input_row_count"], 0)

    def test_expected_outcomes_are_never_read(self):
        (self.root / "expected_outcomes.json").write_text("not valid JSON", encoding="utf-8")
        self.assertEqual(len(prepare_sources(self.root, "fixture")[1]), 17)

    def test_identical_preparation_is_stable_and_changed_delivery_is_blocked(self):
        original, original_rows = prepare_sources(self.root, "fixture")
        self.assertEqual((original, original_rows), prepare_sources(self.root, "fixture"))
        NOTEBOOK["check_immutable_files"](original, original)
        path = "batches/fixture_001_initial/payments.csv"
        (self.root / path).write_bytes(self.source_files[path].replace(b"100.00", b"101.00"))
        changed, _ = prepare_sources(self.root, "fixture")
        old = next(file for file in original if file["batch_id"] == "fixture_001_initial")
        new = next(file for file in changed if file["batch_id"] == "fixture_001_initial")
        with self.assertRaisesRegex(ValueError, "Changed immutable delivery"):
            NOTEBOOK["check_immutable_files"]([new], [old])
        self.assertEqual(new["error_codes"], ["CHECKSUM_MISMATCH"])

    def test_changing_batch_time_is_blocked(self):
        files, _ = prepare_sources(self.root, "fixture")
        manifest = json.loads((self.root / "manifest.json").read_bytes())
        manifest["batches"][0]["batch_as_of_utc"] = "2026-09-03T00:00:00Z"
        (self.root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        changed, _ = prepare_sources(self.root, "fixture")
        with self.assertRaisesRegex(ValueError, "Changed immutable delivery"):
            NOTEBOOK["check_immutable_files"](changed, files)

    def test_additional_columns_are_retained(self):
        path = "batches/fixture_001_initial/payments.csv"
        lines = self.source_files[path].decode("utf-8").splitlines()
        content = (lines[0] + ",extra_note\n" + lines[1] + ", keep this \n").encode("utf-8")
        self.change_source(path, content)
        files, rows = prepare_sources(self.root, "fixture")
        file = next(file for file in files if file["batch_id"] == "fixture_001_initial")
        row = next(row for row in rows if row["batch_id"] == "fixture_001_initial")
        self.assertEqual(file["file_status"], "READY")
        self.assertEqual(json.loads(row["raw_payload"])["extra_note"], " keep this ")

    def test_unparseable_file_is_flagged_and_other_deliveries_continue(self):
        path = "batches/fixture_001_initial/payments.csv"
        content = b'payment_id,amount\n"unterminated\n'
        self.change_source(path, content)
        files, rows = prepare_sources(self.root, "fixture")
        file = next(file for file in files if file["batch_id"] == "fixture_001_initial")
        self.assertEqual(file["error_codes"], ["UNPARSEABLE_FILE"])
        self.assertEqual((self.root / path).read_bytes(), content)
        self.assertEqual(file["file_status"], "FAILED")
        self.assertTrue(any(row["batch_id"] == "fixture_014_recovered" for row in rows))

    def test_csv_structure_errors_fail_instead_of_dropping_values(self):
        for content in (b"id,id\na,b\n", b"id,amount\na,1,extra\n", b"id,amount\na\n"):
            with self.subTest(content=content), self.assertRaises(ValueError):
                parse_records(content, "payments")

    def test_repeated_json_fields_fail_and_leave_the_volume_file_intact(self):
        path = "reference/policies.json"
        content = b'[{"policy_id": "P000001", "policy_id": "P000002"}]\n'
        self.change_source(path, content)
        files, rows = prepare_sources(self.root, "fixture")
        file = next(file for file in files if file["entity"] == "policies")
        self.assertEqual(file["file_status"], "FAILED")
        self.assertEqual(file["error_codes"], ["UNPARSEABLE_FILE"])
        self.assertIn("Repeated JSON field name: policy_id", file["error_detail"])
        self.assertEqual((self.root / path).read_bytes(), content)
        self.assertFalse(any(row["entity"] == "policies" for row in rows))

    def test_manifest_dataset_duplicate_batches_and_unsafe_paths_are_rejected(self):
        original = json.loads((self.root / "manifest.json").read_bytes())
        mutations = (
            lambda manifest: manifest.update(dataset="insurance_premium_demo"),
            lambda manifest: manifest["batches"].append(manifest["batches"][0]),
            lambda manifest: manifest["batches"][0]["file"].update(path="../outside.csv"),
        )
        for mutate in mutations:
            manifest = json.loads(json.dumps(original))
            mutate(manifest)
            (self.root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                prepare_sources(self.root, "fixture")

    def test_default_demo_keeps_all_raw_updates(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_dataset(root, build_demo(DemoConfig()))
            files, rows = prepare_sources(root, "demo")
        self.assertTrue(all(file["file_status"] == "READY" for file in files))
        updates = [row for row in rows if row["batch_id"] == "demo_002_updates"]
        self.assertEqual(len(updates), 349)
        payment_ids = [json.loads(row["raw_payload"])["payment_id"] for row in updates]
        self.assertEqual(sum(not payment_id.startswith("PM") for payment_id in payment_ids), 14)


if __name__ == "__main__":
    unittest.main()
