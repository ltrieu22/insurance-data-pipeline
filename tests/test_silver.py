import copy
import json
import runpy
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from insurance_demo.fixtures import build_fixture
from insurance_demo.generate import DemoConfig, build_demo, payment
from insurance_demo.source_files import write_dataset


ROOT = Path(__file__).resolve().parents[1]
BRONZE = runpy.run_path(str(ROOT / "databricks" / "01_ingest_bronze.py"))
SILVER = runpy.run_path(str(ROOT / "databricks" / "02_transform_silver.py"))
reference_plan = SILVER["reference_plan"]
payment_plan = SILVER["payment_plan"]
validate_row = SILVER["validate_row"]
current_payments = SILVER["current_payments"]


def raw_row(values, entity="payments", number=1, cutoff="2026-09-04T00:00:00Z"):
    return {
        "dataset": "fixture", "entity": entity, "batch_id": "test_delivery",
        "batch_as_of_utc": SILVER["parse_timestamp"](cutoff),
        "source_file": "/test/input.csv", "source_sha256": "test_checksum",
        "source_row_number": number, "raw_payload": json.dumps(values),
        "ingested_at": SILVER["parse_timestamp"](cutoff), "ingestion_run_id": "bronze_test_run",
    }


CUSTOMER = {"customer_id": "C000001", "region": "Uusimaa", "customer_segment": "STANDARD"}
POLICY = {
    "policy_id": "P000001", "customer_id": "C000001", "product_code": "LIFE_BASIC",
    "start_date": "2026-01-01", "end_date": None,
}
PAYMENT = payment("PM000001", "P000001", "2026-09-01", "100.00", "2026-09-01T12:00:00Z")


class SilverValidationTests(unittest.TestCase):
    def test_trim_strings_preserve_id_case_leading_zeros_and_original_payload(self):
        values = {key: f" {value} " for key, value in PAYMENT.items()}
        values.update(payment_id=" 00pM001 ", policy_id=" 000p01 ", amount=" 120.0 ")
        raw = raw_row(values)
        normalized, reasons = validate_row(raw, "payments", {"000p01"})
        self.assertEqual(reasons, [])
        self.assertEqual(normalized["payment_id"], "00pM001")
        self.assertEqual(normalized["policy_id"], "000p01")
        self.assertEqual(normalized["amount"], Decimal("120.00"))
        self.assertEqual(json.loads(raw["raw_payload"]), values)

    def test_amount_format_range_and_positivity(self):
        cases = {
            "12.345": "INVALID_AMOUNT_FORMAT", "12.000": "INVALID_AMOUNT_FORMAT",
            "10000000000.00": "INVALID_AMOUNT_FORMAT", "1e2": "INVALID_AMOUNT_FORMAT",
            "NaN": "INVALID_AMOUNT_FORMAT", "Infinity": "INVALID_AMOUNT_FORMAT",
            "1,200.00": "INVALID_AMOUNT_FORMAT", "EUR 10.00": "INVALID_AMOUNT_FORMAT",
            "0.00": "NON_POSITIVE_AMOUNT", "-0.01": "NON_POSITIVE_AMOUNT",
        }
        for amount, reason in cases.items():
            with self.subTest(amount=amount):
                _, reasons = validate_row(raw_row({**PAYMENT, "amount": amount}), "payments", {"P000001"})
                self.assertEqual(reasons, [reason])
        for amount in ("0.01", "9999999999.99", "00012.0", "12"):
            with self.subTest(amount=amount):
                values, reasons = validate_row(raw_row({**PAYMENT, "amount": amount}), "payments", {"P000001"})
                self.assertEqual(reasons, [])
                self.assertEqual(values["amount"].as_tuple().exponent, -2)

    def test_declared_time_boundaries_and_strict_formats(self):
        cases = (
            ({"payment_date": "2026-09-04", "source_updated_at": "2026-09-04T00:00:00Z"}, []),
            ({"source_updated_at": "2026-09-04T00:00:01Z"}, ["FUTURE_PAYMENT_OR_UPDATE"]),
            ({"payment_date": "2026-09-05"}, ["FUTURE_PAYMENT_OR_UPDATE", "UPDATE_BEFORE_PAYMENT"]),
            ({"source_updated_at": "2026-08-31T23:59:59Z"}, ["UPDATE_BEFORE_PAYMENT"]),
            ({"payment_date": "2026-02-30"}, ["INVALID_DATE_OR_TIMESTAMP"]),
            ({"payment_date": "2026-9-01"}, ["INVALID_DATE_OR_TIMESTAMP"]),
            ({"source_updated_at": "2026-09-01T12:00:00+00:00"}, ["INVALID_DATE_OR_TIMESTAMP"]),
            ({"currency": "eur"}, ["UNSUPPORTED_CURRENCY"]),
        )
        for changes, expected in cases:
            with self.subTest(changes=changes):
                _, reasons = validate_row(raw_row({**PAYMENT, **changes}), "payments", {"P000001"})
                self.assertEqual(reasons, expected)
        _, reasons = validate_row(raw_row(PAYMENT, cutoff="2020-01-01T00:00:00Z"), "payments", {"P000001"})
        self.assertIn("FUTURE_PAYMENT_OR_UPDATE", reasons)

    def test_rejection_counts_once_with_all_reasons_and_provenance(self):
        raw = raw_row({**PAYMENT, "amount": "-10.00", "policy_id": "P_MISSING", "currency": "USD"}, number=7)
        result = payment_plan([raw], {"P000001"}, [])
        self.assertEqual(result["rejected_row_count"], 1)
        rejected = result["rejections"][0]
        self.assertEqual(rejected["reason_codes"], ["NON_POSITIVE_AMOUNT", "UNKNOWN_POLICY", "UNSUPPORTED_CURRENCY"])
        for field in SILVER["PROVENANCE"]:
            self.assertEqual(rejected[field], raw[field])

    def test_null_blank_and_non_string_ids_are_rejected_without_coercion(self):
        for identifier in (None, "", "  ", 123, [], {}):
            with self.subTest(identifier=identifier):
                result = payment_plan([raw_row({**PAYMENT, "payment_id": identifier})], {"P000001"}, [])
                self.assertEqual(result["rejected_row_count"], 1)
                self.assertEqual(result["versions"], [])

    def test_only_accepted_parents_can_satisfy_relationships(self):
        rows = [
            raw_row({**CUSTOMER, "region": "unknown"}, "customers"),
            raw_row(POLICY, "policies"),
        ]
        baseline = reference_plan(rows)
        self.assertEqual(baseline["customers"], [])
        self.assertEqual(baseline["policies"], [])
        self.assertEqual(baseline["rejected_row_count"], 2)
        self.assertEqual(baseline["rejections"][1]["reason_codes"], ["UNKNOWN_CUSTOMER"])
        result = payment_plan([raw_row(PAYMENT)], set(), [])
        self.assertEqual(result["rejections"][0]["reason_codes"], ["UNKNOWN_POLICY"])

    def test_reference_duplicates_and_conflicting_customer_or_policy_ids(self):
        rows = [raw_row(CUSTOMER, "customers"), raw_row(POLICY, "policies")]
        rows += [raw_row({key: f" {value} " for key, value in CUSTOMER.items()}, "customers", number=2)]
        rows += [raw_row({**POLICY, "end_date": " "}, "policies", number=2)]
        result = reference_plan(rows)
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["duplicate_row_count"], 2)
        self.assertEqual(len(result["customers"]), 1)
        self.assertEqual(len(result["policies"]), 1)
        for entity, changed in (
            ("customers", {**CUSTOMER, "region": "Other"}),
            ("policies", {**POLICY, "product_code": "LIFE_PLUS"}),
            ("policies", {**POLICY, "product_code": "INVALID"}),
        ):
            with self.subTest(entity=entity, changed=changed):
                conflict = reference_plan(rows + [raw_row(changed, entity, number=3)])
                self.assertEqual(conflict["status"], "FAILED")
                self.assertEqual(conflict["error_codes"], ["CONFLICTING_REFERENCE_ID"])
                self.assertTrue(any(row["rejection_kind"] == "BATCH_CONFLICT" for row in conflict["rejections"]))

    def test_policy_date_range_and_future_start(self):
        _, reasons = validate_row(raw_row({**POLICY, "end_date": "2025-12-31"}, "policies"), "policies", {"C000001"})
        self.assertEqual(reasons, ["INVALID_POLICY_DATE_RANGE"])
        _, reasons = validate_row(raw_row({**POLICY, "start_date": "2027-01-01"}, "policies"), "policies", {"C000001"})
        self.assertEqual(reasons, [])
        result = payment_plan([raw_row(PAYMENT)], {"P000001"}, [])
        self.assertEqual(result["inserted_count"], 1)

    def test_baseline_identity_allows_only_the_same_reference_delivery(self):
        files = [
            {"entity": "customers", "delivery_fingerprint": "customer_delivery"},
            {"entity": "policies", "delivery_fingerprint": "policy_delivery"},
        ]
        fingerprint, errors = SILVER["check_baseline_identity"](files, [], "reference_v1")
        self.assertEqual(errors, [])
        stored = [{"batch_id": "reference_v1", "fingerprint": fingerprint, "status": "READY"}]
        self.assertEqual(SILVER["check_baseline_identity"](list(reversed(files)), stored, "reference_v1"), (fingerprint, []))
        for status in ("READY", "LOADING"):
            stored[0]["status"] = status
            changed_files = copy.deepcopy(files)
            changed_files[0]["delivery_fingerprint"] = "changed_content_or_time"
            self.assertEqual(SILVER["check_baseline_identity"](changed_files, stored, "reference_v1")[1], ["IMMUTABLE_BASELINE_CHANGED"])
            self.assertEqual(SILVER["check_baseline_identity"](files, stored, "reference_v2")[1], ["IMMUTABLE_BASELINE_CHANGED"])


class SilverVersionTests(unittest.TestCase):
    def test_multiple_versions_keep_evidence_and_only_one_current_candidate(self):
        older = raw_row(PAYMENT)
        newer = raw_row({**PAYMENT, "amount": "120.00", "source_updated_at": "2026-09-02T12:00:00Z"}, number=2)
        result = payment_plan([newer, older, copy.deepcopy(newer)], {"P000001"}, [])
        self.assertEqual(result["inserted_count"], 1)
        self.assertEqual(result["duplicate_row_count"], 1)
        self.assertEqual(result["superseded_version_count"], 1)
        self.assertEqual(len(result["versions"]), 2)
        self.assertEqual(current_payments(result["versions"])["PM000001"]["amount"], Decimal("120.00"))

    def test_incoming_conflicts_block_every_payment_change(self):
        second = raw_row({**PAYMENT, "amount": "101.00"}, number=2)
        unrelated = raw_row({**PAYMENT, "payment_id": "PM000002"}, number=3)
        result = payment_plan([raw_row(PAYMENT), second, unrelated], {"P000001"}, [])
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["versions"], [])
        self.assertEqual(result["inserted_count"], 0)
        self.assertEqual(result["rejected_row_count"], 0)
        self.assertEqual(len(result["rejections"]), 2)

    def test_superseded_incoming_version_is_checked_before_newer_candidate(self):
        history = payment_plan([raw_row(PAYMENT)], {"P000001"}, [])["versions"]
        newer = raw_row({**PAYMENT, "source_updated_at": "2026-09-02T12:00:00Z"})
        conflicting_old = raw_row({**PAYMENT, "amount": "101.00"}, number=2)
        result = payment_plan([newer, conflicting_old], {"P000001"}, history)
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["versions"], [])

    def test_invalid_correction_cannot_conflict_with_or_replace_valid_data(self):
        history = payment_plan([raw_row(PAYMENT)], {"P000001"}, [])["versions"]
        for changes in (
            {"amount": "-10.00"},
            {"amount": "-10.00", "source_updated_at": "2026-09-03T12:00:00Z"},
        ):
            with self.subTest(changes=changes):
                result = payment_plan([raw_row({**PAYMENT, **changes})], {"P000001"}, history)
                self.assertEqual(result["status"], "SUCCESS_WITH_REJECTIONS")
                self.assertEqual(result["versions"], [])
                self.assertEqual(current_payments(history)["PM000001"]["amount"], Decimal("100.00"))

    def test_first_seen_stale_version_is_retained_and_then_checked_for_conflicts(self):
        newer = raw_row({**PAYMENT, "source_updated_at": "2026-09-02T12:00:00Z"})
        history = payment_plan([newer], {"P000001"}, [])["versions"]
        result = payment_plan([raw_row(PAYMENT)], {"P000001"}, history)
        self.assertEqual(result["stale_version_count"], 1)
        self.assertEqual(result["updated_count"], 0)
        self.assertEqual(result["new_version_count"], 1)
        history += result["versions"]
        conflict = payment_plan([raw_row({**PAYMENT, "amount": "101.00"})], {"P000001"}, history)
        self.assertEqual(conflict["status"], "FAILED")

    def test_empty_delivery_never_deletes_current_records(self):
        history = payment_plan([raw_row(PAYMENT)], {"P000001"}, [])["versions"]
        before = copy.deepcopy(history)
        result = payment_plan([], {"P000001"}, history)
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["versions"], [])
        self.assertEqual(history, before)


class SilverSequenceTests(unittest.TestCase):
    def check_sequence(self, exported, dataset):
        manifest = json.loads(exported["manifest.json"])
        oracle = json.loads(exported["expected_outcomes.json"])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            write_dataset(root, exported)
            # Source preparation and planning must work without a readable oracle.
            (root / "expected_outcomes.json").write_text("not pipeline input", encoding="utf-8")
            files, rows = BRONZE["prepare_sources"](root, dataset)
        for raw in rows:
            raw.update(ingested_at=datetime(2026, 10, 1, tzinfo=timezone.utc), ingestion_run_id="bronze_test")
        reference_files = [file for file in files if file["entity"] != "payments"]
        reference_rows = [row for row in rows if row["entity"] != "payments"]
        self.assertEqual(SILVER["delivery_errors"](reference_files, reference_rows, ("customers", "policies"), manifest["baseline"], dataset), [])
        baseline = reference_plan(reference_rows)
        self.assertEqual(baseline["status"], "SUCCESS")
        policies = {row["policy_id"] for row in baseline["policies"]}
        history, results = [], {}
        for index, batch in enumerate(manifest["batches"]):
            batch_files = [file for file in files if file["batch_id"] == batch["batch_id"]]
            batch_rows = [row for row in rows if row["batch_id"] == batch["batch_id"]]
            errors = SILVER["delivery_errors"](batch_files, batch_rows, ("payments",), batch, dataset)
            before = copy.deepcopy(history)
            if errors:
                result = {**SILVER["new_plan"](batch_rows), "status": "FAILED", "error_codes": errors}
            else:
                result = payment_plan(batch_rows, policies, history)
                if result["status"] == "FAILED":
                    self.assertEqual(result["versions"], [])
                    self.assertEqual(history, before)
                else:
                    history.extend(result["versions"])
            expected = oracle["after_batches"][batch["batch_id"]]
            self.assertEqual(result["status"], expected["status"], batch["batch_id"])
            self.assertEqual(result["rejected_row_count"], expected["rejected_row_count_in_this_batch"])
            totals = SILVER["payment_totals"](list(current_payments(history).values()))
            for key, value in totals.items():
                self.assertEqual(value, expected[key], (batch["batch_id"], key))
            if "expected_reason_codes" in expected:
                reasons = set(result["error_codes"])
                for rejected in result["rejections"]:
                    reasons.update(rejected["reason_codes"])
                self.assertEqual(reasons, set(expected["expected_reason_codes"]))
            results[batch["batch_id"]] = result
            if index == 0:
                replay = payment_plan(batch_rows, policies, history)
                self.assertEqual(replay["versions"], [])
                self.assertEqual(replay["inserted_count"], 0)
                self.assertEqual(replay["updated_count"], 0)
        before = copy.deepcopy(history)
        for batch in manifest["batches"]:
            if results[batch["batch_id"]]["status"] != "FAILED":
                batch_rows = [row for row in rows if row["batch_id"] == batch["batch_id"]]
                replay = payment_plan(batch_rows, policies, history)
                self.assertEqual(replay["versions"], [], batch["batch_id"])
                self.assertEqual(replay["inserted_count"] + replay["updated_count"], 0)
        self.assertEqual(history, before)
        return baseline, history, results, oracle, files, rows, manifest

    def test_01_fixture_sequence_failures_and_replays(self):
        _, history, results, _, _, _, _ = self.check_sequence(build_fixture(), "fixture")
        self.assertEqual(len(history), 4)
        current = current_payments(history)
        self.assertEqual(len(current), 3)
        self.assertEqual(sum(row["amount"] for row in current.values()), Decimal("230.00"))
        self.assertEqual(results["fixture_002_duplicate"]["duplicate_row_count"], 1)
        self.assertEqual(results["fixture_004_stale"]["stale_version_count"], 1)
        self.assertEqual(results["fixture_013_normalized_duplicate"]["unchanged_version_count"], 1)
        for batch in ("fixture_009_conflict_current", "fixture_010_conflict_history"):
            self.assertEqual(results[batch]["error_codes"], ["CONFLICTING_PAYMENT_VERSION"])

    def test_02_default_demo_after_fixture(self):
        baseline, history, results, oracle, _, _, _ = self.check_sequence(build_demo(DemoConfig()), "demo")
        self.assertEqual(len(baseline["customers"]), 500)
        self.assertEqual(len(baseline["policies"]), 1000)
        self.assertEqual(len(current_payments(history)), 10200)
        self.assertEqual(len(history), 10300)
        updates = results["demo_002_updates"]
        self.assertEqual(updates["rejected_row_count"], 14)
        self.assertEqual(updates["inserted_count"], 200)
        self.assertEqual(updates["updated_count"], 100)
        self.assertEqual(updates["duplicate_row_count"], 10)
        self.assertEqual(updates["unchanged_version_count"], 25)
        stale = results["demo_003_stale_and_duplicates"]
        self.assertEqual(stale["stale_version_count"], 100)
        self.assertEqual(stale["unchanged_version_count"], 10)
        actual = {json.loads(row["raw_payload"])["payment_id"]: row["reason_codes"] for row in updates["rejections"]}
        expected = {note["payment_id"]: sorted(note["expected_reason_codes"]) for note in oracle["intentional_invalid_rows_in_updates"]}
        self.assertEqual(actual, expected)

    def test_partial_or_mismatched_bronze_is_refused(self):
        _, _, _, _, files, rows, manifest = self.check_sequence(build_fixture(), "fixture")
        batch = manifest["batches"][0]
        selected_files = [file for file in files if file["batch_id"] == batch["batch_id"]]
        selected_rows = [row for row in rows if row["batch_id"] == batch["batch_id"]]
        self.assertIn("MISSING_OR_AMBIGUOUS_FILE", SILVER["delivery_errors"]([], [], ("payments",), batch, "fixture"))
        self.assertIn("INCOMPLETE_BRONZE_ROWS", SILVER["delivery_errors"](selected_files, [], ("payments",), batch, "fixture"))
        changed = copy.deepcopy(selected_rows)
        changed[0]["source_sha256"] = "different"
        self.assertIn("BRONZE_DELIVERY_MISMATCH", SILVER["delivery_errors"](selected_files, changed, ("payments",), batch, "fixture"))
        changed_batch = copy.deepcopy(batch)
        changed_batch["file"]["sha256"] = "different"
        self.assertIn("BRONZE_DELIVERY_MISMATCH", SILVER["delivery_errors"](selected_files, selected_rows, ("payments",), changed_batch, "fixture"))


if __name__ == "__main__":
    unittest.main()
