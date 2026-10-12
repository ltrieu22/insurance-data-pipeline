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
VERIFY = runpy.run_path(str(ROOT / "databricks" / "03_verify_pipeline.py"))


def raw_row(values, entity="payments", number=1, cutoff="2026-09-04T00:00:00Z"):
    return {
        "entity": entity, "batch_id": "test_delivery",
        "batch_as_of_utc": SILVER["parse_timestamp"](cutoff),
        "source_file": "/test/input.csv", "source_sha256": "test_checksum",
        "source_row_number": number, "raw_payload": json.dumps(values),
        "ingested_at": SILVER["parse_timestamp"](cutoff),
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
        self.assertEqual(len(result["rejections"]), 1)
        rejected = result["rejections"][0]
        self.assertEqual(rejected["reason_codes"], ["NON_POSITIVE_AMOUNT", "UNKNOWN_POLICY", "UNSUPPORTED_CURRENCY"])
        for field in SILVER["PROVENANCE"]:
            self.assertEqual(rejected[field], raw[field])

    def test_null_blank_and_non_string_ids_are_rejected_without_coercion(self):
        for identifier in (None, "", "  ", 123, [], {}):
            with self.subTest(identifier=identifier):
                result = payment_plan([raw_row({**PAYMENT, "payment_id": identifier})], {"P000001"}, [])
                self.assertEqual(len(result["rejections"]), 1)
                self.assertEqual(result["changes"], [])

    def test_only_accepted_parents_can_satisfy_relationships(self):
        rows = [
            raw_row({**CUSTOMER, "region": "unknown"}, "customers"),
            raw_row(POLICY, "policies"),
        ]
        baseline = reference_plan(rows)
        self.assertEqual(baseline["customers"], [])
        self.assertEqual(baseline["policies"], [])
        self.assertEqual(len(baseline["rejections"]), 2)
        self.assertEqual(baseline["rejections"][1]["reason_codes"], ["UNKNOWN_CUSTOMER"])
        result = payment_plan([raw_row(PAYMENT)], set(), [])
        self.assertEqual(result["rejections"][0]["reason_codes"], ["UNKNOWN_POLICY"])

    def test_reference_duplicates_and_conflicting_customer_or_policy_ids(self):
        rows = [raw_row(CUSTOMER, "customers"), raw_row(POLICY, "policies")]
        rows += [raw_row({key: f" {value} " for key, value in CUSTOMER.items()}, "customers", number=2)]
        rows += [raw_row({**POLICY, "end_date": " "}, "policies", number=2)]
        result = reference_plan(rows)
        self.assertEqual(result["status"], "SUCCESS")
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
                self.assertTrue(any("CONFLICTING_REFERENCE_ID" in row["reason_codes"] for row in conflict["rejections"]))

    def test_policy_dates_are_outside_the_silver_reporting_scope(self):
        values, reasons = validate_row(raw_row({**POLICY, "start_date": "not-a-date", "end_date": "earlier"}, "policies"), "policies", {"C000001"})
        self.assertEqual(reasons, [])
        self.assertEqual(values, {key: POLICY[key] for key in SILVER["FIELDS"]["policies"]})

    def test_changed_accepted_reference_data_requires_review(self):
        rows = [raw_row(CUSTOMER, "customers"), raw_row(POLICY, "policies")]
        baseline = reference_plan(rows)
        for entity, field, replacement in (
            ("customers", "region", "Other"), ("policies", "product_code", "LIFE_PLUS")
        ):
            stored = baseline[entity]
            SILVER["check_reference_data"](stored, stored, entity)
            for changed in ([{**stored[0], field: replacement}], []):
                with self.subTest(entity=entity), self.assertRaisesRegex(ValueError, "baseline changed"):
                    SILVER["check_reference_data"](changed, stored, entity)


class SilverPaymentTests(unittest.TestCase):
    def test_newest_valid_version_wins_and_duplicates_collapse(self):
        newer = raw_row({**PAYMENT, "amount": "120.00", "source_updated_at": "2026-09-02T12:00:00Z"}, number=2)
        result = payment_plan([newer, raw_row(PAYMENT), copy.deepcopy(newer)], {"P000001"}, [])
        self.assertEqual(len(result["changes"]), 1)
        self.assertEqual(result["changes"][0]["amount"], Decimal("120.00"))
        self.assertEqual(payment_plan([newer], {"P000001"}, result["changes"])["changes"], [])

    def test_incoming_or_current_conflicts_block_every_payment_change(self):
        first = raw_row(PAYMENT)
        conflict = raw_row({**PAYMENT, "amount": "101.00"}, number=2)
        unrelated = raw_row({**PAYMENT, "payment_id": "PM000002"}, number=3)
        current = payment_plan([first], {"P000001"}, [])["changes"]
        for incoming, stored, rejected in (([first, conflict, unrelated], [], 2), ([conflict, unrelated], current, 1)):
            with self.subTest(stored=bool(stored)):
                result = payment_plan(incoming, {"P000001"}, stored)
                self.assertEqual(result["status"], "FAILED")
                self.assertEqual(result["changes"], [])
                self.assertEqual(len(result["rejections"]), rejected)
                self.assertEqual(result["error_codes"], ["CONFLICTING_PAYMENT_VERSION"])

    def test_current_version_conflict_is_checked_even_with_newer_candidate(self):
        current = payment_plan([raw_row(PAYMENT)], {"P000001"}, [])["changes"]
        newer = raw_row({**PAYMENT, "source_updated_at": "2026-09-02T12:00:00Z"})
        conflicting_current = raw_row({**PAYMENT, "amount": "101.00"}, number=2)
        result = payment_plan([newer, conflicting_current], {"P000001"}, current)
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["changes"], [])

    def test_historical_payload_conflicts_are_deliberately_stale_noops(self):
        newer = raw_row({**PAYMENT, "amount": "120.00", "source_updated_at": "2026-09-02T12:00:00Z"})
        current = payment_plan([newer], {"P000001"}, [])["changes"]
        for amount in ("100.00", "101.00"):
            result = payment_plan([raw_row({**PAYMENT, "amount": amount})], {"P000001"}, current)
            self.assertEqual(result["status"], "SUCCESS")
            self.assertEqual(result["changes"], [])
            self.assertEqual(result["rejections"], [])

    def test_invalid_correction_preserves_current_and_valid_unrelated_rows_continue(self):
        current = payment_plan([raw_row(PAYMENT)], {"P000001"}, [])["changes"]
        before = copy.deepcopy(current)
        invalid = raw_row({**PAYMENT, "amount": "-10.00", "source_updated_at": "2026-09-03T12:00:00Z"})
        unrelated = raw_row({**PAYMENT, "payment_id": "PM000002"}, number=2)
        result = payment_plan([invalid, unrelated], {"P000001"}, current)
        self.assertEqual(result["status"], "SUCCESS_WITH_REJECTIONS")
        self.assertEqual(len(result["rejections"]), 1)
        self.assertEqual([row["payment_id"] for row in result["changes"]], ["PM000002"])
        self.assertEqual(current, before)

    def test_empty_delivery_never_deletes_current_records(self):
        current = payment_plan([raw_row(PAYMENT)], {"P000001"}, [])["changes"]
        before = copy.deepcopy(current)
        result = payment_plan([], {"P000001"}, current)
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["changes"], [])
        self.assertEqual(current, before)


class SilverSequenceTests(unittest.TestCase):
    def check_sequence(self, exported, dataset):
        manifest = json.loads(exported["manifest.json"])
        oracle = json.loads(exported["expected_outcomes.json"])
        expected = VERIFY["reduced_expectations"](oracle, dataset)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            write_dataset(root, exported)
            # Processing must work even when the uploaded oracle cannot be read.
            (root / "expected_outcomes.json").write_text("not pipeline input", encoding="utf-8")
            files, rows = BRONZE["prepare_sources"](root, dataset)
        for raw in rows:
            raw["ingested_at"] = datetime(2026, 10, 1, tzinfo=timezone.utc)
        reference_files = [file for file in files if file["entity"] != "payments"]
        reference_rows = [row for row in rows if row["entity"] != "payments"]
        self.assertEqual(SILVER["delivery_errors"](reference_files, reference_rows, ("customers", "policies"), manifest["baseline"]), [])
        baseline = reference_plan(reference_rows)
        self.assertEqual(baseline["status"], "SUCCESS")
        policies = {row["policy_id"] for row in baseline["policies"]}
        current, results, summaries = {}, {}, []
        for index, batch in enumerate(manifest["batches"]):
            batch_files = [file for file in files if file["batch_id"] == batch["batch_id"]]
            batch_rows = [row for row in rows if row["batch_id"] == batch["batch_id"]]
            errors = SILVER["delivery_errors"](batch_files, batch_rows, ("payments",), batch)
            before = copy.deepcopy(current)
            if errors:
                result = SILVER["new_plan"]()
                result.update(status="FAILED", error_codes=errors)
            else:
                result = payment_plan(batch_rows, policies, list(current.values()))
            if result["status"] == "FAILED":
                self.assertEqual(result["changes"], [])
            else:
                for change in result["changes"]:
                    current[change["payment_id"]] = change
            if result["status"] == "FAILED":
                self.assertEqual(current, before)
            target = expected[batch["batch_id"]]
            totals = VERIFY["payment_totals"](list(current.values()))
            for key, value in totals.items():
                self.assertEqual(value, target[key], (batch["batch_id"], key))
            reasons = set(result["error_codes"])
            for rejection in result["rejections"]:
                reasons.update(rejection["reason_codes"])
            if "expected_reason_codes" in target:
                self.assertEqual(reasons, set(target["expected_reason_codes"]))
            summaries.append(SILVER["batch_summary"](batch, batch_rows, result))
            results[batch["batch_id"]] = result
            if index == 0:
                self.assertEqual(payment_plan(batch_rows, policies, list(current.values()))["changes"], [])
        VERIFY["assert_batch_summaries"](summaries, expected)
        actual, rejections, sequence_summaries = SILVER["payment_sequence"](files, rows, manifest["batches"], policies)
        self.assertEqual({row["payment_id"]: row for row in actual}, current)
        self.assertEqual(sequence_summaries, summaries)
        self.assertEqual(len(rejections), sum(summary["rejected_row_count"] for summary in summaries))
        self.assertEqual(
            (actual, rejections, sequence_summaries),
            SILVER["payment_sequence"](files, rows, manifest["batches"], policies),
        )
        before = copy.deepcopy(current)
        for batch in manifest["batches"]:
            if results[batch["batch_id"]]["status"] != "FAILED":
                batch_rows = [row for row in rows if row["batch_id"] == batch["batch_id"]]
                self.assertEqual(payment_plan(batch_rows, policies, list(current.values()))["changes"], [], batch["batch_id"])
        self.assertEqual(current, before)
        return baseline, current, results, oracle, files, rows, manifest

    def test_fixture_intermediate_totals_failures_and_replays(self):
        _, current, results, _, _, _, _ = self.check_sequence(build_fixture(), "fixture")
        self.assertEqual(len(current), 3)
        self.assertEqual(sum(row["amount"] for row in current.values()), Decimal("230.00"))
        self.assertEqual({batch for batch, result in results.items() if result["status"] == "FAILED"}, {
            "fixture_009_conflict_current", "fixture_011_missing_column"
        })
        self.assertEqual(results["fixture_010_conflict_history"]["changes"], [])

    def test_default_demo_totals_invalid_reasons_and_replays(self):
        baseline, current, results, oracle, _, _, _ = self.check_sequence(build_demo(DemoConfig()), "demo")
        self.assertEqual(len(baseline["customers"]), 500)
        self.assertEqual(len(baseline["policies"]), 1000)
        self.assertEqual(len(current), 10200)
        updates = results["demo_002_updates"]
        self.assertEqual(len(updates["rejections"]), 14)
        self.assertEqual(len(updates["changes"]), 300)
        actual = {json.loads(row["raw_payload"])["payment_id"]: row["reason_codes"] for row in updates["rejections"]}
        expected = {note["payment_id"]: sorted(note["expected_reason_codes"]) for note in oracle["intentional_invalid_rows_in_updates"]}
        self.assertEqual(actual, expected)

    def test_missing_incomplete_or_changed_bronze_is_refused(self):
        _, _, _, _, files, rows, manifest = self.check_sequence(build_fixture(), "fixture")
        batch = manifest["batches"][0]
        selected_files = [file for file in files if file["batch_id"] == batch["batch_id"]]
        selected_rows = [row for row in rows if row["batch_id"] == batch["batch_id"]]
        self.assertIn("MISSING_OR_AMBIGUOUS_FILE", SILVER["delivery_errors"]([], [], ("payments",), batch))
        self.assertIn("INCOMPLETE_BRONZE_ROWS", SILVER["delivery_errors"](selected_files, [], ("payments",), batch))
        changed_rows = copy.deepcopy(selected_rows)
        changed_rows[0]["source_sha256"] = "different"
        self.assertIn("BRONZE_DELIVERY_MISMATCH", SILVER["delivery_errors"](selected_files, changed_rows, ("payments",), batch))
        changed_batch = copy.deepcopy(batch)
        changed_batch["file"]["sha256"] = "different"
        self.assertIn("BRONZE_DELIVERY_MISMATCH", SILVER["delivery_errors"](selected_files, selected_rows, ("payments",), changed_batch))


if __name__ == "__main__":
    unittest.main()
