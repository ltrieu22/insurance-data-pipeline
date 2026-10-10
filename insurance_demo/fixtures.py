"""Generate small test batches with fixed expected results."""

import argparse
from pathlib import Path

from .generate import payment
from .source_files import (
    PAYMENT_COLUMNS,
    add_archive,
    add_payment_batch,
    json_bytes,
    reference_exports,
    write_dataset,
)


def build_fixture() -> dict[str, bytes]:
    customers = [{
        "customer_id": "C000001",
        "region": "Uusimaa",
        "customer_segment": "STANDARD",
    }]
    policies = [{
        "policy_id": "P000001",
        "customer_id": "C000001",
        "product_code": "LIFE_BASIC",
        "start_date": "2026-01-01",
        "end_date": None,
    }]
    initial = payment("PM000001", "P000001", "2026-09-01", "100.00", "2026-09-01T12:00:00Z")
    corrected = {**initial, "amount": "120.00", "source_updated_at": "2026-09-02T12:00:00Z"}
    unknown_policy = payment("PM000003", "P_MISSING", "2026-09-02", "60.00", "2026-09-02T12:00:00Z")
    late = payment("PM000002", "P000001", "2026-08-15", "50.00", "2026-09-02T12:00:00Z")
    uncommitted = payment("PM000003", "P000001", "2026-09-02", "40.00", "2026-09-02T12:00:00Z")
    missing_column = {key: value for key, value in uncommitted.items() if key != "payment_id"}
    normalized = {key: f" {value} " for key, value in corrected.items()}
    normalized["amount"] = " 120.0 "
    recovered = {
        **unknown_policy, "policy_id": "P000001", "source_updated_at": "2026-09-03T12:00:00Z"
    }

    # Fixed totals keep the test expectations independent of the pipeline.
    sep_100 = {
        "current_payment_count": 1,
        "total_amount_eur": "100.00",
        "amount_by_month_eur": {"2026-09": "100.00"},
    }
    sep_120 = {
        "current_payment_count": 1,
        "total_amount_eur": "120.00",
        "amount_by_month_eur": {"2026-09": "120.00"},
    }
    with_late = {
        "current_payment_count": 2,
        "total_amount_eur": "170.00",
        "amount_by_month_eur": {"2026-08": "50.00", "2026-09": "120.00"},
    }
    with_recovery = {
        "current_payment_count": 3,
        "total_amount_eur": "230.00",
        "amount_by_month_eur": {"2026-08": "50.00", "2026-09": "180.00"},
    }

    files, baseline = reference_exports(customers, policies)
    baseline["batch_id"] = "fixture_reference_v1"
    batches = []
    expected = {}

    def add_case(
        batch_id, cutoff, rows, state, description,
        *, status="SUCCESS", rejected=0, reasons=(), columns=PAYMENT_COLUMNS,
    ):
        add_payment_batch(files, batches, batch_id, f"{cutoff}T00:00:00Z", rows, description, columns)
        expected[batch_id] = {
            **state,
            "status": status,
            "rejected_row_count_in_this_batch": rejected,
            "expected_reason_codes": list(reasons),
        }

    add_case(
        "fixture_001_initial", "2026-09-02", [initial], sep_100,
        "Insert the first payment.",
    )
    add_case(
        "fixture_002_duplicate", "2026-09-02", [initial, initial.copy()], sep_100,
        "Repeat an accepted version twice; do not count it again.",
    )
    add_case(
        "fixture_003_correction", "2026-09-03", [corrected], sep_120,
        "A newer source version changes EUR 100.00 to EUR 120.00.",
    )
    add_case(
        "fixture_004_stale", "2026-09-03", [initial], sep_120,
        "The older version must not overwrite the correction.",
    )
    add_case(
        "fixture_005_unknown_policy", "2026-09-03", [unknown_policy], sep_120,
        "Retain the invalid record with its rejection reason.",
        status="SUCCESS_WITH_REJECTIONS", rejected=1, reasons=["UNKNOWN_POLICY"],
    )
    add_case(
        "fixture_006_precision", "2026-09-03",
        [{**uncommitted, "payment_id": "BAD_PRECISION", "amount": "12.345"}], sep_120,
        "Reject excess decimal places; do not round.",
        status="SUCCESS_WITH_REJECTIONS", rejected=1, reasons=["INVALID_AMOUNT_FORMAT"],
    )
    add_case(
        "fixture_007_late", "2026-09-03", [late], with_late,
        "Deliver an August payment in September; update August's total.",
    )
    add_case(
        "fixture_008_invalid_correction", "2026-09-04",
        [{**corrected, "amount": "-10.00", "source_updated_at": "2026-09-03T12:00:00Z"}],
        with_late,
        "A rejected newer version must not replace accepted data.",
        status="SUCCESS_WITH_REJECTIONS", rejected=1, reasons=["NON_POSITIVE_AMOUNT"],
    )
    add_case(
        "fixture_009_conflict_current", "2026-09-04",
        [{**corrected, "amount": "130.00"}, uncommitted], with_late,
        "Fail the whole batch; the accompanying valid new payment must not be committed.",
        status="FAILED", reasons=["CONFLICTING_PAYMENT_VERSION"],
    )
    add_case(
        "fixture_010_conflict_history", "2026-09-04", [{**initial, "amount": "101.00"}],
        with_late,
        "Detect a conflict with a superseded accepted version, too.",
        status="FAILED", reasons=["CONFLICTING_PAYMENT_VERSION"],
    )
    add_case(
        "fixture_011_missing_column", "2026-09-04", [missing_column], with_late,
        "Fail the file contract when payment_id is absent.",
        status="FAILED", reasons=["MISSING_REQUIRED_COLUMN"],
        columns=tuple(column for column in PAYMENT_COLUMNS if column != "payment_id"),
    )
    add_case(
        "fixture_012_empty", "2026-09-04", [], with_late,
        "A header-only payment delivery is a valid no-op.",
    )
    add_case(
        "fixture_013_normalized_duplicate", "2026-09-04", [normalized], with_late,
        "Trim strings and compare decimal values before deduplication.",
    )
    add_case(
        "fixture_014_recovered", "2026-09-04", [recovered], with_recovery,
        "A corrected previously rejected payment can be accepted in a later batch.",
    )
    files["manifest.json"] = json_bytes({
        "schema_version": 1,
        "dataset": "insurance_premium_fixture",
        "synthetic": True,
        "baseline": baseline,
        "batches": batches,
        "processing_order": (
            "Use a fresh fixture state. Load the reference baseline, then process batches "
            "in listed order. After each expected failure, continue with the next batch; "
            "the prior published state must remain intact."
        ),
    })
    files["expected_outcomes.json"] = json_bytes({
        "purpose": "Hand-written acceptance oracle; never use this file as a pipeline source or result.",
        "after_batches": expected,
        "replay_check": (
            "Replay fixture_001_initial immediately after its first run: count 1, EUR 100.00. "
            "After the full sequence, replay all previously successful batches: count 3, EUR 230.00."
        ),
        "counting_note": (
            "rejected_row_count_in_this_batch counts row-validation rejections only. "
            "Fatal file/version conflicts are batch failures and carry separate reason codes."
        ),
    })
    add_archive(files, "fixture_sources.zip")
    return files


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/fixtures"))
    args = parser.parse_args()
    try:
        write_dataset(args.output, build_fixture())
    except FileExistsError as error:
        parser.exit(1, f"{error}\n")
    print(f"Archive: {(args.output / 'fixture_sources.zip').resolve()}")


if __name__ == "__main__":
    main()
