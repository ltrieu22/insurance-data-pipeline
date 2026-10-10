"""Generate synthetic customer, policy, and payment exports."""

import argparse
import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from .source_files import (
    add_archive,
    add_payment_batch,
    json_bytes,
    reference_exports,
    write_dataset,
)


REGIONS = ("Uusimaa", "North Ostrobothnia", "Pirkanmaa", "Other")
PRODUCTS = ("LIFE_BASIC", "LIFE_PLUS")


@dataclass(frozen=True)
class DemoConfig:
    seed: int = 42
    customers: int = 500
    policies: int = 1_000
    payments: int = 10_000

    def __post_init__(self) -> None:
        for name in ("customers", "policies", "payments"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be at least 1")


def payment(
    payment_id: str,
    policy_id: str,
    received: str,
    amount: str,
    updated: str,
) -> dict[str, str]:
    return {
        "payment_id": payment_id,
        "policy_id": policy_id,
        "payment_date": received,
        "amount": amount,
        "currency": "EUR",
        "source_updated_at": updated,
    }


def reporting_expectation(records: list[dict], rejected: int = 0) -> dict:
    """Calculate expected totals from clean records, before adding invalid rows."""
    by_month = defaultdict(lambda: Decimal("0.00"))
    for row in records:
        by_month[row["payment_date"][:7]] += Decimal(row["amount"])
    return {
        "status": "SUCCESS_WITH_REJECTIONS" if rejected else "SUCCESS",
        "current_payment_count": len(records),
        "total_amount_eur": format(sum(by_month.values(), Decimal("0.00")), ".2f"),
        "amount_by_month_eur": {
            month: format(amount, ".2f") for month, amount in sorted(by_month.items())
        },
        "rejected_row_count_in_this_batch": rejected,
    }


def invalid_payment_examples(policy_id: str) -> tuple[list[dict], list[dict]]:
    """Create invalid rows and their expected rejection reasons."""
    scenarios = [
        ("missing_payment_id", {"payment_id": ""}, ["MISSING_REQUIRED_VALUE"]),
        ("unknown_policy", {"policy_id": "P_MISSING"}, ["UNKNOWN_POLICY"]),
        ("invalid_amount", {"amount": "not-a-number"}, ["INVALID_AMOUNT_FORMAT"]),
        ("excess_precision", {"amount": "12.345"}, ["INVALID_AMOUNT_FORMAT"]),
        ("amount_overflow", {"amount": "10000000000.00"}, ["INVALID_AMOUNT_FORMAT"]),
        ("zero_amount", {"amount": "0.00"}, ["NON_POSITIVE_AMOUNT"]),
        ("negative_amount", {"amount": "-10.00"}, ["NON_POSITIVE_AMOUNT"]),
        ("unsupported_currency", {"currency": "USD"}, ["UNSUPPORTED_CURRENCY"]),
        ("invalid_date", {"payment_date": "2026-02-30"}, ["INVALID_DATE_OR_TIMESTAMP"]),
        ("invalid_timestamp", {"source_updated_at": "yesterday"}, ["INVALID_DATE_OR_TIMESTAMP"]),
        ("future_payment", {
            "payment_date": "2026-09-04", "source_updated_at": "2026-09-04T12:00:00Z"
        }, ["FUTURE_PAYMENT_OR_UPDATE"]),
        ("future_update", {"source_updated_at": "2026-09-04T12:00:00Z"}, ["FUTURE_PAYMENT_OR_UPDATE"]),
        ("update_before_payment", {"source_updated_at": "2026-09-01T12:00:00Z"}, ["UPDATE_BEFORE_PAYMENT"]),
        ("missing_amount_and_date", {"amount": "", "payment_date": ""}, ["MISSING_REQUIRED_VALUE"]),
    ]
    rows, notes = [], []
    for index, (scenario, changes, reasons) in enumerate(scenarios, start=1):
        row = payment(
            f"BAD{index:06d}", policy_id, "2026-09-02", "50.00", "2026-09-02T14:00:00Z"
        )
        row.update(changes)
        rows.append(row)
        notes.append({
            "scenario": scenario,
            "payment_id": row["payment_id"],
            "expected_reason_codes": reasons,
        })
    return rows, notes


def generate_customers(count: int, rng: random.Random) -> list[dict]:
    customers = []
    for number in range(1, count + 1):
        customers.append({
            "customer_id": f"C{number:06d}",
            "region": rng.choice(REGIONS),
            "customer_segment": "PRIORITY" if rng.random() < 0.2 else "STANDARD",
        })
    return customers


def generate_policies(count: int, customers: list[dict], rng: random.Random) -> list[dict]:
    policies = []
    for number in range(1, count + 1):
        policies.append({
            "policy_id": f"P{number:06d}",
            "customer_id": rng.choice(customers)["customer_id"],
            "product_code": rng.choice(PRODUCTS),
            "start_date": (date(2025, 1, 1) + timedelta(days=rng.randrange(365))).isoformat(),
            "end_date": "2026-12-31" if rng.random() < 0.1 else None,
        })
    return policies


def generate_payments(count: int, policies: list[dict], rng: random.Random) -> list[dict]:
    payments = []
    for number in range(1, count + 1):
        received = (date(2026, 1, 1) + timedelta(days=rng.randrange(243))).isoformat()
        amount = f"{Decimal(rng.randint(2_000, 25_000)) / 100:.2f}"
        payments.append(payment(
            f"PM{number:06d}",
            rng.choice(policies)["policy_id"],
            received,
            amount,
            f"{received}T12:00:00Z",
        ))
    return payments


def build_demo(config: DemoConfig) -> dict[str, bytes]:
    rng = random.Random(config.seed)
    customers = generate_customers(config.customers, rng)
    policies = generate_policies(config.policies, customers, rng)
    initial = generate_payments(config.payments, policies, rng)

    correction_count = min(100, config.payments)
    new_count = max(1, config.payments // 50)
    corrections = []
    for row in initial[:correction_count]:
        corrected = row.copy()
        corrected["amount"] = f"{Decimal(row['amount']) + Decimal('25.00'):.2f}"
        corrected["source_updated_at"] = "2026-09-02T12:00:00Z"
        corrections.append(corrected)

    new_payments = []
    for index in range(1, new_count + 1):
        new_payments.append(payment(
            f"PM{config.payments + index:06d}",
            rng.choice(policies)["policy_id"],
            "2026-08-15" if index % 5 == 0 else "2026-09-02",
            f"{Decimal(rng.randint(2_000, 25_000)) / 100:.2f}",
            "2026-09-02T13:00:00Z",
        ))

    expected_final = corrections + initial[correction_count:] + new_payments
    invalid_rows, invalid_notes = invalid_payment_examples(policies[0]["policy_id"])
    updates = (
        corrections
        + new_payments
        + initial[-min(25, config.payments):]
        + new_payments[:10]
        + invalid_rows
    )
    rng.shuffle(updates)
    stale_and_repeated = initial[:correction_count] + new_payments[:10]
    rng.shuffle(stale_and_repeated)

    files, baseline = reference_exports(customers, policies)
    batches: list[dict] = []
    add_payment_batch(
        files, batches, "demo_001_initial", "2026-09-02T00:00:00Z", initial,
        "Clean initial payment export.",
    )
    add_payment_batch(
        files, batches, "demo_002_updates", "2026-09-03T00:00:00Z", updates,
        "New payments, late receipts, corrections, duplicates, and deliberately invalid rows.",
    )
    add_payment_batch(
        files, batches, "demo_003_stale_and_duplicates", "2026-09-04T00:00:00Z",
        stale_and_repeated,
        "Older corrected versions and already accepted payments; no business changes.",
    )
    files["manifest.json"] = json_bytes({
        "schema_version": 1,
        "dataset": "insurance_premium_demo",
        "synthetic": True,
        "seed": config.seed,
        "configuration": {
            "customers": config.customers,
            "policies": config.policies,
            "initial_payments": config.payments,
        },
        "baseline": baseline,
        "batches": batches,
        "processing_order": "Load the reference baseline, then process batches in listed order.",
    })
    files["expected_outcomes.json"] = json_bytes({
        "purpose": "Verification oracle only; never use this file as a pipeline source or result.",
        "after_batches": {
            "demo_001_initial": reporting_expectation(initial),
            "demo_002_updates": reporting_expectation(expected_final, len(invalid_rows)),
            "demo_003_stale_and_duplicates": reporting_expectation(expected_final),
        },
        "correction_count": correction_count,
        "new_payment_count": new_count,
        "intentional_invalid_rows_in_updates": invalid_notes,
    })
    add_archive(files, "demo_sources.zip")
    return files


def positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/generated"))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--customers", type=positive_int, default=500)
    parser.add_argument("--policies", type=positive_int, default=1_000)
    parser.add_argument("--payments", type=positive_int, default=10_000)
    args = parser.parse_args()
    config = DemoConfig(args.seed, args.customers, args.policies, args.payments)
    try:
        write_dataset(args.output, build_demo(config))
    except FileExistsError as error:
        parser.exit(1, f"{error}\n")
    print(f"Archive: {(args.output / 'demo_sources.zip').resolve()}")


if __name__ == "__main__":
    main()
