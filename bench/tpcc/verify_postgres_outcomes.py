#!/usr/bin/env python3
"""Verify corrected-v2 client outcomes against quiescent PostgreSQL deltas.

This establishes operation completion only. Full state and recovery audits
remain separate requirements for a qualified performance result.
"""
import csv
import json
import sys
from pathlib import Path
from summarize import corrected_client_outcomes


def verify(root, vus, minutes):
    def value(filename, field):
        with (root / filename).open() as file:
            return int(next(csv.DictReader(file))[field])

    outcomes = corrected_client_outcomes((root / "hammerdb.log").read_text(), vus)
    orders = value("district-after.csv", "district_sum") - value("district-before.csv", "district_sum")
    payments = value("payment-after.csv", "payment_history_rows") - value("payment-before.csv", "payment_history_rows")
    errors = []
    if not outcomes["complete"]:
        errors.append("Incomplete, duplicate, or inconsistent client reports")
    if outcomes["other"] or orders != outcomes["positive"] or orders + outcomes["invalid"] != outcomes["neword"]:
        errors.append("NewOrder replies and committed database delta disagree")
    if payments != outcomes["payment"]:
        errors.append("Payment calls and committed history delta disagree")
    if min(orders, payments) <= 0:
        errors.append("No positive business work")
    return dict(valid=not errors, errors=errors, client_outcomes=outcomes,
                neworder_commits=orders, payment_commits=payments,
                district_sum_nopm=orders / minutes,
                scope="operation completion only; financial, order, and recovery audits required")


if __name__ == "__main__":
    root = Path(sys.argv[1])
    result = verify(root, int(sys.argv[2]), int(sys.argv[3]) + int(sys.argv[4]))
    with (root / "outcome-result.json").open("x") as file:
        json.dump(result, file, indent=2)
    print(json.dumps(result))
    sys.exit(0 if result["valid"] else 1)
