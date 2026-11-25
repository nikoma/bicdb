#!/usr/bin/env python3
"""Render the generated tables of docs/postgresql-regression-suite.md from a run.

Usage: report.py <run-dir> <clusters-in.json> [--ref <ref-run-dir>] [--top N] > tables.md
"""
import argparse
import json
import os


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run")
    ap.add_argument("clusters")
    ap.add_argument("--ref")
    ap.add_argument("--top", type=int, default=40)
    args = ap.parse_args()
    with open(os.path.join(args.run, "summary.json")) as f:
        s = json.load(f)
    with open(args.clusters) as f:
        cl = json.load(f)
    ref = {}
    if args.ref:
        with open(os.path.join(args.ref, "summary.json")) as f:
            ref = {t["test"]: t["status"] for t in json.load(f)["tests"]}
    per = cl.get("per_test", {})
    print(f"Run `{s['label']}` ({s['date']}), {s['seconds']} s.\n")
    print("| | tests | pass | fail | failing diff hunks |\n|---|---|---|---|---|")
    for k, name in (("in_scope", "in scope"), ("out_of_scope", "out of scope"), ("all", "all")):
        c = s[k]
        print(f"| {name} | {c['tests']} | {c['pass']} | {c['fail']} | {c['hunks']} |")
    print()
    print("### Per test\n")
    print("| # | test | scope | PostgreSQL 18.4 | BicDB | differing statements | diff hunks | note |")
    print("|---|---|---|---|---|---|---|---|")
    for i, t in enumerate(s["tests"], 1):
        scope = "in" if t["scope"] == "in" else f"out: {t['scope_reason']}"
        blocks = per.get(t["test"], {}).get("blocks", 0) if t["status"] != "pass" else 0
        note = t["note"].replace("|", "/")
        print(f"| {i} | {t['test']} | {scope} | {ref.get(t['test'], '')} | {t['status']} | {blocks} | "
              f"{t['hunks']} | {note} |")
    print()


if __name__ == "__main__":
    main()
