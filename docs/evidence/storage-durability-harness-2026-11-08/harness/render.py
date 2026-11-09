#!/usr/bin/env python3
"""Render the per-fix results table (Markdown) from summarize.py output.

Usage: render.py <summary-rows.json> <fix> [<fix> ...]
"""

import json
import sys


def verdict(row):
    v = []
    if row["fail"]:
        v.append(f"{row['fail']} FAIL")
    if row["reopen_failed"]:
        v.append(f"{row['reopen_failed']} reopen refused")
    if row["harness_error"]:
        v.append(f"{row['harness_error']} harness error")
    return ", ".join(v) or "all pass"


def injection(row):
    if row["kind"] in ("failio", "crash", "recoverycrash"):
        sites = ", ".join(f"`{s}`" for s in row["sites"])
        n = row["n_range"]
        n = f"n={n[0]}" if n[0] == n[1] else f"n={n[0]}..{n[1]}"
        extra = []
        if row["persistent"]:
            extra.append("persistent (`+`)")
        if row["also_fail"]:
            extra.append(f"plus `{row['also_fail']}`")
        if row["power_loss_emulated"]:
            extra.append("power loss emulated")
        if row["end"] == "term":
            extra.append("graceful shutdown")
        what = {"failio": "EIO", "crash": "abort", "recoverycrash": "abort during recovery"}[row["kind"]]
        return f"{what} at {sites} {n}" + (f" ({'; '.join(extra)})" if extra else "")
    if row["kind"] == "torn":
        return f"torn tail appended to `{row['target'] or 'any frame log'}`"
    if row["kind"] == "midfile":
        return f"one byte flipped mid-file in `{row['target'] or 'any frame log'}`"
    if row["control"]:
        return f"negative control `{row['control']}`"
    return "SIGKILL at a random time"


def vstr(codes):
    if not codes:
        return "n/a"
    return " ".join(f"{k}:{v}" for k, v in sorted(codes.items(), key=lambda kv: str(kv[0])))


def main():
    rows = json.load(open(sys.argv[1]))
    fixes = {int(f) for f in sys.argv[2:]}
    print("| Boundary | Mode / profile | Injected failure | Trials | Fired | Result | Acked | Failed (absent) | 08007 committed / not | In flight: present / absent | Violations | Verifier exit codes pre / pre2 / post |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for row in rows:
        if row["fix"] not in fixes:
            continue
        res = row["08007_resolved"]
        infl = row["inflight_outcomes"]
        viol = ", ".join(f"{k} x{v}" for k, v in row["violations"].items()) or "none"
        post = vstr({k: v for k, v in [("0", row["verify_post_clean"]),
                                         ("non-0", row["verify_post_ran"] - row["verify_post_clean"])] if v}) \
            if row["verify_post_ran"] else "n/a"
        print(f"| `{row['boundary']}` | {row['mode']} / {row['profile']} | {injection(row)} | {row['trials']} | "
              f"{row['fired']} | {verdict(row)} | {row['acknowledged_ok']} | {row['failed_non_08007']} | "
              f"{res.get('committed', 0)} / {res.get('not_committed', 0)}"
              f"{' / other ' + str(res['other']) if res.get('other') else ''} | "
              f"{infl.get('present', 0)} / {infl.get('absent', 0)} | {viol} | "
              f"{vstr(row['verify_pre_exit_codes'])} / {vstr(row['verify_pre2_exit_codes'])} / {post} |")


if __name__ == "__main__":
    main()
