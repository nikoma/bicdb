#!/usr/bin/env python3
"""Analyze plan-regret measurements (Step 0 of the tiny-optimizer study).

Input: measurements.jsonl written by `cargo run -p bicdb-bench --bin plan-regret`.
Output: report.md + summary.json next to the input.

Policies compared on every query (all scored against the SAME measured
latencies of the forced candidates):

  heuristic   planner choice without ANALYZE statistics (BicDB today)
  fitted      per-index fitted selectivity constants (fit on the TRAIN split)
              plugged into the planner's own no-statistics cost formulas
  calibrated  fitted constants + per-plan-kind latency coefficients fit on TRAIN
              (latency ~ a_kind + b_kind * rows_fetched) -- the strongest
              non-neural baseline a model would have to beat
  analyze     planner choice after a fresh ANALYZE
  oracle      fastest measured legal candidate

Fitted/calibrated policies are only scored on the TEST split; every report
table that compares them uses the test split for all policies.
"""

import json
import math
import random
import statistics
import sys
from collections import defaultdict
from pathlib import Path

CONDITIONS = ("warm", "flushed")


def load(path):
    with open(path) as handle:
        return [json.loads(line) for line in handle if line.strip()]


def label(sig):
    return sig["label"]


def q_error(estimate, actual):
    estimate, actual = max(float(estimate), 1.0), max(float(actual), 1.0)
    return max(estimate / actual, actual / estimate)


def pct(values, p):
    if not values:
        return float("nan")
    values = sorted(values)
    k = (len(values) - 1) * p / 100.0
    lo, hi = math.floor(k), math.ceil(k)
    return values[lo] if lo == hi else values[lo] + (values[hi] - values[lo]) * (k - lo)


def geo_mean(values):
    values = [max(v, 1e-12) for v in values]
    return math.exp(sum(math.log(v) for v in values) / len(values))


# ------------------------------------------------------------- cost formulas
# Mirrors crates/bicdb-sql/src/engine/records_plan.rs (no-statistics branch).

def nostats_cost(kind, rows, total_rows, fallback):
    if kind == "FullScan":
        return float(total_rows)
    if kind == "IndexLookup":
        return 1.0 + rows * 0.10
    if kind == "IndexRange":
        return 1.0 + rows * 0.25
    return fallback


def fit_selectivities(train):
    """Per (dataset, signature) geometric-mean selectivity of rows fetched."""
    samples = defaultdict(list)
    for record in train:
        total = record["rows"]
        for m in record["measurements"]:
            sig = m["signature"]
            if sig["kind"] in ("IndexLookup", "IndexRange") and m["rows_fetched"] is not None:
                samples[(record["dataset"], label(sig))].append(max(m["rows_fetched"], 1) / total)
    return {key: geo_mean(values) for key, values in samples.items()}


def fit_latency(train, condition):
    """Least squares latency = a + b * rows_fetched per (dataset, kind)."""
    points = defaultdict(list)
    for record in train:
        for m in record["measurements"]:
            if m["rows_fetched"] is None or not m["semantics_ok"]:
                continue
            ms = m[f"{condition}_ms"]
            points[(record["dataset"], m["signature"]["kind"])].append((m["rows_fetched"], ms))
    models = {}
    for key, pts in points.items():
        xs = [x for x, _ in pts]
        ys = [y for _, y in pts]
        if len(pts) < 2 or len(set(xs)) < 2:
            models[key] = (statistics.median(ys), 0.0)
            continue
        mx, my = statistics.fmean(xs), statistics.fmean(ys)
        sxx = sum((x - mx) ** 2 for x in xs)
        sxy = sum((x - mx) * (y - my) for x, y in pts)
        b = max(sxy / sxx, 0.0)
        models[key] = (max(my - b * mx, 0.0), b)
    return models


def fitted_rows(record, sig, selectivities, nostats_rows):
    key = (record["dataset"], label(sig))
    if sig["kind"] == "FullScan":
        return record["rows"]
    if key in selectivities:
        return max(1, round(selectivities[key] * record["rows"]))
    return nostats_rows


def choose(record, policy, selectivities=None, latency_models=None):
    measured = {label(m["signature"]): m for m in record["measurements"]}
    if policy == "heuristic":
        return label(record["nostats"]["chosen"])
    if policy == "analyze":
        return label(record["stats"]["chosen"])
    if policy == "oracle":
        raise ValueError("oracle is condition-specific")
    best, best_cost = None, math.inf
    for cand in record["nostats"]["candidates"]:
        sig = cand["signature"]
        if label(sig) not in measured:
            continue
        rows = fitted_rows(record, sig, selectivities, cand["estimated_rows"])
        if policy == "fitted":
            cost = nostats_cost(sig["kind"], rows, record["rows"], cand["estimated_cost"])
        else:  # calibrated
            a, b = latency_models.get((record["dataset"], sig["kind"]), (None, None))
            if a is None:
                cost = nostats_cost(sig["kind"], rows, record["rows"], cand["estimated_cost"])
                cost *= 1e6  # unknown kind: never preferred over a calibrated one
            else:
                cost = a + b * rows
        if cost < best_cost:
            best, best_cost = label(sig), cost
    return best or label(record["nostats"]["chosen"])


def latency(record, chosen, condition):
    for m in record["measurements"]:
        if label(m["signature"]) == chosen:
            return m[f"{condition}_ms"]
    return None


def oracle(record, condition):
    best = min(
        (m for m in record["measurements"] if m["semantics_ok"]),
        key=lambda m: m[f"{condition}_ms"],
    )
    return label(best["signature"]), best[f"{condition}_ms"]


def policy_metrics(records, condition, chooser):
    lat, regret, catastrophic, two_x, unmeasured = [], [], 0, 0, 0
    oracle_total = 0.0
    for record in records:
        choice = chooser(record)
        ms = latency(record, choice, condition)
        _, best = oracle(record, condition)
        if ms is None:
            unmeasured += 1
            continue
        lat.append(ms)
        oracle_total += best
        ratio = ms / max(best, 1e-9)
        regret.append(ratio)
        catastrophic += ratio > 10.0
        two_x += ratio > 2.0
    total = sum(lat)
    return {
        "queries": len(lat),
        "p50_ms": pct(lat, 50),
        "p95_ms": pct(lat, 95),
        "total_ms": total,
        "workload_regret_pct": 100.0 * (total / oracle_total - 1.0) if oracle_total else float("nan"),
        "median_query_regret": pct(regret, 50),
        "p95_query_regret": pct(regret, 95),
        "max_query_regret": max(regret) if regret else float("nan"),
        "over_2x": two_x,
        "catastrophic_over_10x": catastrophic,
        "unmeasured_choice": unmeasured,
    }


def cardinality_errors(records, selectivities):
    """q-error of per-candidate row estimates vs rows actually fetched, plus
    the full-table output-cardinality estimate vs the true result size."""
    out = defaultdict(list)
    for record in records:
        measured = {label(m["signature"]): m for m in record["measurements"]}
        for source in ("nostats", "stats"):
            for cand in record[source]["candidates"]:
                sig = cand["signature"]
                m = measured.get(label(sig))
                if sig["kind"] in ("IndexLookup", "IndexRange") and m and m["rows_fetched"] is not None:
                    out[f"{source}:index_access"].append(q_error(cand["estimated_rows"], m["rows_fetched"]))
                    if source == "nostats" and selectivities is not None:
                        est = fitted_rows(record, sig, selectivities, cand["estimated_rows"])
                        out["fitted:index_access"].append(q_error(est, m["rows_fetched"]))
                if sig["kind"] == "FullScan" and record["template"] != "status_order_limit":
                    out[f"{source}:query_output"].append(q_error(cand["estimated_rows"], record["result_rows"]))
    return {
        key: {
            "n": len(v),
            "median": pct(v, 50),
            "p90": pct(v, 90),
            "p95": pct(v, 95),
            "max": max(v),
            "share_over_10x": sum(x > 10 for x in v) / len(v),
        }
        for key, v in sorted(out.items())
    }


def noise(records):
    """Relative spread (MAD / median) of repeated measurements."""
    spread = {c: [] for c in CONDITIONS}
    for record in records:
        for m in record["measurements"]:
            for c in CONDITIONS:
                samples = m[f"{c}_samples"]
                if len(samples) >= 3:
                    med = statistics.median(samples)
                    mad = statistics.median(abs(s - med) for s in samples)
                    spread[c].append(mad / max(med, 1e-9))
    return {c: {"median_rel_mad": pct(v, 50), "p95_rel_mad": pct(v, 95)} for c, v in spread.items()}


def fmt(value, digits=2):
    if isinstance(value, float):
        if math.isnan(value):
            return "n/a"
        return f"{value:,.{digits}f}"
    return f"{value:,}" if isinstance(value, int) else str(value)


def main():
    path = Path(sys.argv[1] if len(sys.argv) > 1 else "measurements.jsonl")
    records = load(path)
    bad_semantics = [
        (r["dataset"], r["query_id"], label(m["signature"]))
        for r in records
        for m in r["measurements"]
        if not m["semantics_ok"]
    ]
    unforced = [
        (r["dataset"], r["query_id"], label(m["signature"]))
        for r in records
        for m in r["measurements"]
        if not m["forced_ok"]
    ]
    natural_mismatch = [
        (r["dataset"], r["query_id"]) for r in records
        if not (r["natural_result_matches"] and r["stats_natural_result_matches"])
    ]

    rng = random.Random(7)
    by_dataset = defaultdict(list)
    for r in records:
        by_dataset[r["dataset"]].append(r)
    train, test = [], []
    for ds, rs in sorted(by_dataset.items()):
        rs = sorted(rs, key=lambda r: r["query_id"])
        rng.shuffle(rs)
        half = len(rs) // 2
        train += rs[:half]
        test += rs[half:]

    selectivities = fit_selectivities(train)
    summary = {
        "records": len(records),
        "train": len(train),
        "test": len(test),
        "semantic_mismatches": bad_semantics,
        "candidates_not_forceable": unforced,
        "natural_result_mismatches": natural_mismatch,
        "noise": noise(records),
        "cardinality_q_error_test": cardinality_errors(test, selectivities),
        "cardinality_q_error_all": cardinality_errors(records, None),
        "policies": {},
        "per_dataset": {},
        "per_template_heuristic_regret": {},
        "planning_overhead": {},
    }
    for condition in CONDITIONS:
        models = fit_latency(train, condition)
        choosers = {
            "heuristic": lambda r: choose(r, "heuristic"),
            "fitted": lambda r: choose(r, "fitted", selectivities),
            "calibrated": lambda r, m=models: choose(r, "calibrated", selectivities, m),
            "analyze": lambda r: choose(r, "analyze"),
            "oracle": lambda r, c=condition: oracle(r, c)[0],
        }
        summary["policies"][condition] = {
            name: policy_metrics(test, condition, chooser) for name, chooser in choosers.items()
        }
        summary["policies"][condition + "_all_queries"] = {
            name: policy_metrics(records, condition, choosers[name])
            for name in ("heuristic", "analyze", "oracle")
        }
        for ds, rs in sorted(by_dataset.items()):
            ds_test = [r for r in test if r["dataset"] == ds]
            summary["per_dataset"].setdefault(ds, {})[condition] = {
                name: policy_metrics(ds_test, condition, chooser) for name, chooser in choosers.items()
            }
        templates = defaultdict(list)
        for r in records:
            templates[r["template"]].append(r)
        summary["per_template_heuristic_regret"][condition] = {
            t: {
                "heuristic": policy_metrics(rs, condition, choosers["heuristic"]),
                "analyze": policy_metrics(rs, condition, choosers["analyze"]),
            }
            for t, rs in sorted(templates.items())
        }
    plan_ms = [r["plan_only_ms"] for r in records]
    stats_plan_ms = [r["stats_plan_only_ms"] for r in records]
    oracle_ms = [oracle(r, "warm")[1] for r in records]
    summary["planning_overhead"] = {
        "nostats_plan_only_ms_p50": pct(plan_ms, 50),
        "nostats_plan_only_ms_p95": pct(plan_ms, 95),
        "stats_plan_only_ms_p50": pct(stats_plan_ms, 50),
        "stats_plan_only_ms_p95": pct(stats_plan_ms, 95),
        "oracle_warm_ms_p50": pct(oracle_ms, 50),
        "analyze_ms": {r["dataset"]: r["analyze_ms"] for r in records},
    }

    out_dir = path.parent
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str))

    lines = [f"# Plan-regret Step 0 report\n", f"Source: `{path}` — {len(records)} queries "
             f"({len(train)} train / {len(test)} test).\n"]
    lines.append(f"Semantic mismatches (forced plan ≠ full-scan result): **{len(bad_semantics)}**; "
                 f"candidates that could not be forced: **{len(unforced)}**; "
                 f"natural-plan mismatches: **{len(natural_mismatch)}**.\n")
    for condition in CONDITIONS:
        lines.append(f"\n## Policies — {condition} cache, test split\n")
        lines.append("| policy | p50 ms | p95 ms | total ms | workload regret | p95 query regret | max regret | >2× | >10× |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
        for name, m in summary["policies"][condition].items():
            lines.append(
                f"| {name} | {fmt(m['p50_ms'], 3)} | {fmt(m['p95_ms'], 3)} | {fmt(m['total_ms'], 1)} | "
                f"{fmt(m['workload_regret_pct'], 1)}% | {fmt(m['p95_query_regret'])}× | "
                f"{fmt(m['max_query_regret'])}× | {m['over_2x']} | {m['catastrophic_over_10x']} |"
            )
    lines.append("\n## Per dataset — warm, test split (workload regret % / >10× count)\n")
    lines.append("| dataset | heuristic | fitted | calibrated | analyze |")
    lines.append("|---|---:|---:|---:|---:|")
    for ds, conds in summary["per_dataset"].items():
        m = conds["warm"]
        cells = [f"{fmt(m[p]['workload_regret_pct'], 1)}% / {m[p]['catastrophic_over_10x']}"
                 for p in ("heuristic", "fitted", "calibrated", "analyze")]
        lines.append(f"| {ds} | " + " | ".join(cells) + " |")
    lines.append("\n## Per template — warm, all queries (workload regret %, >10×)\n")
    lines.append("| template | n | heuristic | analyze |")
    lines.append("|---|---:|---:|---:|")
    for t, m in summary["per_template_heuristic_regret"]["warm"].items():
        h, a = m["heuristic"], m["analyze"]
        lines.append(f"| {t} | {h['queries']} | {fmt(h['workload_regret_pct'], 1)}% ({h['catastrophic_over_10x']}) | "
                     f"{fmt(a['workload_regret_pct'], 1)}% ({a['catastrophic_over_10x']}) |")
    lines.append("\n## Cardinality estimation q-error (test split)\n")
    lines.append("| estimator:target | n | median | p90 | p95 | max | share >10× |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for key, m in summary["cardinality_q_error_test"].items():
        lines.append(f"| {key} | {m['n']} | {fmt(m['median'])} | {fmt(m['p90'])} | {fmt(m['p95'])} | "
                     f"{fmt(m['max'])} | {fmt(100 * m['share_over_10x'], 1)}% |")
    po = summary["planning_overhead"]
    lines.append("\n## Planning overhead\n")
    lines.append(f"EXPLAIN-only planning p50 {fmt(po['nostats_plan_only_ms_p50'], 3)} ms "
                 f"(p95 {fmt(po['nostats_plan_only_ms_p95'], 3)}) without stats, "
                 f"{fmt(po['stats_plan_only_ms_p50'], 3)} ms with stats; oracle query p50 "
                 f"{fmt(po['oracle_warm_ms_p50'], 3)} ms.\n")
    nz = summary["noise"]
    lines.append(f"Measurement noise (median relative MAD): warm {fmt(100 * nz['warm']['median_rel_mad'], 1)}%, "
                 f"flushed {fmt(100 * nz['flushed']['median_rel_mad'], 1)}%.\n")
    (out_dir / "report.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
