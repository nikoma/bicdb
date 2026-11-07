#!/usr/bin/env python3
"""Analyze plan-regret measurements (Step 0 of the tiny-optimizer study).

Input: measurements.jsonl written by `cargo run -p bicdb-bench --bin plan-regret`.
Output: report.md + summary.json next to the input.

BicDB has two single-table access-path choosers. The row-set executor that
runs ordinary SELECTs uses a rule-based index locator (most matched index
columns wins, ties broken by probing, full scan only when no index matches);
the cost-based `plan_query` drives EXPLAIN and a few streaming paths. The
harness forces the candidates of the chooser that actually executed.

Policies compared on every query (all scored against the SAME measured
latencies of the forced candidates):

  rule            the executed chooser's own choice (BicDB today)
  heuristic       the cost-based planner's choice without ANALYZE statistics,
                  mapped onto the executed candidates (what EXPLAIN claims)
  fitted          per-index fitted selectivity constants (fit on the TRAIN
                  split) plugged into the planner's no-statistics cost formulas
  calibrated      fitted constants + per-plan-kind latency coefficients fit on
                  TRAIN (latency ~ a_kind + b_kind * rows_fetched) -- the
                  strongest non-neural baseline a model would have to beat
  analyze         the cost-based planner's choice after a fresh ANALYZE
  hybrid          the rule, except that with statistics a candidate with fewer
                  matched columns (discarded without probing) replaces the
                  rule's choice when its estimated rows are HYBRID_RATIO-times
                  smaller (env, default 2); tie probes are kept
  oracle          fastest measured legal candidate

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
HYBRID_RATIO = float(__import__("os").environ.get("HYBRID_RATIO", "2"))


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
            if executed_kind(label(sig)) in ("IndexLookup", "IndexRange") and m["rows_fetched"] is not None:
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
            points[(record["dataset"], executed_kind(label(m["signature"])))].append((m["rows_fetched"], ms))
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
    if executed_kind(label(sig)) == "FullScan":
        return record["rows"]
    if key in selectivities:
        return max(1, round(selectivities[key] * record["rows"]))
    return nostats_rows


def map_planner_label(record, sig):
    """Map a planner signature onto the executed chooser's candidate label."""
    if record["executed_chooser"] == "planner":
        return label(sig)
    kind, index, prefix = sig["kind"], sig["index"], sig["prefix_len"]
    if kind == "FullScan":
        return "FullScan(/0)"
    if kind == "IndexLookup":
        return f"IndexLookup({index}/{prefix})"
    if kind == "IndexRange":
        return f"IndexRange({index}/1)"
    return None


def planner_rows_estimate(record, source, executed_label):
    """The planner's row estimate for the candidate that maps to `executed_label`."""
    decision = record.get(source)
    if not decision:
        return None
    for cand in decision["candidates"]:
        if map_planner_label(record, cand["signature"]) == executed_label:
            return cand["estimated_rows"]
    return None


def executed_kind(lbl):
    return lbl.split("(")[0]


def choose(record, policy, selectivities=None, latency_models=None):
    measured = {label(m["signature"]): m for m in record["measurements"]}
    if policy == "rule":
        return label(record["executed"]["chosen"])
    if policy in ("heuristic", "analyze"):
        decision = record.get("nostats" if policy == "heuristic" else "stats")
        if not decision:
            return label(record["executed"]["chosen"])
        return map_planner_label(record, decision["chosen"])
    if policy == "hybrid":
        # Deterministic hybrid: the executed chooser's candidates and its
        # choice when there are no statistics; with ANALYZE statistics, rank
        # the index candidates by the planner's estimated rows instead of by
        # matched-column count. Never prefers a full scan over an index.
        rule_choice = label(record["executed"]["chosen"])
        if not (record.get("stats") or {}).get("has_statistics"):
            return rule_choice
        # Ties on matched columns were resolved by exact probing, which beats
        # any estimate: keep them. Only candidates the rule discarded WITHOUT
        # probing (fewer matched columns) may override it, and only when the
        # statistics say the rule's choice reads >= 10x more rows.
        def matched(lbl):
            return int(lbl.rsplit("/", 1)[1].rstrip(")"))
        rule_est = planner_rows_estimate(record, "stats", rule_choice)
        if rule_est is None or executed_kind(rule_choice) == "FullScan":
            return rule_choice
        best = (rule_est, rule_choice)
        for lbl in measured:
            if executed_kind(lbl) == "FullScan" or matched(lbl) >= matched(rule_choice):
                continue
            est = planner_rows_estimate(record, "stats", lbl)
            if est is not None and est * HYBRID_RATIO <= rule_est and est < best[0]:
                best = (est, lbl)
        return best[1]
    best, best_cost = None, math.inf
    for lbl, m in measured.items():
        kind = executed_kind(lbl)
        sig = m["signature"]
        planner_est = planner_rows_estimate(record, "nostats", lbl)
        default_rows = planner_est if planner_est is not None else record["rows"]
        rows = fitted_rows(record, sig, selectivities, default_rows)
        if policy == "fitted":
            cost = nostats_cost(kind, rows, record["rows"], 1.0 + rows * 0.10)
        else:  # calibrated
            a, b = latency_models.get((record["dataset"], kind), (None, None))
            if a is None:
                cost = math.inf
            else:
                cost = a + b * rows
        if cost < best_cost:
            best, best_cost = lbl, cost
    return best or label(record["executed"]["chosen"])


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


def natural_metrics(records, condition, key):
    """End-to-end (unforced) latency of the chooser as a client sees it,
    including planning and every probe it ran, against the forced oracle."""
    lat, regret, catastrophic, two_x, oracle_total, missing = [], [], 0, 0, 0.0, 0
    for record in records:
        natural = record.get(key)
        if not natural:
            missing += 1
            continue
        ms = natural[f"{condition}_ms"]
        _, best = oracle(record, condition)
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
        "unmeasured_choice": missing,
    }


def overhead(records, key):
    """Decision-time split for the natural runs (warm medians per query)."""
    out = {}
    for field in ("planner_us", "rule_decision_us", "wasted_probe_us"):
        values = [r[key][field] for r in records if r.get(key) and r[key].get(field) is not None]
        e2e = [r[key]["warm_ms"] * 1000.0 for r in records if r.get(key)]
        out[field] = {
            "p50": pct(values, 50),
            "p95": pct(values, 95),
            "max": max(values) if values else float("nan"),
            "share_of_e2e_pct": 100.0 * sum(values) / sum(e2e) if e2e and sum(e2e) else float("nan"),
        }
    out["overrides"] = sum(bool(r.get(key, {}).get("hybrid_override")) for r in records)
    out["semantic_mismatches"] = sum(not r.get(key, {}).get("semantics_ok", True) for r in records)
    return out


def cardinality_errors(records, selectivities):
    """q-error of the cost-based planner's per-candidate row estimates (and of
    fitted constants) against the rows the corresponding executed access path
    actually fetched, plus the planner's output-cardinality estimate (its
    full-scan row estimate) against the true result size."""
    out = defaultdict(list)
    for record in records:
        for m in record["measurements"]:
            lbl = label(m["signature"])
            if executed_kind(lbl) not in ("IndexLookup", "IndexRange") or m["rows_fetched"] is None:
                continue
            for source in ("nostats", "stats"):
                est = planner_rows_estimate(record, source, lbl)
                if est is not None:
                    out[f"planner_{source}:index_access"].append(q_error(est, m["rows_fetched"]))
            if selectivities is not None:
                planner_est = planner_rows_estimate(record, "nostats", lbl)
                est = fitted_rows(record, m["signature"], selectivities,
                                  planner_est if planner_est is not None else record["rows"])
                out["fitted:index_access"].append(q_error(est, m["rows_fetched"]))
        if record["template"] != "status_order_limit":
            for source in ("nostats", "stats"):
                decision = record.get(source)
                if not decision:
                    continue
                for cand in decision["candidates"]:
                    if cand["signature"]["kind"] == "FullScan":
                        out[f"planner_{source}:query_output"].append(
                            q_error(cand["estimated_rows"], record["result_rows"]))
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
        if not (r["natural_result_matches"] and r["stats_natural_result_matches"] and r["reference_forced"])
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
            "rule": lambda r: choose(r, "rule"),
            "heuristic": lambda r: choose(r, "heuristic"),
            "fitted": lambda r: choose(r, "fitted", selectivities),
            "calibrated": lambda r, m=models: choose(r, "calibrated", selectivities, m),
            "analyze": lambda r: choose(r, "analyze"),
            "hybrid": lambda r: choose(r, "hybrid"),
            "oracle": lambda r, c=condition: oracle(r, c)[0],
        }
        summary["policies"][condition] = {
            name: policy_metrics(test, condition, chooser) for name, chooser in choosers.items()
        }
        if any("natural_stats" in r for r in test):
            summary["policies"][condition]["rule_e2e"] = natural_metrics(test, condition, "natural_stats")
            summary["policies"][condition]["hybrid_e2e"] = natural_metrics(test, condition, "natural_hybrid")
        if any("natural_stats" in r for r in records):
            for ds in sorted(by_dataset):
                ds_test = [r for r in test if r["dataset"] == ds]
                summary["per_dataset"].setdefault(ds, {}).setdefault(condition + "_e2e", {
                    "rule_e2e": natural_metrics(ds_test, condition, "natural_stats"),
                    "hybrid_e2e": natural_metrics(ds_test, condition, "natural_hybrid"),
                })
        summary["policies"][condition + "_all_queries"] = {
            name: policy_metrics(records, condition, choosers[name])
            for name in ("rule", "heuristic", "analyze", "hybrid", "oracle")
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
                "rule": policy_metrics(rs, condition, choosers["rule"]),
                "heuristic": policy_metrics(rs, condition, choosers["heuristic"]),
                "analyze": policy_metrics(rs, condition, choosers["analyze"]),
                "hybrid": policy_metrics(rs, condition, choosers["hybrid"]),
            }
            for t, rs in sorted(templates.items())
        }
    summary["executed_chooser_counts"] = dict(
        sorted(defaultdict(int, {}).items()))
    counts = defaultdict(int)
    for r in records:
        counts[r["executed_chooser"]] += 1
    summary["executed_chooser_counts"] = dict(counts)
    rule_changed = sum(label(r["executed"]["chosen"]) != label(r["executed_after_analyze"]["chosen"]) for r in records)
    summary["rule_choice_changed_by_analyze"] = rule_changed
    unmapped = defaultdict(int)
    for r in records:
        for source in ("nostats", "stats"):
            if r.get(source) and map_planner_label(r, r[source]["chosen"]) not in {label(m["signature"]) for m in r["measurements"]}:
                unmapped[source] += 1
    summary["planner_choice_not_executable"] = dict(unmapped)
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
    lines.append(f"Executed chooser: {summary['executed_chooser_counts']}; rule choice changed by ANALYZE: "
                 f"{summary['rule_choice_changed_by_analyze']}; planner choices with no executable counterpart: "
                 f"{summary['planner_choice_not_executable']}.\n")
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
    lines.append("| dataset | rule | heuristic | fitted | calibrated | analyze | hybrid |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for ds, conds in summary["per_dataset"].items():
        m = conds["warm"]
        cells = [f"{fmt(m[p]['workload_regret_pct'], 1)}% / {m[p]['catastrophic_over_10x']}"
                 for p in ("rule", "heuristic", "fitted", "calibrated", "analyze", "hybrid")]
        lines.append(f"| {ds} | " + " | ".join(cells) + " |")
    lines.append("\n## Per template — warm, all queries (workload regret %, >10×)\n")
    lines.append("| template | n | rule | heuristic | analyze | hybrid |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for t, m in summary["per_template_heuristic_regret"]["warm"].items():
        cells = [f"{fmt(m[p]['workload_regret_pct'], 1)}% ({m[p]['catastrophic_over_10x']})"
                 for p in ("rule", "heuristic", "analyze", "hybrid")]
        lines.append(f"| {t} | {m['rule']['queries']} | " + " | ".join(cells) + " |")
    lines.append("\n## Cardinality estimation q-error (test split)\n")
    lines.append("| estimator:target | n | median | p90 | p95 | max | share >10× |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for key, m in summary["cardinality_q_error_test"].items():
        lines.append(f"| {key} | {m['n']} | {fmt(m['median'])} | {fmt(m['p90'])} | {fmt(m['p95'])} | "
                     f"{fmt(m['max'])} | {fmt(100 * m['share_over_10x'], 1)}% |")
    if any("natural_stats" in r for r in records):
        summary["natural_overhead"] = {
            key: overhead(records, key) for key in ("natural_nostats", "natural_stats", "natural_hybrid")
        }
        lines.append("\n## End-to-end (unforced) per dataset — warm, test split\n")
        lines.append("| dataset | rule e2e regret / >10× | hybrid e2e regret / >10× |")
        lines.append("|---|---:|---:|")
        for ds, conds in summary["per_dataset"].items():
            e2e = conds.get("warm_e2e")
            if not e2e:
                continue
            cells = [f"{fmt(e2e[p]['workload_regret_pct'], 1)}% / {e2e[p]['catastrophic_over_10x']}"
                     for p in ("rule_e2e", "hybrid_e2e")]
            lines.append(f"| {ds} | " + " | ".join(cells) + " |")
        lines.append("\n## Decision overhead in natural runs (µs, warm)\n")
        lines.append("| run | field | p50 | p95 | max | share of end-to-end |")
        lines.append("|---|---|---:|---:|---:|---:|")
        for key, fields in summary["natural_overhead"].items():
            for field in ("planner_us", "rule_decision_us", "wasted_probe_us"):
                m = fields[field]
                lines.append(f"| {key} | {field} | {fmt(m['p50'], 1)} | {fmt(m['p95'], 1)} | "
                             f"{fmt(m['max'], 1)} | {fmt(m['share_of_e2e_pct'], 2)}% |")
            lines.append(f"| {key} | overrides / mismatches | {fields['overrides']} | {fields['semantic_mismatches']} | | |")
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
