#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import os
import re
import statistics
import sys
from pathlib import Path


BAD_SERVER_PATTERN = re.compile(
    r"panic|corruption|No space|MEMORY GUARD TRIPPED|auto-checkpoint .* error",
    re.IGNORECASE,
)

LANES = ("default-durable", "tuned-durable", "cpu-ceiling")
DURABLE_LANES = {"default-durable", "tuned-durable"}
KEEPER_GATES = {
    "throughput_min_relative_gain": 0.02,
    "rss_max_relative_growth": 0.05,
    "wal_max_relative_growth": 0.05,
    "p99_max_relative_growth": 0.10,
}

# Two-sided 95% Student-t critical values (0.975 quantile), indexed by
# degrees of freedom. Campaigns default to six pairs (df=5).
STUDENT_T_95 = {
    1: 12.706205,
    2: 4.302653,
    3: 3.182446,
    4: 2.776445,
    5: 2.570582,
    6: 2.446912,
    7: 2.364624,
    8: 2.306004,
    9: 2.262157,
    10: 2.228139,
    11: 2.200985,
    12: 2.178813,
    13: 2.160369,
    14: 2.144787,
    15: 2.131450,
    16: 2.119905,
    17: 2.109816,
    18: 2.100922,
    19: 2.093024,
    20: 2.085963,
    21: 2.079614,
    22: 2.073873,
    23: 2.068658,
    24: 2.063899,
    25: 2.059539,
    26: 2.055529,
    27: 2.051831,
    28: 2.048407,
    29: 2.045230,
    30: 2.042272,
}


def read_text(path: Path) -> str:
    try:
        return path.read_text(errors="replace")
    except FileNotFoundError:
        return ""


def corrected_client_outcomes(text, expected_vus):
    rows = [list(map(int, row)) for row in re.findall(
        r"BICDB_OUTCOMES (\d+) (\d+) (\d+) (\d+) (\d+) (\d+)(?=\s|$)", text
    )]
    positions = sorted(row[0] for row in rows)
    expected = list(range(2, expected_vus + 2)) if isinstance(expected_vus, int) else []
    complete = bool(expected) and positions == expected
    consistent = all(calls == positive + invalid + other for _, calls, positive, invalid, other, _payment in rows)
    return {
        "complete": complete and consistent,
        "positions": positions,
        **{key: sum(row[i] for row in rows) for i, key in enumerate(
            ("neword", "positive", "invalid", "other", "payment"), start=1
        )},
    }


def read_int(path: Path):
    value = read_text(path).strip()
    try:
        return int(value)
    except ValueError:
        return None


def read_first_int(raw: Path, names: tuple[str, ...]):
    for name in names:
        path = raw / name
        if path.exists():
            return read_int(path)
    return None


def nested(value, *keys):
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value


def read_csv_row(path: Path) -> dict[str, str]:
    try:
        with path.open(newline="") as handle:
            return next(csv.DictReader(handle), {})
    except (FileNotFoundError, StopIteration):
        return {}


def int_field(row: dict[str, str], name: str):
    try:
        return int(row[name])
    except (KeyError, TypeError, ValueError):
        return None


def delta(before: dict[str, str], after: dict[str, str], name: str):
    start, end = int_field(before, name), int_field(after, name)
    return None if start is None or end is None else end - start


def parse_samples(path: Path):
    try:
        with path.open(newline="") as handle:
            rows = [{key: int(value or 0) for key, value in row.items()} for row in csv.DictReader(handle)]
    except FileNotFoundError:
        rows = []
    if not rows:
        return {}, []
    first, last = rows[0], rows[-1]
    ticks = max(1, int(os.sysconf("SC_CLK_TCK")))
    elapsed_ns = max(1, last["epoch_ns"] - first["epoch_ns"])
    user_ticks = max(0, last["utime_ticks"] - first["utime_ticks"])
    system_ticks = max(0, last["stime_ticks"] - first["stime_ticks"])
    cpu_ns = (user_ticks + system_ticks) * 1_000_000_000 // ticks
    summary = {
        "sample_count": len(rows),
        "elapsed_ns": elapsed_ns,
        "user_ns": user_ticks * 1_000_000_000 // ticks,
        "system_ns": system_ticks * 1_000_000_000 // ticks,
        "mean_cores": cpu_ns / elapsed_ns,
        "rss_peak_bytes": max(row["rss_kb"] for row in rows) * 1024,
        "rss_end_bytes": last["rss_kb"] * 1024,
        "rss_hwm_peak_bytes": max(row["rss_hwm_kb"] for row in rows) * 1024,
        "threads_peak": max(row["threads"] for row in rows),
        "minor_faults": max(0, last["minflt"] - first["minflt"]),
        "major_faults": max(0, last["majflt"] - first["majflt"]),
        "read_bytes": max(0, last["read_bytes"] - first["read_bytes"]),
        "write_bytes": max(0, last["write_bytes"] - first["write_bytes"]),
        "mem_available_min_bytes": min(row["mem_available_kb"] for row in rows) * 1024,
        "fs_available_min_bytes": min(row["fs_available_kb"] for row in rows) * 1024,
    }
    return summary, rows


def parse_perf(path: Path):
    events = {}
    for line in read_text(path).splitlines():
        try:
            item = json.loads(line.rstrip(","))
        except json.JSONDecodeError:
            continue
        name = item.get("event")
        raw = str(item.get("counter-value", "")).replace(",", "")
        try:
            value = float(raw)
        except ValueError:
            value = None
        if name:
            events[name] = value
    return events


def parse_allocations(path: Path):
    text = read_text(path)
    count = re.search(r"@allocation_count:\s*(\d+)", text)
    size = re.search(r"@allocated_bytes:\s*(\d+)", text)
    return {
        "allocation_count": int(count.group(1)) if count else None,
        "allocated_bytes": int(size.group(1)) if size else None,
        "profiled_separately": bool(text),
    }


def parse_hammerdb(text: str):
    result = re.search(r"TEST RESULT\s*:\s*System achieved (\d+) NOPM from (\d+).*?TPM", text)
    failed = len(re.findall(r"FINISHED FAILED", text))
    succeeded = len(re.findall(r"FINISHED SUCCESS", text))
    created = re.search(r"(\d+)\s+Virtual Users Created with Monitor VU", text, re.IGNORECASE)
    return {
        "reported_nopm": int(result.group(1)) if result else None,
        "reported_tpm": int(result.group(2)) if result else None,
        "created_vus": int(created.group(1)) if created else None,
        "successful_vus": succeeded,
        "failed_vus": failed,
        "all_complete": "ALL VIRTUAL USERS COMPLETE" in text,
    }


def parse_latencies(raw: Path):
    values = {}
    pattern = re.compile(
        r">>>>> PROC: (?P<name>[A-Z_]+)\s+"
        r"CALLS:\s*(?P<calls>\d+).*?MAX:\s*(?P<max>[0-9.]+)ms.*?"
        r"P99:\s*(?P<p99>[0-9.]+)ms\s+P95:\s*(?P<p95>[0-9.]+)ms\s+"
        r"P50:\s*(?P<p50>[0-9.]+)ms",
        re.IGNORECASE | re.DOTALL,
    )
    profile = read_text(raw / "hammer-tmp" / "hdbxtprofile.log")
    summary_at = profile.rfind(">>>>> SUMMARY OF")
    if summary_at >= 0:
        profile = profile[summary_at:]
    for match in pattern.finditer(profile):
        key = match.group("name").strip().lower()
        values[key] = {
            "calls": int(match.group("calls")),
            "p50_ns": int(float(match.group("p50")) * 1_000_000),
            "p95_ns": int(float(match.group("p95")) * 1_000_000),
            "p99_ns": int(float(match.group("p99")) * 1_000_000),
            "max_ns": int(float(match.group("max")) * 1_000_000),
        }
    return values


def parse_proc_mix(server_text: str):
    lines = [line for line in server_text.splitlines() if "proc_mix neword=" in line]
    if not lines:
        return {}, {}
    line = lines[-1]
    procedures = {
        key: int(value)
        for key, value in re.findall(r"\b(neword|payment|delivery|ostat|slev)=(\d+)", line)
    }
    failures = {
        key: int(value)
        for key, value in re.findall(
            r"\b(serialization_failure|deadlock_detected|no_data_found|other)=(\d+)", line
        )
    }
    return procedures, failures


def parse_checkpoints(server_text: str):
    lines = [line for line in server_text.splitlines() if "bicdb auto-checkpoint:" in line]
    records = []
    pattern = re.compile(
        r"wal (\d+) MB -> (\d+) MB .*?delta (\d+) records, "
        r"phase-0 (\d+) ms, phase-1 (\d+) ms, lock-acquire (\d+) ms, "
        r"truncate (\d+) ms, total (\d+) ms"
    )
    for line in lines:
        match = pattern.search(line)
        if match:
            before, after, dirty, phase0, phase1, acquire, truncate, total = map(int, match.groups())
            records.append(
                {"wal_before_bytes": before * 1024 * 1024,
                 "wal_after_bytes": after * 1024 * 1024,
                 "dirty_records": dirty, "phase0_ns": phase0 * 1_000_000,
                 "phase1_ns": phase1 * 1_000_000,
                 "lock_acquire_ns": acquire * 1_000_000,
                 "truncate_ns": truncate * 1_000_000, "total_ns": total * 1_000_000}
            )
    failures = len(re.findall(r"auto-checkpoint .* error", server_text, re.IGNORECASE))
    return {"attempted": len(lines) + failures, "completed": len(records), "failed": failures,
            "cycles": records}


def trial(trial_dir: Path):
    raw = trial_dir / "raw"
    metadata = json.loads(read_text(trial_dir / "metadata.json"))
    before, after = read_csv_row(raw / "stats_before.csv"), read_csv_row(raw / "stats_after.csv")
    dsum_before, dsum_after = read_int(raw / "dsum_before"), read_int(raw / "dsum_after")
    dsum_delta = None if dsum_before is None or dsum_after is None else dsum_after - dsum_before
    total_minutes = metadata["workload"]["rampup_minutes"] + metadata["workload"]["duration_minutes"]
    dsum_nopm = None if dsum_delta is None else dsum_delta / max(1, total_minutes)
    server_text = read_text(raw / "server.log")
    hammer = parse_hammerdb(read_text(raw / "hammerdb.log"))
    hammer_exit_code = read_first_int(
        raw, ("hammerdb_exit_code", "hammerdb_exit_status", "hammer_exit_status")
    )
    expected_active_vus = read_first_int(raw, ("hammerdb_expected_active_vus",))
    if expected_active_vus is None:
        expected_active_vus = nested(metadata, "workload", "vu")
    expected_total_vus = (
        expected_active_vus + 1 if isinstance(expected_active_vus, int) else None
    )
    hammer.update(
        {
            "exit_code": hammer_exit_code,
            "expected_active_vus": expected_active_vus,
            "expected_total_vus": expected_total_vus,
        }
    )
    resources, _ = parse_samples(raw / "samples.csv")
    procedures, handled_failures = parse_proc_mix(server_text)
    procedure_fields = {
        "neword": "proc_neword", "payment": "proc_payment",
        "delivery": "proc_delivery", "ostat": "proc_orderstatus",
        "slev": "proc_stocklevel",
    }
    exact_procedures = {
        name: value for name, field in procedure_fields.items()
        if (value := delta(before, after, field)) is not None
    }
    if exact_procedures:
        procedures = exact_procedures
    committed_payments = delta(
        read_csv_row(raw / "payment_before.csv"),
        read_csv_row(raw / "payment_after.csv"),
        "payment_history_rows",
    )
    # CALL success can include a procedure's handled ROLLBACK. This gate
    # measures completed business work, not just protocol acknowledgements.
    # Exact counters are required; periodic trace snapshots can miss the tail.
    business_completion = {}
    client_outcomes = None
    if nested(metadata, "workload", "client_outcomes"):
        client_outcomes = corrected_client_outcomes(read_text(raw / "hammerdb.log"), expected_active_vus)
    for name, committed in (("payment", committed_payments), ("neword", dsum_delta)):
        calls = exact_procedures.get(name)
        expected_rollbacks = 0
        client_verified = True
        if client_outcomes is not None:
            expected_rollbacks = client_outcomes["invalid"] if name == "neword" else 0
            client_verified = client_outcomes["complete"] and calls == client_outcomes[name]
            if name == "neword":
                client_verified = client_verified and client_outcomes["other"] == 0 and committed == client_outcomes["positive"]
        verified = (
            calls is not None and calls > 0 and committed is not None
            and committed == calls - expected_rollbacks and client_verified
        )
        business_completion[name] = {
            "calls": calls, "committed": committed,
            "call_commit_gap": None if calls is None or committed is None else calls - committed,
            "expected_invalid_item_rollbacks": expected_rollbacks,
            "complete": verified,
        }
    failure_fields = {
        "serialization_failure": "routine_serialization_failure",
        "deadlock_detected": "routine_deadlock_detected",
        "no_data_found": "routine_no_data_found", "other": "routine_other",
    }
    exact_failures = {
        name: value for name, field in failure_fields.items()
        if (value := delta(before, after, field)) is not None
    }
    if exact_failures:
        handled_failures = exact_failures
    checkpoints = parse_checkpoints(server_text)
    failed_queries = delta(before, after, "failed_queries")
    harness_error_lines = [
        line.strip()
        for line in read_text(raw / "harness-errors.log").splitlines()
        if line.strip()
    ]

    perf_path = raw / "perf.json"
    perf_events = parse_perf(perf_path)
    perf_requested_flag = nested(metadata, "profiling", "perf_stat")
    perf_requested = (
        bool(perf_requested_flag) if perf_requested_flag is not None else perf_path.exists()
    )
    perf_exit_code = read_first_int(raw, ("perf_exit_code", "perf_exit_status"))
    perf_collected = any(value is not None for value in perf_events.values())

    graceful_flag = nested(metadata, "reopen", "graceful_check")
    if graceful_flag is None:
        graceful_flag = nested(metadata, "profiling", "graceful_reopen")
    graceful_reopen_ns = read_first_int(raw, ("graceful_reopen_ns", "recovery_ns"))
    graceful_reopen_dsum = read_first_int(raw, ("graceful_reopen_dsum", "recovery_dsum"))
    graceful_requested = (
        bool(graceful_flag)
        if graceful_flag is not None
        else graceful_reopen_ns is not None or graceful_reopen_dsum is not None
    )
    graceful_matches = (
        None
        if dsum_after is None or graceful_reopen_dsum is None
        else dsum_after == graceful_reopen_dsum
    )

    reasons = []
    terminal_assignments = sorted(
        [list(map(int, match)) for match in re.findall(
            r"BICDB_TERMINAL_ASSIGNMENT (\d+) (\d+) (\d+)",
            read_text(raw / "hammerdb.log"),
        )]
    )
    if nested(metadata, "workload", "terminal_assignment_capture"):
        expected_positions = list(range(2, expected_active_vus + 2)) if isinstance(expected_active_vus, int) else []
        if [row[0] for row in terminal_assignments] != expected_positions:
            reasons.append("terminal assignment capture is missing or duplicated")
        if any(row[1] < 1 or row[2] < 1 for row in terminal_assignments):
            reasons.append("terminal assignment contains a non-positive warehouse/district")
        if nested(metadata, "workload", "terminal_assignment_mode") == "replay":
            try:
                expected_assignments = sorted(
                    [list(map(int, line.split())) for line in
                     read_text(raw / "terminal-assignments.tsv").splitlines() if line.strip()]
                )
            except ValueError:
                expected_assignments = None
            if terminal_assignments != expected_assignments:
                reasons.append("terminal assignments do not match replay input")
    if dsum_delta is None or dsum_delta < 0:
        reasons.append("invalid district-sum delta")
    if nested(metadata, "workload", "require_business_completion"):
        for name, completion in business_completion.items():
            if not completion["complete"]:
                reasons.append(
                    f"{name} business completion not verified: "
                    f"{completion['committed']} committed / {completion['calls']} exact calls"
                )
    if hammer_exit_code is None:
        reasons.append("HammerDB exit code is missing")
    elif hammer_exit_code != 0:
        reasons.append(f"HammerDB exited with status {hammer_exit_code}")
    if hammer["failed_vus"]:
        reasons.append(f"{hammer['failed_vus']} HammerDB VUs failed")
    if not hammer["all_complete"]:
        reasons.append("HammerDB did not report completion")
    if expected_total_vus is None:
        reasons.append("expected HammerDB VU count is missing")
    else:
        if hammer["created_vus"] != expected_total_vus:
            reasons.append(
                "HammerDB created "
                f"{hammer['created_vus']} VUs; expected {expected_total_vus} including monitor"
            )
        if hammer["successful_vus"] != expected_total_vus:
            reasons.append(
                "HammerDB completed "
                f"{hammer['successful_vus']} VUs successfully; expected {expected_total_vus} "
                "including monitor"
            )
    if failed_queries is None:
        reasons.append("failed query count is missing")
    elif failed_queries:
        reasons.append(f"server reported {failed_queries} failed queries")
    if harness_error_lines:
        reasons.append(f"harness reported {len(harness_error_lines)} error(s)")
    if perf_requested and not perf_collected:
        reasons.append("requested perf counters are missing")
    if graceful_requested:
        if graceful_reopen_ns is None:
            reasons.append("graceful reopen time is missing")
        if graceful_reopen_dsum is None:
            reasons.append("graceful reopen district sum is missing")
        elif graceful_matches is False:
            reasons.append(
                "graceful reopen district sum does not match the post-run district sum"
            )
    if BAD_SERVER_PATTERN.search(server_text):
        reasons.append("server log contains a fatal/checkpoint error")
    reopen_server_text = read_text(raw / "graceful-reopen-server.log")
    if not reopen_server_text:
        reopen_server_text = read_text(raw / "recovery-server.log")
    if graceful_requested and BAD_SERVER_PATTERN.search(reopen_server_text):
        reasons.append("graceful reopen server log contains a fatal/checkpoint error")
    memtrace = re.findall(r"commit_seq=(\d+) last_seq=(\d+)", server_text)
    watermark_gap = None
    if memtrace:
        commit_seq, last_seq = map(int, memtrace[-1])
        watermark_gap = commit_seq - last_seq
        if watermark_gap > 1:
            reasons.append(f"watermark gap is {watermark_gap}")
    wal = {
        "start_bytes": read_int(raw / "wal_before_bytes"),
        "end_bytes": read_int(raw / "wal_after_bytes"),
        "generated_bytes": delta(before, after, "wal_bytes_written"),
        "write_calls": delta(before, after, "wal_write_calls"),
        "sync_calls": delta(before, after, "wal_sync_calls"),
        "commits_written": delta(before, after, "wal_commits_written"),
        "max_batch_commits": int_field(after, "wal_max_batch_commits"),
    }
    if wal["write_calls"]:
        wal["mean_commits_per_write"] = wal["commits_written"] / wal["write_calls"]
        wal["mean_bytes_per_write"] = wal["generated_bytes"] / wal["write_calls"]
    else:
        wal["mean_commits_per_write"] = None
        wal["mean_bytes_per_write"] = None
    initial_open_ns = read_int(raw / "startup_ns")
    graceful_reopen = {
        "requested": graceful_requested,
        "duration_ns": graceful_reopen_ns,
        "district_sum": graceful_reopen_dsum,
        "district_sum_matches_post_run": graceful_matches,
    }
    result = {
        "schema_version": 2,
        "run": metadata,
        "terminal_assignments": terminal_assignments,
        "validity": {"valid": not reasons, "reasons": reasons, "watermark_gap": watermark_gap},
        "duration_ns": read_int(raw / "workload_ns"),
        "throughput": {"district_sum_before": dsum_before, "district_sum_after": dsum_after,
                       "district_sum_delta": dsum_delta, "district_sum_nopm": dsum_nopm,
                       "hammerdb": hammer},
        "work": {"server_queries": delta(before, after, "queries_executed"),
                 "committed_payments": committed_payments,
                 "business_completion": business_completion,
                 "client_outcomes": client_outcomes,
                 "failed_queries": failed_queries,
                 "writes_executed": delta(before, after, "writes_executed"),
                 "commit_seq_delta": delta(before, after, "wal_written_seq"),
                 "procedures": procedures, "handled_failures": handled_failures},
        "latency_ns": {"transactions": parse_latencies(raw),
                       "queue": {"p50": int_field(after, "query_queue_wait_p50_ns"),
                                 "p95": int_field(after, "query_queue_wait_p95_ns"),
                                 "p99": int_field(after, "query_queue_wait_p99_ns")}},
        "cpu": {**resources, "perf": perf_events},
        "memory": {"rss_peak_bytes": resources.get("rss_peak_bytes"),
                   "rss_end_bytes": resources.get("rss_end_bytes"),
                   "rss_hwm_peak_bytes": resources.get("rss_hwm_peak_bytes"),
                   **parse_allocations(raw / "allocations.txt")},
        "wal": wal,
        "checkpoints": checkpoints,
        "startup": {"initial_open_ns": initial_open_ns},
        "graceful_reopen": graceful_reopen,
        # Compatibility for existing report consumers. This is not crash recovery.
        "recovery": {"initial_open_ns": initial_open_ns,
                     "post_run_open_ns": graceful_reopen_ns},
        "harness": {"hammerdb_exit_code": hammer_exit_code,
                    "errors": harness_error_lines,
                    "perf_requested": perf_requested,
                    "perf_exit_code": perf_exit_code,
                    "perf_collected": perf_collected},
        "artifacts": {"raw": "raw", "metadata": "metadata.json"},
    }
    (trial_dir / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


def describe(values):
    values = list(values)
    if not values:
        return {
            "n": 0,
            "values": [],
            "mean": None,
            "median": None,
            "min": None,
            "max": None,
            "stdev": None,
            "cv": None,
            "ci95_normal": None,
            "ci95_student_t": None,
        }
    mean = statistics.fmean(values)
    stdev = statistics.stdev(values) if len(values) > 1 else None
    # Normal approximation is labeled explicitly; raw values remain authoritative.
    margin = 1.96 * stdev / math.sqrt(len(values)) if stdev is not None else None
    critical = STUDENT_T_95.get(len(values) - 1)
    student_margin = (
        critical * stdev / math.sqrt(len(values))
        if critical is not None and stdev is not None
        else None
    )
    return {"n": len(values), "values": values, "mean": mean,
            "median": statistics.median(values), "min": min(values), "max": max(values),
            "stdev": stdev, "cv": stdev / mean if stdev is not None and mean else None,
            "ci95_normal": [mean - margin, mean + margin] if margin is not None else None,
            "ci95_student_t": ([mean - student_margin, mean + student_margin]
                               if student_margin is not None else None)}


def profiling_kind(result):
    return nested(result, "run", "profiling", "kind")


def is_throughput_trial(result):
    kind = profiling_kind(result)
    return not nested(result, "run", "warmup") and kind in (None, "throughput")


def markdown_number(value, precision=1):
    if value is None:
        return "n/a"
    return f"{value:.{precision}f}"


def campaign(campaign_dir: Path):
    results = []
    for path in sorted((campaign_dir / "trials").glob("*/result.json")):
        results.append(json.loads(read_text(path)))
    lanes = {}
    for lane in LANES:
        measured = [
            r for r in results if r["run"]["lane"] == lane and is_throughput_trial(r)
        ]
        valid = [r for r in measured if r["validity"]["valid"]]
        reopen_values = [
            nested(r, "graceful_reopen", "duration_ns")
            if "graceful_reopen" in r
            else nested(r, "recovery", "post_run_open_ns")
            for r in valid
        ]
        reopen_summary = describe([value for value in reopen_values if value is not None])
        lanes[lane] = {
            "trials": len(measured), "valid_trials": len(valid),
            "district_sum_nopm": describe([r["throughput"]["district_sum_nopm"] for r in valid]),
            "rss_peak_bytes": describe([r["memory"]["rss_peak_bytes"] for r in valid
                                        if r["memory"]["rss_peak_bytes"] is not None]),
            "wal_bytes_written": describe([r["wal"]["generated_bytes"] for r in valid
                                            if r["wal"]["generated_bytes"] is not None]),
            "graceful_reopen_ns": reopen_summary,
            # Compatibility for consumers of schema version 1 summaries.
            "recovery_ns": reopen_summary,
            "invalid": [{"trial_id": r["run"]["trial_id"], "reasons": r["validity"]["reasons"]}
                        for r in measured if not r["validity"]["valid"]],
        }
    summary = {"schema_version": 2, "campaign": campaign_dir.name,
               "trial_count": len(results), "lanes": lanes}
    (campaign_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    lines = [f"# TPC-C campaign {campaign_dir.name}", "", "| Lane | Valid | Mean NOPM | Median NOPM | CV | Peak RSS mean |", "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for lane, data in lanes.items():
        nopm = data["district_sum_nopm"]
        rss = data["rss_peak_bytes"]
        lines.append(
            f"| `{lane}` | {data['valid_trials']}/{data['trials']} | "
            f"{markdown_number(nopm['mean'])} | {markdown_number(nopm['median'])} | "
            f"{markdown_number(nopm['cv'], 4)} | {markdown_number(rss['mean'], 0)} |"
        )
    lines.extend(["", "Machine-readable evidence: `summary.json` and each `trials/*/result.json`.", ""])
    (campaign_dir / "summary.md").write_text("\n".join(lines))
    return summary


COMPARISON_IDENTITY_FIELDS = (
    ("seed manifest", ("seed", "manifest_sha256")),
    ("host", ("host", "hostname")),
    ("CPU model", ("host", "cpu_model")),
    ("logical CPU count", ("host", "logical_cpus")),
    ("filesystem", ("host", "filesystem")),
    ("Rust toolchain", ("source", "rustc")),
    ("VU count", ("workload", "vu")),
    ("ramp-up", ("workload", "rampup_minutes")),
    ("duration", ("workload", "duration_minutes")),
    ("time profiling", ("workload", "time_profile")),
    ("common environment", ("server", "common_env")),
    ("lane environment", ("server", "lane_env")),
)

ARM_IDENTITY_FIELDS = (
    ("binary hash", ("source", "binary_sha256")),
    ("source commit", ("source", "git_commit")),
    ("Rust toolchain", ("source", "rustc")),
    ("seed manifest", ("seed", "manifest_sha256")),
    ("host", ("host", "hostname")),
    ("CPU model", ("host", "cpu_model")),
    ("logical CPU count", ("host", "logical_cpus")),
    ("filesystem", ("host", "filesystem")),
    ("VU count", ("workload", "vu")),
    ("ramp-up", ("workload", "rampup_minutes")),
    ("duration", ("workload", "duration_minutes")),
    ("time profiling", ("workload", "time_profile")),
    ("common environment", ("server", "common_env")),
    ("lane environment", ("server", "lane_env")),
    ("profile kind", ("profiling", "kind")),
)


def trial_name(result):
    return nested(result, "run", "trial_id") or "unknown"


def normalized_server_args(result):
    args = nested(result, "run", "server", "args")
    if not isinstance(args, list):
        return None
    normalized = list(args)
    if len(normalized) >= 2 and normalized[0] == "serve-pg":
        normalized[1] = "<runtime-data-dir>"
    return normalized


def source_state(result):
    return (
        nested(result, "run", "source", "state_sha256")
        or nested(result, "run", "source", "diff_sha256")
    )


def within_arm_mismatches(results, side):
    if not results:
        return []
    reference = results[0]
    values = {
        label: nested(reference, "run", *path) for label, path in ARM_IDENTITY_FIELDS
    }
    values["source state"] = source_state(reference)
    values["server arguments"] = normalized_server_args(reference)
    issues = []
    for label, value in values.items():
        if value is None:
            issues.append(f"{side} {label} is missing in {trial_name(reference)}")
    for result in results[1:]:
        current = {
            label: nested(result, "run", *path) for label, path in ARM_IDENTITY_FIELDS
        }
        current["source state"] = source_state(result)
        current["server arguments"] = normalized_server_args(result)
        for label, reference_value in values.items():
            if current[label] != reference_value:
                issues.append(
                    f"{side} {label} differs in {trial_name(result)}"
                )
    return issues


def select_throughput(results, lane):
    return [
        result
        for result in results
        if nested(result, "run", "lane") == lane and is_throughput_trial(result)
    ]


def select_latency(results, lane):
    return [
        result
        for result in results
        if nested(result, "run", "lane") == lane and profiling_kind(result) == "latency"
    ]


def pair_results(baseline, candidate, key):
    issues = []

    def index(results, side):
        indexed = {}
        for result in results:
            value = key(result)
            if value in indexed:
                issues.append(f"duplicate {side} pairing key {value!r}")
            else:
                indexed[value] = result
        return indexed

    baseline_by_key = index(baseline, "baseline")
    candidate_by_key = index(candidate, "candidate")
    missing_candidate = sorted(set(baseline_by_key) - set(candidate_by_key), key=str)
    missing_baseline = sorted(set(candidate_by_key) - set(baseline_by_key), key=str)
    if missing_candidate:
        issues.append(f"candidate is missing paired trials {missing_candidate}")
    if missing_baseline:
        issues.append(f"baseline is missing paired trials {missing_baseline}")
    pairs = [
        (baseline_by_key[value], candidate_by_key[value])
        for value in sorted(set(baseline_by_key) & set(candidate_by_key), key=str)
    ]
    if not pairs:
        issues.append("no paired trials")
    return pairs, issues


def relative_metric(pairs, accessor):
    baseline_values = []
    candidate_values = []
    relative_changes = []
    missing = []
    for baseline, candidate in pairs:
        baseline_value = accessor(baseline)
        candidate_value = accessor(candidate)
        if (
            not isinstance(baseline_value, (int, float))
            or isinstance(baseline_value, bool)
            or not isinstance(candidate_value, (int, float))
            or isinstance(candidate_value, bool)
            or baseline_value == 0
        ):
            missing.append([trial_name(baseline), trial_name(candidate)])
            continue
        baseline_values.append(baseline_value)
        candidate_values.append(candidate_value)
        relative_changes.append(candidate_value / baseline_value - 1.0)
    return {
        "baseline": describe(baseline_values),
        "candidate": describe(candidate_values),
        "paired_relative_change": describe(relative_changes),
        "missing_pairs": missing,
    }


def comparison_mismatches(pairs):
    mismatches = []
    for baseline, candidate in pairs:
        if (nested(baseline, "run", "workload", "terminal_assignment_capture")
                or nested(candidate, "run", "workload", "terminal_assignment_capture")):
            if (not baseline.get("terminal_assignments")
                    or baseline.get("terminal_assignments") != candidate.get("terminal_assignments")):
                mismatches.append(
                    f"terminal assignments differ or are missing for pair {trial_name(baseline)}/{trial_name(candidate)}"
                )
        if nested(baseline, "run", "position") != nested(candidate, "run", "position"):
            mismatches.append(
                f"position differs for pair {trial_name(baseline)}/{trial_name(candidate)}"
            )
        for label, path in COMPARISON_IDENTITY_FIELDS:
            baseline_value = nested(baseline, "run", *path)
            candidate_value = nested(candidate, "run", *path)
            if baseline_value is None and candidate_value is None:
                continue
            if baseline_value != candidate_value:
                mismatches.append(
                    f"{label} differs for pair {trial_name(baseline)}/{trial_name(candidate)}"
                )
        baseline_args = normalized_server_args(baseline)
        candidate_args = normalized_server_args(candidate)
        if baseline_args != candidate_args:
            mismatches.append(
                "server arguments differ for pair "
                f"{trial_name(baseline)}/{trial_name(candidate)}"
            )
    return mismatches


def invalid_trials(results):
    return [
        trial_name(result)
        for result in results
        if not nested(result, "validity", "valid")
    ]


def compare_lane(baseline_results, candidate_results, lane):
    baseline = select_throughput(baseline_results, lane)
    candidate = select_throughput(candidate_results, lane)
    pairs, pair_issues = pair_results(
        baseline, candidate, lambda result: nested(result, "run", "rep")
    )
    compatibility_issues = comparison_mismatches(pairs)
    compatibility_issues.extend(within_arm_mismatches(baseline, "baseline"))
    compatibility_issues.extend(within_arm_mismatches(candidate, "candidate"))

    baseline_latency = select_latency(baseline_results, lane)
    candidate_latency = select_latency(candidate_results, lane)
    if baseline_latency or candidate_latency:
        latency_pairs, latency_pair_issues = pair_results(
            baseline_latency,
            candidate_latency,
            lambda result: nested(result, "run", "position"),
        )
        compatibility_issues.extend(comparison_mismatches(latency_pairs))
        compatibility_issues.extend(
            within_arm_mismatches(baseline_latency, "baseline latency")
        )
        compatibility_issues.extend(
            within_arm_mismatches(candidate_latency, "candidate latency")
        )
    else:
        latency_pairs, latency_pair_issues = pairs, []

    throughput = relative_metric(
        pairs, lambda result: nested(result, "throughput", "district_sum_nopm")
    )
    rss = relative_metric(
        pairs, lambda result: nested(result, "memory", "rss_peak_bytes")
    )
    wal = relative_metric(pairs, lambda result: nested(result, "wal", "generated_bytes"))

    procedure_names = sorted(
        {
            name
            for baseline_result, candidate_result in latency_pairs
            for result in (baseline_result, candidate_result)
            for name in (nested(result, "latency_ns", "transactions") or {})
        }
    )
    p99 = {
        name: relative_metric(
            latency_pairs,
            lambda result, procedure=name: nested(
                result, "latency_ns", "transactions", procedure, "p99_ns"
            ),
        )
        for name in procedure_names
    }

    candidate_invalid = invalid_trials(candidate + candidate_latency)
    baseline_invalid = invalid_trials(baseline + baseline_latency)
    reject_reasons = []
    inconclusive_reasons = []
    if candidate_invalid:
        reject_reasons.append(f"candidate has invalid trials: {candidate_invalid}")
    if baseline_invalid:
        inconclusive_reasons.append(f"baseline has invalid trials: {baseline_invalid}")
    inconclusive_reasons.extend(pair_issues)
    inconclusive_reasons.extend(latency_pair_issues)
    inconclusive_reasons.extend(compatibility_issues)

    required_metrics = {
        "throughput": throughput,
        "peak RSS": rss,
        "WAL bytes": wal,
    }
    for label, metric in required_metrics.items():
        if metric["missing_pairs"]:
            inconclusive_reasons.append(f"{label} is missing for one or more pairs")
    if not procedure_names:
        inconclusive_reasons.append("p99 latency data is missing")
    for name, metric in p99.items():
        if metric["missing_pairs"]:
            inconclusive_reasons.append(f"{name} p99 latency is missing for one or more pairs")

    comparison_ready = not baseline_invalid and not pair_issues and not latency_pair_issues
    comparison_ready = comparison_ready and not compatibility_issues
    comparison_ready = comparison_ready and not any(
        metric["missing_pairs"] for metric in required_metrics.values()
    )
    comparison_ready = comparison_ready and bool(procedure_names)
    comparison_ready = comparison_ready and not any(
        metric["missing_pairs"] for metric in p99.values()
    )

    if comparison_ready and not candidate_invalid:
        throughput_change = throughput["paired_relative_change"]
        throughput_mean = throughput_change["mean"]
        throughput_ci = throughput_change["ci95_student_t"]
        if throughput_change["n"] < 2 or throughput_ci is None:
            inconclusive_reasons.append(
                "2-31 throughput pairs are required for the paired Student-t 95% CI"
            )
        elif throughput_ci[1] <= 0:
            reject_reasons.append("paired throughput 95% CI is non-positive")
        elif throughput_ci[0] <= 0:
            inconclusive_reasons.append("paired throughput 95% CI crosses zero")
        elif throughput_mean < KEEPER_GATES["throughput_min_relative_gain"]:
            reject_reasons.append("throughput gain is below the 2% keeper threshold")

        rss_growth = rss["paired_relative_change"]["mean"]
        if rss_growth > KEEPER_GATES["rss_max_relative_growth"]:
            reject_reasons.append("peak RSS growth exceeds 5%")
        wal_growth = wal["paired_relative_change"]["mean"]
        if wal_growth > KEEPER_GATES["wal_max_relative_growth"]:
            reject_reasons.append("WAL growth exceeds 5%")
        for name, metric in p99.items():
            if metric["paired_relative_change"]["mean"] > KEEPER_GATES["p99_max_relative_growth"]:
                reject_reasons.append(f"{name} p99 latency regression exceeds 10%")

    if reject_reasons:
        decision = "reject"
        reasons = reject_reasons + inconclusive_reasons
    elif inconclusive_reasons:
        decision = "inconclusive"
        reasons = inconclusive_reasons
    elif lane not in DURABLE_LANES:
        decision = "inconclusive"
        reasons = ["the non-durable CPU ceiling is diagnostic and cannot be a keeper"]
    else:
        decision = "keeper"
        reasons = []

    return {
        "lane": lane,
        "decision": decision,
        "reasons": reasons,
        "paired_trials": len(pairs),
        "gates": KEEPER_GATES,
        "metrics": {
            "district_sum_nopm": throughput,
            "rss_peak_bytes": rss,
            "wal_generated_bytes": wal,
            "transaction_p99_ns": p99,
        },
    }


def load_campaign_results(campaign_dir: Path):
    return [
        json.loads(read_text(path))
        for path in sorted((campaign_dir / "trials").glob("*/result.json"))
    ]


def compare_campaigns(baseline_dir: Path, candidate_dir: Path, lane=None):
    baseline_results = load_campaign_results(baseline_dir)
    candidate_results = load_campaign_results(candidate_dir)
    lanes = (lane,) if lane else LANES
    comparisons = {
        lane_name: compare_lane(baseline_results, candidate_results, lane_name)
        for lane_name in lanes
    }
    result = {
        "schema_version": 1,
        "baseline": str(baseline_dir),
        "candidate": str(candidate_dir),
        "lanes": comparisons,
    }
    if lane:
        result["decision"] = comparisons[lane]["decision"]
        result["reasons"] = comparisons[lane]["reasons"]
    return result


def main():
    if len(sys.argv) == 3 and sys.argv[1] in {"trial", "campaign"}:
        path = Path(sys.argv[2]).resolve()
        trial(path) if sys.argv[1] == "trial" else campaign(path)
        return
    if len(sys.argv) in {4, 5} and sys.argv[1] == "compare":
        lane = sys.argv[4] if len(sys.argv) == 5 else None
        if lane is not None and lane not in LANES:
            raise SystemExit(f"unknown lane: {lane}")
        result = compare_campaigns(
            Path(sys.argv[2]).resolve(), Path(sys.argv[3]).resolve(), lane
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return
    raise SystemExit(
        "usage: summarize.py trial|campaign PATH\n"
        "       summarize.py compare BASELINE_CAMPAIGN CANDIDATE_CAMPAIGN [LANE]"
    )


if __name__ == "__main__":
    main()
