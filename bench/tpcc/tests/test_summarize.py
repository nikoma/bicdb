import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "summarize.py"
SPEC = importlib.util.spec_from_file_location("tpcc_summarize", MODULE_PATH)
SUMMARIZE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SUMMARIZE)


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(value))


def make_trial(root):
    trial_dir = root / "trial"
    raw = trial_dir / "raw"
    metadata = {
        "schema_version": 1,
        "trial_id": "rep1-pos1-default-durable",
        "lane": "default-durable",
        "rep": 1,
        "position": 1,
        "warmup": False,
        "source": {
            "rustc": "rustc-test",
            "binary_sha256": "binary-test",
            "git_commit": "commit-test",
            "state_sha256": "source-test",
        },
        "seed": {"manifest_sha256": "seed-test"},
        "host": {
            "hostname": "host-test",
            "cpu_model": "cpu-test",
            "logical_cpus": 8,
            "filesystem": "disk-test",
        },
        "workload": {
            "vu": 2,
            "rampup_minutes": 1,
            "duration_minutes": 2,
            "time_profile": True,
        },
        "server": {
            "common_env": [],
            "lane_env": [],
            "args": ["serve-pg", "/tmp/trial-data", "--host", "127.0.0.1"],
        },
        "profiling": {"kind": "throughput", "perf_stat": True},
        "reopen": {"graceful_check": True},
    }
    write(trial_dir / "metadata.json", json.dumps(metadata))
    write(raw / "dsum_before", "100\n")
    write(raw / "dsum_after", "400\n")
    write(raw / "graceful_reopen_dsum", "400\n")
    write(raw / "graceful_reopen_ns", "123456\n")
    write(raw / "startup_ns", "456\n")
    write(raw / "workload_ns", "180000000000\n")
    write(raw / "hammerdb_exit_code", "0\n")
    write(raw / "hammerdb_expected_active_vus", "2\n")
    write(raw / "perf_exit_code", "130\n")
    write(raw / "perf.json", '{"event":"cycles","counter-value":"100"}\n')
    write(
        raw / "hammerdb.log",
        "3 Virtual Users Created with Monitor VU\n"
        "Vuser 1:FINISHED SUCCESS\n"
        "Vuser 2:FINISHED SUCCESS\n"
        "Vuser 3:FINISHED SUCCESS\n"
        "ALL VIRTUAL USERS COMPLETE\n"
        "TEST RESULT : System achieved 100 NOPM from 200 PostgreSQL TPM\n",
    )
    header = (
        "failed_queries,queries_executed,writes_executed,wal_written_seq,"
        "wal_bytes_written,wal_write_calls,wal_sync_calls,wal_commits_written,"
        "wal_max_batch_commits\n"
    )
    write(raw / "stats_before.csv", header + "0,10,0,1,0,0,0,0,1\n")
    write(raw / "stats_after.csv", header + "0,110,10,21,1000,10,10,20,2\n")
    return trial_dir


def synthetic_result(rep, nopm, rss=1000, wal=1000, p99=1000, valid=True, lane=None):
    lane = lane or "default-durable"
    return {
        "run": {
            "trial_id": f"rep{rep}-{lane}",
            "lane": lane,
            "rep": rep,
            "position": rep,
            "warmup": False,
            "profiling": {"kind": "throughput"},
            "source": {
                "rustc": "rustc-test",
                "binary_sha256": "binary-test",
                "git_commit": "commit-test",
                "state_sha256": "source-test",
            },
            "seed": {"manifest_sha256": "seed-test"},
            "host": {
                "hostname": "host-test",
                "cpu_model": "cpu-test",
                "logical_cpus": 8,
                "filesystem": "disk-test",
            },
            "workload": {
                "vu": 8,
                "rampup_minutes": 1,
                "duration_minutes": 2,
                "time_profile": True,
            },
            "server": {
                "common_env": [],
                "lane_env": [],
                "args": [
                    "serve-pg",
                    f"/tmp/baseline-rep{rep}",
                    "--host",
                    "127.0.0.1",
                ],
            },
        },
        "validity": {"valid": valid, "reasons": [] if valid else ["synthetic failure"]},
        "throughput": {"district_sum_nopm": nopm},
        "memory": {"rss_peak_bytes": rss},
        "wal": {"generated_bytes": wal},
        "latency_ns": {"transactions": {"neword": {"p99_ns": p99}}},
    }


class TrialValidityTests(unittest.TestCase):
    def test_corrected_outcomes_require_all_clients_and_real_commits(self):
        with tempfile.TemporaryDirectory() as tmp:
            trial = make_trial(Path(tmp))
            raw = trial / "raw"
            metadata = json.loads((trial / "metadata.json").read_text())
            metadata["workload"].update(require_business_completion=True, client_outcomes=True)
            write(trial / "metadata.json", json.dumps(metadata))
            for filename, counts in (("stats_before.csv", "0,0"), ("stats_after.csv", "302,200")):
                lines = (raw / filename).read_text().splitlines()
                write(raw / filename, lines[0] + ",proc_neword,proc_payment\n" + lines[1] + "," + counts + "\n")
            write(raw / "payment_before.csv", "payment_history_rows\n1000\n")
            write(raw / "payment_after.csv", "payment_history_rows\n1200\n")
            original = (raw / "hammerdb.log").read_text()
            reports = "BICDB_OUTCOMES 2 151 150 1 0 100\nBICDB_OUTCOMES 3 151 150 1 0 100\n"
            write(raw / "hammerdb.log", original + reports)
            self.assertTrue(SUMMARIZE.trial(trial)["validity"]["valid"])
            for broken in (reports.splitlines()[0] + "\n", reports + reports,
                           reports.replace("150 1 0", "149 1 1"),
                           reports.replace("150 1 0", "150 2 0")):
                write(raw / "hammerdb.log", original + broken)
                self.assertFalse(SUMMARIZE.trial(trial)["validity"]["valid"], broken)
            write(raw / "hammerdb.log", original + reports)
            write(raw / "dsum_after", "399\n")
            self.assertFalse(SUMMARIZE.trial(trial)["validity"]["valid"])

    def test_strict_business_completion_requires_exact_completed_work(self):
        with tempfile.TemporaryDirectory() as tmp:
            trial = make_trial(Path(tmp))
            metadata = json.loads((trial / "metadata.json").read_text())
            metadata["workload"]["require_business_completion"] = True
            write(trial / "metadata.json", json.dumps(metadata))
            raw = trial / "raw"
            self.assertFalse(SUMMARIZE.trial(trial)["validity"]["valid"])
            for filename, counts in (("stats_before.csv", "10,20"),
                                     ("stats_after.csv", "310,220")):
                lines = (raw / filename).read_text().splitlines()
                write(raw / filename, lines[0] + ",proc_neword,proc_payment\n"
                      + lines[1] + "," + counts + "\n")
            write(raw / "payment_before.csv", "payment_history_rows\n1000\n")
            for committed, valid in ((200, True), (199, False), (201, False), (-1, False)):
                write(raw / "payment_after.csv", f"payment_history_rows\n{1000 + committed}\n")
                result = SUMMARIZE.trial(trial)
                self.assertEqual(result["validity"]["valid"], valid, result["validity"])
                self.assertEqual(result["work"]["business_completion"]["payment"]["call_commit_gap"],
                                 200 - committed)
            write(raw / "payment_after.csv", "payment_history_rows\n1200\n")
            write(raw / "dsum_after", "399\n")
            self.assertFalse(SUMMARIZE.trial(trial)["work"]["business_completion"]["neword"]["complete"])

    def test_terminal_assignment_capture_and_replay_are_checked(self):
        with tempfile.TemporaryDirectory() as tmp:
            trial = make_trial(Path(tmp))
            metadata = json.loads((trial / "metadata.json").read_text())
            metadata["workload"].update(terminal_assignment_capture=True, terminal_assignment_mode="replay")
            write(trial / "metadata.json", json.dumps(metadata))
            log_path = trial / "raw/hammerdb.log"
            base_log = log_path.read_text()
            write(trial / "raw/terminal-assignments.tsv", "2 4 8\n3 7 1\n")
            write(log_path, base_log + "Vuser 3:BICDB_TERMINAL_ASSIGNMENT 3 7 1\nVuser 2:BICDB_TERMINAL_ASSIGNMENT 2 4 8\n")
            result = SUMMARIZE.trial(trial)
            self.assertTrue(result["validity"]["valid"], result["validity"])
            self.assertEqual(result["terminal_assignments"], [[2, 4, 8], [3, 7, 1]])
            write(log_path, base_log + "Vuser 2:BICDB_TERMINAL_ASSIGNMENT 2 4 8\n")
            self.assertFalse(SUMMARIZE.trial(trial)["validity"]["valid"])
            write(log_path, base_log + "BICDB_TERMINAL_ASSIGNMENT 2 4 8\nBICDB_TERMINAL_ASSIGNMENT 2 4 8\n")
            self.assertFalse(SUMMARIZE.trial(trial)["validity"]["valid"])
            write(log_path, base_log + "BICDB_TERMINAL_ASSIGNMENT 2 4 8\nBICDB_TERMINAL_ASSIGNMENT 3 9 1\n")
            self.assertIn("terminal assignments do not match replay input", SUMMARIZE.trial(trial)["validity"]["reasons"])

    def test_payment_work_requires_history_state_not_procedure_calls(self):
        with tempfile.TemporaryDirectory() as temporary:
            trial = make_trial(Path(temporary))
            self.assertIsNone(SUMMARIZE.trial(trial)["work"]["committed_payments"])
            raw = trial / "raw"
            write(raw / "payment_before.csv", "payment_history_rows\n480000\n")
            write(raw / "payment_after.csv", "payment_history_rows\n580000\n")
            self.assertEqual(SUMMARIZE.trial(trial)["work"]["committed_payments"], 100000)
            write(raw / "payment_after.csv", "payment_count\n480000\n")
            self.assertIsNone(SUMMARIZE.trial(trial)["work"]["committed_payments"])

    def test_valid_trial_records_graceful_reopen_and_legacy_alias(self):
        with tempfile.TemporaryDirectory() as temporary:
            result = SUMMARIZE.trial(make_trial(Path(temporary)))

        self.assertTrue(result["validity"]["valid"], result["validity"]["reasons"])
        self.assertEqual(result["throughput"]["hammerdb"]["expected_total_vus"], 3)
        self.assertEqual(result["graceful_reopen"]["duration_ns"], 123456)
        self.assertTrue(result["graceful_reopen"]["district_sum_matches_post_run"])
        self.assertEqual(result["recovery"]["post_run_open_ns"], 123456)
        self.assertTrue(result["harness"]["perf_collected"])

    def test_each_release_evidence_failure_invalidates_trial(self):
        cases = {
            "hammer exit": (
                lambda raw: write(raw / "hammerdb_exit_code", "9\n"),
                "HammerDB exited with status 9",
            ),
            "failed query": (
                lambda raw: write(
                    raw / "stats_after.csv",
                    "failed_queries,queries_executed,writes_executed,wal_written_seq,"
                    "wal_bytes_written,wal_write_calls,wal_sync_calls,wal_commits_written,"
                    "wal_max_batch_commits\n1,110,10,21,1000,10,10,20,2\n",
                ),
                "server reported 1 failed queries",
            ),
            "harness error": (
                lambda raw: write(raw / "harness-errors.log", "server vanished\n"),
                "harness reported 1 error(s)",
            ),
            "incomplete VUs": (
                lambda raw: write(
                    raw / "hammerdb.log",
                    "3 Virtual Users Created with Monitor VU\n"
                    "Vuser 1:FINISHED SUCCESS\n"
                    "Vuser 2:FINISHED SUCCESS\n"
                    "ALL VIRTUAL USERS COMPLETE\n",
                ),
                "HammerDB completed 2 VUs successfully; expected 3 including monitor",
            ),
            "reopen mismatch": (
                lambda raw: write(raw / "graceful_reopen_dsum", "399\n"),
                "graceful reopen district sum does not match",
            ),
            "missing perf": (
                lambda raw: write(raw / "perf.json", ""),
                "requested perf counters are missing",
            ),
            "reopen server failure": (
                lambda raw: write(raw / "graceful-reopen-server.log", "panic: reopen failed\n"),
                "graceful reopen server log contains",
            ),
        }
        for name, (mutate, expected_reason) in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                trial_dir = make_trial(Path(temporary))
                mutate(trial_dir / "raw")
                result = SUMMARIZE.trial(trial_dir)
                self.assertFalse(result["validity"]["valid"])
                self.assertTrue(
                    any(expected_reason in reason for reason in result["validity"]["reasons"]),
                    result["validity"]["reasons"],
                )


class AggregateTests(unittest.TestCase):
    def test_empty_campaign_uses_null_and_na(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign_dir = Path(temporary) / "empty"
            (campaign_dir / "trials").mkdir(parents=True)
            summary = SUMMARIZE.campaign(campaign_dir)
            markdown = (campaign_dir / "summary.md").read_text()

        aggregate = summary["lanes"]["default-durable"]["district_sum_nopm"]
        self.assertEqual(aggregate["n"], 0)
        self.assertIsNone(aggregate["mean"])
        self.assertIsNone(aggregate["ci95_normal"])
        self.assertIsNone(aggregate["ci95_student_t"])
        self.assertIn("n/a", markdown)


class ComparisonTests(unittest.TestCase):
    def test_terminal_layout_must_match_within_pair(self):
        baseline = synthetic_result(1, 100)
        candidate = synthetic_result(1, 104)
        baseline["run"]["workload"]["terminal_assignment_capture"] = True
        baseline["terminal_assignments"] = [[2, 4, 8], [3, 7, 1]]
        candidate["terminal_assignments"] = [[2, 4, 8], [3, 7, 1]]
        self.assertEqual(SUMMARIZE.comparison_mismatches([(baseline, candidate)]), [])
        candidate["terminal_assignments"][1][1] = 8
        self.assertTrue(any("terminal assignments" in reason for reason in
                            SUMMARIZE.comparison_mismatches([(baseline, candidate)])))

    def setUp(self):
        self.baseline_nopm = [100.0, 102.0, 98.0, 101.0, 99.0, 103.0]
        self.baseline = [
            synthetic_result(rep, nopm) for rep, nopm in enumerate(self.baseline_nopm, 1)
        ]

    def candidate(self, changes, rss_growth=0.02, wal_growth=0.02, p99_growth=0.05):
        results = [
            synthetic_result(
                rep,
                nopm * (1.0 + changes[rep - 1]),
                rss=1000 * (1.0 + rss_growth),
                wal=1000 * (1.0 + wal_growth),
                p99=1000 * (1.0 + p99_growth),
            )
            for rep, nopm in enumerate(self.baseline_nopm, 1)
        ]
        for result in results:
            result["run"]["server"]["args"][1] = (
                f"/tmp/candidate-rep{result['run']['rep']}"
            )
        return results

    def test_keeper_requires_practical_gain_and_positive_paired_ci(self):
        comparison = SUMMARIZE.compare_lane(
            self.baseline, self.candidate([0.03] * 6), "default-durable"
        )

        self.assertEqual(comparison["decision"], "keeper", comparison["reasons"])
        paired = comparison["metrics"]["district_sum_nopm"]["paired_relative_change"]
        self.assertGreaterEqual(paired["mean"], 0.02)
        self.assertGreater(paired["ci95_student_t"][0], 0)

    def test_resource_budget_violation_rejects(self):
        comparison = SUMMARIZE.compare_lane(
            self.baseline,
            self.candidate([0.03] * 6, rss_growth=0.06),
            "default-durable",
        )

        self.assertEqual(comparison["decision"], "reject")
        self.assertTrue(any("RSS" in reason for reason in comparison["reasons"]))

    def test_ci_crossing_zero_is_inconclusive(self):
        comparison = SUMMARIZE.compare_lane(
            self.baseline,
            self.candidate([0.08, -0.03, 0.07, -0.02, 0.08, -0.03]),
            "default-durable",
        )

        self.assertEqual(comparison["decision"], "inconclusive")
        self.assertTrue(any("crosses zero" in reason for reason in comparison["reasons"]))

    def test_student_t_gate_does_not_promote_normal_only_separation(self):
        comparison = SUMMARIZE.compare_lane(
            self.baseline,
            self.candidate([0.06, 0.06, 0.06, 0.0, 0.0, 0.0]),
            "default-durable",
        )

        paired = comparison["metrics"]["district_sum_nopm"]["paired_relative_change"]
        self.assertGreater(paired["ci95_normal"][0], 0)
        self.assertLessEqual(paired["ci95_student_t"][0], 0)
        self.assertEqual(comparison["decision"], "inconclusive")

    def test_mixed_arm_provenance_or_configuration_is_inconclusive(self):
        cases = {
            "binary hash": lambda result: result["run"]["source"].update(
                binary_sha256="different-binary"
            ),
            "source state": lambda result: result["run"]["source"].update(
                state_sha256="different-source"
            ),
            "seed manifest": lambda result: result["run"]["seed"].update(
                manifest_sha256="different-seed"
            ),
            "server arguments": lambda result: result["run"]["server"]["args"].extend(
                ["--max-active-writes", "8"]
            ),
        }
        for label, mutate in cases.items():
            with self.subTest(label=label):
                candidate = self.candidate([0.03] * 6)
                mutate(candidate[-1])
                comparison = SUMMARIZE.compare_lane(
                    self.baseline, candidate, "default-durable"
                )
                self.assertEqual(comparison["decision"], "inconclusive")
                self.assertTrue(
                    any(label in reason for reason in comparison["reasons"]),
                    comparison["reasons"],
                )


if __name__ == "__main__":
    unittest.main()
