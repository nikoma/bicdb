#!/usr/bin/env python3
"""Synthetic offline controls only: no stock client, BicDB, or network runs."""
import copy
import json
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import reconcile as R
import test_verify as T

BOOTSTRAP = "019a80b8-6594-7000-8000-000000000099"
IDENTITY = {"build_source_claim": "a" * 40, "client_binary_sha256": "1" * 64}
CONFIG = T.encode({"cluster_id": "lab", "range_image_bootstrap_id": BOOTSTRAP,
                   "transport": {"dev_localhost_plaintext": False}})
TOPOLOGY = T.encode({"cluster_id": "lab", "range_image_bootstrap_id": BOOTSTRAP})


def write_records(directory, plan_raw, rows, run, witness=True, torn=False):
    directory.mkdir()
    (directory / "config.json").write_bytes(CONFIG)
    (directory / "topology.json").write_bytes(TOPOLOGY)
    (directory / "plan.json").write_bytes(plan_raw)
    raw_rows, acks = [], {}
    actual_plan = R.V.decode(plan_raw)
    for row in rows:
        row = copy.deepcopy(row)
        row.update(run_id=run, sequence=len(raw_rows) + 1,
                   monotonic_elapsed_ns=len(raw_rows), previous_record_sha256=R.V.digest(raw_rows[-1]) if raw_rows else None)
        data = row["data"]
        if row["kind"] == "run_started":
            data.update(plan_sha256=R.V.digest(plan_raw), config_sha256=R.V.digest(CONFIG),
                        topology_sha256=R.V.digest(TOPOLOGY), phase=actual_plan["phase"],
                        operations=len(actual_plan["operations"]))
        if row["kind"] == "logical_attempt" and data["command"]["operation"] == "strong_read":
            data["command"].setdefault("acknowledged_by", None)
        if row["kind"] == "logical_terminal" and data["outcome"] == "acknowledged":
            data["detail"]["durable_ack_receipt"] = acks[data["logical_id"]]
        raw = T.encode(row)
        raw_rows.append(raw)
        if row["kind"] == "sdk_ack_observed":
            acks[data["logical_id"]] = {"sequence": row["sequence"], "record_sha256": R.V.digest(raw)}
    journal = b"\n".join(raw_rows) + b"\n" + (b'{"torn":' if torn else b"")
    (directory / "journal.jsonl").write_bytes(journal)
    summary = rows[-1]["data"] if rows[-1]["kind"] == "run_completed" else None
    witness_path = None
    exit_status = None
    if witness and summary is not None:
        witness_path = directory / "stdout.json"
        witness_path.write_bytes(T.encode({"summary": summary, "completion_receipt": {
            "sequence": len(raw_rows), "record_sha256": R.V.digest(raw_rows[-1])}}))
        exit_status = 2 if summary["observer_fenced"] else 0 if summary["all_operations_succeeded"] else 1
    return {"directory": str(directory), "identity": IDENTITY,
            "plan_sha256": R.V.digest(plan_raw), "journal_sha256": R.V.digest(journal),
            "completion_witness": str(witness_path) if witness_path else None,
            "completion_witness_sha256": R.V.digest(witness_path.read_bytes()) if witness_path else None,
            "exit_status": exit_status}


class Cases(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.serial = 1

    def source(self, value=None, key="k", failed=False, complete=True, torn=False, planned=1):
        f = T.Fixture(value)
        run = str(uuid.uuid5(uuid.UUID(T.RUN), str(self.serial)))
        self.serial += 1
        f.plan["run_id"] = run
        f.plan["operations"][0]["key"] = key
        if planned > 1:
            f.plan["operations"] = [{"operation": "append", "logical_id": f"w-{i}",
                                     "namespace": "probe", "key": f"{key}-{i}", "value": value}
                                    for i in range(planned)]
        f.plan_raw = T.encode(f.plan)
        f.records[0]["data"].update(operations=planned, plan_sha256=R.V.digest(f.plan_raw))
        f.rebuild()
        if planned == 1:
            f.write(fail=failed)
            if complete:
                f.complete()
        # A large unfinished source includes ALL its planned writes; no fake RPC evidence.
        return write_records(self.root / f"source-{self.serial}", f.plan_raw, f.records, run, complete, torn)

    def index(self, sources):
        read = self.root / "read-client"
        if not read.exists():
            read.mkdir(); (read / "config.json").write_bytes(CONFIG); (read / "topology.json").write_bytes(TOPOLOGY)
        spec = {"format": 1, "cluster_id": "lab", "bootstrap_id": BOOTSTRAP,
                "read_client": {"identity": IDENTITY, "directory": str(read),
                                "config_sha256": R.V.digest(CONFIG), "topology_sha256": R.V.digest(TOPOLOGY)},
                "sources": sources}
        path = self.root / "index.json"
        path.write_bytes(T.encode(spec))
        return path

    def read_results(self, manifest, files, absent=False, omit_last=False):
        result_items = []
        for batch in manifest["batches"]:
            if omit_last and batch is manifest["batches"][-1]:
                continue
            raw_plan = files[batch["filename"]]
            plan = R.V.decode(raw_plan)
            run = plan["run_id"]
            f = T.Fixture()
            f.plan = plan; f.plan_raw = raw_plan
            f.records[0]["data"].update(operations=len(plan["operations"]), plan_sha256=R.V.digest(raw_plan))
            f.rebuild()
            for number, command in enumerate(plan["operations"]):
                name = command["logical_id"]
                f.attempt(name)
                req_id = number + 1
                rpc = str(uuid.uuid5(uuid.UUID(run), name))
                request = {"protocol_version": 1, "cluster_id": "lab", "request_id": req_id,
                           "caller_node_id": "driver", "destination_node_id": "node",
                           "request": {"NativeEventGet": {"mode": "strong", "business_request_id": run,
                                                           "challenge": rpc, "namespace": command["namespace"], "key": command["key"]}}}
                value = {"presence": "absent"} if absent else {"presence": "present", "value": command["expected"]}
                response = {"protocol_version": 1, "cluster_id": "lab", "request_id": req_id,
                            "source_node_id": "node", "response": {"Ok": {"NativeEventGet": {
                                "business_request_id": run, "challenge": rpc, "result": {"kind": "value", "value": value}}}}}
                for phase in ("invoked", "request_frame_before_dispatch", "response_frame_before_decode", "validated_transport_result_ok"):
                    f.reservation += 1
                    dispatched = phase in {"response_frame_before_decode", "validated_transport_result_ok"}
                    evidence = {"attempt_id": rpc, "journal_sequence": f.reservation,
                                "phase_unix_ns": f.reservation, "elapsed_ns": f.reservation, "phase": phase,
                                "operation": "native_event_get", "caller": "driver", "destination": "node",
                                "topology_generation": 1 if phase != "invoked" else None,
                                "address": "127.0.0.1:1" if phase != "invoked" else None,
                                "request_id": req_id if phase != "invoked" else None,
                                "raw_frame": T.framed(request) if phase == "request_frame_before_dispatch" else
                                T.framed(response) if phase == "response_frame_before_decode" else None,
                                "bytes_written": len(T.framed(request)) if dispatched else 0,
                                "may_have_dispatched": dispatched, "error_chain": []}
                    f.add("sdk_rpc", {"logical_id": name, "actual_sdk_evidence": evidence})
                f.terminal(name, "expected_value_absent" if absent else "exact_match",
                           {"sdk_reply": {"found": not absent, "value": None if absent else command["expected"]},
                            "prior_ack_observed": False})
            f.complete()
            archived = write_records(self.root / f"result-{batch['index']}", raw_plan, f.records, run)
            archived.pop("identity"); archived.pop("plan_sha256"); archived["index"] = batch["index"]
            result_items.append(archived)
        path = self.root / "results.json"
        path.write_bytes(T.encode({"format": 1, "batches": result_items}))
        return path

    def test_exact_dedupe_preserves_ack_failed_retry_identities(self):
        index = self.index([self.source({"n": 1}, failed=True), self.source({"n": 1})])
        manifest, files = R.derive(index)
        self.assertEqual(len(manifest["targets"]), 1)
        self.assertEqual(len(manifest["original_writes"]), 2)
        self.assertTrue(manifest["original_writes"][0]["original_write_error_unresolved"])
        self.assertTrue(manifest["original_writes"][1]["ack_ledger_fsync_return_linked"])
        self.assertEqual(len(manifest["targets"][0]["source_identities"]), 2)
        plan = R.V.decode(files[manifest["batches"][0]["filename"]])
        self.assertEqual(plan["operations"][0]["operation"], "strong_read")
        self.assertIsNone(plan["operations"][0]["acknowledged_by"])

    def test_conflicting_payload_and_ack_histories_never_choose_one(self):
        manifest, _ = R.derive(self.index([self.source({"n": 1}), self.source({"n": 2})]))
        self.assertEqual(len(manifest["targets"]), 2)
        self.assertEqual(manifest["conflicts"][0]["classification"], "conflicting_payload_histories")
        self.assertEqual(len(manifest["conflicts"][0]["acknowledged_target_ids"]), 2)

    def test_incomplete_and_corrupt_source_includes_unseen_planned_writes(self):
        source = self.source(planned=3, complete=False, torn=True)
        manifest, _ = R.derive(self.index([source]))
        self.assertEqual(len(manifest["targets"]), 3)
        self.assertFalse(manifest["all_source_evidence_complete"])
        self.assertTrue(all(w["possibly_attempted"] and not w["confirmed_unattempted"] for w in manifest["original_writes"]))

    def test_complete_witnessed_fence_can_confirm_unattempted_write(self):
        f = T.Fixture()
        f.plan["operations"].insert(0, {"operation": "strong_read", "logical_id": "r", "namespace": "probe", "key": "k", "expected": None})
        f.plan_raw = T.encode(f.plan); f.records[0]["data"]["operations"] = 2; f.rebuild()
        f.attempt("r"); f.rpc("r", fail=True)
        f.terminal("r", "read_error", {"error": {"debug": "RpcEvidenceFailure"}, "observer_fenced": True}); f.complete()
        source = write_records(self.root / "source-fenced", f.plan_raw, f.records, T.RUN)
        manifest, _ = R.derive(self.index([source]))
        self.assertTrue(manifest["all_source_evidence_complete"])
        self.assertEqual(manifest["targets"], [])
        self.assertEqual(len(manifest["confirmed_unattempted_source_identities"]), 1)

    def test_all_batches_are_required_not_subset_pass(self):
        index = self.index([self.source(key="a", planned=1024, complete=False), self.source(key="last")])
        manifest, files = R.derive(index)
        self.assertEqual([len(b["target_ids"]) for b in manifest["batches"]], [1024, 1])
        output = self.root / "bundle"; R.generate(index, output)
        results = self.read_results(manifest, files, omit_last=True)
        report = R.coverage(index, output, results)
        self.assertFalse(report["reconciliation_evidence_complete"])
        self.assertEqual(len(report["not_verified_target_ids"]), 1)

    def test_present_null_and_actual_all_batch_coverage(self):
        index = self.index([self.source(None)])
        manifest, files = R.derive(index)
        output = self.root / "bundle"; R.generate(index, output)
        report = R.coverage(index, output, self.read_results(manifest, files))
        self.assertTrue(report["reconciliation_evidence_complete"], report)
        self.assertEqual(len(report["verified_present_target_ids"]), 1)
        self.assertFalse(report["physical_qualification"])
        self.assertFalse(report["restore_lineage_qualified"])

    def test_absence_after_visible_ack_is_failure(self):
        index = self.index([self.source(None)])
        manifest, files = R.derive(index)
        output = self.root / "bundle"; R.generate(index, output)
        report = R.coverage(index, output, self.read_results(manifest, files, absent=True))
        self.assertFalse(report["reconciliation_evidence_complete"])
        self.assertEqual(report["failed_targets"][0]["outcome"], "observed_sdk_acknowledged_value_absent")

    def test_unacknowledged_absence_does_not_resolve_unknown_write(self):
        index = self.index([self.source(failed=True)])
        manifest, files = R.derive(index)
        output = self.root / "bundle"; R.generate(index, output)
        report = R.coverage(index, output, self.read_results(manifest, files, absent=True))
        self.assertTrue(report["reconciliation_evidence_complete"], report)
        self.assertEqual(len(report["original_unresolved_source_identities"]), 1)
        self.assertFalse(report["source_write_uncertainty_resolved"])

    def test_wrong_cluster_or_bootstrap_refused_before_output(self):
        index = self.index([self.source()])
        for field, value in (("cluster_id", "other"), ("bootstrap_id", T.RUN)):
            spec = R.V.decode(index.read_bytes()); spec[field] = value; index.write_bytes(T.encode(spec))
            with self.assertRaises(R.V.Invalid):
                R.generate(index, self.root / "must-not-exist")
            self.assertFalse((self.root / "must-not-exist").exists())

    def test_source_binary_mismatch_refused(self):
        source = self.source(); source["identity"] = dict(IDENTITY, client_binary_sha256="2" * 64)
        with self.assertRaisesRegex(R.V.Invalid, "source/binary"):
            R.derive(self.index([source]))

    def test_plan_or_journal_hash_change_is_not_silent(self):
        source = self.source(); source["journal_sha256"] = "0" * 64
        with self.assertRaisesRegex(R.V.Invalid, "journal hash"):
            R.derive(self.index([source]))

    def test_duplicate_run_history_is_not_collapsed(self):
        source = self.source()
        with self.assertRaisesRegex(R.V.Invalid, "duplicate run UUID"):
            R.derive(self.index([source, source]))

    def test_explicit_caps_refuse_entire_generation(self):
        index = self.index([self.source(key="a"), self.source(key="b")])
        with patch.object(R, "MAX_TARGETS", 1):
            with self.assertRaisesRegex(R.V.Invalid, "no subset"):
                R.generate(index, self.root / "not-created")
        self.assertFalse((self.root / "not-created").exists())

    def test_changed_generated_plan_or_missing_manifest_refused(self):
        index = self.index([self.source()]); manifest, files = R.derive(index)
        output = self.root / "bundle"; R.generate(index, output)
        results = self.read_results(manifest, files)
        path = output / manifest["batches"][0]["filename"]; path.write_bytes(path.read_bytes() + b" ")
        with self.assertRaisesRegex(R.V.Invalid, "differs"):
            R.coverage(index, output, results)

    def test_output_directory_never_overwrites_previous_plans(self):
        index = self.index([self.source()]); output = self.root / "bundle"; R.generate(index, output)
        before = {p.name: p.read_bytes() for p in output.iterdir()}
        with self.assertRaises(FileExistsError):
            R.generate(index, output)
        self.assertEqual(before, {p.name: p.read_bytes() for p in output.iterdir()})

    def test_numeric_lexeme_is_copied_without_decimal_or_float_roundtrip(self):
        source = self.source("LEXICAL", planned=2, complete=False)
        directory = Path(source["directory"])
        raw_plan = (directory / "plan.json").read_bytes().replace(b'"LEXICAL"', b'1.2300e+999')
        (directory / "plan.json").write_bytes(raw_plan)
        row = R.V.decode((directory / "journal.jsonl").read_bytes())
        row["data"]["plan_sha256"] = R.V.digest(raw_plan)
        journal = T.encode(row) + b"\n"
        (directory / "journal.jsonl").write_bytes(journal)
        source.update(plan_sha256=R.V.digest(raw_plan), journal_sha256=R.V.digest(journal))
        manifest, files = R.derive(self.index([source]))
        plan = files[manifest["batches"][0]["filename"]]
        self.assertEqual(plan.count(b'"expected":1.2300e+999'), 2)
        self.assertNotIn(b"Infinity", plan)
        self.assertTrue(all(t["expected_value_raw_sha256"] == R.V.digest(b'1.2300e+999') for t in manifest["targets"]))

    def test_identical_value_different_lexical_encoding_not_silently_selected(self):
        first = self.source({"n": 1}, planned=2, complete=False)
        second = self.source({"n": 1}, planned=2, complete=False)
        directory = Path(second["directory"])
        raw_plan = (directory / "plan.json").read_bytes().replace(b'{"n":1}', b'{ "n" : 1 }')
        (directory / "plan.json").write_bytes(raw_plan)
        row = R.V.decode((directory / "journal.jsonl").read_bytes()); row["data"]["plan_sha256"] = R.V.digest(raw_plan)
        journal = T.encode(row) + b"\n"; (directory / "journal.jsonl").write_bytes(journal)
        second.update(plan_sha256=R.V.digest(raw_plan), journal_sha256=R.V.digest(journal))
        manifest, _ = R.derive(self.index([first, second]))
        self.assertEqual(len(manifest["targets"]), 4)
        self.assertTrue(all(c["classification"] == "different_exact_lexical_encodings_inconclusive" for c in manifest["conflicts"]))

    def test_every_complete_read_cannot_hide_original_conflicting_history(self):
        index = self.index([self.source(1), self.source(2)])
        manifest, files = R.derive(index)
        output = self.root / "bundle"; R.generate(index, output)
        report = R.coverage(index, output, self.read_results(manifest, files))
        self.assertTrue(report["all_target_observations_covered"])
        self.assertFalse(report["reconciliation_evidence_complete"])
        self.assertEqual(len(report["source_payload_conflicts"]), 1)

    def test_modified_result_plan_cannot_pass_as_complete_batch(self):
        index = self.index([self.source()]); manifest, files = R.derive(index)
        output = self.root / "bundle"; R.generate(index, output)
        results = self.read_results(manifest, files)
        data = R.V.decode(results.read_bytes()); directory = Path(data["batches"][0]["directory"])
        path = directory / "plan.json"; path.write_bytes(path.read_bytes().replace(b'"strong_read"', b'"append"'))
        with self.assertRaisesRegex(R.V.Invalid, "different/subset"):
            R.coverage(index, output, results)

    def test_read_result_missing_completion_is_not_credited_as_subset_success(self):
        index = self.index([self.source()]); manifest, files = R.derive(index)
        output = self.root / "bundle"; R.generate(index, output)
        results = self.read_results(manifest, files)
        data = R.V.decode(results.read_bytes()); item = data["batches"][0]
        journal = Path(item["directory"]) / "journal.jsonl"
        lines = journal.read_bytes().splitlines(keepends=True); journal.write_bytes(b"".join(lines[:-1]))
        item.update(journal_sha256=R.V.digest(journal.read_bytes()), completion_witness=None,
                    completion_witness_sha256=None, exit_status=None)
        results.write_bytes(T.encode(data))
        report = R.coverage(index, output, results)
        self.assertFalse(report["reconciliation_evidence_complete"])
        self.assertEqual(len(report["not_verified_target_ids"]), 1)

    def test_extra_output_plan_is_not_ignored(self):
        index = self.index([self.source()]); manifest, files = R.derive(index)
        output = self.root / "bundle"; R.generate(index, output)
        results = self.read_results(manifest, files)
        (output / "extra-plan.json").write_bytes(b'{}')
        with self.assertRaisesRegex(R.V.Invalid, "unexpected/missing"):
            R.coverage(index, output, results)

    def test_output_cap_fails_before_writing_any_subset(self):
        index = self.index([self.source()])
        with patch.object(R, "MAX_OUTPUT", 1):
            with self.assertRaisesRegex(R.V.Invalid, "output byte cap"):
                R.generate(index, self.root / "not-created")
        self.assertFalse((self.root / "not-created").exists())

    def empty_result_index(self):
        path = self.root / "no-results.json"
        path.write_bytes(T.encode({"format": 1, "batches": []}))
        return path

    def test_all_read_source_never_vacuously_passes_write_reconciliation(self):
        f = T.Fixture(read=True)
        f.plan["operations"] = [f.plan["operations"][1]]
        f.plan["operations"][0]["acknowledged_by"] = None
        f.plan_raw = T.encode(f.plan); f.rebuild(); f.attempt("r"); f.rpc("r", value=None)
        f.terminal("r", "exact_match", {"sdk_reply": {"found": True, "value": None}, "prior_ack_observed": False})
        f.complete()
        source = write_records(self.root / "all-read", f.plan_raw, f.records, T.RUN)
        index = self.index([source]); output = self.root / "bundle"
        generated = R.generate(index, output)
        self.assertTrue(generated["all_source_evidence_complete"])
        self.assertFalse(generated["has_original_write_corpus"])
        self.assertEqual(generated["no_target_reason"], "no_original_writes")
        result = R.coverage(index, output, self.empty_result_index())
        self.assertFalse(result["reconciliation_evidence_complete"])
        self.assertFalse(result["all_target_observations_covered"])
        self.assertFalse(result["has_visible_source_ack_sample"])
        self.assertFalse(result["has_linked_source_ack_sample"])

    def test_confirmed_unattempted_only_corpus_does_not_pass_empty_target_set(self):
        f = T.Fixture()
        f.plan["operations"].insert(0, {"operation": "strong_read", "logical_id": "r", "namespace": "probe", "key": "k", "expected": None})
        f.plan_raw = T.encode(f.plan); f.rebuild(); f.attempt("r"); f.rpc("r", fail=True)
        f.terminal("r", "read_error", {"error": {"debug": "RpcEvidenceFailure"}, "observer_fenced": True}); f.complete()
        source = write_records(self.root / "unattempted", f.plan_raw, f.records, T.RUN)
        index = self.index([source]); output = self.root / "bundle"
        generated = R.generate(index, output)
        self.assertTrue(generated["has_original_write_corpus"])
        self.assertFalse(generated["has_eligible_target_corpus"])
        self.assertEqual(generated["no_target_reason"], "all_original_writes_confirmed_unattempted")
        result = R.coverage(index, output, self.empty_result_index())
        self.assertFalse(result["reconciliation_evidence_complete"])
        self.assertEqual(len(result["confirmed_unattempted_source_identities"]), 1)

    def test_zero_ack_uncertain_only_observations_remain_useful_and_explicit(self):
        index = self.index([self.source(failed=True)])
        manifest, files = R.derive(index); output = self.root / "bundle"; R.generate(index, output)
        result = R.coverage(index, output, self.read_results(manifest, files, absent=True))
        self.assertTrue(result["reconciliation_evidence_complete"])
        self.assertFalse(result["has_visible_source_ack_sample"])
        self.assertFalse(result["has_linked_source_ack_sample"])
        self.assertFalse(result["source_write_uncertainty_resolved"])
        self.assertFalse(result["physical_qualification"])

    def test_changed_source_verifier_consumed_digest_is_refused(self):
        index = self.index([self.source()])
        original_verify = R.V.verify

        def changed(*args, **kwargs):
            report = original_verify(*args, **kwargs)
            report["journal_input"]["read_bytes_sha256"] = "0" * 64
            return report

        with patch.object(R.V, "verify", side_effect=changed):
            with self.assertRaisesRegex(R.V.Invalid, "incomplete source journal"):
                R.generate(index, self.root / "not-created")
        self.assertFalse((self.root / "not-created").exists())

    def test_changed_read_result_verifier_consumed_size_is_refused(self):
        index = self.index([self.source()]); manifest, files = R.derive(index)
        output = self.root / "bundle"; R.generate(index, output)
        results = self.read_results(manifest, files)
        result_directory = Path(R.V.decode(results.read_bytes())["batches"][0]["directory"])
        original_verify = R.V.verify

        def changed(*args, **kwargs):
            report = original_verify(*args, **kwargs)
            if Path(args[1]).parent == result_directory:
                report["journal_input"]["bytes_read"] -= 1
            return report

        with patch.object(R.V, "verify", side_effect=changed):
            with self.assertRaisesRegex(R.V.Invalid, "incomplete read-result journal"):
                R.coverage(index, output, results)

    def test_changed_source_verifier_reported_plan_only_is_refused(self):
        index = self.index([self.source()])
        original_verify = R.V.verify

        def changed(*args, **kwargs):
            report = original_verify(*args, **kwargs)
            report["run_manifest_claims"]["plan_sha256"] = "0" * 64
            return report

        with patch.object(R.V, "verify", side_effect=changed):
            with self.assertRaisesRegex(R.V.Invalid, "different source plan"):
                R.generate(index, self.root / "not-created")
        self.assertFalse((self.root / "not-created").exists())

    def test_changed_read_result_verifier_reported_plan_only_is_refused(self):
        index = self.index([self.source()]); manifest, files = R.derive(index)
        output = self.root / "bundle"; R.generate(index, output)
        results = self.read_results(manifest, files)
        result_directory = Path(R.V.decode(results.read_bytes())["batches"][0]["directory"])
        original_verify = R.V.verify

        def changed(*args, **kwargs):
            report = original_verify(*args, **kwargs)
            if Path(args[1]).parent == result_directory:
                report["run_manifest_claims"]["plan_sha256"] = "0" * 64
            return report

        with patch.object(R.V, "verify", side_effect=changed):
            with self.assertRaisesRegex(R.V.Invalid, "different read-result plan"):
                R.coverage(index, output, results)


if __name__ == "__main__":
    unittest.main()
