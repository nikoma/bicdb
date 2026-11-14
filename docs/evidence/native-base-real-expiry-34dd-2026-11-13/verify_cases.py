#!/usr/bin/env python3
"""Verify baseline logical outcomes; optional private hashes never open a DB."""
import argparse,contextlib,hashlib,json,pathlib,re,sys,zipfile
if not __debug__:raise SystemExit('Run without Python -O; assertions required')
SOURCE='34dded55efef6ed989cdf7d141d5751d4f2a2391'
ORIGINAL='a493a07ec6519949277d9813d105a7300025afc06059fcda8bf00ead074af573'
PUBLIC='3c378baa1d877254de40b8bb9f25ec7f4d3b4df0922893bddafe0b80e8bd41f8'
def verify(projection,projection_manifest,original=None):
    assert hashlib.sha256(projection.read_bytes()).hexdigest()==PUBLIC
    summary = {"source": SOURCE, "source_version": "1.0.494-beta", "run": 43502925624,
        "core_job": 132153142118, "artifact_id": 14075351623, "workflow_conclusion": "failure",
        "zip_sha256": hashlib.sha256(projection.read_bytes()).hexdigest(), "zip_bytes": projection.stat().st_size,
        "qualification": False, "cases": []}
    with zipfile.ZipFile(projection) as archive, contextlib.ExitStack() as stack:
        private = stack.enter_context(zipfile.ZipFile(original)) if original else None
        if original:
            assert original.stat().st_size == 740555 and hashlib.sha256(original.read_bytes()).hexdigest() == ORIGINAL
            for member in projection_manifest['included_members']:
                retained = private.read(member['path'])
                assert len(retained) == member['bytes'] and hashlib.sha256(retained).hexdigest() == member['sha256']
                assert retained == archive.read(member['path'])
        names = archive.namelist()
        assert archive.read("source-revision.txt").decode().strip() == SOURCE
        summary["zip_entries"] = len(names)
        summary["test_result"] = re.findall(r"test result:.*", archive.read("tests.log").decode())
        def read(root, name):
            return json.loads(archive.read(root + name))
        for name in names:
            if not name.endswith("execution-identity.json"):
                continue
            root = name.rsplit("/", 1)[0] + "/"
            identity = read(root, "execution-identity.json")
            assert identity["source_commit"] == SOURCE
            report = read(root, "registered-native-base-real-role-expiry-outcome.json")
            before, first, after = [report[k] for k in ("before", "after_first_expiry", "after_fresh_request")]
            xid = report["issued_native_xid"]
            assert xid and report["parent_budget_ms_unchanged"] == 5000
            assert report["request_order_unchanged"] == ["Role", "OWN", "Finished", "actual_host"]
            events = report["actual_cut_events"]
            assert len(events) == 2
            assert events[0]["event"] == "actual_cut_entered" and events[1]["event"] == "actual_cut_wait_finished"
            assert not any(e["fake_cancellation_or_deadline_renewal"] for e in events)
            assert not events[0]["original_role"]["expired_by_clock"]
            assert events[1]["original_role"]["expired_by_clock"]
            assert events[1]["original_role"]["original_check_error"] is not None
            assert all(not e["original_parent"]["expired_by_clock"] and e["original_parent"]["original_check_error"] is None for e in events)
            assert report["first_exact_error"] == "canceling statement due to query timeout"
            for snapshot in (before, first, after):
                assert all(snapshot[k]["diagnostic_error"] is None for k in ("owner", "image", "native", "logical"))
                assert snapshot["owner"]["value"]["actual_owner"] == before["owner"]["value"]["actual_owner"] == report["actual_target_attempt"]
                assert snapshot["exact_full_payload_physical_collections"] == ["__bic_cluster_events"]
                assert not snapshot["logical"]["value"]["writer_installed"]
                for k in ("rows", "schema", "namespace_ids", "open", "birth", "B", "raw", "slots", "protected_catalogue_uuid", "writer_installed"):
                    assert snapshot["logical"]["value"][k] == before["logical"]["value"][k], k
            assert before["native"]["value"]["base"] is None
            owner = first["image"]["value"]["native_base"]
            assert owner["attempted"] and not owner["verified"] and owner["receipt"]["native_page_xid"] == xid
            app = read(root, "ack-ledger.json")
            assert len(app) == 2
            for ack in app:
                assert ack["operation"] == "ClusterEventClient.append" and ack["outcome"] == "returned" and ack["actual_api_error"] is None
                assert hashlib.sha256(json.dumps(ack["value"], sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest() == ack["payload_json_sha256"]
            selected = before["acknowledged_logical_api"]
            assert selected in app
            for snapshot in (before, first, after):
                assert selected["actual_full_record"] in snapshot["logical"]["value"]["rows"]["__bic_cluster_events"]
            ledger = read(root, "source-frame-ledger.json")
            frames = [f for f in ledger if f.get("operation") == "source_image_frame"]
            assert all(f["outcome"] == "returned" and f["exact_error"] is None for f in frames)
            manifest = next(f["actual_reply"]["payload"]["Manifest"] for f in frames if "Manifest" in f["actual_reply"]["payload"])
            assert manifest["checksum_sha256"] == report["same_manifest_sha256"]
            assert report['first_progress'] is None and report['real_original_role_deadline_wait_not_fake_cancel']
            assert not report['original_e1ec_failure_cause_claimed'] and report['no_cold_powerloss_or_serving_qualification']
            for frame in frames:
                assert frame['original_parent_live_at_reply'] and frame['original_own_live_at_reply']
                assert frame['actual_reply']['request'] == frame['intent']
                assert frame['actual_reply']['manifest_sha256'] == manifest['checksum_sha256']
            assert len(frames) == 18
            assert report['retry_request_windows'] != report['first_request_windows']
            for snap in (first, after):
                native = snap['native']['value']
                assert native['same_live_database_open'] and native['read_only_fixed_keys']
                assert not native['authority_granted'] and not native['cold_or_powerloss_commit_proof']
            anchor = [manifest["range_index"], manifest["h0"], manifest["h"]]
            closes = [f["actual_reply"] for f in ledger if f.get("operation") == "remote_old_close"]
            assert len(closes) == 3
            assert all(c["constraints"]["native_point"] == anchor for c in closes)
            collection = next(f for f in read(root, "remote-old-ledger.json") if f["operation"] == "remote_old_fresh_all_closed_collection")
            assert collection["native_before"] == collection["native_after"] and collection["original_mutation_requests_expired"]
            if private:
                custody = json.loads(private.read(root + 'custody-manifest.json'))
                assert custody['source'] == SOURCE and custody['status'] == 'observed_stable_with_live_actual_owners'
                assert custody['all_five_metadata_service_db_native_journal_guards_held'] and custody['engine_owners_alive']
                assert not custody['engine_reopened'] and not custody['listeners_drained'] and not custody['atomic_powerloss_snapshot_claimed']
                size = 0
                for file in custody['files']:
                    data = private.read(root + 'custody/' + file['path'])
                    assert len(data) == file['bytes'] and hashlib.sha256(data).hexdigest() == file['sha256']
                    size += len(data)
                assert size == custody['total_bytes']
            else:
                # Public omission metadata identifies bytes without inspecting them.
                omitted = [v for v in projection_manifest['omitted_members'] if v['path'].startswith(root + 'custody/')]
                custody = {'files': omitted, 'status': 'observed_stable_with_live_actual_owners'}
                size = sum(v['bytes'] for v in omitted)
            assert len(custody['files']) == 96
            cleanup = read(root, "cleanup.json")
            assert cleanup["errors"] == [] and read(root, "evidence-errors.json")["errors"] == []
            result = {"source_case": identity["source_case"], "first_exact_error": report["first_exact_error"],
                "retry_exact_error": report["retry_exact_error"], "retry_progress": report["retry_progress"],
                "issued_native_xid": xid, "native_classification": report["same_open_native_classification"],
                "same_original_A": True, "actual_role_expired": True, "original_parent_stayed_live": True,
                "real_wait_us": events[1]["elapsed_us"], "parent_remaining_us_after_wait": events[1]["original_parent"]["remaining_us"],
                "role_remaining_us_after_wait": events[1]["original_role"]["remaining_us"],
                "app_returned_event_operations": 2, "selected_ack_full_payload_before_after_retry": True,
                "physical_record_collection": "__bic_cluster_events", "ordinary_sql_or_strong_serving_claim": False,
                "quorum_native_anchor_R_H0_H": anchor, "old_native_records_unchanged": True,
                "actual_source_frames": len(frames), "source_frame_journal_entries": len(ledger),
                "custody_files_identified": len(custody["files"]), "custody_bytes_identified": size, "private_custody_hashes_recomputed_this_invocation": bool(private),
                "custody_mode": custody["status"], "cleanup_errors": [], "evidence_errors": []}
            first_native = first["native"]["value"]
            after_native = after["native"]["value"]
            if identity["source_case"] == "TargetNativeBaseRoleExpiryBeforePut":
                assert report["retry_progress"] is None
                assert report["retry_exact_error"] == "cluster error: native target image: Registered immutable native-base publication missing"
                for native in (first_native, after_native, report["original_xid_after_retry"]["value"]):
                    assert native["native_tx_status_api_observations"][str(xid)] == "Aborted"
                    assert native["base"] is None and native["attempt_archive"] is None
                assert owner["actual_commit_returned"] is None
                assert not after["image"]["value"]["native_base"]["verified"]
                assert cleanup["primary_failure_preserved"] is not None
                result.update(classification="failed", demonstrated_defect="same-open proved-abort native metadata owner cannot resume under fresh authority", native_tx_status="Aborted", base_or_archive_present=False, fresh_authority_recovery="failed")
            else:
                assert identity["source_case"] == "TargetNativeBaseRoleExpiryAfterCommit"
                assert report["retry_exact_error"] is None
                assert owner["actual_commit_returned"] == xid
                assert first_native["base"] == first_native["attempt_archive"] == owner["receipt"] == after_native["base"] == after_native["attempt_archive"]
                assert first_native["native_tx_status_api_observations"] == after_native["native_tx_status_api_observations"] == {str(xid): "Committed"}
                assert report["retry_progress"]["NativeBase"]["native_page_xid"] == xid
                assert report["retry_progress"]["NativeBase"]["range_index"] == anchor[0]
                assert after["image"]["value"]["native_base"]["verified"]
                assert cleanup["primary_failure_preserved"] is None
                result.update(classification="passed", native_tx_status="Committed", exact_committed_base_archive_and_owner_match=True, fresh_authority_same_xid_reconciled=True)
            summary["cases"].append(result)
    assert len(summary["cases"]) == 2
    projection_identity = projection_manifest
    summary["public_projection"] = {k: projection_identity[k] for k in ("projection_zip_sha256", "projection_zip_bytes", "projection_fixture_db_custody_present", "original_archive_unchanged")}
    summary["limits"] = [
        "1 passed/1 failed; abort-resume acceptance remains failed and runtime unfixed",
        "actual-clock test cuts characterize these attempts; they do not establish original e1ec failure cause",
        "same-open native status/API observations, no archived-engine open or cold/powerloss qualification",
        "event-storage APP ledger, not ordinary PostgreSQL table-row or Serving proof",
        "one selected-range event record/177bytes/R1, no index/FTS parity or large-data campaign",
        "no catalogue adoption/NEW/decision/Open/Ready/Serving/source retirement or physical scale-out",
        "private raw custody remains a guarded live-owner stable copy, not stopped/atomic/reopened snapshot",
    ]
    summary['raw_private_custody_inspected_this_invocation'] = bool(original)
    summary['public_verifier_grants_no_native_authority'] = True
    return summary
if __name__ == '__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--original',type=pathlib.Path);a=p.parse_args();r=pathlib.Path(__file__).resolve().parent
    print(json.dumps(verify(r/'artifacts/public-logical.zip',json.loads((r/'artifacts/projection-manifest.json').read_text()),a.original),indent=2))
