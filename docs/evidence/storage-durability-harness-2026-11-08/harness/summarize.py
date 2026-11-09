#!/usr/bin/env python3
"""Aggregate trial JSON lines into per-boundary rows and per-fix summaries.

Usage: summarize.py <results-dir> <out.json>
"""

import collections
import glob
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import campaign  # noqa: E402


def site_of(rec):
    inj = rec.get("injected") or {}
    spec = inj.get("BICDB_TEST_FAIL_IO") or inj.get("BICDB_TEST_CRASH_AT") or ""
    return spec.split(",")[0].rsplit(":", 1)[0] if spec else None


def main():
    results_dir, out = sys.argv[1], sys.argv[2]
    defs = {b["name"]: b for b in campaign.BOUNDARIES}
    rows = []
    for path in sorted(glob.glob(os.path.join(results_dir, "*.jsonl"))):
        name = os.path.basename(path)[:-6]
        recs = [json.loads(line) for line in open(path) if line.strip()]
        b = defs.get(name) or defs.get(name.split("-", 1)[-1], {})
        row = collections.OrderedDict()
        row["boundary"] = name
        row["fix"] = b.get("fix")
        row["mode"] = b.get("mode")
        row["profile"] = b.get("profile", "stress")
        row["kind"] = b.get("kind")
        row["sites"] = b.get("sites")
        row["n_range"] = [b.get("nmin"), b.get("nmax")] if b.get("sites") else None
        row["persistent"] = bool(b.get("persistent"))
        row["also_fail"] = b.get("also_fail")
        row["power_loss_emulated"] = bool(b.get("volatile"))
        row["end"] = b.get("end", "kill")
        row["target"] = b.get("target")
        row["control"] = b.get("control")
        row["trials"] = len(recs)
        row["fired"] = sum(bool(r.get("fired")) for r in recs)
        row["pass"] = sum(r.get("result") == "PASS" for r in recs)
        row["fail"] = sum(r.get("result") == "FAIL" for r in recs)
        row["reopen_failed"] = sum(str(r.get("result", "")).startswith("REOPEN") for r in recs)
        row["harness_error"] = sum(r.get("result") == "HARNESS_ERROR" for r in recs)
        fired_sites = collections.Counter(site_of(r) for r in recs if r.get("fired") and site_of(r))
        row["fired_by_site"] = dict(fired_sites)
        marker_sites = collections.Counter(
            ((r.get("cycle1") or {}).get("marker") or {}).get("site") for r in recs
            if (r.get("cycle1") or {}).get("marker"))
        row["fence_sites_recorded"] = dict(marker_sites)
        tot = collections.Counter()
        res = collections.Counter()
        infl = collections.Counter()
        viol = collections.Counter()
        codes = collections.Counter()
        for r in recs:
            last = r.get("reconcile_2") or r.get("reconcile_1")
            if last:
                tot.update(last["classes"])
                res.update(last["resolutions"])
                infl.update(last["inflight_outcomes"])
            for key in ("reconcile_1", "reconcile_2"):
                if r.get(key):
                    viol.update(r[key]["violations"])
            for key in ("cycle1", "cycle2"):
                codes.update(((r.get(key) or {}).get("clients") or {}).get("codes") or {})
        row["acknowledged_ok"] = tot["ok"]
        row["failed_non_08007"] = tot["failed"]
        row["unknown_08007"] = tot["unknown_08007"]
        row["08007_resolved"] = dict(res)
        row["inflight_commit_sent"] = tot["inflight_commit_sent"]
        row["inflight_outcomes"] = dict(infl)
        row["inflight_no_commit_sent"] = tot["inflight_no_commit"]
        row["sqlstates_seen"] = dict(codes)
        row["violations"] = dict(viol)
        vpre = [r.get("verify_pre_recovery") or {} for r in recs]
        vpost = [r.get("verify_post_recovery") or {} for r in recs]
        vpre2 = [r.get("verify_pre_recovery_2") or {} for r in recs]
        row["verify_pre2_ran"] = sum(bool(v.get("ran")) for v in vpre2)
        row["verify_pre2_clean"] = sum(bool(v.get("clean")) for v in vpre2)
        row["verify_pre2_exit_codes"] = dict(collections.Counter(v.get("exit") for v in vpre2 if v.get("ran")))
        row["verify_pre_ran"] = sum(bool(v.get("ran")) for v in vpre)
        row["verify_pre_clean"] = sum(bool(v.get("clean")) for v in vpre)
        row["verify_pre_exit_codes"] = dict(collections.Counter(v.get("exit") for v in vpre if v.get("ran")))
        row["verify_post_ran"] = sum(bool(v.get("ran")) for v in vpost)
        row["verify_post_clean"] = sum(bool(v.get("clean")) for v in vpost)
        row["verify_left_files_unchanged"] = all(v.get("unchanged", True) for v in vpre + vpre2 + vpost)
        if b.get("kind") == "midfile":
            row["midfile_refused"] = sum(bool(r.get("midfile_refused")) for r in recs)
            row["midfile_damaged_file_unchanged"] = sum(bool(r.get("midfile_damaged_file_unchanged")) for r in recs)
            changed = collections.Counter()
            for r in recs:
                for f in r.get("midfile_changed_files") or []:
                    changed["paged/store.wal*" if f.startswith("paged/store.wal") else f] += 1
            row["files_changed_by_refused_open"] = dict(changed)
            row["damaged_files"] = dict(collections.Counter(
                (r.get("tamper") or {}).get("file", "").split("/")[0] for r in recs))
        if b.get("kind") == "torn":
            row["torn_files"] = dict(collections.Counter((r.get("tamper") or {}).get("file") for r in recs))
            row["torn_how"] = dict(collections.Counter(
                (r.get("tamper") or {}).get("how", "").split(" ")[0] for r in recs))
        if b.get("kind") == "recoverycrash":
            row["recovery_aborted_at_point"] = sum(
                bool((r.get("recovery_crash") or {}).get("crash_point_reached")) for r in recs)
        failures = [
            {k: r.get(k) for k in ("rep", "result", "injected", "tamper", "error", "server_log_tail")}
            | {"violations_1": (r.get("reconcile_1") or {}).get("violation_samples"),
               "violations_2": (r.get("reconcile_2") or {}).get("violation_samples")}
            for r in recs if r.get("result") != "PASS"]
        row["failure_samples"] = failures[:5]
        rows.append(row)
    json.dump(rows, open(out, "w"), indent=1)
    for row in rows:
        print(f"{row['boundary']:<44} trials={row['trials']:<3} fired={row['fired']:<3} "
              f"pass={row['pass']:<3} fail={row['fail']} reopenfail={row['reopen_failed']} "
              f"herr={row['harness_error']} ok={row['acknowledged_ok']} 08007={row['unknown_08007']} "
              f"viol={row['violations']}")


if __name__ == "__main__":
    main()
