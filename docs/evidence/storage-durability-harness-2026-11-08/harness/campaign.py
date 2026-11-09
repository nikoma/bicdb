#!/usr/bin/env python3
"""Boundary definitions for the storage-durability evidence campaign.

Usage: campaign.py <group> <port> [--reps N] [--only name,...] [--tag T]
Runs every boundary of <group> sequentially through trial.py.
"""

import os
import subprocess
import sys

B = "/root/bicdb-durability-s3/evid"
BIN = f"{B}/bin/bicdb"
VER = f"{B}/bin/bicdb-verifier"


def fio(name, fix, mode, sites, nmin, nmax, reps, **kw):
    return dict(name=name, fix=fix, mode=mode, kind="failio", sites=sites, nmin=nmin,
                nmax=nmax, reps=reps, **kw)


def crash(name, fix, mode, sites, nmin, nmax, reps, **kw):
    return dict(name=name, fix=fix, mode=mode, kind="crash", sites=sites, nmin=nmin,
                nmax=nmax, reps=reps, **kw)


BOUNDARIES = [
    # ---- #55 fail-closed write/fsync failures
    fio("f55-txlog-write-paged", 55, "paged", ["transaction_log.write"], 20, 400, 20),
    fio("f55-txlog-sync-paged", 55, "paged", ["transaction_log.sync"], 20, 400, 20),
    fio("f55-txlog-sync-repairfail-paged", 55, "paged", ["transaction_log.sync"], 20, 400, 20,
        also_fail="transaction_log.repair:1"),
    fio("f55-txlog-write-dead-paged", 55, "paged", ["transaction_log.write"], 20, 400, 10,
        persistent=True),
    fio("f55-txlog-cut-paged", 55, "paged", ["transaction_log.cut_rename", "transaction_log.cut_sync"], 2, 4, 20),
    fio("f55-txlog-cut-shutdown-paged", 55, "paged", ["transaction_log.cut_rename", "transaction_log.cut_sync"], 2, 2, 20,
        end="term"),
    fio("f55-txlog-cutcopy-shutdown-paged", 55, "paged", ["transaction_log.cut_copy"], 2, 2, 10, end="term"),
    fio("f55-pagelog-write-paged", 55, "paged", ["page_log.write"], 20, 2000, 20),
    fio("f55-pagelog-sync-paged", 55, "paged", ["page_log.sync"], 20, 400, 20),
    fio("f55-pagelog-sync-dead-paged", 55, "paged", ["page_log.sync"], 20, 400, 10, persistent=True),
    fio("f55-pagelog-seal-paged", 55, "paged", ["page_log.seal_sync", "page_log.seal_dir_sync"], 1, 10, 20),
    fio("f55-pagefile-sync-paged", 55, "paged", ["page_file.sync"], 1, 20, 20),
    fio("f55-pagefile-write-paged", 55, "paged", ["page_file.write"], 1, 200, 10),
    fio("f55-txlog-write-embedded", 55, "embedded", ["transaction_log.write"], 20, 400, 20),
    fio("f55-txlog-sync-embedded", 55, "embedded", ["transaction_log.sync"], 20, 400, 20),
    fio("f55-txlog-sync-repairfail-embedded", 55, "embedded", ["transaction_log.sync"], 20, 400, 10,
        also_fail="transaction_log.repair:1"),
    fio("f55-txlog-cut-embedded", 55, "embedded", ["transaction_log.cut_rename", "transaction_log.cut_sync"], 2, 3, 20,
        duration=12),
    fio("f55-txlog-cut-shutdown-embedded", 55, "embedded", ["transaction_log.cut_rename", "transaction_log.cut_sync"], 2, 2, 20,
        end="term"),
    crash("f55-crash-commit-paged", 55, "paged",
          ["commit.after_txlog_durable", "commit.after_page_apply", "commit.after_marker"], 10, 400, 20),
    crash("f55-crash-commit-paged-powerloss", 55, "paged",
          ["commit.after_txlog_durable", "commit.after_page_apply", "commit.after_marker"], 10, 400, 20,
          volatile=True),
    # ---- #74 frame-log torn tails and mid-file damage
    fio("f74-framefail-paged", 74, "paged", ["frame_log.write", "frame_log.short_write", "frame_log.sync"], 5, 300, 30),
    fio("f74-framefail-embedded", 74, "embedded", ["frame_log.write", "frame_log.short_write", "frame_log.sync"], 5, 300, 30),
    fio("f74-framerepairfail-paged", 74, "paged", ["frame_log.short_write"], 5, 300, 15,
        also_fail="frame_log.repair:1"),
    fio("f74-framerepairfail-embedded", 74, "embedded", ["frame_log.short_write"], 5, 300, 15,
        also_fail="frame_log.repair:1"),
    dict(name="f74-torn-txlog-paged", fix=74, mode="paged", kind="torn", target="transactions.log", reps=20),
    dict(name="f74-torn-synclog-paged", fix=74, mode="paged", kind="torn", target="sync.log", reps=20),
    dict(name="f74-torn-any-paged", fix=74, mode="paged", kind="torn", target="", reps=20),
    dict(name="f74-torn-txlog-embedded", fix=74, mode="embedded", kind="torn", target="transactions.log", reps=20),
    dict(name="f74-torn-segments-embedded", fix=74, mode="embedded", kind="torn", target="segments", reps=20),
    dict(name="f74-torn-any-embedded", fix=74, mode="embedded", kind="torn", target="", reps=20),
    dict(name="f74-midfile-txlog-paged", fix=74, mode="paged", kind="midfile", target="transactions.log", reps=15),
    dict(name="f74-midfile-synclog-paged", fix=74, mode="paged", kind="midfile", target="sync.log", reps=15),
    dict(name="f74-midfile-any-paged", fix=74, mode="paged", kind="midfile", target="", reps=15),
    dict(name="f74-midfile-txlog-embedded", fix=74, mode="embedded", kind="midfile", target="transactions.log", reps=15),
    dict(name="f74-midfile-segments-embedded", fix=74, mode="embedded", kind="midfile", target="segments", reps=15),
    dict(name="f74-midfile-any-embedded", fix=74, mode="embedded", kind="midfile", target="", reps=15),
    # ---- #67 crash-safe page-log reset and horizon-bounded redo
    crash("f67-crash-reset-paged", 67, "paged",
          ["wal_reset.after_seal", "wal_reset.after_segment_delete", "wal_reset.after_segments_removed",
           "wal_reset.after_active_truncate"], 1, 8, 40),
    crash("f67-crash-reset-paged-powerloss", 67, "paged",
          ["wal_reset.after_seal", "wal_reset.after_segment_delete", "wal_reset.after_segments_removed",
           "wal_reset.after_active_truncate"], 1, 8, 40, volatile=True),
    crash("f67-crash-checkpoint-paged", 67, "paged",
          ["checkpoint.after_wal_reset", "checkpoint.after_publish", "checkpoint.after_page_flush",
           "checkpoint.after_wal_sync", "checkpoint_step.after_finish", "checkpoint_step.after_meta_writeback",
           "wal_seal.after_rename"], 1, 8, 40),
    crash("f67-crash-checkpoint-paged-powerloss", 67, "paged",
          ["checkpoint.after_wal_reset", "checkpoint.after_publish", "checkpoint.after_page_flush",
           "checkpoint.after_wal_sync", "checkpoint_step.after_finish", "checkpoint_step.after_meta_writeback",
           "wal_seal.after_rename"], 1, 8, 40, volatile=True),
    fio("f67-resetfail-paged", 67, "paged",
        ["page_log.reset_unlink", "page_log.reset_dir_sync", "page_log.reset", "page_log.reset_sync"], 1, 8, 40),
    fio("f67-resetfail-shutdown-paged", 67, "paged",
        ["page_log.reset_unlink", "page_log.reset_dir_sync", "page_log.reset", "page_log.reset_sync"], 1, 3, 20,
        end="term"),
    dict(name="f67-recoverycrash-paged", fix=67, mode="paged", kind="recoverycrash",
         sites=["page_recovery.after_scan", "page_recovery.after_page_replay", "page_recovery.after_flush",
                "open.after_recovery_checkpoint", "open.after_txlog_floor", "wal_reset.after_seal",
                "wal_reset.after_segment_delete", "wal_reset.after_segments_removed",
                "wal_reset.after_active_truncate", "checkpoint.after_publish", "checkpoint.after_wal_reset"],
         nmin=1, nmax=2, reps=60),
    dict(name="f67-recoverycrash-paged-powerloss", fix=67, mode="paged", kind="recoverycrash",
         sites=["page_recovery.after_scan", "page_recovery.after_page_replay", "page_recovery.after_flush",
                "open.after_recovery_checkpoint", "open.after_txlog_floor", "wal_reset.after_seal",
                "wal_reset.after_segment_delete", "wal_reset.after_segments_removed",
                "wal_reset.after_active_truncate", "checkpoint.after_publish", "checkpoint.after_wal_reset"],
         nmin=1, nmax=2, reps=40, volatile=True),
    # ---- plain SIGKILL at random times (all fixes)
    dict(name="all-sigkill-paged-stress", fix=0, mode="paged", kind="kill", reps=40),
    dict(name="all-sigkill-paged-default", fix=0, mode="paged", kind="kill", reps=20, profile="default"),
    dict(name="all-sigkill-embedded-stress", fix=0, mode="embedded", kind="kill", reps=30),
    dict(name="all-sigkill-embedded-default", fix=0, mode="embedded", kind="kill", reps=15, profile="default"),
    # ---- diagnostics: which stress knob produces the plain-SIGKILL recovery failures
    dict(name="diag-sigkill-paged-smallpool", fix=0, mode="paged", kind="kill", reps=40, profile="smallpool"),
    dict(name="diag-sigkill-paged-smallwal", fix=0, mode="paged", kind="kill", reps=40, profile="smallwal"),
    dict(name="diag-sigkill-paged-default-long", fix=0, mode="paged", kind="kill", reps=30, profile="default",
         duration=20),
    # ---- negative controls: the reconciler must flag deliberate damage
    *[dict(name=f"control-{c}", fix=0, mode="paged", kind="kill", reps=2, control=c)
      for c in ("delete_acked", "stray_row", "hot_rollback", "payload_corrupt", "resurrect_deleted")],
]


def command(b, port, reps, tag):
    out = f"{B}/results/{tag}{b['name']}.jsonl"
    cmd = ["python3", "-u", f"{B}/trial.py", "--name", f"{tag}{b['name']}", "--bin", BIN,
           "--verifier", VER, "--mode", b["mode"], "--kind", b["kind"],
           "--profile", b.get("profile", "stress"), "--reps", str(reps), "--port", str(port),
           "--workroot", f"{B}/work", "--out", out]
    if b.get("sites"):
        cmd += ["--sites", *b["sites"], "--nmin", str(b["nmin"]), "--nmax", str(b["nmax"])]
    if b.get("persistent"):
        cmd.append("--persistent")
    if b.get("also_fail"):
        cmd += ["--also-fail", b["also_fail"]]
    if b.get("volatile"):
        cmd.append("--volatile")
    if "target" in b:
        cmd += ["--target", b["target"]]
    if b.get("duration"):
        cmd += ["--duration", str(b["duration"])]
    if b.get("end"):
        cmd += ["--end", b["end"]]
    if b.get("control"):
        cmd += ["--control", b["control"]]
    return cmd


def main():
    prefix = sys.argv[1]
    port = int(sys.argv[2])
    reps = None
    tag = ""
    only = None
    args = sys.argv[3:]
    while args:
        flag = args.pop(0)
        if flag == "--reps":
            reps = int(args.pop(0))
        elif flag == "--tag":
            tag = args.pop(0)
        elif flag == "--only":
            only = set(args.pop(0).split(","))
    os.makedirs(f"{B}/results", exist_ok=True)
    for b in BOUNDARIES:
        if not b["name"].startswith(prefix) or (only and b["name"] not in only):
            continue
        cmd = command(b, port, reps or b["reps"], tag)
        print("RUN", " ".join(cmd), flush=True)
        subprocess.run(cmd)


if __name__ == "__main__":
    main()
