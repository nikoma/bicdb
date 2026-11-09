#!/usr/bin/env python3
"""Replay recovery of a saved crash state twice and reconcile it against the
ledger lines of the cycles that precede it.

Usage: replay.py <trial-dir> <crashed-N> <port>
"""

import json
import os
import shutil
import subprocess
import sys
import types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import trial  # noqa: E402

B = "/root/bicdb-durability-s3/evid"


def filtered_ledger(src, dst, max_cycle):
    os.makedirs(dst)
    for name in os.listdir(src):
        toks = set()
        out = []
        for line in open(os.path.join(src, name)):
            rec = json.loads(line)
            if rec["ev"] == "attempt":
                if rec["cycle"] <= max_cycle:
                    toks.add(rec["tok"])
                else:
                    continue
            elif rec["tok"] not in toks:
                continue
            out.append(line)
        open(os.path.join(dst, name), "w").writelines(out)


def main():
    tdir, crashed, port = sys.argv[1], sys.argv[2], int(sys.argv[3])
    cycle = int(crashed.split("-")[1])
    args = types.SimpleNamespace(bin=f"{B}/bin/bicdb", port=port, mode="paged",
                                 base_env={}, verifier=None)
    trial.ARGS = args
    out = {"state": crashed, "replays": []}
    for i in (1, 2):
        rdir = os.path.join(tdir, f"replay-{crashed}-{i}")
        shutil.rmtree(rdir, ignore_errors=True)
        os.makedirs(rdir)
        shutil.copytree(os.path.join(tdir, crashed), os.path.join(rdir, "db"), symlinks=True)
        filtered_ledger(os.path.join(tdir, "ledger"), os.path.join(rdir, "ledger"), cycle)
        log = os.path.join(rdir, "server.log")
        srv = trial.Server(args, os.path.join(rdir, "db"), log, {})
        if not srv.wait_ready():
            srv.kill()
            tail = open(log, "rb").read()[-600:].decode(errors="replace")
            out["replays"].append({"opened": False, "tail": tail.strip().splitlines()[-1:]})
        else:
            rec, _, _ = trial.reconcile(args, os.path.join(rdir, "ledger"), "replay", {}, False)
            srv.term()
            out["replays"].append({"opened": True, "rows": rec["rows"], "violations": rec["violations"],
                                   "samples": rec["violation_samples"][:3]})
        shutil.rmtree(rdir)
    print(json.dumps(out))


if __name__ == "__main__":
    main()
