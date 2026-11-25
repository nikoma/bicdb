#!/bin/sh
# Run the PostgreSQL 18.4 regression suite on a remote host (the build host), never locally.
#
#   scripts/pg18-regress/run-remote.sh <ssh-host> <run-name> --target ref
#   scripts/pg18-regress/run-remote.sh <ssh-host> <run-name> --target bicdb --bicdb-bin /path/to/bicdb
#
# Copies this directory to <host>:/root/pg18-regress-cache/harness, runs pg_regress_compat.py there
# capped (systemd-run, MemoryMax, Nice=15) into /root/pg18-regress-cache/runs/<run-name>, waits, and
# copies summary.json/summary.md/status.tsv/diffs/results back to ./pg18-regress-runs/<run-name>/.
# The remote host needs docker (image postgres:18.4), python3 and systemd.
set -eu
H=$1; NAME=$2; shift 2
HERE=$(cd "$(dirname "$0")" && pwd)
R=/root/pg18-regress-cache
UNIT=pgregress-run-$(echo "$NAME" | tr -c 'a-zA-Z0-9\n' '-')
ssh "$H" "mkdir -p $R/harness $R/runs"
scp -q "$HERE/pg_regress_compat.py" "$HERE/scope.tsv" "$HERE/analyze.py" "$H:$R/harness/"
ssh "$H" "systemctl is-active --quiet $UNIT" && { echo "run $NAME already active"; exit 3; }
ssh "$H" "systemctl reset-failed $UNIT 2>/dev/null; systemd-run --quiet --unit=$UNIT --collect -p MemoryMax=2G -p Nice=15 -p CPUWeight=20 \
  sh -c 'python3 -I $R/harness/pg_regress_compat.py --out $R/runs/$NAME --label $NAME $* > $R/runs/$NAME.log 2>&1'"
sleep 10
while ssh "$H" "systemctl is-active --quiet $UNIT"; do sleep 30; done
ssh "$H" "tail -3 $R/runs/$NAME.log"
mkdir -p "pg18-regress-runs/$NAME"
ssh "$H" "cd $R/runs/$NAME && tar -cf - summary.json summary.md status.tsv diffs results" | tar -xf - -C "pg18-regress-runs/$NAME"
echo "copied to pg18-regress-runs/$NAME"
