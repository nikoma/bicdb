#!/bin/sh
# Usage: evid_unit.sh <name> <workdir> <shell command>
# Runs <command> on s3ny as a niced, capped transient unit; waits; tails the log.
set -e
NAME=$1; WD=$2; CMD=$3; H=root@s3ny; B=/root/bicdb-durability-s3
L=$B/evid/log-$NAME.log
ssh $H "rm -f $L && systemd-run --quiet --unit=durability-s3-evid-$NAME-\$(date +%s) --collect -p LimitCORE=0 -p MemoryMax=16G -p MemorySwapMax=0 -p CPUWeight=10 -p IOWeight=10 -p Nice=19 -E RUSTUP_HOME=$B/rustup -E CARGO_HOME=$B/cargo -E CARGO_TARGET_DIR=$B/target-evid -E CARGO_INCREMENTAL=0 -E CARGO_BUILD_JOBS=6 -E TMPDIR=$B/tmp-evid -E PATH=$B/cargo/bin:/usr/bin:/bin --working-directory=$WD sh -c '{ $CMD; } > $L 2>&1; echo exit=\$? >> $L'"
