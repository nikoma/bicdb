#!/usr/bin/env bash
# Runs the BicDB Jepsen suite.
#
# Every node is a local process, so no SSH and no containers are needed; the
# test runs with :ssh {:dummy? true}. JDK 21 is required by the Jepsen
# dependency tree (java.util.SequencedCollection).
set -euo pipefail
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
export BICDB_BIN="${BICDB_BIN:-/root/bicdb/target/debug/bicdb}"
cd "$(dirname "$0")"
exec lein run test "$@"
