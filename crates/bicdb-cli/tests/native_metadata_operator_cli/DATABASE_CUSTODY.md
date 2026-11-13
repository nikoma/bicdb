# Opt-in failure-only database custody

This test-only successor preserves the original 3ee failed cohort/evidence. It
changes no production admission, retry, deadline, read or write behavior. The
existing small diagnostic allowlist remains separate and unchanged in scope.

A focused CI runner may explicitly set:

```sh
export BICDB_TEST_CLI_DB_EVIDENCE_ROOT="$RUNNER_TEMP/native-cli-db-custody"
```

The workflow must separately retain that subtree. It must **not** recursively
upload the fixture root, its sibling TLS files or the private quarantine paths.
This source adds the helper/interface; it does not itself dispatch or rerun CI.

`Processes` records the explicit `dbN` root passed positionally to every actual
spawn. Failure Drop first kills/waits the current children, checks every recorded
spawn against those actual waits or earlier explicit kill/waits, and only then
invokes the checked-in Python helper without a shell. A live/unwaited spawn
refuses capture. All root names must be exact fixture children `db0`–`db15`.

| Environment cap | Default | Accepted override |
| --- | --- | --- |
| `BICDB_TEST_CLI_DB_MAX_BYTES` | 512 MiB logical regular-file bytes | 1 byte through 8 GiB |
| `BICDB_TEST_CLI_DB_MAX_SECONDS` | 30 s from capture setup | 1–120 s |
| `BICDB_TEST_CLI_DB_MIN_FREE_BYTES` | 128 MiB available-space floor | 16 MiB–64 GiB |
| `BICDB_TEST_CLI_DB_MAX_ENTRIES` | 16,384 files/directories | 1–65,536 |

These are declared resource limits for the current small fixture, **not** a
claim that an 8 GiB workload or recovery has been qualified. Time/disk checks
occur between entries and 64 KiB chunks, without renewing the original capture
deadline. Read/fsync/kill/wait can be kernel-uncancellable; no hard OS-I/O bound is
claimed. Rust owns the copier process and applies an additional 125 s stop/wait
boundary, reporting PARTIAL on failure/timeout rather than abandoning a worker.

Every source component is opened through anchored directory FDs and
`O_NOFOLLOW`; symlinks, nonregular files and hardlinked regular files refuse the
whole capture. TLS/key/certificate filename extensions and PEM private-key
markers (including markers crossing chunk boundaries) refuse capture. Only
explicit database roots are walked. No engine is opened, repaired or replayed,
and no source database file/directory is fsynced. Source identities/lengths/
mtime/ctime are checked before and after copying; original read atime is not
claimed unchanged.

Copying starts in a new mode-0700 private quarantine **outside** the requested
artifact subtree. A prewritten PARTIAL manifest exists before source copying.
Every completed file records relative path, source type/identity/mode/logical
length, streamed count/hash and destination fsync/mode; directories are fsynced
bottom-up. Only a complete stopped-root copy is renamed into the artifact
subtree, with a final manifest and successful helper result. Any refused path,
cap, read/write/hash/currentness or publication error makes the result PARTIAL;
no partial payload is deliberately published. Publication/fsync failures may be
ambiguous and must be interpreted together with helper exit/stderr, never by
finding a directory or isolated `complete` field. Staging remains quarantined
on interruption; cleanup/inspection requires the retained manifest and owner
scope, not an automatic restore.

A successful copy preserves stopped test bytes. It does **not** establish
source power-loss durability, the durable result of an uncertain append,
post-failure ACK reconciliation or recovery/readiness. Those require separately
scoped actual engine checks against an isolated copy. No TLS keys should be
uploaded. No original 3ee failure is reclassified by this helper.

The actual copier is exercised with small real files and actual short child
waits, without Cargo:

```sh
python3 crates/bicdb-cli/tests/native_metadata_operator_cli/test_database_custody.py
```

The 16 filesystem/helper-process controls cover complete nested copy/hash/modes, actual helper subprocess/manifest protocol, source
metadata preservation and sibling TLS exclusion, symlink/hardlink refusal, TLS
filenames and cross-chunk private-key markers, byte/time/actual-free-space caps,
unwaited-child refusal, overlapping destinations and root entry counting. Four
publication controls cover actual final receipt write/fsync followed by a
returned error, final directory-fsync failure, failed quarantine rollback with
truthful payload-presence reporting, and persistent receipt failure with an
independent PARTIAL diagnostic. Final manifest persistence is inside the same
publication rollback boundary. A Rust RAII child owner also kill/waits on any
helper polling error, not just timeout. When
`/tmp` has no ordinary free space, these <256 KiB temporary helper cases use the
already mounted `/dev/shm`; the real disk-floor check remains enabled. This is
helper validation only. Rust glue and actual CLI failure capture still require
the next remotely compiled observer/custody cohort.
