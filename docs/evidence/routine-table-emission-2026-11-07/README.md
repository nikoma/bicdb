# EHR monitoring routine compatibility — source regressions verified; EHR monitoring still unqualified

Engine implementation: `f17ac7c02a1bd28bc64c2355069675c6b22c97bc`.
Parent/main at start: `565227218dde395d0e1b96b9a29673b2212f7b85`.

An actual hosted EHR monitoring run on the preceding timezone-fixed engine
created/approved a template and started enrollment, but its worker failed with
SQLSTATE 42601. A read-only diagnostic against that owned synthetic database
reduced the failure to `monitoring_occurrences`: `open_h := ...` was parsed as
an OPEN cursor statement. The same function emits its declared output values
with bare `RETURN NEXT`, which was explicitly unsupported.

Statement keywords now use the existing identifier-boundary helper, so ordinary
variables beginning with OPEN/FETCH/CLOSE/RAISE/EXECUTE remain assignments.
Bare RETURN NEXT snapshots the named output parameters into the set result,
casts to the declared output types, continues execution and leaves FOUND alone.
Expression-bearing RETURN NEXT remains explicitly unsupported. Profiling,
symbol/binding, effect and Oracle traversal visitors handle the new leaf node.

Causal evidence on the unchanged parent plus the new test:

- `before.txt`: three regressions FAIL (cursor parse; two RETURN NEXT cases).
  The expression rejection control passes before the fix and is not causal proof.
- `after.txt`: all four tests PASS with the implementation, same command/profile.
- Command: `cargo --config profile.dev.debug=0 test -p bicdb-sql --test routine_table_emission -- --test-threads=1`.

The intermediate compile failure from adding the leaf node before updating
all exhaustive visitors remains on the build host. It is not a passing result.
All source/build/application fixtures are public or synthetic on that host.
Clinical records and original clinical keys remain on production hosts.

All source suites passed on this implementation: 763 SQL library tests (four
existing ignored), 39 routine/domain integration tests across 12 targets,
188 protocol tests plus the procedural-migration test, and 140 pgwire library
tests. Formatting and diff checks passed. The logs and status record are retained.

The frozen development pair uses source manifest SHA-256
`9edc2712fee8e6cba4c8db52814fa04c0d2bb0d3f831a486fdd08c6264121820`
(1,411 inputs). Its cold paged fixture `/tmp/walknorth-ehr-bicdb-paged-m3klgml7`
passed the actual 168 migrations, replay, restricted API identity, patient and
meal operations, tenant refusal, encryption and engine restart.

The actual hosted diagnostic `/dev/shm/ehr-bicdb-hosted-root-20261105/ehr-bicdb-hosted-pAxpGk`
started all four real packages and passed web/static/anonymous refusal,
meal/mobile and acknowledged encrypted email checks. Monitoring still FAILED:
its worker reported 42501 and did not prepare the expected reminder. The original
45-second expectation was retained. The readonly occurrence diagnostic now
executes successfully, but emits 5,001 rows with one identical due date: named
make_interval arguments are dropped in procedural scalar expressions. A later,
separate fix is required; this is not a qualified monitoring release.

No production admission, production cutover, HA acceptance, clinical
data-transfer/recovery acceptance or complete EHR acceptance is claimed.
