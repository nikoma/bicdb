# Routine ownership, ordinary-role JSON constraints and cold relation lookup

Engine and tool source `a4d766d3f7fd92e15ff22beb214b285a70f76d27` was built and
qualified, then merged into remote `main` at
`14ad3e1172ec3009fac65a90037ba547e5117251`. The binary is a development
`oracle_server` example, not a published production release or verified
application deployment.

Implemented and tested:

- `IS JSON` CHECK constraints allow valid inserts/updates by ordinary writers,
  reject invalid JSON, and preserve administrative-function denial, including
  after `SET ROLE`, in both PostgreSQL and Oracle preview profiles. This change exempts the
  exact value-only predicate helper; administrative functions retain their gate.
- Native `ALTER FUNCTION ... OWNER TO` checks namespace and input signature.
  Native `ALTER PROCEDURE ... OWNER TO` uses the same ownership/membership checks.
  Mismatched identities are rejected before changing metadata. Transaction
  rollback, restart, ordinary-role calls and unrelated-role denial are checked.
- The [routine-owner migration phase](../../oracle-routine-owner-migration.md)
  checks the exact exported inventory and explicit role mapping, changes all
  routine owners in one transaction, and verifies unchanged definitions before
  commit. Check-only, resume identity, wrong modes, missing/superuser targets,
  foreign ownership, extra routines and later-DDL rollback have controls.
- Cold relation lookups use point metadata reads. Four SQL unit cases verify
  that unrelated view definitions are not enumerated, exact namespace/quoted
  identity is retained, same-count raw collection replacement invalidates memo
  entries, and transactional view drop/rollback refreshes cached results. Two
  native profile cases cover prepared reads, DDL visibility, restart and denial
  of unrelated data. No comparative speedup is claimed.

Validation passed: 728 SQL library tests (4 ignored), 44 authorization/JSON
integration tests, 183 PostgreSQL protocol tests and 48 Oracle HR protocol tests.
The migration-tool run reports 208 tests: 204 passed, 4 skipped because external
Oracle reference configurations were unavailable to that run. The 13 focused
checks are repeated subsets of those 208 tests, not additional coverage totals.
[summary.json](summary.json) records the skipped/ignored names, binary identity
and resource observations.

With Rust, Java 17 or later and Maven available, build the JDBC adapter and
server example before running the native tool checks:

```sh
mvn -f examples/oracle/jdbc/pom.xml package
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 \
  cargo build --locked -p bicdb-pgwire --example oracle_server
BICDB_ORACLE_SERVER="$PWD/target/debug/examples/oracle_server" \
  python3 -m unittest discover -s examples/oracle/idempiere -p 'test_*.py' -v
```

These controls qualify the tested semantics and migration tool. They do not
establish complete source security equivalence, privilege closure, execution of
every exported routine, full iDempiere behavior, recovery, or production
qualification. Fresh full-export ownership/data qualification and original
application reruns are separate gates. The prior unsuccessful 89-test result
remains in the [pristine baseline checkpoint](../idempiere-pristine-owner-baseline-2026-11-06/README.md).
