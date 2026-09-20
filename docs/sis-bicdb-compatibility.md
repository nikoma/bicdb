# SIS SQL compatibility verification

This change follows the SIS provisioning/runtime report against BicDB main
`a95c842c` (which already includes the migration parser fixes in `894c114c`).
It changes BicDB, not Carrier or the SIS application. No production database was
used. Localhost ran formatting and repository checks only; compilation and tests
ran on the existing remote host with at most two Cargo build jobs for this task.

## Reproduced and fixed

| Failure | BicDB change and verification |
| --- | --- |
| DO rejected when its text contains an advisory-function name | Pgwire only considers top-level SELECT statements for its lock-call parser. A real nested catalog-loop/dynamic-GRANT fixture runs over pgwire. |
| Generic grant loop finished without granting the function | Resolve regproc/regprocedure values using function catalogs, not relation catalogs; expose default-argument counts in pg_proc. The test checks the recipient's actual EXECUTE privilege for a defaulted function. Removed the authored-function marker-specific grant shortcut. |
| Missing pg_timezone_names | IANA catalog with names, aliases, abbreviations, current offsets, and DST; tests cover EXISTS, IN, qualification and a correlated candidate CTE. |
| Scalar IN (SELECT ...) without FROM failed | Session expression evaluation, including NULL, NOT IN, and empty-set semantics. |
| Hashed advisory-lock keys rejected | hashtextextended plus expression evaluation in the caller's SQL transaction; two connections verify exclusion, different keys, and rollback release. Hash vectors were compared with PostgreSQL 16, including Unicode and negative seeds. |
| BEGIN; SET LOCAL in one message lost transaction state | Explicit transaction batches execute through persistent connection state. Tested with a subsequent parameterized lock query using the local setting. |
| SKIP LOCKED returned a row held by another connection | Transaction row locks shared with DML, skip-before-limit, joined OF-alias selection, NOWAIT, savepoint release, privileges and RLS. Protocol Describe does not acquire row locks. |

Standalone wire probes also passed for AT TIME ZONE, string_to_array,
jsonb_to_recordset, regex operators, generate_series and ON CONFLICT. This is
representative construct coverage, not execution of every SIS action.

The generated SIS role script contained 13 DO blocks. A scratch run of the
initial parser patch completed 12 and correctly rejected the signing-key block
without a supplied key. It also reported missing application relations/functions
because that scratch database had no SIS schema. **That run is not a successful
full provisioning result.** On the final rebuilt binary, all 13 extracted DO
blocks completed in a transaction with a test signing key and existing
provisioning prerequisites. The actual grant probe returned true for the
recipient's EXECUTE privilege, time-zone validation passed, and two concurrent
connections returned row 2 after row 1 was locked. These probes still do not
replace the missing full-schema SIS deployment and behavior-suite run.

## Regression checks

The SQL unit suite passed 313 tests (three existing ignored tests); the six new
SQL integration tests passed. The pgwire suite passed 91 unit tests and 159
protocol tests (one separate existing ignored test). After strengthening UPDATE
RLS enforcement for locking reads, the focused SQL tests passed again. The final
advisory-key transaction change is checked by a further pgwire suite run and a
rollback regression for a failing key function; both passed. Final focused
checks passed six SQL tests and ten tokio-postgres protocol tests.

GitHub-hosted checks were unavailable for this validation run. This is not a
passing CI result.

Formatting, license/notice synchronization, and the repository dependency/API
boundary checks passed. The PostgreSQL-derived hash notice is included in the
source file and in synchronized package notices. No broad engine benchmark or
SIS production deployment result is claimed.

## Integration work still required

- Apply the Carrier catalog-relation grant fix and update the SIS pins to a main
  revision containing these BicDB changes. Old 1.0.349 binaries lack both this
  work and the earlier guarded-DDL/CONTINUE fixes.
- Run fresh SIS migrations, the complete role SQL with a test signing key, and
  the actual Node behavior suite on BicDB. Test restricted SQL roles, signed
  operations, multiple tenants/principals, and reused pooled connections.
  A successful health check is insufficient.
- Verify application retry handling: ordinary FOR UPDATE contention and changed
  snapshot rows return `40001`. NOWAIT uses `55P03`; SKIP LOCKED skips occupied
  rows. This is not PostgreSQL's default blocking behavior.

See [transaction details](postgresql-transactions.md#locking-reads) for supported
locking shapes and [procedural limits](postgresql-procedural.md) for remaining
PL/pgSQL limitations. FOR SHARE, outer-join locking, locking without primary keys,
and grouped/window/DISTINCT/FETCH locking queries remain explicitly unsupported.
Dynamic EXECUTE INTO/USING and other procedural forms are not implemented by this
patch. Do not infer that untested application paths pass, or promise that only a
fixed number of application issues remain.

The PostgreSQL-derived hash implementation retains its separate license and
notice; product licensing and previously granted rights remain unchanged.
