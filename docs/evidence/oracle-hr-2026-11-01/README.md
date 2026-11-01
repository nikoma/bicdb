# First Oracle HR schema: local functional acceptance

The adapted Oracle Human Resources schema ran on BicDB source
`72b83846cdeaddd11ee0891e251edd57dc9d8531` using the regular psycopg 3.3.4
PostgreSQL driver. The upstream Oracle sample revision is
`6660bad68c07bd143430ace58565b3f727e17263`.

The [report](report.json) records the development binary digest, all seven
initial table counts, the successful parameterized query, view and procedure
checks, and the counts reverified after a graceful server restart with fsync
enabled. The committed procedure call increased JOB_HISTORY from 10 to 11;
the rejected call left that count unchanged. A separate post-restart probe
verified that UPDATE through EMP_DETAILS_VIEW fails with SQLSTATE `0A000`
and leaves the employee salary unchanged.

| Object | Initial rows | After restart |
| --- | ---: | ---: |
| REGIONS | 5 | 5 |
| COUNTRIES | 25 | 25 |
| LOCATIONS | 23 | 23 |
| DEPARTMENTS | 27 | 27 |
| JOBS | 19 | 19 |
| EMPLOYEES | 107 | 107 |
| JOB_HISTORY | 10 | 11 |
| EMP_DETAILS_VIEW | 106 | 106 |

The Rust pgwire acceptance separately covers native binary/typed binds,
DATE time preservation, sequence evaluation, the Oracle procedure-call block,
rollback, profile mismatch rejection and PostgreSQL behavior. The SQL library
suite passed 320 tests with three pre-existing ignored tests. The full pgwire
suite and workspace all-target compilation passed locally. Python preparation
tests verify the pinned source and generated migration.

The [profile contract](../../oracle-hr-profile.md) contains reproduction steps,
the exact source adaptations and remaining limitations. The compiled runtime
source is pinned above; subsequent acceptance/documentation changes do not
change that executable's runtime implementation.

This is a small, local functional acceptance of an adapted published schema.
It is not an Oracle Database differential comparison, production performance
measurement, Oracle driver certification or arbitrary application migration
qualification. The source retains its MIT license and original bytes,
including upstream whitespace.
