# Routine security declarations

Routine authority now follows declaration attributes. Body literals, comments,
argument defaults and quoted identifiers cannot supply SECURITY DEFINER.
Function declarations use the PostgreSQL AST; dollar-quoted procedures use the
existing procedure-header parsing proxy. Unrecognized declarations confer no
definer authority.

Readers recompute the cached security flag from the saved declaration, including
records written by an older implementation that classified body text as an
attribute. Recorded ownership is still required for definer execution; missing
ownership retains invoker behavior. The saved source and owner remain intact.

The correction also distinguishes qualified user functions from administrative
builtins. A schema starting with `bicdb_` does not make its functions admin APIs.
Unqualified and pg_catalog-qualified admin calls retain their effective-role
checks. A regression verifies that an ordinary role can execute its granted
function in such a schema while actual admin calls remain denied.

The PostgreSQL 18.4 comparison matches **5/5 permission cases**: declaration
metadata, denial of owner-table reads by default and explicit invoker functions,
permitted explicit-definer access, and caller-identity restoration. The prior
candidate matched 3/5 because its admin guard blocked qualified user functions
as well as misclassifying security metadata. Two separate library regressions
reproduced unintended owner-table access and stale flags before the fix; both
pass after correction.

Six process-reopen checks use a database created by the prior reader. That reader
reproduces the incorrect access in a disposable fixture. The patched reader
denies it without recreating the function, preserves explicit-definer access
and retains the correction on another reopen. The corrected authority semantics
require the patched runtime; prior readers retain their earlier behavior.

Reproduce the native comparison against isolated databases with two existing
fixture schemas:

```sh
python examples/oracle/reference/postgres_routine_security.py \
  --schemas security_fixture_a security_fixture_b --output /private/security.json
```

Set ORACLE_CONTROL_PG_DSN and BICDB_PG_DSN privately. The fixture creates its own
role, table and functions, then removes them. The target must use the native
PostgreSQL profile. Ten independent Oracle declaration/value regressions and four
native attribute controls also pass, including body text containing the same
words. Oracle AUTHID and package security remain separate qualification work.

Validation passes **1,353 Rust tests**, with three existing ignores, **58 Python
tests** and all **47 retained reference-report families** on the same development
candidate. The [validation report](validation.json) records the binary hash,
individual outcomes and resource guards. Format, whitespace, license and
open-core boundary checks pass. Original definition acceptance remains 62/62;
NULL-input diagnostics remain 1/14 and full application execution is unqualified.
