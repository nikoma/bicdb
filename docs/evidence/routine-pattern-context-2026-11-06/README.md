# Routine pattern predicates

The real restricted email worker test reached its deliberate plaintext rejection
case but received SQLSTATE 0A000 instead of the helper's intended 22023: LIKE
predicates in generic stored-function expressions were sent to a pure evaluator
without their parameter/local context.

SQL functions now use the existing row evaluator for LIKE/ILIKE predicates.
Procedural fallback expressions evaluate both operands through the current
session before using the existing pattern matcher. NOT LIKE, ILIKE, local
variables, nested builtins and SQL NULL results are covered. Existing unsupported
LIKE ANY behavior remains a truthful error.

Both new tests fail on unpatched 8663afb (0/2) and pass with the fixes (2/2).
The SQL unit suite passes 717 tests, with four pre-existing ignored tests;
four focused integration suites and all 183 pgwire protocol tests pass. Exact
results and artifact/source hashes are retained. Builds used bounded concurrency,
no incremental compilation, and active RAM/disk headroom guards; completed test
executables were hashed and retired. Production has not been cut over.
