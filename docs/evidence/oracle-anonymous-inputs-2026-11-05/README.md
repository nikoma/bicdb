# Bound Oracle anonymous submissions

This bounded checkpoint accepts input parameters in raw Oracle anonymous blocks
submitted over PostgreSQL extended protocol with `oracle-preview-v1` selected.
It does not qualify a complete JDBC adapter, LOB locators or the whole application.

The server captures parameter positions from Oracle tokens, excluding strings,
comments and native dollar-quoted routine bodies. It infers supported declaration
and SQL input types without executing the block during preparation. Binding uses
the captured namespace and lowers the Oracle source again with decoded values;
the generated delimiter includes the actual literal content.

A failed preview statement restores its uncommitted work before the transport
allows the caller transaction to continue. Prior pending writes survive. Explicit
commits inside a block remain durable while later uncommitted writes roll back.
Exclusive execution is selected before an ordinary block transaction boundary, and a
statement that crossed such a boundary cannot be replayed automatically. Oracle
procedural mutations start a new pending unit after their previous unit ended;
ordinary standalone updates retain their existing lock/error behavior.

The [native Oracle comparison](transaction-comparison.json) matches eight controls
against Oracle 23.26.3.0.0, using only nonce tables removed by the runner. It covers
a bound failing block, prior pending writes, later writes, caller rollback,
explicit commits and rollback after a committed block fails. The fixture is
[anonymous_host_transactions.py](../../../examples/oracle/reference/anonymous_host_transactions.py).
Use its common reference-runner connection arguments with `--reference oracle`.

Protocol regressions cover input metadata, repeated execution, declaration defaults
and assignments, literal/comment boundaries, NULL, Unicode, transaction rollback,
commit durability and independent PostgreSQL failed-transaction policy. The
language contract remains version 27; no stored routine definitions are rewritten.

Parse/Bind failures and errors without confirmed cleanup retain conservative
transaction-failure handling. Callable OUT parameters, complete Oracle transaction
semantics and production qualification remain open. Stock PostgreSQL JDBC may
split an anonymous block before transmission; its application integration remains
a separate delivery gate. The accepted application runtime is not replaced by this
server-level checkpoint.

The [local validation record](validation.json) retains 993 passing Rust tests,
zero failures and three pre-existing ignored benchmarks. The checks include SQL
and transport libraries, 45 Oracle-profile protocol tests, 171 general protocol
tests, routine ownership/assignments, transaction system types/uniqueness and
both profiles on a 2 MiB recursive-query worker stack. Workspace all-target
compilation, formatting and whitespace checks pass. Thirteen retained independent
Oracle/PostgreSQL reference families also pass against the frozen candidate.
These results precede integration with later changes on main; any subsequent
combined-build checks are recorded separately.
