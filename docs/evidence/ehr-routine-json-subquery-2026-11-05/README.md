# JSON aggregate subquery types inside stored-routine expressions

Base: `9bee2c6c63b7bfe74ac0ea285839c4bc1eebfcb6`.

The rebuilt hosted EHR demonstrated a remaining COALESCE case: a stored routine
returns `jsonb_build_object('items', COALESCE((SELECT jsonb_agg(to_jsonb(x)) FROM
(SELECT ... GROUP BY ...) x), '[]'))`. The scalar-subquery type helper declined
derived sources, leaving its JSON type unknown and embedding the fallback as a
string. Ordinary SQL-expression and simpler routine cases already passed.

The helper now recognizes the declared JSON/JSONB return type of built-in JSON
constructors and aggregates before requiring input-column metadata. It examines
the AST only; it executes neither a source nor a routine body. Other uncertain
source/CTE cases retain their existing conservative handling.

- `negative.log`: the new stored-routine regression fails with string array/
  object fallbacks before the fix (exit 101).
- `positive.log`: all 63 tests pass across JSON SRFs, Carrier workflows,
  type consistency and routine assignments. The regression verifies empty,
  populated and empty-again grouped array/object summaries.
- `protocol.log`: all 180 pgwire protocol tests pass.
- `rustfmt --check` and `git diff --check` pass.

The actual hosted loader, buffer and distorter writes, history, corrections and
access refusals pass on the preceding `9bee2c6` binary. Its remaining empty-list
failure is addressed by this source change and still needs the rebuilt hosted
run. Program creation separately commits its insert/audit, then crashes on the
model read; native debugging shows deep recursive boolean RLS evaluation. That
repair, duplicate-goal verification, full clinical qualification and production
cutover remain open. No production data or services were changed.
