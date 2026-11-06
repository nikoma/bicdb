# Live Oracle source view dependency qualification

The public dependency capture tool ran against Oracle **23.26.3.0.0**, using the
original application schema owner and a quiesced source. All **62 standalone
routine definitions and 160 view definitions** matched the saved export exactly.
It captured **1,291 static dictionary relationships** and produced a dependency
order for all **160 views**. That order differs from manifest order, so applying
views alphabetically cannot be assumed to provide dependency ordering.

[Validation](validation.json) identifies the snapshot, tool source, private-report
hash, resource observations, and container restoration. Capture only reads
source code and dictionaries. The reference container started from a stopped
state and was restored to that state. Client process-group RSS does not measure
Oracle's Docker process; its configured container memory limit and host available
memory are recorded separately. The run followed cleanup of superseded closed
test databases after an unprivileged-volume-space preflight failure; a separate
initial attempt used a Python environment missing the Oracle client library.
Neither preflight attempt changed source data or code.

This qualifies source code identity and static same-schema view ordering, not
installation under that order, target types, dynamic SQL, external schemas,
privileges, runtime values, or complete application behavior. The full original
snapshot index phase separately remains incomplete at 267/268 because datetime
TRUNC expression indexing is not executable in its tested native candidate.
