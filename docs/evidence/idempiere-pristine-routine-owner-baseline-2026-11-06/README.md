# Pristine baseline after relation and routine owner mapping

A cold clone of the original-source pristine database completed the public
relation-owner and routine-owner migration phases with engine/tool source
`a4d766d3f7fd92e15ff22beb214b285a70f76d27`.

The dedicated non-superuser application role owns the namespace, 915 tables,
160 views, 48 functions and 14 procedures. Full catalog readback passed.
Original stored routine and view definitions were then compared to the retained
installation reports, allowing only the explicitly mapped owner fields.

After owner mapping, all checks passed:

- Saved-source value equality for 915 tables and 185,471 rows.
- All 3,826 source-declared foreign keys and 888 affected-table fingerprints.
- All 3,826 orphan counts, each zero and equal to the saved Oracle reference.
- All 160 view projections' labels and broad type families.
- One nonempty source table's count through the actual application role.
- Final definition-preservation checks and a clean database shutdown with exit zero.

The seven public migration/checking phases exited zero. The memory guard
completed without an interruption. [summary.json](summary.json) records source,
binary and retained-report hashes, counts and resource observations, without
publishing application data, credentials, endpoints or private paths. Hashes
identify retained evidence; they do not replace independent reproduction.

The unchanged original application tests run on separate disposable clones of
this baseline. Their outcome is a separate gate. Owner mapping does not prove
execution of every routine, source privilege equivalence, privilege closure,
authentication, exact view descriptors/values, full application behavior,
recovery, live change capture, or production qualification.

The [routine-owner tool checkpoint](../oracle-routine-owners-2026-11-06/README.md)
contains independent PostgreSQL/preview controls and broader SQL/protocol
validation. The earlier [relation-only application result](../idempiere-pristine-owner-baseline-2026-11-06/README.md)
remains an unsuccessful 89-test run; this baseline checkpoint does not relabel
that result or claim that its application rerun has passed.
