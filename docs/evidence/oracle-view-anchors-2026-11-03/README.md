# View-column type anchors

Oracle preview `%TYPE` resolution now accepts named view columns in addition
to table columns. Resolution uses persisted view descriptors in the definition's
namespace and never executes the view query. Local numeric precision and text
length constraints retain the existing table-anchor rules; formal and character
return sizes retain their existing unconstrained signature behavior. This lowers
to the already supported scalar declarations and adds no new persisted format.

The public `view_anchors` suite reuses the table-anchor scalar contracts through
an explicitly projected view, with quoted column names. Run it against dedicated
Oracle and preview schemas using the existing comparison entrypoint:

```sh
python examples/oracle/reference/routine_namespaces.py --suite view_anchors \
  --oracle-config /private/reference.json --output /private/view-anchors.json
```

Set BICDB_PG_DSN privately. Fixtures remove their own functions, views and tables.
The reference reports, native PostgreSQL controls, and local validation summary
in this directory identify the candidate binary and the checks actually run.
The preceding binary rejected the new fixture at definition setup with 0A000.

This is a prerequisite for Oracle dictionary-view anchors, not an implementation
of USER_TAB_COLUMNS or USER_TABLES. The application definition baseline has not
been requalified by this change. Dictionary contents, user visibility, view DDL
invalidation, cross-schema privileges and full application workflows remain open.
The running application and database are unchanged.

The candidate passes **41/41** view-anchor Oracle comparisons and **41/41**
retained table-anchor comparisons. Independent native PostgreSQL controls pass
**19/19**. Local checks pass **466** SQL unit tests and **25** Oracle-driver tests,
with three existing SQL unit ignores, plus **25** Python reference tests. Builds
use one compiler, disabled incremental compilation and an active 8 GiB available
RAM floor. Cleanup reclaimed 638,758,360 bytes of completed test executables;
the current runnable binary and required readers are preserved.
