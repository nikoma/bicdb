# Original zoom-provider test after contextual LIKE parsing

The unchanged original iDempiere `GenericZoomProviderTest` passes its one
invocation on a disposable clone of the original Oracle-schema baseline over
PostgreSQL JDBC. It previously errored while parsing an explicit `LIKE ...
ESCAPE` suffix. The contextual grammar fix resolves that observed error without
changing application source, class bytes or assertions.

The [provenance record](summary.json) identifies backend source `1562154`,
adapter source `3db8c5d`, artifact hashes, original source/class/JAR hashes,
the result XML hash and four verified runtime class origins. Source bytes also
match upstream revision `da0730a`. The JVM and database exited zero and were
confirmed stopped. The closed passed database was removed, reclaiming
573,104,128 allocated bytes; logs, XML, provenance and earlier failed evidence
remain private alongside the protected pristine baseline.

This is a retest, not an additional distinct invocation. It qualifies this
selected original class on these revisions. It does not establish complete
Oracle LIKE semantics: explicit NULL escape values, error priorities and
collations have separate reference and implementation gates. Other original
application failures, the remaining classes, UI workflows, paged application
acceptance and the full compatibility programme remain open. No full application
or production qualification is claimed.
