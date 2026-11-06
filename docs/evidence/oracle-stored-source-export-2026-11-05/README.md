# Stored-source catalog capture checkpoint

The exporter previously selected only `FUNCTION`, `PROCEDURE` and `TYPE` from
`USER_SOURCE`. It now preserves package specifications/bodies, type bodies and
trigger definitions in distinct catalog/manifest groups. A versioned marker
records source-kind coverage. Enabled and disabled trigger states remain
distinct, and absent or inconsistent dictionary data fails before publication.

The [validation record](validation-results.json) separates:

- **Eight new offline tests:** category retention, package/type declaration and
  body relationships, exact DDL hashes, blank/empty/wrapped text handling,
  malformed source lines and missing/duplicate/unknown trigger status. Fake
  source queries honor the requested kind filter, so omitting a new category
  does not accidentally pass. Mocked full exports verify both output catalogs
  and verify that incomplete source prevents publication.
- **Five existing temporal-export tests** and **14 existing migration/value
  tests**, all passing.
- **One live Oracle acceptance test, skipped:** the test is executable but its
  required empty disposable Oracle owner was not provisioned for this batch.
  Package/type-body/trigger capture is not live-qualified by the offline tests.
- **An offline legacy-DDL reconstruction control:** the retained application's
  62 routine and one type DDL strings are reconstructed into dictionary-like
  lines and remain byte-for-byte identical after grouping. This does not query
  Oracle or recover independently verified original dictionary line numbers.

No package compiler, trigger runtime, object method execution or session-state
support is added by this change. Those programme gates remain outstanding.
The source row codec and SQL projection are unchanged. Existing private exports
were not rewritten; a new export is required to receive the source-capture
marker. The FK migration and application pipeline retain their pinned inputs.

Follow the [source contract and live acceptance command](../../oracle-stored-source-export.md)
before relying on these new groups for migration. No native binary was built or
accepted application upgraded in this batch, and no GitHub workflows were
changed or dispatched.
