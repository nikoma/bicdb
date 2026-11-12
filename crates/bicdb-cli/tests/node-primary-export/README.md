# Bounded primary-record recovery export

This Node test copies the four committed `bicdb-core/tests/fixtures/decimal-predicate-legacy-main482*`
stores. An explicitly pinned older reader enumerates every acknowledged row,
including the conflicting-key fixture. Only the copies receive corrupt index
and CDC sidecars. Normal indexed admission must refuse them. Primary-record
export must retain every row without decoding those unrelated sidecars, and
must preserve their exact bytes through another process and cold-copy restore.

Run an already built CLI; no Rust test or helper is compiled:

```sh
node qualify.mjs /absolute/path/to/config.json
```

The JSON configuration requires `binary`, `binarySha256`, `source`, `oldReader`,
`oldReaderSha256`, `fixtureRoot` and an owned private `outputRoot`. Set
`expectExport: true` for qualification. `expectExport: false` requires the old
failure as a negative control and never qualifies recovery export.

The index-disabled handle cannot perform foreground mutations. This recovery
path does not certify an index generation or grant indexed query/write
admission; normal open still validates and repairs the catalog. Reader format,
encryption, directory ownership and pending-restore fences remain in effect.
