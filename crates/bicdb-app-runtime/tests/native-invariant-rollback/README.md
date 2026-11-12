# Native invariant rollback regression

This standalone Node test uses four immutable ABI-v2 packages produced by the
pinned official Carrier compiler. They have identical resource contracts except
for release-derived schema versions. The packages contain no product schema,
external dependency, network integration or real identity provider.

The test proves that code rollback cannot remove an active invariant or replace
its expression with an older weaker one. It also proves that a compatible code
rollback remains available. Accepted rows and rejection results survive process
restart and cold-copy restore. Requests use the authenticated tenant; clients do
not supply the server-owned tenant field.

Run with an already built BicDB CLI and a JSON configuration:

```json
{
  "binary": "/absolute/path/to/bicdb",
  "binarySha256": "the executable's SHA-256",
  "source": "the executable's source commit",
  "outputRoot": "/absolute/path/to/private/test-runs",
  "expectProtected": true
}
```

```bash
node qualify.mjs config.json
```

`expectProtected: false` is a negative-control mode. It requires the old unsafe
behavior to be observed; it does not qualify that binary as safe. A protected
result must additionally report `actualProtection: true`.

The source for each fixture is in `source/`. Compiler source and executable hash
are pinned in `package-lock.json`. These test packages use the deterministic
public test seed `0x2a` repeated 32 times and key ID
`qualification-rollback-guard`. They are trusted only in these disposable stores.
Regeneration uses `carrier check --target bicdb --profile production` and
`carrier build --target bicdb`; signed package bytes are copied unchanged from
official compiler output. This test does not build or execute Rust tests.
