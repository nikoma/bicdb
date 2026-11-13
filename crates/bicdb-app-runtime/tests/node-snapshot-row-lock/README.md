# Native snapshot-lock regression (Node)

This source-authored fixture reproduces a row changing while an authenticated
application transaction waits for `get_for_update`. Its sole provider is a
loopback Node barrier. No ERP store, real issuer, account or payment provider is
used. All compiler and native inputs require explicit SHA256 pins.

Build only the Carrier application package (no Cargo invocation):

```sh
node build.mjs /path/to/carrier COMPILER_SHA256 /new/private/build-directory
```

Then provide a JSON configuration with `native: {path, sha256}` and
`packagePins: "/new/private/build-directory/pins.json"`. If artifacts were
transported to another host, explicit `artifact` and `publicKey` paths may be
supplied; package bytes must still match the original build pin. Run:

```sh
node qualify.mjs /path/to/config.json /new/private/native-output-directory
```

Required results:

- Read committed reads0 before waiting and1 after obtaining the lock; exactly
  one post-lock provider callback observes1.
- Repeatable-read and serializable return409 with **zero** post-lock callbacks.
  Returning409 only at commit after exposing stale0 is a failure.
- Foreign-tenant and role-denied POST lock paths produce no barrier event and
  do not change the owned row.
- Serializable own pending updates and inserts remain lock-readable and commit.
- Actual SIGKILL restart and a closed-store cold copy retain committed rows,
  release process locks, and preserve all three isolation behaviors.

Each request DTO/claim set and response or transport failure is retained before
assertions. Contenders settle with `allSettled`; authenticated readback precedes
result gates. Original2s provider/barrier and30s HTTP/readiness limits remain.
The suite collects internal callback failures across all cases and exits2 when
any occur; a final409 alone cannot turn the original498 defect into a pass.
Node/source generation or execution against an old binary never qualifies a
new binary. This regression makes no claim that stock timeout causality or a
whole financial deployment has been resolved.
