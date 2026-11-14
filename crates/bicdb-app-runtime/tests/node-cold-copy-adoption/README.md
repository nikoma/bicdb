# Cold-copy custody helper (qualification only)

`qualifyColdCopy` copies a genuinely stopped source and calls the first-party
`bicdb backup adopt-copy SOURCE TARGET` before the target's first application
open. The caller supplies the stopped child's `closedSource` object with `pid`,
`exitCode` and `signalCode`; its exit must have been awaited. The helper verifies
that PID is absent and records full source/target file hashes and modes.

Ordinary stores can have no active `native_origin_root_v1` flag, including fresh
standalone stores. Official `NotRequired` is valid only with both trees unchanged
and both flags still inactive. This helper does not enable HA or mint a root.

With an active flag, the helper requires an unadopted copy's genuine custody
refusal, refusal of an unauthorized copied source, official adoption with the
same root identity/next generation/predecessor, and an idempotent retry. A plain
copy of the adopted target must still be refused. Only the copied root record
may change. No checksum or restore authorization is created by the harness.

Subprocess output is collected through `close`, including spawn failures. The
original30s CLI qualification bound remains. The caller then runs its original
authenticated recovery assertions and calls `assertColdCopySourceUnchanged`;
the original source stays at its original path, closed and byte-identical.

These checks establish results only against the exact explicitly pinned native
binary actually exercised. Syntax or fake-process helper tests do not qualify
native custody behavior. Run no fixture against customer stores or real issuers.
