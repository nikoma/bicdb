# Native read catalogue custody correction (source only)

This isolated correction starts from read-foundation commit
`8fce6e0f26c1164b39e448ea7b23104e84e7e5ed`. It imports no Abort, Joint, learner
registration controller or unpublished full-prototype format. The original
missing-only custody witness is preserved: corrected baseline `3d0041e09`, run
43218099779/Core131182402124, compiled and failed its intact-value assertion in
6.30 seconds. The intact stopped/cold twin returned the full acknowledged R1
value. Removing only n1's decision catalogue allowed a false clean-zero path
whose Strong result was None. This is a scoped actual-service witness, not a
claim of permanent global loss, Azure causality or production qualification.

## Necessary refusal and genuine first creation

`RangeWriteStore::open` now records whether its ORIGINAL read decoded and
verified an existing envelope. Enforced-root NotFound returns the dedicated
`BicDbError::NativeRangeCatalogueCustodyMissing` before creating or migrating any
catalogue. It cannot become existing provenance because a later file appeared,
a store wrote it, policy is absent, or native/raw counters happen to be zero.
Fresh policy sealing under the actual service/Store owner re-fsyncs only its
unchanged verified catalogue before native policy WAL: a visible file from an
earlier sync ambiguity is not durability evidence. Generic open gains no new
unconditional rewrite path. Reading unchanged old recovery refusals may still
re-fsync them; that path
explicitly requires successful original load. Legacy unbound stores keep their
existing missing-file semantics and serialized catalogue bytes.

A private, nonserializable `FreshNativeCatalogueBootstrap` is constructed ONLY
inside the atomically NEW enforced native factory, borrowing that actual DB and
actual native-WAL birth. Strict reopen never returns this capability. After
native birth and the new reader fence, before completion/factory exposure, its
consumer creates the exact empty format-12 enforced catalogue with create_new,
writes the complete checked envelope, syncs the file and syncs the directory.
It never overwrites an existing or partial decision file. Factory failure leaves
an unexposed partial root; neither an existing no-policy root nor an original
creation failure can acquire a new bootstrap identity by retrying create.

The loaded-existing check is revalidated at policy sealing and again through
actual service validation before and after native policy publication. It checks
same root/open, fsync, no catalogue persistence uncertainty and exact verified
on-disk catalogue equality to the actual Store state, with a 64 MiB refusal
bound. This check uses native record reads without reacquiring the native
journal. A prior native writer-policy record must validate and match the actual
birth; malformed prior policy is an error, never absence. Neither a valid prior
policy nor an absent one grants reconstruction of missing decision custody.

## Reader boundary and crash cuts

`native_catalogue_custody_v1` is a mandatory new node-local reader capability
for the revised writer path. It is durable before fresh birth/factory exposure,
is retained across format replacements and is excluded from portable schema
reader features. The old unknown-feature predicate rejects actual metadata
bytes containing this capability. The test exercises the same validator with
the preceding known-flag set; it is not an execution of an old binary. Earlier
private born2 prototypes without the capability cannot silently reseal a new
writer policy. Existing missing-catalogue prototypes are quarantined. Birth1
bytes and its missing-file contract remain unchanged.

| Cut | Supported result |
| --- | --- |
| Before catalogue creation or injected file-sync failure | No factory exposure, no native policy or writer hook. A partial/missing file never licenses reset; missing enforced opens repeatedly refuse without writing. |
| File and directory durable, before first policy | The exact empty existing catalogue is independently readable. Normal completed fresh factory/schema/genesis and fresh actual HostMQ are still required before any policy installation. An intentionally failed partial factory remains unexposed. |
| Native policy durable, before hook publication | Catalogue was already durable. The existing actual TLS injection still fences the old hook and Prepare/Strong. A fresh actual policy retry can proceed only with intact verified existing custody. |

Four new `native_catalogue_custody_` source controls cover completed fresh and
all-owner normal cold preservation; repeated missing/no-policy refusal and
no recreate; all three creation/sync cuts; actual stored reader-capability
bytes and portable-feature exclusion. The two old writer-refusal fixtures now
compare exact before/after existing empty catalogue bytes while retaining all
no-Prepare/no-rows/no-WAL/no-authorized-writer assertions. The actual TLS
policy-after-WAL failure control additionally preserves catalogue bytes.
Driver's separate acceptance successor retains the original ACK/Strong twin
and requires ONLY the dedicated missing-custody refusal across repeated cold
opens, with protected native-policy digests unchanged.

## Explicit limits and integration

This is a missing-custody correction, not valid-old-copy rollback protection.
Neither a catalogue UUID nor this provenance alone proves that a valid same-root
old catalogue contains every acknowledged decision. Protected monotone decision
anchors/full-body obligations and their recovery are a separate contract.
No reset, native R-to-B cast, missing-to-zero inference, new MQ authority or
conditional "no policy therefore fresh" exemption is introduced.

The full prototype's independent Abort-format-15 factory already owns initial
catalogue/StoreUUID registration. Composition MUST unify that factory using only
its actual private fresh authority and exact no-history bootstrap; it must not
accept an arbitrary existing file, overwrite CQ history or disable either
reader fence. This read-only branch deliberately does not implement or claim
that composition. No local Cargo runs are performed under current disk
constraints. Formatting, diff, source version checks and focused remote CI
remain distinct; the source controls have no executed passing claim yet.
