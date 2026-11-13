# Actual Native factory roots and allocation origin scope

Exact private source `1f14ae1d7ed9ebdbde8a9cd2ae4af9f4bedb7c1f`, inherited
1.0.490-beta. This is unmerged prerequisite work, not cross-node qualification.

A Native root can be activated from the actual atomic directory creation fact
held by the creating open, or from its durable private prepared factory record.
An existing empty directory is insufficient. The protected Native range factory
carries its own actual atomic create into this path before creating backings.
The private root record is fsynced before its sticky local format fence; new
containing directory entries are synced too. Its checksum supplies integrity,
not peer or import authority. No public constructor accepts a root UUID or a
caller-supplied root capsule as authority.

On the qualified Unix path, the root binds the claimed directory's OS device,
inode and creation time. Refusal preflight is read-only, followed by a check
under the actual directory lock. An ordinary copied store is refused without
changing its bytes. Moving the same physical directory preserves identity.
Live replacement is refused by source observations, mutation admission, Native
WAL capture and protected image bindings. Physical custody on other platforms
or filesystems without the required OS identity remains unqualified; this is
not a claim of general portable-directory identity.

A backing records the active Native root only at actual allocation. Earlier
born or legacy collections remain unscoped. Portable contracts strip the root,
birth and local namespace; snapshot import, schema refresh and contract restore
preserve an existing target's local state or allocate a genuinely new target
identity. They cannot adopt or retrofit a donor root. Retained WAL keeps the
actual root scope with its write-time backing contract and requires that root
even after every scoped collection has been dropped. The opaque source-root
observation in actual batch/live capture is discarded by JSON serialization.
Protected image bindings also pin this observation and every backing's actual
origin scope; a new observation does not retroactively scope old backings.

The final source passed 51 focused Core tests across seven parent harnesses:
eight origin controls, fourteen existing birth/WAL controls, six image-binding
controls, ten actual protected Native factory controls, eight namespace controls,
two directory-lock controls and three snapshot/schema reader controls. Zero
failed; one pre-existing frozen-reader test remains ignored. Page/Core/SQL/wire
all-target compilation and twenty-nine inherited source versions passed.

Two actual root-publication directory-sync EIO cases cover both storage modes.
The handle fences; reopen and retry finish the same actual prepared root, keep
old backings unscoped, preserve exact independent Native ACK hashes and survive
two further opens. Four prior actual WAL write/sync EIO cases are included in
the birth/WAL controls. These are in-process tests, not independent crash or KVM
qualification. Source root tests exercise both Embedded and Paged storage;
protected image tests remain Paged only.

All three runs and their raw bytes are retained. V1 passed 48 checks. V2's
WAL-only fixture correctly hit an earlier catalog fence because its internal
catalog log was still scoped. V3 allocates the independent ACK/catalog log
before activation, asserts all remaining collections are unscoped, and preserves
the exact WAL-history refusal assertion. Production code is unchanged from V2.
The final count is 51; parent-harness accounting excludes invented subcase totals.

Still required: authenticated source/peer acceptance, actual producers at BOTH
protected Native image and legacy logical-import boundaries, source allocation
and publisher facts, durable target/root/generation mappings and record lineage,
revocation/cursor checks, the read-only SQL resolver and the unchanged original
promoted undecided rename positive. Raw copied metadata, local root observations
and checksums alone grant none of that authority. Existing unowned journals or
targets must not be retroactively authenticated. Full combined regression,
independent process/EIO/KVM, application/UI/restore, physical capacity and approved
performance gates remain open. No main delivery, production binary, release,
deployment, experimental production promotion or new cloud spend is implied.
