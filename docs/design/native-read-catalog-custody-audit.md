# Native read candidate: missing range-catalog custody

Classification update: **confirmed unsafe Strong absence in the unmerged
candidate**. The original inspection was a source-backed hypothesis at
480db3b5. The [paired runtime reproduction and unchanged retained artifact](../evidence/native-read-catalogue-loss-2026-11-12/README.md)
at 3d0041e09 returned the acknowledged value from the intact copy and successful
absence after removing only its twin's n1 catalog. This is an open delivery gate,
not an established defect in a deployed single-host release or an explanation
of the historical Azure trial.

The inspected isolated candidate is
`480db3b52820919b0c18a48e15cccda38fe7c1c1`, based on public main486. The larger
native Abort/Joint prototype is a different source tree; its additional
catalog-identity protections cannot qualify this candidate.

## Source evidence

* `crates/bicdb-core/src/distribution_range_consensus.rs`, `RangeWriteStore::open`:
  missing catalog bytes initialize empty in-memory range history. Born-v2 writer
  enforcement permits this first-open case and assigns a new ephemeral open ID.
* `crates/bicdb-core/src/distribution_native_writer.rs`,
  `SealedHostWriterEnrollment::from_actual`: fresh metadata authority, actual
  native birth, schema and the current store open are checked. Those facts alone
  do not establish that an earlier durable range catalog survived.
* `crates/bicdb-core/src/db/range_image_native/writer_enrollment.rs`,
  `install_enrolled_native_coordinator`: a previous native writer policy is
  checked for the same physical birth before publishing a new store-open binding.
  Same birth does not imply preservation of a quorum-committed-but-unapplied
  command in a separate catalog.

The suspected unsafe cut is a replica with durable range commitment evidence
but no corresponding native application yet. Losing only its catalog could make
both exposed positions appear zero. A fresh quorum, unchanged birth and equal
zero positions would not by themselves prove that this is a clean replica.
Whether the complete read/recovery path actually admits it remains to be tested.

## Smallest deterministic reproduction

1. Use three genuine born-v2 stores, actual metadata/data authority and an
   external expected value. Obtain ACK1 through voters 0 and 1, retaining the
   actual command/quorum evidence on 1 while withholding its native Apply.
2. Stop every owner of replica 1; copy its stopped files. Remove only its actual
   range-write catalog from the copy. Preserve native bytes, physical birth,
   schema, namespace identities and metadata history. Do not fabricate positions,
   roles, votes, terms, acknowledgments or receipts.
3. Keep the old acknowledged leader unavailable. Strictly reopen the copied
   replica and attempt fresh enrollment and genuine new-leader recovery using
   survivors 1 and 2.
4. Exercise the actual typed gateway Strong read. Required result: acknowledged
   value after legitimate recovery, or explicit bounded refusal/unavailable.
   Successful absence or an invented clean history is a failure. An earlier
   strict-open refusal is a valid safety result and must be recorded as such.

Preserve the baseline execution before implementing a repair. Record whether
failure/refusal occurs at open, enrollment, recovery, readiness or read. A green
test that merely deletes a file and expects an unrelated error is insufficient.

## Repair constraints under review

Any repair must distinguish genuine first bootstrap from loss after prior
enrollment. A previous actual native writer policy may provide part of that
distinction, but a newly synthesized empty file must not allow the next restart
to bypass refusal. Initial catalog persistence and native policy publication need
an explicit crash-safe order. Repeated interrupted bootstrap, directory-sync
failure, cold restart and same-birth reopen require positive and negative tests.
Persistent catalog identity and reader-compatibility implications remain under
review; no repair is claimed here.

The passing focused read cohort and any passing broad run at the inspected
revision do not close a scenario they did not execute. Production admission,
complete relocation and physical qualification remain unchanged.
