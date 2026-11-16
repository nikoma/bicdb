# Cell rekey: closed-root physical gate lifecycle

Preserved source `2ce4c48202d4d1bb1ef759b08a41dc36f431af6d` failed
remaining-workspace job132756718432 in Tests43696050976. The original
`signed_cell_key_rotation_uses_two_attested_leases_and_activates_next_manifest`
returned `Database(Backup("enrolled physical backup gate is missing"))` at
`bicdb-cell/src/lib.rs:7589`. Full raw39,194,032 bytes has SHA256
`2c5178564320d017b562fb54dad65516f9c7b20edc31214c45577d470dccb535`.
The original source canary was written; activation-report, next-manifest and
reopened-canary assertions were not reached by that invocation.

Source inspection identifies the prerequisite gap: rotation deliberately omits
source `.backup-write.lock` while copying enrolled `format.json`; it had neither
claimed nor initialized the fresh target gate. A guarded mesh-secret write or
`persist_rotation_metadata` can reach storage's exact missing-enrolled-gate
refusal. The raw outer error does not identify which internal write ran first.
Staged files may already exist: this was not an effect-free failure.

The isolated correction starts at root source
`6adcbc429e47d34bcdb9afe34395f93937c32e74`. Package versions, workflows,
crypto bindings, journal format, reader capabilities and deadlines are unchanged.

* Fresh staging keeps exclusive directory creation, then retains a real
  directory handle and acquires the actual DirectoryLock. Before any gate or
  payload write, the claim must match that handle and the stage must be empty
  except its own lock. A competing normal Core owner, even after closing, is
  refused without cleaning its populated tree.
* The additive Page observation holds an actual read-only directory handle.
  It cannot serialize, mint, transfer or consume a writer claim. Unix opens
  directory descriptors, never another `store.lock` descriptor. Windows uses
  metadata-only access, full sharing and backup/reparse flags with actual file
  identity. There is no `None == None` fallback. Existing strict claim validation
  is unchanged and repeated around the observation; only the existing linear
  API can transfer an actual claim into an engine writer.
* Closed source/stage claims retain actual physical EX. The fresh stage creates
  its OWN gate before FORMAT copying; the source inode is never copied and its
  reader flag is never cleared. Nested guarded IO borrows the SAME EX. Original
  authenticated-copy, tamper and plaintext checks remain.
* Prepared and SourceRetired retries claim staging, require an existing enrolled
  gate and check the staged digest before publication. Missing gates are refused
  by the unchanged generic initializer. The already-new-key fast path also needs
  closed-root/gate custody before its existing status return.
* Lexical EX borrows end before moves. Actual EX and directory custody protect
  the namespace effect. Old physical scopes/bindings are dropped before
  installed-name acquisition, with directory claims retained for final readback.
  Retained directory observation grants no generic engine writer transfer.
  Fresh-copy cleanup requires the SAME successfully authorized stage owner.

Digest exclusions remain exact: root `store.lock` and root physical gate only.
No Page-child claims are added: current digest code traverses nested lock files,
so extending that coverage would require a separate audit.

New source controls use real Core/Page objects in one process:

1. Normal Core inserts/reads a complete record and closes between stage mkdir
   and first claim. Rotation must refuse and preserve both databases.
2. Actual Prepared staging has a distinct local gate. A real held stage claim
   refuses; deliberate gate deletion under its real closed/EX owner then refuses
   without inode recreation or source retirement.
3. Completed rotation/full new-key read precedes deliberate active-gate removal;
   the already-new-key status path must refuse rather than report Activated.
4. Actual SourceRetired checkpoint plus owned stage tampering refuses before
   candidate publication and preserves the original retired full record.
5. Page handle/claim comparisons reject wrong, moved and replaced names without
   releasing/spending claims. Only the existing linear API then creates a writer
   and commits the full replacement payload.

Original signed-two-lease/manifest/canary/reopen Cell assertions and Core
checkpoint/attachment/index/old-key assertions are preserved byte-for-byte.

| Evidence scope | State |
| --- | --- |
| Original2ce Cell invocation | Failed; exact outer error retained |
| Gate prerequisite and original checks | Passed source inspection only |
| Corrected compilation and old/new runtime controls | Not tested |
| Windows/macOS runtime behavior | Not tested; no platform qualification |
| Cross-call source-head inclusion, namespace/cold association and HA zero-ACK loss | Inconclusive/open; not established here |

The rotation journal binds keys and staging digest, not a later source head
after `stop_after: Prepared`. A source reopen/new ACK/close before resume is a
separate unexecuted active-view rollback concern; retired ciphertext may still
retain the record. Per-call EX does not prove cross-call inclusion. This fix
does not close general cold association, automatic recovery, predecessor
retirement, native Serving or physical qualification.
