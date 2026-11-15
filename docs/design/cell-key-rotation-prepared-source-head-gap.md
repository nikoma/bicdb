# Prepared key rotation does not enforce a frozen source across calls

Classification: **failed source safety check for automatic cold resume;
runtime reproduction not tested**. This is separate from the executed missing
physical-gate failure in candidate2ce. That run never reached this scenario.
No durable erasure or physical qualification is established.

The rotation implementation and its original integration fixture are byte
identical in main **1.0.519-beta / `587dd6e4775e454cc9e8224d5fed76e8795c6ff6`**
and candidate **`2ce4c48202d4d1bb1ef759b08a41dc36f431af6d`**:

| Path | SHA-256 |
| --- | --- |
| `crates/bicdb-core/src/encryption_rotation.rs` | `2e21c17593edb37fd1f04766399c467e9b5304173b267b05b2afd30771ec4597` |
| `crates/bicdb-core/tests/encryption.rs` | `58bc9052e6bb9b8633c31c9f36aa629ab5b5092d0c0b5ae628c92b422aa225ab` |

`RotationJournal` retains bindings, counters and the staged-tree digest, but no
original source-head association (`encryption_rotation.rs:63–75,207–219`). A real
`stop_after: Prepared` returns before renames and releases that call's owners
(224–228). Resume claims the current source and checks the stage digest
(229–244), then retires the source and publishes the stage (250–266). The final
active-tree digest checks the original staged bytes, so it cannot establish
inclusion of source writes accepted after preparation. Normal Core open does
not consume the parent key-rotation journal to fence such writes.

The offline operation requires the source to remain closed. Holding its kernel
claim during each call does not enforce that condition between calls or after
a process crash. A gate-lifecycle repair must not be advertised as closing this
separate boundary or qualifying automatic zero-loss resume.

## Smallest deterministic regression

Extend the genuine public-API fixture
`bound_key_rotation_is_complete_crash_resumable_and_never_mixes_epochs` in
`crates/bicdb-core/tests/encryption.rs:1015`, preserving its existing assertions.
Implement the additional case after the fresh-stage gate prerequisite works:

1. Create the real fsynced, bound encrypted database with an original full
   record. Close it; invoke the public rotator with `stop_after: Prepared`.
   Require the actual Prepared report, `activated=false`, original source
   present and retired absent. Preserve the report and source/stage hashes.
2. Open that same source using the old key and ordinary Core API. Insert a
   distinct full record, retain its successful return, read it back, close and
   reopen to verify it. Keep the complete record, payload hash and ciphertext
   hashes. Do not synthesize an ACK, edit the journal or replay the insert.
3. Resume the same transition using default options. Preserve its Result and
   actual source/stage/retired layout before assertions. Successful activation
   must expose both records under the new key. Otherwise it must refuse before
   stale-stage publication and preserve the acknowledged source. An error after
   stale publication does not meet the refusal requirement.
4. If an old-key retired tree exists, independently inspect it and retain the
   full acknowledged record there. This distinguishes an active-view rollback
   from erasure. A later repair does not change the original operation's result.

A missing Prepared prerequisite is an unreached/failing cut. Do not ignore the
case, accept expected loss, change deadlines or manufacture a checkpoint.
Repeat the real Prepared/crash/reopen sequence in a separate process before
claiming cold recovery. The current positive fixture does not mutate the source
between checkpoints and therefore does not establish this guarantee.

## Open qualification boundary

The newly active view could omit an acknowledged source record while the
retained old ciphertext still contains it. This is an **active-view rollback
risk, not observed durable erasure**. A reviewed persistent admission fence or
actual source-head/intent validation is still required for automatic safe resume.
The signed two-lease Cell wrapper, manifest activation and old-key retirement
need their own integration evidence. The proposed regression is unimplemented
and unexecuted; no production or physical gate is cleared.
