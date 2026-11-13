# Full rollback index retry

Private source `731eae2bad65514d2fa59202d87fba6814ebce63` passed nine focused tests. The new regression first ends the real SQL transaction, then observes a full index rebuild refusing a live unrelated Native writer. After that writer commits its independent acknowledgement, the same full ROLLBACK must restore the exact Native index definitions, SQL descriptor, acknowledged data and independent write through two clean reopens. Embedded-memory behavior is checked separately in the same regression.

The unchanged production code at `fbe13644ef3210f140cb4badd36c90c44823a056` failed the Native-index assertion: the second ROLLBACK returned success but `source_payload_idx` was absent. The fix retains a RestoreIndex undo entry until the actual Native create_index succeeds. Native full-build admission is unchanged.

The remaining eight selected regressions cover the six index dependency/identity cases, unrelated Native writes during rename, and the existing rename journal retry contract. Raw negative output remains preserved. These are focused checks on the stated private source, with original application assertions unchanged; they do not certify a public merge, full application acceptance, crash recovery or HA.

The earlier proposed savepoint fixture at `be2f975a` was reverted before execution: even after a foreign writer finishes, that savepoint still retains its own pending Native transaction. This full rollback fixture does not rely on that invalid assumption. Initial rebuild refusal while foreign owners remain is still a limitation, and the process-local retained undo does not prove durable recovery after process death.
