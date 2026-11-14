# Registered empty-slot continuity

Source baseline: `1742776ab02957a053aded2ad4f665500d9325bf`
(1.0.512-beta). This is a source correction awaiting compilation and runtime
validation, not a qualification result.

The original required native suite at checkout
`4700100a7741ffe402e0c0281201e4f1a6d0f0a6`, run `43581326157`, job
`132406489716`, finished with 280 passed, 43 failed and one ignored. Thirteen
remote NEW/DECISION cases failed at `remote_new_tests.rs:217`, while issuing
the actual target participant's Materialize request. The response was:

```
cluster node n4 rejected RPC cluster_data_operation_failed: cluster error: native range image: remote NEW original Registered hook/open changed
```

The complete original native log remains unchanged; its decompressed SHA-256 is
`54510f8f08528b3f411114a1fd5ae735a12f39bb74be7e0c23e02f3bb5ef3ec8`.
The intended later NEW/DECISION cuts were not reached in these cases. The log
does not separately record the active bit and database-open Weak state.

The source establishes a contradiction independently of that missing diagnostic:
the only genuine receiver-hook capture validates the actual current empty slot,
whose registration is inactive. Its full borrowed validator rejects an active
registration. The remote NEW memory validator instead required that same
registration to be active. An empty registration cannot satisfy both checks.

The correction keeps write admission and original-slot continuity separate:

- `active` retains its meaning: false for an empty slot, true for an installed
  authority, and false after in-place write fencing.
- A private monotonic `revoked` marker starts false at registration construction
  and becomes true on actual slot replacement or slot destruction. None-to-None
  replacement retires the old registration even if another owner retains its Arc.
- The Registered memory check requires an unrevoked, inactive original
  registration and the live original database-open lifetime. The full check also
  retains actual database-open identity, actual slot Arc identity, empty hook and
  absent native writer checks. Capture still requires actual never-installed
  native writer records. The marker itself grants no admission or image writes.

All construction paths were inspected: the initial database empty slot, public
trusted-hook install/clear, native enrolled install, and assigned enrolled
replacement retaining the old slot. The two native enrollment callers create
new registrations; assigned retries retain the original unpublished registration
and an already Installed attempt does not reinstall it. No current sealed caller
passes a current or retired registration back into installation. This remains a
private producer invariant: these setters do not gain a new admission API or an
idempotent reinstall path in this correction. A future caller must not rebind a
retired registration; its revocation can never be reset. The retaining replacement
returns the old whole slot and
drops it after its publication guard; its destruction cannot revive an old
registration. Slot moves preserve the marker; there is no Slot Clone or restore
producer. No caller can reset the marker.

`deactivate_native_writer` remains an in-place admission fence. The real native
enrollment path calls it before fallible native policy publication, including
when the actual slot is empty. It does not replace or retire that slot. A later
successful installation performs actual replacement and revokes the original;
native-record checks continue to refuse an uncertain or ever-installed receiver.

Four new real born-database lifecycle component tests cover the valid inactive
empty slot (including the in-place empty fence), retained None-to-None/ABA
replacement, actual public install/clear, and actual database destruction while
the original registration remains strongly retained. The thirteen existing
pipeline test bodies and all original phase, authority and timeout checks remain
unchanged. These new component tests have not been compiled or executed in this
batch. They do not establish successful relocation, Serving, cold recovery or
physical scale-out.
