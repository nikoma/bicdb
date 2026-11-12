# Learner metadata: all-owner cold recovery design

Design only; no runtime changes or Cargo. The prior same-service transition's
three new controls actually passed in CI `43176666917`; the whole native_ cohort
still failed (237 passes / 2 failures / 2 ignored). Same-DB backend reopen in that
cohort did not close every target DB owner. It is not the cold result proposed here.

## Minimum persisted fact and ordering

Add a bounded original-configuration sidecar in the owned metadata directory,
written atomically with file AND directory fsync BEFORE protected Prepare. Store
the full original C, format/version and integrity checksum; any context/term/index
labels are audit only. Hash C with the EXACT existing bounded algorithm used by
`BicDb::prepare_native_metadata_store` (`SHA256(bounded_json(C,4MiB))`). Once the
actual native record exists, its immutable initial-C SHA—not the sidecar's own
checksum—authenticates that original configuration FACT. Derive StoreUUID, actual
birth/incarnation/bootstrap/restore from the native record/current DB, not labels
in the sidecar. A decoded sidecar never becomes HostMQ or metadata authority.

Acquire the real DB-root and metadata-directory/owner claims before inspecting or
writing these files. Keep them through sidecar fsync, StoreUUID allocation,
protected Prepare, exact state publication and Registered readback. The new
owner slot is retained across errors as in the current transition. Writing the
sidecar before UUID allocation makes a sidecar-only cut unambiguously not a
protected metadata identity. No protected record/state may be overwritten by
an orphan-sidecar retry. Reader fences and actual from-birth capability remain
mandatory; old private roots without a matching sidecar cannot gain recovery by
inventing historical C from fresh metadata.

## Cold producer and branches

A new private `NativeEnrolledLearnerRoot::reopen` (proposed API) must replace use
of `reopen_uninitialized` for this purpose. It takes NEW actual OWN learner MQ,
loads the bounded existing learner manifest, strictly opens the SAME actual DB
birth, verifies portable schema and local namespace identities, and requires the
current bound Active Learner/no replicas/incarnation/pin/schema/restore. It never
creates a DB, overwrites schema, issues genesis or blesses a birth declaration.
The cached C used to locate the metadata leader is only a connection hint;
current authorization comes from the sealed fresh MQ plus actual local admission.

Under the owner claims, select exclusively from ACTUAL protected state:

| Native record / files | Permitted continuation |
| --- | --- |
| No record, no state, no sidecar | Normal fresh learner initialization, only with fresh own MQ |
| No record, no state, valid sidecar only | No metadata identity exists. Revalidate fresh own MQ, re-fsync matching C (or replace an unprotected orphan with freshly authorized current C) BEFORE allocating the first protected UUID. This is not recovery of historical authority |
| No record but ANY metadata state, or malformed/unknown protected state | Refuse; never infer uninitialized or reset from missing/ambiguous evidence |
| Prepared, state absent, matching original full-C sidecar | Construct exact original never-exposed learner state with the ACTUAL protected StoreUUID; fsync/read back, then finish the SAME registration |
| Prepared, state present | Decode/checksum/validate; require SAME protected UUID and EXACT original never-voted state below. Finish registration without replacing/regenerating identity |
| Prepared, sidecar missing/wrong hash/birth or original C unavailable | Quarantine; fresh current C is not a substitute for original C |
| Registered (may have been exposed) | Existing strict `reopen_native_owned` ONLY. Preserve UUID/term/vote/log/commit and actual birth; sidecar can NEVER justify term-zero fallback |
| Registered with state absent/wrong UUID/damaged checksum | Refuse, even if the original C sidecar is intact |

For Prepared, validate exact state: format/native UUID/cluster/node/original C;
term 0, no vote, Follower/no leader, base/commit/log 0/empty and the exact original
restore field. No nonzero term/log may be reset because the record still says
Prepared. Registered requires its ordinary historical-state checks instead;
being currently a Learner does not permit discarding learned terms or logs.

## Historical C versus fresh current C

Fresh MQ independently validates the current local bound birth and unassigned
learner role. Original C is only the protected initialization fact. Current C
may advance while local identity/schema/restore remain stable: resume EXACT
original C, then learn current C through real authenticated Raft catch-up.
Never write current C into an existing original-C state file to make it look
like the original prepare, and never refresh old audit context into authority.

There is an additional existing liveness boundary: `handle_append_request`
checks its leader is an original COMMITTED voter BEFORE it installs the incoming
snapshot. Therefore the smallest supported changed-C case must retain a current
leader that is a voter in original C. If a newly elected leader is absent/nonvoter
there, existing catch-up cannot complete; explicitly refuse or add a DISTINCT
fresh-MQ-bound catch-up admission later. Do not remove the committed-voter check
or treat a sidecar/new leader ID as that proof. Changed local birth/incarnation,
pins, schema, restore or data assignment are unsupported transitions, not quiet
rebase cases. Missing original C under Prepared always refuses.

## New cold host; no fabricated drain

Current `NativeUnassignedLearnerHost::start` requires the pre-init birth surface;
current hot transition requires a real provisional handle/drain. A cold metadata
constructor must take the strictly reopened root, create ONE new actual service/
RangeWriteStore and owned MStore, and set a permanently metadata-only attachment
BEFORE its FIRST listener. It must not fabricate `DrainedClusterDataListener` for
a dead process or open another service alongside a live owner. Actual new-open
root ownership is the cold exclusivity proof; same-open owner attachment and
bound native ACK checks remain mandatory. Retain every object/new stopping
listener on errors; original cancellation/deadline applies through post-bind.

As a necessary refusal seam, native `require_listener_database` should require
actual Registered runtime identity as well as owner/open binding. It currently
checks the latter; a retained Prepared object must never be attachable to a
countable endpoint through a lower generic constructor. Keep restrictive-state
persistence possible, but do not grant Append ACK/vote/read authority from
Prepared presence. Existing initial-all-C factories attach their metadata only
after actual registration and must retain that separate purpose.

## Smallest actual tests and open scope

For each cut, close/drop ALL target listener/service/provider/MStore/factory/DB
owners and guards, preserve hashes/UUID/namespace evidence, copy and fsync the
root, and strictly reopen the COPY. The four genuine metadata voters remain the
source of NEW own learner MQ; this is target-all-owner restart, not yet a whole-
five-process outage qualification. Test sidecar-only, Prepared-before-state,
Prepared-after-state, Registered-before-bind, and Registered-after real Append
with a nonzero term/commit. Require SAME protected UUID for every record-bearing
case, actual bound TLS ACK/catch-up, unchanged rows/UID/no data floor/writer and
no false vote. Corrupt/delete the state/sidecar and change current identity only
via genuine contexts; assert the specified unchanged refusal. Include current-C
advance with an old-voter leader, plus explicit refusal for a new leader absent
from original C. Preserve old failed/quarantined source cohorts separately.

Prepared older roots lacking full C, arbitrary membership/source transitions,
post-promotion cold-host policy and same-UUID old term/vote-envelope rollback
remain explicit gates. Neither a sidecar nor initialization identity proves
those, and no HA/automatic-join/user-ACK claim follows from this design.
