# Enrolled learner: retained same-service metadata start

This source implements the next finite component after committed learner birth
enrollment. It is not an automatic join loop, full crash recovery, promotion,
data readiness or HA qualification. New controls are source-only until run.
It uses existing protocol 27 and changes no persisted metadata record shape.

`NativeUnassignedLearnerHost::start_owned_metadata` accepts a NEW actual learner's
OWN HostMQ, not the enrollment audit result. It checks current Active Learner/no
replicas, committed exact actual birth/incarnation/pin/schema/bootstrap/restore,
and enrolled old native voters. It revalidates actual DB-open and local namespace
identities. The original MQ and operation cancellation/deadline remain binding.

The actual provisional listener must stop and drain. Its opaque proof binds the
SAME service Arc and actual RangeWriteStore-open UUID. A private retained phase
owns that proof, any actual MStore produced, the client/provider and new listener.
Only the new listener captures a metadata-only service mode. The old listener's
captured provisional Some is never changed or broadened by Weak expiry. No second
data service or range store is opened on the DB.

A narrow learner-host constructor fills an actual MStore owner slot as soon as
its UUID exists, BEFORE protected Prepare and first state persistence. The former initializer delegates to the same private
body, retaining its previous Result<Self>/drop-on-error behavior for old callers.
The host's slot remains on cancellation/persist/registration failure. No retained
in-memory state can stand in for actual protected Registered initialization.
Protected Prepared records are explicit quarantine, including a state-file cut;
no new StoreUUID or term-zero reconstruction occurs. The initial-all-C pre-vote
permit is never used for this existing-cluster learner.

Constructor/reopen work runs outside service/metadata mutexes. A verified actual
DB read spans construction so replacing the inner DB cannot initialize a backend
for a stale service. Nested reads are try-only and may refuse busy. No metadata
mutex is held when reading actual protected records. The brief same-service mode
transition uses nonwaiting metadata/service guards and acquires no DB latch.

Only after checking the actual Registered native receipt and SAME StoreUUID may
a NEW metadata-only listener bind. It has the same actual owned MStore/provider;
ordinary data/control stores and range recovery runtime are unwired. A post-bind
original-scope failure requests shutdown and RETAINS the stopping handle and
counter. A fresh retry first drains it, then reuses the same actual owner.
Actual bind errors retain the backend without publishing a listener.

`stop_owned_metadata` genuinely drains; `close_stopped_owned_metadata` refuses
outstanding owner references. A later start strictly reopens the existing actual
Registered UUID/state on the SAME owned DB/service, preserving term/vote/log. It
does not initialize another term-zero store. This is a live same-DB backend restart
control, not a strict all-owner cold-process restart claim.

The new actual TLS control sources cover:

- Old provisional Append refusal, real drain and unchanged service/range-store
  identity; protected metadata initialization; same-identity start retry; premature
  promotion/election refusal; real leader snapshot/Append and current-birth bound
  ACK; same-DB strict backend reopen preserving term/commit/UUID; unchanged local
  namespace, empty rows/no native floor/no writer; held-latch ordinary-data refusal
  and a genuine TLS candidate's vote refusal while the target is still Learner.
- Error cuts AFTER actual protected Prepare and AFTER actual durable state-file
  publication, before registration: actual retained owner and matching native
  StoreUUID, no metadata listener, unchanged file/absence and Prepared quarantine
  on independently fresh retry. This is not a physical power-loss or fsync-error
  qualification.
- Cancellation after actual Registered ownership, real occupied-port bind failure,
  cancellation after actual listener bind, and independently fresh retries that
  retain the SAME MStore Arc/StoreUUID and genuinely drain the stopping listener.

Open failure gates remain explicit. Prepared-before-state lacks an existing
learner-purpose exact-C continuation; the protected record stores the initial C
hash, not a full recoverable configuration. This batch quarantines all such cuts
rather than relabeling initial-voter preparation or recreating state. Strict
all-owner cold learner-host loading after metadata initialization is also separate
from the existing pre-init factory loader, which correctly refuses protected
metadata records. Those gates need a bounded supported recovery path before the
final fault-safe milestone. Same-UUID old term/vote-envelope rollback is not solved
by initialization identity alone.

Registration orchestration, real promotion through metadata membership, ordinary
listener release, new-node data/joint handoff and first user ACK remain separate.
