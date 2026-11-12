# Native unassigned learner admission

Source review at integration commit
`9f7f0024eddc74694ba4bea066d30cacb1f64ca7`, based on main
**1.0.481-beta**, `3cce710c689ba8ec7a820538113e389481e04669`.
This is unmerged implementation and a review of remaining wiring.
The initial source review preceded compilation; the separately pinned CI
receipt below records later results. This is not an executed join, a release
or production qualification.

`distribution_native_host/learner_factory.rs` adds an actual fresh-store
factory for a registered but unassigned metadata learner. It requires a live
authenticated host-metadata-quorum observation for that requester, its current
incarnation and pinned mutual-TLS identity. The learner must have no assigned
data replicas. Existing active metadata voters must have enforced native births.
The factory creates protected storage with an empty initial range set; it does
not invent an applied range prefix, readiness, a metadata vote or writer authority.

`db/range_image_native/learner_schema.rs` installs only the portable record and
index schema into that actual fresh, unwritten factory. It verifies the source
fingerprint, preserves logical schema identity, creates fresh local namespace
identities and strictly reopens the store before use. SQL structural records
are explicitly unsupported. Generic native schema import remains refused.
Interruption or mismatch leaves the factory quarantined rather than resetting
an existing root. The durable manifest records identity and configuration;
fresh quorum evidence is still required after reopen.

The accompanying genuine-store learner test source exercises learner vote
refusal, refusal by a warm generic metadata store after protected enrollment,
and actual-owned-store catch-up before promotion. At `25aebf446105`, that lower
control passed in CI; the outer factory APIs were not called by that control.
Promotion requires committed native enrollment and actual recorded metadata
replication progress; a client-supplied installed-position declaration is not
substituted for that progress.

## Remaining operator sequence

1. Register the new identity as an unbound metadata learner with no data replicas.
2. Obtain actual authenticated schema and current host-quorum evidence; create
   the protected fresh factory without ordinary read/write access.
3. Observe that actual factory over a bounded, birth-bound authenticated RPC
   and commit its binding through the existing metadata consensus group.
4. Stop and drain the provisional listener while retaining the same actual
   service and database open; initialize or strictly reopen its owned metadata
   store, then catch up. Never reset an existing protected store to term zero.
5. Record actual birth-bound acknowledgments and commit learner promotion
   before accepting its votes or making it eligible for data placement.
6. Automatically place data through the separately protected joint handoff,
   restore and validate the required committed history, then expose readiness.
7. Recover interrupted work outside database guards with persisted progress,
   bounded retries and explicit refusal diagnostics; finish cleanup only after
   the new domain is durably authorized and all required ownership drains.

The new factory implements part of step 2 and a configuration-only proposal.
It does not provide the enrollment RPC/controller, automatic election and
heartbeat scheduling, the ordinary data-service transition, first user ACK,
automatic relocation or a command-line four-to-five-node workflow. Those are
open integration gates. Metadata quorum does not stand in for data quorum.
Record-schema support also does not establish transparent cross-range SQL or
atomic multi-range business transactions.

The original Azure qualification remains failed. A subsequent qualification
must retain a complete external attempt/ACK/uncertainty ledger, verify strong
read correctness and complete bounded convergence before measuring stable
five-node throughput. No physical-capacity or production-admission claim
follows from this source.

## Validation receipt and remaining cancellation test

At `25aebf446105da552b9ba65205811271e00898b7`,
[all-target compilation passed](https://github.com/nikoma/bicdb/actions/runs/43161126843).
The genuine lower schema/owned-store learner control passed inside a failed
Core run (827 passed, four failed, eight ignored). This does not establish the
outer factory's create/reopen/proposal behavior.

Source `1865e6b98f8bda489ec5390d339dc7c398fcc451` adds an actual four-member
metadata TLS setup and a fifth registered requester exercising those APIs,
fresh metadata after strict reopen, stable protected birth/local namespace,
wrong-certificate/business refusal and independently canceled callers. The
test is not compiled or executed yet. Its schema bundle comes from an actual
source database in the same process; it does not test a schema-export RPC.
No data is assigned or relocated and no user commit is acknowledged.

The same source checks caller cancellation before opening a saved root and
rechecks the original quorum observation after manifest fsync. The new
controls cover canceled entry and canceled observations before proposals.
Deterministic cancellation specifically during manifest persistence remains
an additional unexecuted cut to cover; a generic caller token alone must not
renew the original observation after that write.
