# Main508 handoff driver: protected metadata context adapters

This source successor starts at combined candidate
`d9327c386495389b95407fe88ed3c92f028f92e5` (1.0.510-beta). It adapts the
product `NativeJoinedDataHandoff` introduced by delivered main508
`f50acde6b0a2e923b92b31ab4b24d96f0dea5d6e` to the candidate's stricter
protected-native publication boundaries. The delivered main508 tree equals
`c2eeb335f40413bca85e766e7a91936f1e193f8f`; its original source and results
are distinct from this combined candidate.

Two actual callers needed changes:

1. The driver still called `publish_actual_member(Role, session)`. That
   role-only path leaves the actual MStore point absent, which the candidate's
   protected-init1 native sink refuses. Each existing bounded retry now keeps
   its genuine recipient Role and business ID, acquires HostOWN through the
   recipient's actual transport, and calls the existing
   `publish_actual_owned_member`. The request is a child of the original drive
   caller linked to the **original driver session** and capped by the **same
   original Role deadline**. Both actual cancellation ancestries and their
   earliest deadline remain effective. The session is lifetime custody, not a
   substitute for current authority.
2. Initial serving preparation used the observer that supplies no local
   metadata point. It now captures the actual attached current-MStore point
   with the same HostOWN/business/request, **before** taking the service lock,
   and passes that point to the existing committed-decision observer. Native
   publication still requires the actual protected open/Store identity and
   current version. No MStore acquisition is moved under an engine guard.

The retry count, timeout classification, request ceilings, native decision
body and retained publication round are unchanged. Errors still propagate;
unknown outcomes do not become success or allocate a replacement transaction.
The existing original Role, OWN, native WAL/catalogue and final publication
checks remain mandatory. Neither adapter serializes or manufactures authority.

The follow-up to `c388dd37f61096a99b026e0582af1b2830e20df2` restores explicit
session ancestry at this new OWN/publication boundary. `self.session` remains
stored on the driver, and the source capture retains its cancellation token
while that capture exists. That does not prove every later prepared quorum or
finalized image retains the same token: the finalized image is a physical fact,
not a session-authority token. The combined request therefore checks both real
parents directly rather than relying on indirect custody. This only tightens
cancellation; it adds no deadline, retry or authority renewal.

The removed manual fifth-member fixture at
`cea9269680f792c3060043878a32c547ff2e3ab8` was also inspected. Its initial
serving preparation used the ordinary re-enrolled writer and legacy
Image/Recovery lease. It had no genuine `Closed` writer-domain transition,
closed lease or current-point adaptation to copy. Its earlier Role/OWN fix now
belongs in this product driver's caller, because main508 delegates the fixture
to the driver. Historical d811 failures and the earlier fixture correction
remain at their original source pins.

The semantic port preserves the candidate's Registered FIRST transfer,
selected range, additional-range attempt map, strict no-reenrollment branch
and original-owner cleanup. The incoming `joined_data_handoff_parts` getter
returns actual runtime/transport identities only. This adapter changes none of
those constructors or grants, and does not turn a legacy Image/Recovery lease
into Registered/Joined remote A or into a fabricated CLOSED-domain proof.

Important limits remain. The product driver has no normal operator caller and
rejects a target already holding any range replica. Its dropped-transfer
restart has its own actual Recovery-session preflight and unique-owner rules;
it is not an unknown-result reset for the remote protocols. Main508 documents
same-process safe-but-stuck consuming windows. In particular, a failure during
serving preparation can still occur after the driver consumes its retained
decision into local leases; adding the mandatory metadata point does not
repair that custody window. Normal INITIAL process maintenance also refuses
wholesale policy replacement while retaining the original writer. This patch
does not bypass that refusal to attach the driver to production.

The [source index](native-main508-owned-context-source-index.json) records the
exact compared source and preserved guards. Direct Rust formatting, version
consistency and diff checks passed; no Cargo invocation, build, tests, CI job,
push or Azure experiment was run for this leaf. Existing required repository
CI must validate the coherent final head. These adapters alone establish no
resume guarantee, second populated range, automatic scale-out, throughput,
cold recovery or production qualification.
