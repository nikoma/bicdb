# Bounded owned-learner status snapshot

This is an unversioned source correction based on `62313619ddd7fa2625067e169c95218e65cd5e06`. It has not been compiled or executed. It does not establish a passing CLI, native handoff, physical deployment or recovery qualification.

The failed candidate `326c363477477950f76bdb51fe6425cab85dc362` actually executed `native_metadata_operator_actual_cli_single_join_fresh_becomes_local_metadata_voter`. Its `elect` subprocess failed at `crates/bicdb-cli/tests/native_metadata_operator_cli.rs:76` with `owned learner metadata status busy` (REST raw lines 695622–695623). The complete original run and failure remain preserved. This is separate from main519's `replicated_state.rs:313` address-in-use failure, whose test was not reached in candidate326.

`NativeMetadataLearnerHost::elect_metadata` first invokes the retained host's actual election action and then calls `refresh` under the same original five-second child token. The reported status error occurs in that refresh, after the election action returned. The CLI error therefore does not prove absence or rollback of election effects. This correction never repeats an election.

`refresh` already acquires a genuine Host metadata observation, validates the actual factory and owned membership, and releases its schema/namespace DB read guard before obtaining the owned metadata snapshot. Its only two snapshot callers select the original Fresh or Cold owner. Previously both snapshots used an instantaneous metadata mutex acquisition.

The private status helper now receives the **same** observation, business ID and caller token. Each acquisition uses at most five milliseconds and the smaller remaining original caller/observation window, with both liveness checks before and after acquisition. It creates no token, observation, Role, metadata write or membership grant. The Fresh owner's saved-open/listener check and both owners' actual store UUID, status, listener ownership and stopping checks remain. Final observation/caller checks precede returning the snapshot. No service, DB, journal or publication guard is retained while waiting; no network operation runs under the metadata guard. The pre-existing full owned-member/catch-up validation helper is unchanged.

Three new source test definitions use the existing genuine fifth-factory enrollment and promotion fixture, real TLS Host metadata observations and the actual owned metadata mutex:

- A selected-thread test rendezvous pauses at the production snapshot helper. A peer thread holds the real mutex until a real failed acquisition is observed, then releases it. The original read must return the same store identity and status.
- The same real contention cancels only the original status caller while its independently acquired genuine observation remains live. The helper must return `QueryCanceled` while the mutex remains held.
- The mutex remains held through the original five-second earliest window. The helper must return `QueryTimedOut` while still held. This is original-scope/coupled expiry, not independent OWN-only expiry.

The controls compare actual metadata status/topology/store UUID and native incarnation/commit sequence/namespace IDs/rows before and after, then use a separately fresh real observation to read the same owner. The test rendezvous retains only atomic test state; it grants no ownership and changes no metadata. Cleanup watchdogs do not change production budgets. These are component cuts at the actual CLI refresh sink. The original public CLI test and all existing assertions remain unchanged; the Cold branch shares the production helper but has no new separate cold contention control in this slice.

Only standalone formatting, source-preservation and diff checks are permitted for this leaf. Rust compilation, all three new controls, and the original CLI end-to-end acceptance are **not executed** here. Actual broad CI must validate the coherent successor. Other FIRST setup failures, large-catalogue DATA-policy timeout, legacy port allocation and physical/operator/cold recovery limits remain open.
