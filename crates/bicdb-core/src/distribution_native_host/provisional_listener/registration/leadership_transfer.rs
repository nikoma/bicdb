//! Operator-requested transfer of metadata leadership from THIS leader to one
//! named voter, so that the metadata leader and a range's leader coincide.
//!
//! Protocol, all of it bounded:
//! 1. This node's own store opens a transfer window: it must be the anchored
//!    leader and the target another voter of the committed and of the pending
//!    configuration. While the window is open the store refuses every new
//!    metadata proposal.
//! 2. Catch-up: this node replicates until nothing is uncommitted and the
//!    target acknowledged its whole log (accepted replies only).
//! 3. One request asks the target to campaign now. The target checks, against
//!    its own state, that the authenticated caller is the leader it follows
//!    in its current term and that its own log ends where the leader's does.
//! 4. The target's own supervisor runs an ordinary election. This node reads
//!    the outcome from its own store: it follows the target in a higher term
//!    only after the target's append reached it.
//!
//! Failure. Whatever happens the window is closed before this returns and it
//! also expires by itself. If the target could not be brought up to date or
//! asked, this node still leads its term and proposes again. If an election
//! was started and its outcome is not the target, the error names what this
//! node's store knows; the host status field `metadata_leader` stays current.
use super::*;
use serde::Serialize;

/// Bound of the catch-up of the target.
const CATCH_UP: Duration = Duration::from_millis(1500);
/// Bound of the request and of the wait for the election's outcome.
const HANDOVER: Duration = Duration::from_millis(3000);
/// Budget of one append or of the request itself.
const REQUEST: Duration = Duration::from_millis(1000);

fn transfer_error(detail: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("metadata leadership transfer: {}", detail.into()))
}

/// A completed transfer, as this (former) leader's own store observed it.
/// Diagnostic DATA only.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct NativeMetadataLeadershipTransferReport {
    pub from: ClusterNodeId,
    pub to: ClusterNodeId,
    pub from_term: u64,
    pub term: u64,
}

impl NativeMetadataControlBackend {
    pub(in crate::distribution_native_host) fn transfer_metadata_leadership(
        &self,
        to: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<NativeMetadataLeadershipTransferReport> {
        parent.check()?;
        let actual = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("metadata leadership transfer: service busy"))?
            .listener_open_guard();
        let term = {
            let mut store = self.lock_metadata(parent)?;
            store.require_listener_database(&actual)?;
            store.begin_leadership_transfer(to, CATCH_UP + HANDOVER)?
        };
        let outcome = self.hand_over(to, term, &actual, parent);
        // Proposals resume here if this node still leads. The window also
        // expires by itself should this bounded close not get the store.
        let close = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() + Duration::from_secs(1));
        if let Ok(mut store) = self.lock_metadata(&close) {
            store.end_leadership_transfer(term);
        }
        outcome
    }

    fn hand_over(
        &self,
        to: &ClusterNodeId,
        term: u64,
        actual: &crate::distribution_data::ClusterServiceOpenGuard,
        parent: &CancellationToken,
    ) -> Result<NativeMetadataLeadershipTransferReport> {
        let store = |token: &CancellationToken| {
            let store = self.lock_metadata(token)?;
            store.require_listener_database(actual)?;
            Ok::<_, BicDbError>(store)
        };
        let catch_up = parent.child_with_deadline(Instant::now() + CATCH_UP);
        let mut last = String::from("no reply yet");
        let request = loop {
            let pending = match store(&catch_up) {
                // A refusal here is final: this node no longer leads its term.
                Ok(store) => match store.leadership_transfer_request(to)? {
                    Some(request) => break request,
                    None => {
                        let status = store.status();
                        Some(
                            (status.commit_index >= status.last_log_index)
                                .then(|| store.append_request_for_peer(to, 16)),
                        )
                    }
                },
                Err(error) => {
                    last = error.to_string();
                    None
                }
            };
            match pending {
                None => {}
                // Something is uncommitted: one ordinary round to every member.
                Some(None) => {
                    if let Err(error) = self.heartbeat(&catch_up) {
                        last = error.to_string();
                    }
                }
                Some(Some(Err(error))) => last = error.to_string(),
                Some(Some(Ok(append))) => {
                    let token = catch_up.child_with_deadline(Instant::now() + REQUEST);
                    match self
                        .transport
                        .append_metadata_cancellable(to, append, &token)
                    {
                        // Only a reply the owned store accepts counts.
                        Ok(reply) => match store(&catch_up)
                            .and_then(|mut store| store.record_append_response(to, &reply))
                        {
                            Ok(_) => last = format!("{to} replied (success={})", reply.success),
                            Err(error) => last = error.to_string(),
                        },
                        Err(error) => last = error.to_string(),
                    }
                }
            }
            if catch_up.check().is_err() {
                parent.check()?;
                let lag = store(parent)
                    .map(|store| store.leadership_transfer_lag(to))
                    .unwrap_or_default();
                return Err(transfer_error(format!(
                    "refused: {to} was not brought up to date within {} ms ({lag}); last: {last}; this node keeps leading term {term}",
                    CATCH_UP.as_millis()
                )));
            }
            std::thread::sleep(Duration::from_millis(10));
        };

        let handover = parent.child_with_deadline(Instant::now() + HANDOVER);
        let reply = self
            .transport
            .request_metadata_leadership_transfer(
                request,
                &handover.child_with_deadline(Instant::now() + REQUEST),
            )
            .map_err(|error| {
                transfer_error(format!(
                    "failed: {to} could not be asked to campaign: {error}; this node keeps leading term {term}"
                ))
            })?;
        if !reply.accepted {
            return Err(transfer_error(format!(
                "refused by {to}: {}; this node keeps leading term {term}",
                reply.refusal.as_deref().unwrap_or("no reason given")
            )));
        }
        // The reply is not the outcome. Only this node's own store is.
        loop {
            let status = store(&handover).map(|store| store.status());
            if let Ok(status) = &status {
                if status.current_term > term {
                    match (&status.role, &status.leader_id) {
                        (MetadataConsensusRole::Follower, Some(leader)) if leader == to => {
                            return Ok(NativeMetadataLeadershipTransferReport {
                                from: self.node_id.clone(),
                                to: to.clone(),
                                from_term: term,
                                term: status.current_term,
                            });
                        }
                        (MetadataConsensusRole::Follower, Some(leader)) => {
                            return Err(transfer_error(format!(
                                "failed: {leader}, not {to}, leads metadata in term {}",
                                status.current_term
                            )));
                        }
                        (MetadataConsensusRole::Leader, _) => {
                            return Err(transfer_error(format!(
                                "failed: {to} did not win; this node was elected again in term {}",
                                status.current_term
                            )));
                        }
                        _ => {}
                    }
                }
            }
            if handover.check().is_err() {
                parent.check()?;
                return Err(transfer_error(match status {
                    Ok(status)
                        if status.role == MetadataConsensusRole::Leader
                            && status.current_term == term =>
                    {
                        format!(
                            "timed out: {to} did not start an election within {} ms; this node keeps leading term {term}",
                            HANDOVER.as_millis()
                        )
                    }
                    Ok(status) => format!(
                        "timed out: this node left term {term} for term {} and knows no leader yet; an election is in progress, read `metadata_leader` in the status",
                        status.current_term
                    ),
                    Err(error) => format!("timed out: own metadata unreadable: {error}"),
                }));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl NativeMetadataControlHost {
    /// Transfer metadata leadership from this host to `to`. Accepted only on
    /// the running control listener of the current metadata leader.
    pub(in crate::distribution_native_host) fn transfer_metadata_leadership(
        &self,
        to: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<NativeMetadataLeadershipTransferReport> {
        if !self.is_running() {
            return Err(invalid(
                "metadata leadership transfer requires the running control listener",
            ));
        }
        self.backend.transfer_metadata_leadership(to, parent)
    }
}
