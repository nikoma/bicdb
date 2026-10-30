//! Durable planned handoff. The old primary stops admitting commits before
//! issuing an offer; only the named standby at the exact final prefix accepts.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwitchoverOffer {
    pub cluster_id: String,
    pub source_node_id: String,
    pub target_node_id: String,
    pub source_epoch: u64,
    pub target_epoch: u64,
    pub commit_seq: u64,
}

impl BicDb {
    pub fn planned_switchover(&self) -> Option<SwitchoverOffer> {
        self.ha_node.read(|file| file.switchover.clone())
    }

    /// Serializes with every in-flight commit, flushes the final prefix and
    /// durably refuses further writes. Retrying the same request is idempotent.
    pub fn drain_for_switchover(&self, target: &str) -> Result<SwitchoverOffer> {
        let _admission = self.serializable_commit_admission.write();
        if !self.config.replication.enabled
            || !self.config.fsync
            || self.ha_node.role() != NodeRole::Primary
        {
            return Err(replication_error(
                "planned switchover requires a durable replication primary",
            ));
        }
        if target.trim().is_empty() || target == self.config.replication.node_id {
            return Err(replication_error(
                "switchover needs a different target node",
            ));
        }
        if let Some(offer) = self.planned_switchover() {
            if offer.target_node_id == target {
                return Ok(offer);
            }
            return Err(replication_error(
                "primary is already drained for another target",
            ));
        }
        self.ensure_writable("planned switchover")?;
        let seq = self.last_committed_tx.load(AtomicOrdering::SeqCst);
        if seq != self.current_commit_seq() {
            return Err(replication_error(
                "cannot hand off an incomplete applied prefix",
            ));
        }
        self.tx_log.write_durable(seq)?;
        let term = self
            .replication_election_term()
            .checked_add(1)
            .ok_or_else(|| replication_error("election term exhausted"))?;
        let offer = SwitchoverOffer {
            cluster_id: self.config.replication.cluster_id.clone(),
            source_node_id: self.config.replication.node_id.clone(),
            target_node_id: target.into(),
            source_epoch: self.replication_epoch(),
            target_epoch: term,
            commit_seq: seq,
        };
        self.ha_node.with_file(|file| {
            file.election_term = term;
            file.voted = Some((term, target.into()));
            file.switchover = Some(offer.clone());
        })?;
        Ok(offer)
    }

    /// Accept only an authenticated offer from the currently followed primary.
    /// The transport authenticates its sender; this method validates history.
    pub fn accept_switchover(
        &self,
        offer: &SwitchoverOffer,
        sql_address: Option<String>,
    ) -> Result<()> {
        let _admission = self.serializable_commit_admission.write();
        let status = self.ha_node_status();
        if !self.replication_verification_healthy()
            || !self.config.fsync
            || self.ha_node.role() != NodeRole::Standby
            || offer.cluster_id != self.config.replication.cluster_id
            || offer.target_node_id != self.config.replication.node_id
            || status.primary_id.as_deref() != Some(&offer.source_node_id)
            || status.data_epoch != offer.source_epoch
            || offer.target_epoch <= status.epoch
            || offer.target_epoch < self.replication_election_term()
            || self.last_applied_commit_seq() != offer.commit_seq
            || status.voted.as_ref().is_some_and(|(term, node)| {
                *term >= offer.target_epoch && node != &offer.target_node_id
            })
        {
            return Err(replication_error("switchover offer does not match this standby's identity, term or exact durable prefix"));
        }
        self.tx_log.write_durable(offer.commit_seq)?;
        self.ha_node.with_file(|file| {
            file.role = Some(NodeRole::Primary);
            file.epoch = offer.target_epoch;
            file.election_term = offer.target_epoch;
            file.data_epoch = offer.target_epoch;
            file.voted = Some((offer.target_epoch, offer.target_node_id.clone()));
            file.epoch_starts
                .insert(offer.target_epoch, offer.commit_seq);
            file.primary_id = Some(offer.target_node_id.clone());
            file.primary_sql_address = sql_address;
            file.slots.clear();
            file.switchover = None;
        })?;
        self.sync_quorum
            .close("planned promotion requires a restart before accepting writes");
        Ok(())
    }
}
