//! One owned bounded metadata-enrollment attempt. Its actual controller/store
//! owners and operation slot outlive a canceled caller until real worker exit.
use super::*;
use std::sync::mpsc;

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct NativeMetadataMaintenanceReport {
    pub node: Option<ClusterNodeId>,
    pub stage: &'static str,
    pub receiver_store_id: Option<Uuid>,
    pub installed_through_index: Option<u64>,
    pub error: Option<String>,
}
#[derive(Default)]
pub(super) struct NativeMetadataMaintenanceState {
    cursor: usize,
    backoff: BTreeMap<ClusterNodeId, (Instant, u8)>,
}
impl NativeMetadataRegistrationController {
    fn choose(&self, parent: &CancellationToken) -> Result<Option<ClusterNodeId>> {
        let snapshot = self.bootstrap(parent)?;
        if snapshot.metadata_leader_id.as_ref() != Some(&self.launch.seed.spec.node_id) {
            return Err(invalid(
                "automatic enrollment requires current metadata leader",
            ));
        }
        let candidates = snapshot
            .topology
            .nodes
            .values()
            .filter(|m| {
                m.lifecycle == ClusterNodeLifecycle::Active
                    && !snapshot
                        .topology
                        .ranges
                        .values()
                        .any(|r| r.replicas.iter().any(|p| p.node_id == m.id))
            })
            .map(|m| m.id.clone())
            .collect::<Vec<_>>();
        let mut state = self
            .maintenance
            .try_lock()
            .ok_or_else(|| invalid("metadata maintenance selection busy"))?;
        state.backoff.retain(|id, _| candidates.contains(id));
        for offset in 0..candidates.len() {
            let index = (state.cursor + offset) % candidates.len();
            let id = &candidates[index];
            if state
                .backoff
                .get(id)
                .is_none_or(|(until, _)| Instant::now() >= *until)
            {
                state.cursor = (index + 1) % candidates.len();
                return Ok(Some(id.clone()));
            }
        }
        Ok(None)
    }
    pub(crate) fn maintain(
        self: &Arc<Self>,
        parent: &CancellationToken,
    ) -> Result<NativeMetadataMaintenanceReport> {
        parent.check()?;
        if self
            .driving
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(invalid(
                "metadata controller operation still owns its real worker",
            ));
        }
        let token = parent.child_with_deadline(Instant::now() + Duration::from_millis(900));
        let worker_token = token.clone();
        let controller = Arc::clone(self);
        let (tx, rx) = mpsc::sync_channel(1);
        let spawned = std::thread::Builder::new()
            .name("native-metadata-enrollment".into())
            .spawn(move || {
                struct Owned(Arc<NativeMetadataRegistrationController>);
                impl Drop for Owned {
                    fn drop(&mut self) {
                        self.0.driving.store(false, Ordering::Release);
                    }
                }
                let _operation = Owned(Arc::clone(&controller));
                let selected = controller.choose(&worker_token);
                let result = match selected {
                    Ok(Some(node)) => {
                        let result = controller.maintain_one(&node, &worker_token);
                        if let Some(mut state) = controller.maintenance.try_lock() {
                            if result
                                .as_ref()
                                .is_ok_and(|r| r.stage == "promoted_and_installed")
                            {
                                state.backoff.insert(
                                    node.clone(),
                                    (Instant::now() + Duration::from_secs(5), 0),
                                );
                            } else if result.is_ok() {
                                state.backoff.remove(&node);
                            } else {
                                let failures = state
                                    .backoff
                                    .get(&node)
                                    .map_or(0, |(_, n)| n.saturating_add(1))
                                    .min(5);
                                state.backoff.insert(
                                    node.clone(),
                                    (
                                        Instant::now()
                                            + Duration::from_millis((200u64 << failures).min(5000)),
                                        failures,
                                    ),
                                );
                            }
                        }
                        result.map_err(|e| invalid(&format!("metadata learner {node}: {e}")))
                    }
                    Ok(None) => Ok(NativeMetadataMaintenanceReport {
                        node: None,
                        stage: "idle_or_backoff",
                        receiver_store_id: None,
                        installed_through_index: None,
                        error: None,
                    }),
                    Err(error) => Err(error),
                };
                let _ = tx.send(result);
            });
        if let Err(error) = spawned {
            self.driving.store(false, Ordering::Release);
            return Err(error.into());
        }
        loop {
            if let Err(error) = token.check() {
                token.cancel();
                return Err(error);
            }
            match rx.recv_timeout(Duration::from_millis(5)) {
                Ok(result) => {
                    token.check()?;
                    return result;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(invalid("metadata maintenance worker exited without result"))
                }
            }
        }
    }
    fn verify_installed_member(
        &self,
        target: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<NativeMetadataMaintenanceReport> {
        let business = Uuid::now_v7();
        let first = self.launch.transport.acquire_host_metadata_observation(
            &self.launch.seed.spec.node_id,
            business,
            Duration::from_secs(5),
            parent,
        )?;
        self.launch
            .transport
            .install_topology(first.committed_topology(business)?.clone())?;
        let open = self
            .launch
            .service
            .try_lock()
            .ok_or_else(|| invalid("voter installation service busy"))?
            .listener_open_guard();
        let (request, required) = {
            let store = self.launch.lock_initial_metadata(parent)?;
            store.require_listener_database(&open)?;
            store.require_native_control_leader(&first, business)?;
            let member = store
                .committed_topology()
                .nodes
                .get(target)
                .ok_or_else(|| invalid("installed voter absent"))?;
            if member.lifecycle != ClusterNodeLifecycle::Active
                || member.metadata_role != MetadataMemberRole::Voter
                || member.native_store_birth_v1.is_none()
            {
                return Err(invalid(
                    "installation verification requires actual configured bound voter",
                ));
            }
            let required = store.status().commit_index;
            (store.append_request_from(required, 16)?, required)
        };
        let reply = self
            .launch
            .transport
            .append_metadata_cancellable(target, request, parent)?;
        let id = reply
            .native_receiver
            .as_ref()
            .ok_or_else(|| invalid("installed voter lacks actual owned ACK"))?
            .metadata_store_id();
        {
            let mut store = self.launch.lock_initial_metadata(parent)?;
            store.require_listener_database(&open)?;
            store.record_append_response(target, &reply)?;
        }
        if !reply.success || reply.match_index < required {
            return Err(invalid(
                "actual voter has not installed committed promotion/configuration",
            ));
        }
        let post_business = Uuid::now_v7();
        let post = self.launch.transport.acquire_host_metadata_observation(
            &self.launch.seed.spec.node_id,
            post_business,
            Duration::from_secs(5),
            parent,
        )?;
        let store = self.launch.lock_initial_metadata(parent)?;
        store.require_listener_database(&open)?;
        store.require_native_control_leader(&post, post_business)?;
        if post.context_id() == first.context_id()
            || post.committed_topology(post_business)? != first.committed_topology(business)?
        {
            return Err(invalid("actual promoted installation CONFIG changed"));
        }
        parent.check()?;
        Ok(NativeMetadataMaintenanceReport {
            node: Some(target.clone()),
            stage: "promoted_and_installed",
            receiver_store_id: Some(id),
            installed_through_index: Some(required),
            error: None,
        })
    }
    fn maintain_one(
        &self,
        target: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<NativeMetadataMaintenanceReport> {
        let snapshot = self.bootstrap(parent)?;
        let member = snapshot
            .topology
            .nodes
            .get(target)
            .ok_or_else(|| invalid("automatic learner disappeared"))?;
        if member.address.parse::<std::net::SocketAddr>().is_err() {
            return Err(invalid(
                "automatic native metadata requires numeric learner endpoint",
            ));
        }
        if member.metadata_role == MetadataMemberRole::Voter {
            return self.verify_installed_member(target, parent);
        }
        if member.native_store_birth_v1.is_none() {
            self.launch
                .enroll_registered_native_learner(target, parent)?;
            return Ok(NativeMetadataMaintenanceReport {
                node: Some(target.clone()),
                stage: "birth_bound",
                receiver_store_id: None,
                installed_through_index: None,
                error: None,
            });
        }
        let business = Uuid::now_v7();
        let first = self.launch.transport.acquire_host_metadata_observation(
            &self.launch.seed.spec.node_id,
            business,
            Duration::from_secs(5),
            parent,
        )?;
        self.launch
            .transport
            .install_topology(first.committed_topology(business)?.clone())?;
        let open = self
            .launch
            .service
            .try_lock()
            .ok_or_else(|| invalid("maintenance service busy"))?
            .listener_open_guard();
        let mut readiness = None;
        for _ in 0..3 {
            let request = {
                let store = self.launch.lock_initial_metadata(parent)?;
                store.require_listener_database(&open)?;
                store.require_native_control_leader(&first, business)?;
                store.append_request_for_peer(target, 16)?
            };
            let reply = self
                .launch
                .transport
                .append_metadata_cancellable(target, request, parent)?;
            let mut store = self.launch.lock_initial_metadata(parent)?;
            store.require_listener_database(&open)?;
            store.record_append_response(target, &reply)?;
            if reply.success && reply.match_index >= store.status().commit_index {
                readiness = Some(store.observe_native_learner_promotion_readiness(
                    &first, business, target, &reply, parent,
                )?);
                break;
            }
        }
        let proof =
            readiness.ok_or_else(|| invalid("bounded actual learner catch-up incomplete"))?;
        let expected_store_id = proof.receiver_store_id();
        // Independently fresh authority AFTER the actual protected bound ACK.
        let post_business = Uuid::now_v7();
        let post = self.launch.transport.acquire_host_metadata_observation(
            &self.launch.seed.spec.node_id,
            post_business,
            Duration::from_secs(5),
            parent,
        )?;
        if post.context_id() == first.context_id() {
            return Err(invalid(
                "promotion requires distinct fresh metadata context",
            ));
        }
        let entry = {
            let mut store = self.launch.lock_initial_metadata(parent)?;
            store.require_listener_database(&open)?;
            store.propose_native_learner_promotion(
                &proof,
                &post,
                post_business,
                &self.config,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| invalid("metadata wall clock precedes epoch"))?
                    .as_millis()
                    .try_into()
                    .map_err(|_| invalid("metadata timestamp overflow"))?,
                parent,
            )?
        };
        // Normal proposal replication still requires genuine old/new quorums.
        self.launch.heartbeat_initial_metadata(parent)?;
        let (request, index) = {
            let store = self.launch.lock_initial_metadata(parent)?;
            store.require_listener_database(&open)?;
            let index = entry
                .as_ref()
                .map_or(store.status().commit_index, |e| e.index);
            if store.status().role != MetadataConsensusRole::Leader
                || store.status().commit_index < index
                || store
                    .committed_topology()
                    .nodes
                    .get(target)
                    .is_none_or(|m| m.metadata_role != MetadataMemberRole::Voter)
            {
                return Err(invalid(
                    "actual old/new voter quorums have not committed promotion",
                ));
            }
            // Actual source snapshot covers committed promotion, not merely
            // an ACK of the learner's earlier pre-promotion tail.
            (store.append_request_from(index, 16)?, index)
        };
        self.launch.publish_initial_committed_metadata(parent)?;
        let reply = self
            .launch
            .transport
            .append_metadata_cancellable(target, request, parent)?;
        {
            let mut store = self.launch.lock_initial_metadata(parent)?;
            store.require_listener_database(&open)?;
            store.record_append_response(target, &reply)?;
        }
        if !reply.success
            || reply.match_index < index
            || reply
                .native_receiver
                .as_ref()
                .is_none_or(|r| r.metadata_store_id() != expected_store_id)
        {
            return Err(invalid(
                "promoted learner has not acknowledged installed CONFIG on SAME owned store",
            ));
        }
        let final_business = Uuid::now_v7();
        let final_mq = self.launch.transport.acquire_host_metadata_observation(
            &self.launch.seed.spec.node_id,
            final_business,
            Duration::from_secs(5),
            parent,
        )?;
        let store = self.launch.lock_initial_metadata(parent)?;
        store.require_listener_database(&open)?;
        store.require_native_control_leader(&final_mq, final_business)?;
        if store
            .committed_topology()
            .nodes
            .get(target)
            .is_none_or(|m| {
                m.lifecycle != ClusterNodeLifecycle::Active
                    || m.metadata_role != MetadataMemberRole::Voter
            })
        {
            return Err(invalid("actual promoted member changed before completion"));
        }
        parent.check()?;
        Ok(NativeMetadataMaintenanceReport {
            node: Some(target.clone()),
            stage: "promoted_and_installed",
            receiver_store_id: Some(expected_store_id),
            installed_through_index: Some(index),
            error: None,
        })
    }
}
