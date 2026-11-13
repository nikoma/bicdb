//! Discovery DATA and retained donor custody. Neither decoding nor a table key
//! grants capture authority; the actual recipient performs all local checks.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum NativeSourceDiscoveryOperationV1 {
    Start,
    Describe,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeSourceDiscoveryRequestV1 {
    pub version: u32,
    pub cluster_id: ClusterId,
    pub bootstrap_id: Uuid,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub relocation_id: crate::distribution::RelocationId,
    pub plan_id: String,
    pub handoff_session: Uuid,
    pub nonce: Uuid,
    pub scope_sha256: String,
    pub source_id: ClusterNodeId,
    pub source_birth: NativeStoreBirthV1,
    pub target_id: ClusterNodeId,
    pub target_birth: NativeStoreBirthV1,
    pub target_attempt: Uuid,
    pub operation: NativeSourceDiscoveryOperationV1,
    pub business: Uuid,
    pub challenge: Uuid,
    pub budget_ms: u64,
}
impl NativeSourceDiscoveryRequestV1 {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_plan(
        plan: &crate::distribution_range_handoff::NativeJointHandoffPlan,
        source: ClusterNodeId,
        target: ClusterNodeId,
        attempt: Uuid,
        operation: NativeSourceDiscoveryOperationV1,
        own: &HostMetadataReadObservation,
        business: Uuid,
        budget_ms: u64,
    ) -> Result<Self> {
        own.require_live_request(business)?;
        let current = own.committed_topology(business)?;
        let p = plan.promise();
        let request = Self {
            version: 1,
            cluster_id: current.cluster_id.clone(),
            bootstrap_id: current
                .range_image_bootstrap_id
                .ok_or_else(|| invalid("discovery bootstrap absent"))?,
            range_id: plan.old_range().id,
            range_epoch: plan.old_range().epoch,
            relocation_id: p.relocation_id,
            plan_id: p.plan_id.clone(),
            handoff_session: p.session_id,
            nonce: p.nonce,
            scope_sha256: plan.checksum_sha256()?,
            source_birth: plan.enrolled_birth(&source)?.clone(),
            target_birth: plan.enrolled_birth(&target)?.clone(),
            source_id: source,
            target_id: target,
            target_attempt: attempt,
            operation,
            business,
            challenge: Uuid::now_v7(),
            budget_ms,
        };
        request.require_plan(plan)?;
        Ok(request)
    }

    pub(crate) fn require_shape(&self) -> Result<()> {
        bounded_json(self, 16 * 1024)?;
        if self.version != 1
            || self.bootstrap_id.is_nil()
            || self.range_id.get() == 0
            || self.range_epoch == 0
            || self.relocation_id.get() == 0
            || self.handoff_session.is_nil()
            || self.nonce.is_nil()
            || self.target_attempt.is_nil()
            || self.business.is_nil()
            || self.challenge.is_nil()
            || !(1..=5000).contains(&self.budget_ms)
            || self.plan_id.is_empty()
            || self.plan_id.len() > 1024
            || self.source_id == self.target_id
            || self.scope_sha256.len() != 64
            || hex::decode(&self.scope_sha256).is_err()
        {
            return Err(invalid("invalid bounded native source discovery request"));
        }
        Ok(())
    }
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        self.require_shape()?;
        let token =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(self.budget_ms));
        token.check()?;
        Ok(token)
    }
    pub(crate) fn require_plan(
        &self,
        plan: &crate::distribution_range_handoff::NativeJointHandoffPlan,
    ) -> Result<()> {
        self.require_shape()?;
        let p = plan.promise();
        if self.range_id != plan.old_range().id
            || self.range_epoch != plan.old_range().epoch
            || self.relocation_id != p.relocation_id
            || self.plan_id != p.plan_id
            || self.handoff_session != p.session_id
            || self.nonce != p.nonce
            || self.scope_sha256 != plan.checksum_sha256()?
            || !plan.old_members().contains_key(&self.source_id)
            || plan.old_members().contains_key(&self.target_id)
            || !plan.new_members().contains_key(&self.target_id)
            || plan.enrolled_birth(&self.source_id)? != &self.source_birth
            || plan.enrolled_birth(&self.target_id)? != &self.target_birth
        {
            return Err(invalid(
                "source discovery differs from actual retained handoff plan",
            ));
        }
        Ok(())
    }
    fn capture_binding(&self) -> Self {
        let mut v = self.clone();
        v.operation = NativeSourceDiscoveryOperationV1::Start;
        v.business = Uuid::nil();
        v.challenge = Uuid::nil();
        v.budget_ms = 0;
        v
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeSourceDiscoveryReplyV1 {
    pub request: NativeSourceDiscoveryRequestV1,
    pub manifest: NativeRemoteImageManifestV1,
    pub source_metadata_context: Uuid,
    pub source_metadata_term: u64,
    pub source_metadata_read_index: u64,
    pub checksum_sha256: String,
}
impl NativeSourceDiscoveryReplyV1 {
    pub(crate) fn checksum(&self) -> Result<String> {
        let mut v = self.clone();
        v.checksum_sha256.clear();
        Ok(hex::encode(Sha256::digest(bounded_json(
            &("native-source-discovery-v1", v),
            2 * 1024 * 1024 + 32 * 1024,
        )?)))
    }
}
pub(crate) struct ActualNativeSourceDiscovery {
    reply: NativeSourceDiscoveryReplyV1,
}
impl ActualNativeSourceDiscovery {
    pub(crate) fn from_actual_source(
        request: NativeSourceDiscoveryRequestV1,
        source: &NativeRetainedRemoteImageSource,
        own: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<Self> {
        own.require_live_request(business)?;
        {
            let actual = source
                .session
                .try_lock()
                .ok_or_else(|| invalid("source discovery session busy before capture"))?;
            request.require_plan(&actual.old.plan())?;
            if actual.target_attempt != request.target_attempt
                || actual.old.selected_source_id() != &request.source_id
            {
                return Err(invalid(
                    "source discovery differs from exact retained donor/attempt",
                ));
            }
        }
        let manifest = source.discover_current(
            request.operation == NativeSourceDiscoveryOperationV1::Start,
            own,
            business,
            parent,
        )?;
        manifest.require_shape()?;
        if manifest.cluster_id != request.cluster_id
            || manifest.bootstrap_id != request.bootstrap_id
            || manifest.source_id != request.source_id
            || manifest.target_id != request.target_id
            || manifest.source_birth != request.source_birth
            || manifest.target_birth != request.target_birth
            || manifest.target_attempt != request.target_attempt
        {
            return Err(invalid(
                "actual source discovery manifest differs from original capture request",
            ));
        }
        let mut reply = NativeSourceDiscoveryReplyV1 {
            request,
            manifest,
            source_metadata_context: own.context_id(),
            source_metadata_term: own.term(),
            source_metadata_read_index: own.read_index(),
            checksum_sha256: String::new(),
        };
        reply.checksum_sha256 = reply.checksum()?;
        Ok(Self { reply })
    }
    pub(crate) fn into_reply(self) -> NativeSourceDiscoveryReplyV1 {
        self.reply
    }
}

pub(crate) struct NativeSourceDiscoveryAttempt {
    binding: NativeSourceDiscoveryRequestV1,
    requests: Mutex<BTreeMap<Uuid, Vec<u8>>>,
    driving: AtomicBool,
    source: Mutex<Option<Arc<NativeRetainedRemoteImageSource>>>,
}
pub(crate) struct NativeSourceDiscoveryClaim(Arc<NativeSourceDiscoveryAttempt>);
impl Drop for NativeSourceDiscoveryClaim {
    fn drop(&mut self) {
        self.0.driving.store(false, Ordering::Release);
    }
}
impl NativeSourceDiscoveryAttempt {
    pub(crate) fn claim(self: &Arc<Self>) -> Result<NativeSourceDiscoveryClaim> {
        self.driving
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| invalid("actual source discovery already active"))?;
        Ok(NativeSourceDiscoveryClaim(self.clone()))
    }
    pub(crate) fn source(&self) -> Result<Option<Arc<NativeRetainedRemoteImageSource>>> {
        Ok(self
            .source
            .try_lock()
            .ok_or_else(|| invalid("discovery custody busy"))?
            .clone())
    }
    pub(crate) fn retain_source(
        &self,
        source: NativeRetainedRemoteImageSource,
    ) -> Result<Arc<NativeRetainedRemoteImageSource>> {
        let mut slot = self
            .source
            .try_lock()
            .ok_or_else(|| invalid("discovery custody busy"))?;
        if slot.is_some() {
            return Err(invalid("actual discovery source cannot be replaced"));
        }
        let source = Arc::new(source);
        *slot = Some(source.clone());
        Ok(source)
    }
}
impl NativeRemoteImageSourceRegistry {
    pub(crate) fn source_discovery_attempt(
        &self,
        request: &NativeSourceDiscoveryRequestV1,
    ) -> Result<Arc<NativeSourceDiscoveryAttempt>> {
        self.require_original_process()?;
        request.require_shape()?;
        let attempt = {
            let mut table = self
                .discoveries
                .try_lock()
                .ok_or_else(|| invalid("source discovery table busy"))?;
            if let Some(old) = table.get(&request.range_id) {
                if old.binding != request.capture_binding() {
                    return Err(invalid(
                        "source discovery retains another exact capture/attempt",
                    ));
                }
                old.clone()
            } else {
                if request.operation != NativeSourceDiscoveryOperationV1::Start {
                    return Err(invalid("source discovery has not started"));
                }
                if table.len() >= 8 {
                    return Err(invalid(
                        "source discovery custody capacity; no timeout eviction",
                    ));
                }
                let owner = Arc::new(NativeSourceDiscoveryAttempt {
                    binding: request.capture_binding(),
                    requests: Mutex::new(BTreeMap::new()),
                    driving: AtomicBool::new(false),
                    source: Mutex::new(None),
                });
                table.insert(request.range_id, owner.clone());
                owner
            }
        };
        let bytes = bounded_json(request, 16 * 1024)?;
        let mut issued = attempt
            .requests
            .try_lock()
            .ok_or_else(|| invalid("source discovery request custody busy"))?;
        if let Some(old) = issued.get(&request.challenge) {
            if old != &bytes {
                return Err(invalid(
                    "source discovery challenge reused with different exact request bytes",
                ));
            }
        } else {
            if issued.len() >= 64 {
                return Err(invalid(
                    "source discovery request custody capacity; retained requests not evicted",
                ));
            }
            issued.insert(request.challenge, bytes);
        }
        drop(issued);
        Ok(attempt)
    }
    #[cfg(test)]
    pub(crate) fn test_discovery_state(&self, range: RangeId) -> Result<serde_json::Value> {
        let owner = self
            .discoveries
            .try_lock()
            .ok_or_else(|| invalid("discovery table busy"))?
            .get(&range)
            .cloned();
        let Some(owner) = owner else {
            return Ok(serde_json::Value::Null);
        };
        let source = owner.source()?;
        let state = if let Some(source) = source {
            let held = source
                .session
                .try_lock()
                .ok_or_else(|| invalid("discovery source busy"))?;
            serde_json::json!({"source_session":held.source_session,"capture_attempted":held.capture_attempted,
                "capture_retained":held.capture.is_some(),"published":held.published,"manifest":held.manifest})
        } else {
            serde_json::Value::Null
        };
        Ok(serde_json::json!({"binding":owner.binding,"source":state}))
    }
    #[cfg(test)]
    pub(crate) fn test_cancel_original_process(&self) -> Result<()> {
        self.original_process
            .get()
            .ok_or_else(|| invalid("original source process absent"))?
            .cancel();
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn test_drop_next_discovery_reply(&self) {
        self.drop_next_discovery_reply
            .store(true, Ordering::Release);
    }
    #[cfg(test)]
    pub(crate) fn test_take_discovery_reply_loss(&self) -> bool {
        self.drop_next_discovery_reply.swap(false, Ordering::AcqRel)
    }
}
impl NativeRetainedRemoteImageSource {
    /// Continue only this retained session. A captured-but-unpublished exact
    /// manifest can be revalidated; an unknown capture is never replaced.
    pub(crate) fn discover_current(
        &self,
        start: bool,
        own: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<NativeRemoteImageManifestV1> {
        self.registry.retain(&self.session)?;
        let mut actual = self
            .session
            .try_lock()
            .ok_or_else(|| invalid("actual discovery source session busy"))?;
        if !actual.capture_attempted {
            if !start {
                return Err(invalid("actual source capture has not started"));
            }
            actual.begin(Duration::from_secs(5), own, business, parent)?;
        } else {
            actual.require_source_current(own, business, parent)?;
            actual.published = true;
        }
        actual.require_source_current(own, business, parent)?;
        Ok(actual.manifest()?.clone())
    }
}
