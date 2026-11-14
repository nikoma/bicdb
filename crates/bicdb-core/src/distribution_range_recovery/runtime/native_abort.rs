//! Every participant obtains its own fresh role and original candidate. No
//! request contains a claimed quorum. Native/Store IO remains under the SAME
//! service, while metadata and peer RPCs occur without service/DB latches.
use super::*;
use std::collections::VecDeque;
use std::sync::mpsc;

impl RangeRecoveryRuntime {
    pub(crate) fn observe_native_abort_promise(
        &self,
        actor: &MetadataReadParticipant,
        request: &NativeAbortRequest,
        parent: &CancellationToken,
    ) -> Result<NativeAbortPromiseObservation> {
        let _operation = self.reserve_observer(request.scope.range_id)?;
        self.observe_native_abort_promise_inner(actor, request, parent)
    }
    fn observe_native_abort_promise_inner(
        &self,
        actor: &MetadataReadParticipant,
        request: &NativeAbortRequest,
        parent: &CancellationToken,
    ) -> Result<NativeAbortPromiseObservation> {
        let token = request.cancellation(parent)?;
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let witness = self.provider.witness_recovery_role(
            actor,
            business,
            request.scope.range_id,
            request.scope.range_epoch,
            request.scope.recovery_nonce,
            challenge,
            RecoveryObservationRoleAdmission::purpose(&request.scope)?,
            &token,
        )?;
        let role = RecoveryObservationRoleAdmission::from_witness(
            witness,
            actor,
            &self.local_node_id,
            business,
            challenge,
            &request.scope,
        )?;
        let evidence = {
            let service = loop {
                token.check()?;
                if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                    break service;
                }
            };
            service.observe_existing_native_abort_promise(&role, request, &token)?
        };
        token.check()?;
        NativeAbortPromiseObservation::from_local(evidence, self.provider.local_participant())
    }
    fn abort_role(
        &self,
        actor: &MetadataReadParticipant,
        request: &NativeAbortRequest,
        token: &CancellationToken,
    ) -> Result<RecoveryRoleAdmission> {
        let scope = &request.scope;
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let witness = self.provider.witness_recovery_role(
            actor,
            business,
            scope.range_id,
            scope.range_epoch,
            scope.recovery_nonce,
            challenge,
            scope.purpose.clone(),
            token,
        )?;
        let role = RecoveryRoleAdmission::from_witness(
            witness,
            actor,
            &self.local_node_id,
            business,
            scope.range_id,
            scope.range_epoch,
            scope.recovery_nonce,
            challenge,
            &scope.purpose,
        )?;
        scope.require_role(&role)?;
        Ok(role)
    }
    pub(crate) fn observe_native_abort_candidate(
        &self,
        actor: &MetadataReadParticipant,
        request: &NativeAbortRequest,
        parent: &CancellationToken,
    ) -> Result<NativeAbortCandidateObservation> {
        let _operation = self.reserve_observer(request.scope.range_id)?;
        self.observe_native_abort_candidate_inner(actor, request, parent)
    }
    fn observe_native_abort_candidate_inner(
        &self,
        actor: &MetadataReadParticipant,
        request: &NativeAbortRequest,
        parent: &CancellationToken,
    ) -> Result<NativeAbortCandidateObservation> {
        let token = request.cancellation(parent)?;
        let scope = &request.scope;
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let witness = self.provider.witness_recovery_role(
            actor,
            business,
            scope.range_id,
            scope.range_epoch,
            scope.recovery_nonce,
            challenge,
            RecoveryObservationRoleAdmission::purpose(scope)?,
            &token,
        )?;
        let role = RecoveryObservationRoleAdmission::from_witness(
            witness,
            actor,
            &self.local_node_id,
            business,
            challenge,
            scope,
        )?;
        let evidence = {
            let service = loop {
                token.check()?;
                if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                    break service;
                }
            };
            service.observe_native_abort_candidate(&role, request, &token)?
        };
        token.check()?;
        NativeAbortCandidateObservation::from_local(evidence, self.provider.local_participant())
    }
    fn fetch_native_abort_candidate(
        &self,
        actor: &MetadataReadParticipant,
        request: &NativeAbortRequest,
        token: &CancellationToken,
    ) -> Result<NativeAbortCandidateObservation> {
        if request.origin_node_id == self.local_node_id {
            self.observe_native_abort_candidate_inner(actor, request, token)
        } else {
            self.participant_transport
                .as_ref()
                .ok_or_else(|| invalid("native abort candidate transport is not wired"))?
                .observe_native_abort_candidate(&request.origin_node_id, request, token)
        }
    }
    pub(crate) fn promise_native_abort(
        &self,
        actor: &MetadataReadParticipant,
        request: &NativeAbortRequest,
        parent: &CancellationToken,
    ) -> Result<NativeAbortPromiseObservation> {
        let _operation = self.reserve(request.scope.range_id)?;
        let token = request.cancellation(parent)?;
        let candidate = self.fetch_native_abort_candidate(actor, request, &token)?;
        self.promise_native_abort_inner(actor, request, &candidate, &token)
    }
    fn promise_native_abort_inner(
        &self,
        actor: &MetadataReadParticipant,
        request: &NativeAbortRequest,
        candidate: &NativeAbortCandidateObservation,
        token: &CancellationToken,
    ) -> Result<NativeAbortPromiseObservation> {
        // Independently fresh MQ AFTER candidate collection; no coordinator DTO
        // may substitute for the recipient's actual current authority.
        let role = self.abort_role(actor, request, token)?;
        candidate.require_for_role(&role, request)?;
        {
            let service = loop {
                token.check()?;
                if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                    break service;
                }
            };
            service.require_persisted_recovery_scope(&request.scope)?;
        }
        let frozen = self.frozen(&request.scope, token, false)?;
        let evidence = {
            let mut service = loop {
                token.check()?;
                if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                    break service;
                }
            };
            service.promise_native_abort(&role, request, candidate, &frozen, token)?
        };
        token.check()?;
        NativeAbortPromiseObservation::from_local(evidence, self.provider.local_participant())
    }
    /// Current receiver independently collects actual durable promises. The
    /// returned receipt is local metadata history, not an ACK/read permission.
    pub(crate) fn resolve_native_abort(
        &self,
        actor: &MetadataReadParticipant,
        request: &NativeAbortRequest,
        parent: &CancellationToken,
    ) -> Result<crate::db::NativeRecoveryAbortedReceipt> {
        let operation = self.reserve(request.scope.range_id)?;
        let token = request.cancellation(parent)?;
        let candidate = Arc::new(self.fetch_native_abort_candidate(actor, request, &token)?);
        let mut vote_request = request.clone();
        vote_request.candidate_sha256 = Some(candidate.sha256().into());
        let role = self.abort_role(actor, &vote_request, &token)?;
        // Only the actual current leader may ask peers to CREATE promises.
        // Other recipients collect fresh readonly projections of votes which
        // already exist; they never delegate the leader's transport identity.
        let creates_promises = role.local_node_id() == role.initiator_node_id();
        let members = role
            .member_credentials()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut round = NativeAbortPromiseRound::begin(
            role,
            Arc::clone(&candidate),
            vote_request.clone(),
            &token,
        )?;
        // A local failure is not a fabricated vote. An actual irreversible
        // observation poisons the round through record just like a remote one.
        let mut failures = BTreeMap::new();
        let local = if creates_promises {
            self.promise_native_abort_inner(actor, &vote_request, &candidate, &token)
        } else {
            self.observe_native_abort_promise_inner(actor, &vote_request, &token)
        };
        match local {
            Ok(observation) => round.record(observation)?,
            Err(error) => {
                failures.insert(
                    self.local_node_id.clone(),
                    error.to_string().chars().take(1024).collect::<String>(),
                );
            }
        }
        let mut queue = members
            .into_iter()
            .filter(|n| n != &self.local_node_id)
            .collect::<VecDeque<_>>();
        let workers = 4.min(queue.len());
        let worker_token = token.child();
        // Each promise worker holds this operation's range context. Teardown
        // waits, bounded, for them so a resolved abort does not leave the next
        // recovery step on this range refused as busy.
        let mut promise_workers = CollectionWorkers::new(worker_token.clone());
        if !round.has_quorum() {
            let transport = self
                .participant_transport
                .as_ref()
                .ok_or_else(|| invalid("native abort promise transport is not wired"))?;
            let (jobs, rx) = mpsc::sync_channel::<ClusterNodeId>(workers.max(1));
            let rx = Arc::new(Mutex::new(rx));
            let (tx, results) = mpsc::sync_channel(workers.max(1));
            for _ in 0..workers {
                let rx = Arc::clone(&rx);
                let tx = tx.clone();
                let transport = Arc::clone(transport);
                let request = vote_request.clone();
                let child = worker_token.clone();
                let operation = Arc::clone(&operation);
                let handle = std::thread::Builder::new()
                    .name("native-abort-promise".into())
                    .spawn(move || {
                        let _operation = operation;
                        while child.check().is_ok() {
                            let destination = match rx.lock().recv_timeout(LATCH_POLL) {
                                Ok(node) => node,
                                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                            };
                            let result = child.check().and_then(|_| {
                                if creates_promises {
                                    transport.promise_native_abort(&destination, &request, &child)
                                } else {
                                    transport.observe_native_abort_promise(
                                        &destination,
                                        &request,
                                        &child,
                                    )
                                }
                            });
                            if tx.send((destination, result)).is_err() {
                                break;
                            }
                        }
                    })?;
                promise_workers.push(handle);
            }
            drop(tx);
            let mut outstanding = 0;
            loop {
                token.check()?;
                while outstanding < workers {
                    let Some(node) = queue.pop_front() else {
                        break;
                    };
                    jobs.try_send(node)
                        .map_err(|_| invalid("bounded abort job queue invariant failed"))?;
                    outstanding += 1;
                }
                // Consume every already received contradiction before using an
                // available majority; a held minority retains its Operation.
                while let Ok((node, result)) = results.try_recv() {
                    outstanding -= 1;
                    match result {
                        Ok(observation) => round.record(observation)?,
                        Err(error) => {
                            failures.insert(
                                node,
                                error.to_string().chars().take(1024).collect::<String>(),
                            );
                        }
                    }
                }
                if round.has_quorum() {
                    break;
                }
                if outstanding == 0 && queue.is_empty() {
                    return Err(invalid(&format!("actual permanent AbortPromise quorum unavailable; participant failures: {failures:?}")));
                }
                match results.recv_timeout(LATCH_POLL) {
                    Ok((node, result)) => {
                        outstanding -= 1;
                        match result {
                            Ok(observation) => round.record(observation)?,
                            Err(error) => {
                                failures.insert(
                                    node,
                                    error.to_string().chars().take(1024).collect::<String>(),
                                );
                            }
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        return Err(invalid("abort workers stopped before quorum"))
                    }
                }
            }
        }
        let quorum = round.finish()?;
        // Only the separately obtained post-QQ context can publish a decision.
        let fresh = self.abort_role(actor, &vote_request, &token)?;
        quorum.require_recipient_role(&fresh)?;
        let frozen = self.frozen(&request.scope, &token, false)?;
        let receipt = {
            let mut service = loop {
                token.check()?;
                if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                    break service;
                }
            };
            service.apply_native_abort_quorum(&fresh, &quorum, &frozen, &token)?
        };
        token.check()?;
        Ok(receipt)
    }
}
