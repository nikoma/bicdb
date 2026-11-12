//! Orderly ALL target-owner drop, fsynced copy and strict actual reopen. The
//! four original TLS metadata voters remain live; not five-process outage/HA.
use super::*;
use crate::distribution_consensus::MetadataConsensusStore;
use crate::distribution_native_host::NativeColdLearnerMetadataHost;
use std::path::{Path, PathBuf};

const META: &str = "native-learner-metadata";
const ORIGINAL: &str = "native-learner-metadata-original-c.json";
fn namespaces(factory: &NativeUnassignedLearnerFactory) -> Vec<(String, Uuid)> {
    let mut ids = factory
        .database
        .read()
        .collections()
        .into_iter()
        .map(|m| (m.name, m.local_namespace_id.unwrap()))
        .collect::<Vec<_>>();
    ids.sort();
    ids
}
fn copy_closed(source: &Path, target: &Path) {
    std::fs::create_dir(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let kind = entry.file_type().unwrap();
        assert!(!kind.is_symlink(), "cold fixture refuses alias copy");
        let dest = target.join(entry.file_name());
        if kind.is_dir() {
            copy_closed(&entry.path(), &dest);
        } else {
            assert!(kind.is_file());
            std::fs::copy(entry.path(), &dest).unwrap();
            std::fs::File::open(&dest).unwrap().sync_all().unwrap();
        }
    }
    std::fs::File::open(target).unwrap().sync_all().unwrap();
    std::fs::File::open(target.parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
}
fn open_cold(f: &EnrollmentFixture, root: &Path) -> NativeColdLearnerMetadataHost {
    let parent = CancellationToken::uncancelable();
    let (mq, business) = own_mq(f, &parent);
    NativeColdLearnerMetadataHost::reopen(
        root,
        db_config(),
        &mq,
        business,
        f.networks[4].clone(),
        &parent,
    )
    .unwrap()
}
fn start_cold(f: &EnrollmentFixture, host: &mut NativeColdLearnerMetadataHost) -> Result<Uuid> {
    let parent = CancellationToken::uncancelable();
    let (mq, business) = own_mq(f, &parent);
    host.start(&mq, business, &parent)
}
fn catch_up(f: &EnrollmentFixture, host: &NativeColdLearnerMetadataHost, id: Uuid) {
    let before = f.launches[0].metadata.lock().status().commit_index;
    let request = f.launches[0]
        .metadata
        .lock()
        .append_request_from(1, 16)
        .unwrap();
    let response = f.launches[0]
        .transport
        .append_metadata(&node(4), request)
        .unwrap();
    assert!(response.success && response.native_provenance.is_some());
    assert_eq!(
        response
            .native_receiver
            .as_ref()
            .unwrap()
            .metadata_store_id(),
        id
    );
    let factory = host.test_factory();
    assert_eq!(
        response.native_receiver.as_ref().unwrap().native_birth(),
        &crate::distribution::NativeStoreBirthV1::from_actual_native(
            &factory
                .database
                .read()
                .native_recovery_admission(
                    &factory.spec.cluster_id,
                    factory.spec.bootstrap_id,
                    &factory.spec.node_id,
                    factory.spec.member_incarnation
                )
                .unwrap()
        )
        .unwrap()
    );
    f.launches[0]
        .metadata
        .lock()
        .record_append_response(&node(4), &response)
        .unwrap();
    let metadata = host.test_metadata().unwrap();
    let mut store = metadata.lock();
    assert!(store.status().current_term > 0 && store.status().commit_index >= before);
    assert_eq!(
        store.committed_topology().nodes[&node(4)].metadata_role,
        MetadataMemberRole::Learner
    );
    assert!(store.start_election().is_err());
    assert!(store
        .committed_topology()
        .ranges
        .values()
        .all(|r| r.replicas.iter().all(|replica| replica.node_id != node(4))));
    drop(store);
    drop(metadata);
    assert!(!factory.database.read().commit_admission_ready());
    assert!(factory
        .database
        .read()
        .scan_collection("items")
        .unwrap()
        .is_empty());
    let range = f.launches[0]
        .metadata
        .lock()
        .committed_topology()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone();
    let error = f.launches[0]
        .transport
        .application_event(
            &node(4),
            crate::ClusterEventRequest {
                range_id: range.id,
                range_epoch: range.epoch,
                operation: crate::ClusterEventOperation::Scan {
                    namespace: "items".into(),
                    after: None,
                    limit: 1,
                },
            },
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("metadata endpoints only"),
        "{error}"
    );
}

fn durable_state(root: &Path) -> serde_json::Value {
    let envelope: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            root.join(META)
                .join(crate::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE),
        )
        .unwrap(),
    )
    .unwrap();
    let mut state = envelope["state"].clone();
    // Ordinary strict reopen clears only these volatile Raft labels.
    state.as_object_mut().unwrap().remove("role");
    state.as_object_mut().unwrap().remove("leader_id");
    state
}

/// Original factory/host is gone before copying. No old DB Arc, MStore,
/// provider, backend, listener or guard is used to reconstruct the cold owner.
fn closed_cut(
    cut: NativeMetadataInitializationCut,
) -> (
    EnrollmentFixture,
    PathBuf,
    Option<Uuid>,
    Vec<(String, Uuid)>,
) {
    let f = EnrollmentFixture::new();
    let (factory, mut host) = f.factory("all-owner-original");
    let token = CancellationToken::uncancelable();
    f.launches[0]
        .enroll_registered_native_learner(&node(4), &token)
        .unwrap();
    let original = factory.database.read().data_path().to_path_buf();
    let ids = namespaces(&factory);
    let (mq, business) = own_mq(&f, &token);
    let error = host
        .test_start_owned_metadata_with(
            &mq,
            business,
            &token,
            |actual| {
                if actual == cut {
                    Err(std::io::Error::other("actual cold learner initialization cut").into())
                } else {
                    Ok(())
                }
            },
            |_| Ok(()),
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("actual cold learner initialization cut"),
        "{error}"
    );
    let id = factory
        .database
        .read()
        .native_metadata_initialization_record(
            &factory.spec.cluster_id,
            factory.spec.bootstrap_id,
            &factory.spec.node_id,
            factory.spec.member_incarnation,
        )
        .unwrap()
        .map(|record| {
            assert!(!record.registered());
            assert!(record.initial_namespace_ids().is_some());
            record.store_id()
        });
    if cut == NativeMetadataInitializationCut::AfterOriginalConfiguration {
        assert!(id.is_none());
    } else {
        assert!(id.is_some());
    }
    assert!(original.join(META).join(ORIGINAL).is_file());
    host.shutdown().unwrap();
    drop(factory);
    let copied = f.root.path().join("all-owner-copied");
    copy_closed(&original, &copied);
    eprintln!(
        "NATIVE_COLD_LEARNER_CUT {cut:?} ORIGINAL={} COPY={} UUID={id:?}",
        original.display(),
        copied.display()
    );
    (f, copied, id, ids)
}

#[test]
fn native_learner_cold_all_owners_sidecar_and_both_prepared_cuts_keep_uuid_and_actual_tls_ack() {
    for cut in [
        NativeMetadataInitializationCut::AfterOriginalConfiguration,
        NativeMetadataInitializationCut::AfterProtectedPrepare,
        NativeMetadataInitializationCut::AfterStateFile,
    ] {
        let (f, copied, before_id, ids) = closed_cut(cut);
        let mut host = open_cold(&f, &copied);
        assert!(host.test_metadata().is_none());
        let id = start_cold(&f, &mut host).unwrap();
        if let Some(original) = before_id {
            assert_eq!(id, original);
        }
        assert_eq!(namespaces(host.test_factory()), ids);
        assert_eq!(protected_id(host.test_factory()), (id, true));
        catch_up(&f, &host, id);
        let before = host.test_metadata().unwrap().lock().status();
        let before_state = durable_state(&copied);
        host.stop(&CancellationToken::uncancelable()).unwrap();
        drop(host);
        // ALL copied target owners end again. Registered does not recreate T0.
        let mut again = open_cold(&f, &copied);
        assert_eq!(start_cold(&f, &mut again).unwrap(), id);
        let metadata = again.test_metadata().unwrap();
        assert_eq!(metadata.lock().status().current_term, before.current_term);
        assert_eq!(metadata.lock().status().commit_index, before.commit_index);
        assert_eq!(metadata.lock().status().voted_for, before.voted_for);
        assert_eq!(
            metadata.lock().status().last_log_index,
            before.last_log_index
        );
        assert_eq!(metadata.lock().status().last_log_term, before.last_log_term);
        assert_eq!(
            durable_state(&copied),
            before_state,
            "term/vote/log/commit/C/restore/UUID must survive the all-owner reopen"
        );
        assert_eq!(namespaces(again.test_factory()), ids);
        drop(metadata);
        again.stop(&CancellationToken::uncancelable()).unwrap();
        drop(again);
        f.shutdown();
    }
}

#[test]
fn native_learner_cold_prepared_missing_or_changed_original_and_nonzero_vote_never_reset() {
    for kind in [
        "missing-original",
        "changed-original",
        "nonzero-vote",
        "wrong-uuid",
    ] {
        let (f, copied, id, _) = closed_cut(NativeMetadataInitializationCut::AfterStateFile);
        let metadata_root = copied.join(META);
        match kind {
            "missing-original" => std::fs::remove_file(metadata_root.join(ORIGINAL)).unwrap(),
            "changed-original" => {
                MetadataConsensusStore::test_corrupt_cold_original_configuration(&metadata_root)
                    .unwrap()
            }
            "nonzero-vote" => {
                MetadataConsensusStore::test_corrupt_cold_learner_state(&metadata_root, false)
                    .unwrap()
            }
            "wrong-uuid" => {
                MetadataConsensusStore::test_corrupt_cold_learner_state(&metadata_root, true)
                    .unwrap()
            }
            _ => unreachable!(),
        }
        let state =
            std::fs::read(metadata_root.join(crate::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE))
                .unwrap();
        let mut host = open_cold(&f, &copied);
        let record = host
            .test_factory()
            .database
            .read()
            .test_native_metadata_record_bytes()
            .unwrap();
        let error = start_cold(&f, &mut host).unwrap_err();
        assert!(error.to_string().contains("Prepared"), "{kind}: {error}");
        assert_eq!(protected_id(host.test_factory()), (id.unwrap(), false));
        assert_eq!(
            host.test_factory()
                .database
                .read()
                .test_native_metadata_record_bytes()
                .unwrap(),
            record
        );
        assert_eq!(
            std::fs::read(metadata_root.join(crate::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE))
                .unwrap(),
            state
        );
        assert!(
            !host.test_custody().2,
            "no listener is published for a quarantined constructor"
        );
        drop(host);
        f.shutdown();
    }
}

#[test]
fn native_learner_cold_registered_loss_refuses_and_bind_cancel_retry_keeps_real_owner() {
    let (f, copied, before, ids) = closed_cut(NativeMetadataInitializationCut::AfterStateFile);
    let parent = CancellationToken::uncancelable();
    let mut host = open_cold(&f, &copied);
    let address = f.launches[0].metadata.lock().committed_topology().nodes[&node(4)]
        .address
        .clone();
    let occupied = std::net::TcpListener::bind(address).unwrap();
    let error = start_cold(&f, &mut host).unwrap_err();
    assert!(
        error
            .to_string()
            .to_ascii_lowercase()
            .contains("address already in use"),
        "{error}"
    );
    assert_eq!(host.test_custody(), (false, true, false, false));
    let retained = host.test_metadata().unwrap();
    let id = retained.lock().test_native_metadata_identity().unwrap();
    assert_eq!(Some(id), before);
    drop(occupied);
    let original_mq_cancel = parent.child();
    let (mq, business) = own_mq(&f, &original_mq_cancel);
    let error = host
        .test_start_with(&mq, business, &parent, |phase| {
            if phase == "after-bind" {
                original_mq_cancel.cancel();
            }
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(error, BicDbError::QueryCanceled), "{error}");
    assert_eq!(host.test_custody(), (false, true, true, true));
    let original_store = host.test_metadata().unwrap();
    assert!(Arc::ptr_eq(&retained, &original_store));
    assert_eq!(Some(id), before);
    assert_eq!(start_cold(&f, &mut host).unwrap(), id);
    assert!(Arc::ptr_eq(&original_store, &host.test_metadata().unwrap()));
    catch_up(&f, &host, id);
    drop(original_store);
    drop(retained);
    host.stop(&parent).unwrap();
    drop(host);
    for kind in ["missing-state", "wrong-uuid"] {
        let negative = f.root.path().join(format!("registered-{kind}"));
        copy_closed(&copied, &negative);
        let root = negative.join(META);
        if kind == "missing-state" {
            std::fs::remove_file(root.join(crate::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE))
                .unwrap();
        } else {
            MetadataConsensusStore::test_corrupt_cold_learner_state(&root, true).unwrap();
        }
        let mut bad = open_cold(&f, &negative);
        let native = bad
            .test_factory()
            .database
            .read()
            .test_native_metadata_record_bytes()
            .unwrap();
        let error = start_cold(&f, &mut bad).unwrap_err();
        assert!(
            error.to_string().contains("protected metadata state")
                || error.to_string().contains("protected store identity"),
            "{error}"
        );
        assert_eq!(protected_id(bad.test_factory()), (id, true));
        assert_eq!(
            bad.test_factory()
                .database
                .read()
                .test_native_metadata_record_bytes()
                .unwrap(),
            native
        );
        assert_eq!(namespaces(bad.test_factory()), ids);
        assert!(!bad.test_custody().2);
        drop(bad);
    }
    f.shutdown();
}

/// Exact pre-binding codec, including deny_unknown_fields and the original
/// checksum domain. Input below is read from actual committed native pages.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct OldNativeMetadataRecord {
    version: u32,
    store_id: Uuid,
    cluster_id: crate::ClusterId,
    bootstrap_id: Uuid,
    node_id: ClusterNodeId,
    incarnation: u64,
    birth_id: Uuid,
    birth_sha256: String,
    restore_activation: Option<crate::distribution_consensus::MetadataRestoreActivation>,
    initial_topology_sha256: String,
    registered: bool,
    checksum_sha256: String,
}
impl OldNativeMetadataRecord {
    fn checksum(&self) -> String {
        use sha2::Digest;
        let mut plain = self.clone();
        plain.checksum_sha256.clear();
        hex::encode(sha2::Sha256::digest(
            crate::distribution_range_image::bounded_json(
                &("NativeMetadataStoreIdentity-v1", plain),
                64 * 1024,
            )
            .unwrap(),
        ))
    }
}
#[test]
fn native_learner_cold_actual_uid_receipt_old_decoder_refuses_and_none_voter_bytes_unchanged() {
    let (f, copied, _, _) = closed_cut(NativeMetadataInitializationCut::AfterStateFile);
    let mut host = open_cold(&f, &copied);
    let actual = host
        .test_factory()
        .database
        .read()
        .test_native_metadata_record_bytes()
        .unwrap();
    let error = serde_json::from_slice::<OldNativeMetadataRecord>(&actual)
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("initial_namespace_ids"),
        "{error}"
    );
    let mut stripped: serde_json::Value = serde_json::from_slice(&actual).unwrap();
    stripped
        .as_object_mut()
        .unwrap()
        .remove("initial_namespace_ids");
    let stripped: OldNativeMetadataRecord = serde_json::from_value(stripped).unwrap();
    assert_ne!(
        stripped.checksum(),
        stripped.checksum_sha256,
        "even ignoring the field cannot authenticate the protected new receipt"
    );
    let voter = f.seeds[0]
        .database
        .read()
        .test_native_metadata_record_bytes()
        .unwrap();
    let legacy: OldNativeMetadataRecord = serde_json::from_slice(&voter).unwrap();
    assert_eq!(legacy.checksum(), legacy.checksum_sha256);
    assert_eq!(
        serde_json::to_vec(&legacy).unwrap(),
        voter,
        "None must preserve actual old voter bytes"
    );
    let id = start_cold(&f, &mut host).unwrap();
    assert_eq!(protected_id(host.test_factory()), (id, true));
    host.stop(&CancellationToken::uncancelable()).unwrap();
    drop(host);
    f.shutdown();
}
