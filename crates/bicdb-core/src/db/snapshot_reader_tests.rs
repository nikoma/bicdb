use super::*;
use ed25519_dalek::{Signer, SigningKey};

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default().with_fsync(true).with_storage_mode(mode)
}

fn all_keys() -> RangeDescriptor {
    let node = crate::ClusterNodeId::new("source").unwrap();
    RangeDescriptor {
        id: crate::RangeId::new(1).unwrap(),
        start_token: 0,
        end_token: None,
        epoch: 1,
        replicas: vec![crate::RangeReplica {
            id: crate::ReplicaId::new(1).unwrap(),
            node_id: node.clone(),
            role: crate::RangeReplicaRole::Voter,
        }],
        leader: node,
        approximate_bytes: 0,
        approximate_qps: 0,
        placement: crate::PlacementPolicy::default(),
    }
}

#[test]
fn shared_snapshot_apply_installs_capabilities_and_invalidates_readiness_once() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let source_path = root.path().join("source");
        let mut source =
            BicDb::open_with_config(&source_path, config(StorageMode::ServerPaged)).unwrap();
        source.enable_sql_rename_intents().unwrap();
        source.create_collection("business").unwrap();
        source
            .insert("business", Record::new("acknowledged"))
            .unwrap();
        source.close().unwrap();
        let source =
            BicDb::open_with_config(&source_path, config(StorageMode::ServerPaged)).unwrap();
        let target_path = root.path().join("target");
        let mut target = BicDb::open_with_config(&target_path, config(mode)).unwrap();
        target.create_collection("business").unwrap();
        target.schema_compatibility_fingerprint().unwrap();
        assert!(target.schema_compatibility.verified_sha256().is_some());
        let range = all_keys();
        source
            .for_each_range_snapshot_batch(
                "business",
                &range,
                None,
                &RangeSnapshotOptions::default(),
                |batch| {
                    target.apply_range_snapshot_batch_preinstalled(
                        &range,
                        &CollectionMeta::standard("business"),
                        &batch,
                    )?;
                    assert_eq!(
                        format::required_reader_features(&target_path)?,
                        vec![format::SQL_RENAME_INTENTS_FEATURE_FLAG.to_string()]
                    );
                    assert!(target.schema_compatibility.verified_sha256().is_none());
                    let generation = target.schema_compatibility.generation();
                    target.apply_range_snapshot_batch_preinstalled(
                        &range,
                        &CollectionMeta::standard("business"),
                        &batch,
                    )?;
                    assert_eq!(target.schema_compatibility.generation(), generation);
                    Ok(true)
                },
            )
            .unwrap();
        assert!(target.get("business", "acknowledged").unwrap().is_some());
        target.close().unwrap();
        source.close().unwrap();
    }
}

#[test]
fn unprotected_schema_bundle_keeps_the_frozen_v1_checksum_payload() {
    #[derive(Serialize)]
    struct LegacyPayload<'a> {
        format_version: u32,
        fingerprint: &'a SchemaCompatibilityFingerprint,
        collections: &'a [CollectionMeta],
        indexes: &'a [IndexDefinition],
        structural_records: &'a [ClusterSchemaCatalogRecord],
    }
    let root = tempfile::tempdir().unwrap();
    let mut source = BicDb::open(root.path()).unwrap();
    source.create_collection("business").unwrap();
    let bundle = source.cluster_schema_bundle().unwrap();
    assert_eq!(bundle.format_version, 1);
    let original = serde_json::to_vec(&LegacyPayload {
        format_version: 1,
        fingerprint: &bundle.fingerprint,
        collections: &bundle.collections,
        indexes: &bundle.indexes,
        structural_records: &bundle.structural_records,
    })
    .unwrap();
    assert_eq!(
        hex::encode(sha2::Sha256::digest(original)),
        bundle.checksum_sha256
    );
    source.close().unwrap();
}

#[test]
fn signed_activation_installs_capabilities_before_schema_and_resumes_after_reopen() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut source = BicDb::open(root.path().join("source")).unwrap();
        source.enable_sql_catalog_intents().unwrap();
        source.enable_sql_rename_intents().unwrap();
        source.create_collection("business").unwrap();
        let bundle = source.cluster_schema_bundle().unwrap();
        let key = SigningKey::from_bytes(&[39; 32]);
        let signature = key.sign(&bundle.signing_message().unwrap());
        let trusted = BTreeMap::from([("reader-control".into(), key.verifying_key().to_bytes())]);
        let signed = SignedClusterSchemaBundle {
            format_version: SIGNED_CLUSTER_SCHEMA_BUNDLE_FORMAT_VERSION,
            bundle: bundle.clone(),
            signer_key_id: "reader-control".into(),
            signature_ed25519_hex: hex::encode(signature.to_bytes()),
        };
        let target_path = root.path().join("target");
        let mut target = BicDb::open_with_config(&target_path, config(mode.clone())).unwrap();
        let limits = ClusterSchemaActivationLimits::default();
        target
            .stage_signed_cluster_schema_bundle(signed, &trusted, limits.stage, 1)
            .unwrap();
        assert!(format::required_reader_features(&target_path)
            .unwrap()
            .is_empty());
        target
            .begin_cluster_schema_activation(&trusted, limits, 2)
            .unwrap();
        target
            .advance_cluster_schema_activation(&trusted, limits, 3)
            .unwrap();
        assert_eq!(
            format::required_reader_features(&target_path).unwrap(),
            bundle.required_reader_features
        );
        target.close().unwrap();
        let mut target = BicDb::open_with_config(&target_path, config(mode)).unwrap();
        let result = target
            .advance_cluster_schema_activation(&trusted, limits, 4)
            .unwrap();
        assert_eq!(result.state.phase, ClusterSchemaActivationPhase::Complete);
        assert_eq!(
            target.schema_compatibility_fingerprint().unwrap(),
            bundle.fingerprint
        );
        target.close().unwrap();
        source.close().unwrap();
    }
}
