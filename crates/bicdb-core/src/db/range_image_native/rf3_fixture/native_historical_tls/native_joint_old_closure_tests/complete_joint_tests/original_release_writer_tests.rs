//! ORIGINAL-TTL release controls. The replacement policy comes from actual
//! Host MQ after genuine all-final native publication; request expiry is NOT
//! accepted as evidence of the writer-registration refusal.
use super::*;
use crate::distribution_native_writer::SealedHostWriterEnrollment;
use crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt;

#[derive(Clone, Copy)]
pub(super) enum OriginalReleaseControl {
    ImageTarget,
    SurvivingVoter,
}
impl OriginalReleaseControl {
    fn node(self) -> ClusterNodeId {
        match self {
            Self::ImageTarget => node("n3"),
            Self::SurvivingVoter => node("n1"),
        }
    }
}
pub(super) struct OriginalReleaseAttempt {
    control: OriginalReleaseControl,
    policy: SealedHostWriterEnrollment,
    receipt: Arc<NativeJointCatalogueActivationReceipt>,
    image_owner: Option<crate::distribution_data::NativeJointImageReleaseOwner>,
}

#[test]
fn native_joint_original_image_release_refuses_hot_writer_reinstallation_before_owner_drop() {
    assert_eq!(
        run_complete_joint_with_original_release_control(
            0,
            Some(OriginalReleaseControl::ImageTarget)
        ),
        JointTestOutcome::OriginalReleaseWriterRejected,
    );
}
#[test]
fn native_joint_original_survivor_release_refuses_hot_writer_reinstallation_before_owner_drop() {
    assert_eq!(
        run_complete_joint_with_original_release_control(
            0,
            Some(OriginalReleaseControl::SurvivingVoter)
        ),
        JointTestOutcome::OriginalReleaseWriterRejected,
    );
}

pub(super) fn prepare(
    fixture: &EnforcedNetwork,
    published: &Arc<crate::distribution_range_handoff::NativeJointPublishedServingQuorum>,
    parent: &CancellationToken,
    control: OriginalReleaseControl,
) -> OriginalReleaseAttempt {
    let local = control.node();
    // Each attempt uses a distinct real Host MQ and complete full-member open
    // phase. Its 4s authority is never extended; pre-receipt expiry may retry
    // finitely, but a returned ORIGINAL receipt must remain live through cut.
    for attempt in 0..3 {
        let result = (|| -> crate::Result<OriginalReleaseAttempt> {
            let business = Uuid::now_v7();
            let mq = fixture.clients[&local].acquire_host_metadata_observation(
                &node("n1"),
                business,
                Duration::from_secs(5),
                parent,
            )?;
            let policy = {
                let service = fixture.native.services[&local].lock();
                let database = fixture.native.databases[&local].read();
                service.prepare_native_host_writer_enrollment(&database, mq, business)?
            };
            let mut round = published.begin_fresh_open_phase(Duration::from_secs(4))?;
            for member in [node("n1"), node("n2"), node("n3")] {
                round.acquire_actual_member(&fixture.clients[&member], &node("n1"))?;
            }
            let round = Arc::new(round.finish()?);
            let open = round.acquire_local_open(&fixture.clients[&local], &node("n1"))?;
            let receipt = Arc::new(open.persist_local_catalogue_activation(parent)?);
            let image_owner = match control {
                OriginalReleaseControl::ImageTarget => {
                    Some(open.retain_image_release_owner(receipt.clone())?)
                }
                OriginalReleaseControl::SurvivingVoter => None,
            };
            receipt.require_live()?;
            Ok(OriginalReleaseAttempt {
                control,
                policy,
                receipt,
                image_owner,
            })
        })();
        match result {
            Ok(release) => return release,
            Err(crate::BicDbError::QueryTimedOut) if attempt < 2 => {
                eprintln!("ACTUAL_JOINT_ORIGINAL_RELEASE pre-receipt-timeout node={local} attempt={attempt}");
            }
            Err(error) => {
                panic!("actual original-release prerequisite failed node={local}: {error}")
            }
        }
    }
    unreachable!("finite genuine prerequisite attempts")
}

pub(super) fn reject_after_reenrollment(
    fixture: &EnforcedNetwork,
    runtimes: &BTreeMap<ClusterNodeId, Arc<NativeJointHandoffRuntime>>,
    parent: &CancellationToken,
    range: crate::RangeId,
    key: &str,
    acknowledged: &crate::Record,
    mut release: OriginalReleaseAttempt,
) {
    let local = release.control.node();
    let catalogue_path = fixture.native.databases[&local]
        .read()
        .data_path()
        .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG);
    let before_catalogue = std::fs::read(&catalogue_path).unwrap();
    let before_point = {
        let database = fixture.native.databases[&local].read();
        let source = database.current_native_range_source_lineage(range).unwrap();
        (
            source.range_index(),
            source.actual_local_visibility_sequence(),
            source.history_generation(),
            source.logical_genesis_sha256().to_owned(),
            source.logical_history_sha256().to_owned(),
            source.native_receipt_sha256().to_owned(),
            database.local_namespace_identity("items").unwrap(),
        )
    };
    release
        .receipt
        .test_original_request_live()
        .expect("ORIGINAL 4s request must be live before actual reinstall");
    release.receipt.require_live().unwrap();
    let _replacement_guard = fixture.native.databases[&local]
        .read()
        .install_enrolled_native_coordinator(fixture.coordinators[&local].clone(), release.policy)
        .expect("genuine same-config fresh Host policy must actually install");
    release
        .receipt
        .test_original_request_live()
        .expect("reinstall cut must retain ORIGINAL request TTL");
    let error = match release.control {
        OriginalReleaseControl::ImageTarget => {
            release.image_owner.as_mut().unwrap().release(parent)
        }
        OriginalReleaseControl::SurvivingVoter => {
            runtimes[&local].release_activated_new_member(release.receipt.clone())
        }
    }
    .expect_err("ORIGINAL release must refuse replacement before actual owner drop/arm")
    .to_string();
    assert!(
        error.contains("writer") || error.contains("registration"),
        "must reject actual writer identity, not expiry/owner-drain: {error}"
    );
    release
        .receipt
        .test_original_request_live()
        .expect("expiry must not satisfy writer replacement negative");
    assert_eq!(std::fs::read(catalogue_path).unwrap(), before_catalogue);
    {
        let database = fixture.native.databases[&local].read();
        assert!(
            database.range_mutation_gate().try_enter(range).is_err(),
            "original actual owner must remain frozen after refusal"
        );
        let source = database.current_native_range_source_lineage(range).unwrap();
        assert_eq!(
            (
                source.range_index(),
                source.actual_local_visibility_sequence(),
                source.history_generation(),
                source.logical_genesis_sha256().to_owned(),
                source.logical_history_sha256().to_owned(),
                source.native_receipt_sha256().to_owned(),
                database.local_namespace_identity("items").unwrap(),
            ),
            before_point
        );
        assert_eq!(
            database.get("items", key).unwrap().as_ref(),
            Some(acknowledged)
        );
    }
    eprintln!("ACTUAL_JOINT_ORIGINAL_RELEASE_WRITER_REFUSED node={local} original-request-still-live=true actual-fresh-host-policy-installed=true exact-catalogue-native-point-UID-ACK-preserved=true still-frozen=true error={error}");
}
