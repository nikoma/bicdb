//! Error-selection unit tests; no storage, replication or HA qualification.
use super::*;

#[test]
fn event_client_unknown_write_survives_later_refusal_and_refresh_or_deadline() {
    for fallback in [
        invalid("discovery failed after write"),
        crate::BicDbError::QueryTimedOut,
    ] {
        let mut retained = EventOperationError::default();
        retained.write_failed(crate::BicDbError::CommitOutcomeUnknown {
            sequence: Some(71),
            transaction_id: Some(19),
            incarnation: "original-database-open".into(),
            detail: "original commit wait".into(),
        });
        retained.write_failed(invalid("later actual route is no longer leader"));
        let crate::BicDbError::CommitOutcomeUnknown {
            sequence,
            transaction_id,
            incarnation,
            detail,
        } = retained.take_or(fallback)
        else {
            panic!("later route/discovery refusal must not assert rollback of the earlier unknown write");
        };
        assert_eq!(sequence, Some(71));
        assert_eq!(transaction_id, Some(19));
        assert_eq!(incarnation, "original-database-open");
        assert_eq!(detail, "original commit wait");
    }
}
#[test]
fn event_client_no_operation_keeps_discovery_error() {
    let mut retained = EventOperationError::default();
    assert_eq!(
        retained
            .take_or(invalid("no event cluster member is reachable"))
            .to_string(),
        invalid("no event cluster member is reachable").to_string()
    );
}
