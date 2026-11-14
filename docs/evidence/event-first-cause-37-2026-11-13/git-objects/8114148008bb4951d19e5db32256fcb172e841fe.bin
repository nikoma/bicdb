//! Typed in-process completion channel controls, not native commit evidence.
use super::*;

#[test]
fn event_client_out_of_order_worker_unknown_survives_later_refusal_and_cleanup() {
    for wrong_kind in [false, true] {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let parent = crate::CancellationToken::uncancelable().child_with_deadline(deadline);
        let operation = ClusterEventOperation::Append {
            namespace: "completion-unit".into(),
            key: "same-key".into(),
            value: serde_json::json!({"same":true}),
        };
        let (sender, receiver) = std::sync::mpsc::channel();
        let (first_done, wait_first) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let first = sender.clone();
            scope.spawn(move || {
                // Attempt1 finishes before attempt0: actual completion order
                // differs from launch order without a fabricated transport ACK.
                first
                    .send((
                        1,
                        std::time::Instant::now(),
                        Err(crate::BicDbError::CommitOutcomeUnknown {
                            sequence: Some(71),
                            transaction_id: Some(19),
                            incarnation: "original-open".into(),
                            detail: "original uncertain write".into(),
                        }),
                    ))
                    .unwrap();
                first_done.send(()).unwrap();
            });
            scope.spawn(move || {
                wait_first.recv().unwrap();
                let later = if wrong_kind {
                    Ok(ClusterEventResponse::Value { value: None })
                } else {
                    Err(invalid("later worker no longer leads"))
                };
                sender.send((0, std::time::Instant::now(), later)).unwrap();
            });
            let mut flights = InFlight {
                replies: receiver,
                attempts: vec![(0, parent.child()), (1, parent.child())],
                completed: Vec::new(),
            };
            let mut errors = EventOperationError::default();
            assert!(matches!(
                flights
                    .wait(None, deadline, &parent, &operation, &mut errors)
                    .unwrap(),
                Waited::Pending
            ));
            flights.hang_up(&operation, &mut errors);
            assert_eq!(flights.completed.len(), 2);
            assert_eq!(
                flights
                    .completed
                    .iter()
                    .map(|c| c.attempt)
                    .collect::<Vec<_>>(),
                vec![1, 0]
            );
            assert!(flights.completed.iter().all(|c| c.completed_at < deadline));
            assert!(matches!(
                &flights.completed[0].outcome,
                CompletedOutcome::Error(0)
            ));
            if wrong_kind {
                assert!(matches!(
                    &flights.completed[1].outcome,
                    CompletedOutcome::RejectedResponse {
                        response: ClusterEventResponse::Value { value: None },
                        error: 1
                    }
                ));
            } else {
                assert!(matches!(
                    &flights.completed[1].outcome,
                    CompletedOutcome::Error(1)
                ));
            }
            assert_eq!(
                errors.errors.len(),
                2,
                "both actual typed errors survive until final selection"
            );
            let crate::BicDbError::CommitOutcomeUnknown {
                sequence,
                transaction_id,
                incarnation,
                detail,
            } = errors.take_or(crate::BicDbError::QueryTimedOut)
            else {
                panic!("refusal or wrong-kind response cannot resolve uncertain append")
            };
            assert_eq!(sequence, Some(71));
            assert_eq!(transaction_id, Some(19));
            assert_eq!(incarnation, "original-open");
            assert_eq!(detail, "original uncertain write");
        });
    }
}

#[test]
fn event_client_typed_same_operation_response_keeps_all_worker_outcomes_until_cleanup() {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let parent = crate::CancellationToken::uncancelable().child_with_deadline(deadline);
    let operation = ClusterEventOperation::Append {
        namespace: "completion-unit".into(),
        key: "same".into(),
        value: serde_json::json!({}),
    };
    let (sender, receiver) = std::sync::mpsc::channel();
    sender
        .send((
            0,
            std::time::Instant::now(),
            Ok(ClusterEventResponse::Appended { inserted: true }),
        ))
        .unwrap();
    sender
        .send((1, std::time::Instant::now(), Err(invalid("sibling ended"))))
        .unwrap();
    drop(sender);
    let mut flights = InFlight {
        replies: receiver,
        attempts: vec![(0, parent.child()), (1, parent.child())],
        completed: Vec::new(),
    };
    let mut errors = EventOperationError::default();
    let Waited::Answered(index) = flights
        .wait(None, deadline, &parent, &operation, &mut errors)
        .unwrap()
    else {
        panic!("same-operation typed completion must be retained")
    };
    flights.hang_up(&operation, &mut errors);
    assert_eq!(flights.completed.len(), 2);
    assert!(matches!(
        &flights.completed[0].outcome,
        CompletedOutcome::Response(ClusterEventResponse::Appended { inserted: true })
    ));
    assert!(matches!(
        &flights.completed[1].outcome,
        CompletedOutcome::Error(0)
    ));
    assert!(matches!(
        flights.take_response(index).unwrap(),
        ClusterEventResponse::Appended { inserted: true }
    ));
}
