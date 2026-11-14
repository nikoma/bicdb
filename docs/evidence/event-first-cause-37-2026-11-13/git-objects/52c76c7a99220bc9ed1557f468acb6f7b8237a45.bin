//! Actual local legacy mTLS Ping/evidence controls; no business commit or HA proof.
use super::*;

struct CancelAndFail {
    journal: Arc<Journal>,
    parent: CancellationToken,
    phase: &'static str,
    cause: &'static str,
}
impl ClusterRpcEvidenceObserver for CancelAndFail {
    fn record(&self, record: &ClusterRpcEvidence) -> io::Result<()> {
        self.journal.record(record)?;
        if record.phase == self.phase {
            self.parent.cancel();
            return Err(io::Error::other(self.cause));
        }
        Ok(())
    }
}

#[test]
fn actual_rpc_evidence_first_cause_wins_over_original_cancellation_without_admitting_success() {
    for (phase, cause, dispatched) in [
        (
            "invoked",
            "actual invoked callback failure before dispatch",
            false,
        ),
        (
            "response_frame_before_decode",
            "actual response callback failure before original cancellation override",
            true,
        ),
    ] {
        let fixture = Fixture::new();
        let parent = CancellationToken::uncancelable().child();
        let sink = Arc::new(CancelAndFail {
            journal: Journal::new(&fixture.root.path().join("priority.jsonl"), None, false),
            parent: parent.clone(),
            phase,
            cause,
        });
        let transport = fixture
            .transport()
            .with_rpc_evidence_observer(sink, ClusterRpcEvidenceLimits::default())
            .unwrap();
        let result =
            transport.call_cancellable(&parent, &fixture.config.node_id, ClusterDataRequest::Ping);
        let next_id = transport.next_request_id.load(Ordering::SeqCst);
        let fenced = transport.require_rpc_evidence_current();
        let records = fixture.records("priority.jsonl");
        eprintln!(
            "actual_rpc_evidence_first_cause_priority {}",
            serde_json::json!({
                "phase":phase,"result":format!("{:?}",result),"future_fence":format!("{:?}",fenced),
                "parent_canceled":parent.is_cancel_requested(),"next_request_id":next_id,"records":records,
            })
        );
        assert!(parent.is_cancel_requested());
        assert!(
            matches!(&result,Err(BicDbError::RpcEvidenceFailure{may_have_dispatched,detail})
            if *may_have_dispatched==dispatched && detail==cause),
            "{result:?}"
        );
        assert!(
            matches!(&fenced,Err(BicDbError::RpcEvidenceFailure{may_have_dispatched,detail})
            if *may_have_dispatched==dispatched && detail==cause),
            "{fenced:?}"
        );
        assert_eq!(next_id, if dispatched { 2 } else { 1 });
        assert!(transport
            .call(&fixture.config.node_id, ClusterDataRequest::Ping)
            .is_err());
        assert_eq!(
            transport.next_request_id.load(Ordering::SeqCst),
            next_id,
            "fenced call cannot allocate a new actual request"
        );
        if dispatched {
            let response = records
                .iter()
                .find(|r| r["phase"] == "response_frame_before_decode")
                .unwrap();
            let data = frame(response);
            let decoded: ClusterDataResponseEnvelope = serde_json::from_slice(&data[4..]).unwrap();
            assert!(
                matches!(decoded.response, Ok(ClusterDataResponse::Pong)),
                "actual Pong was retained but not admitted as success"
            );
        } else {
            assert!(records.iter().all(|r| r["may_have_dispatched"] == false));
        }
    }
}

struct ConcurrentCause {
    journal: Arc<Journal>,
    reply_entered: std::sync::mpsc::Sender<()>,
    release_reply: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}
const FIRST_CAUSE: &str = "actual second-request callback failure before dispatch";
impl ClusterRpcEvidenceObserver for ConcurrentCause {
    fn record(&self, record: &ClusterRpcEvidence) -> io::Result<()> {
        self.journal.record(record)?;
        if record.request_id == Some(1) && record.phase == "response_frame_before_decode" {
            self.reply_entered.send(()).unwrap();
            self.release_reply
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
        }
        if record.request_id == Some(2) && record.phase == "request_frame_before_dispatch" {
            return Err(io::Error::other(FIRST_CAUSE));
        }
        Ok(())
    }
}

#[test]
fn actual_rpc_evidence_first_cause_survives_concurrent_fence_before_primary_worker_report() {
    let fixture = Fixture::new();
    let (reply_entered, wait_reply) = std::sync::mpsc::channel();
    let (release_reply, wait_release) = std::sync::mpsc::channel();
    let sink = Arc::new(ConcurrentCause {
        journal: Journal::new(
            &fixture.root.path().join("concurrent-cause.jsonl"),
            None,
            false,
        ),
        reply_entered,
        release_reply: std::sync::Mutex::new(wait_release),
    });
    let transport = Arc::new(
        fixture
            .transport()
            .with_rpc_evidence_observer(sink, ClusterRpcEvidenceLimits::default())
            .unwrap(),
    );
    let node = fixture.config.node_id.clone();
    let first_transport = transport.clone();
    let first = std::thread::spawn(move || first_transport.call(&node, ClusterDataRequest::Ping));
    wait_reply.recv_timeout(Duration::from_secs(2)).unwrap();
    let (primary_ready, wait_primary) = std::sync::mpsc::channel();
    let (report_primary, wait_report) = std::sync::mpsc::channel();
    let second_transport = transport.clone();
    let node = fixture.config.node_id.clone();
    let second = std::thread::spawn(move || {
        let result = second_transport.call(&node, ClusterDataRequest::Ping);
        // Hold the actual original error on its producer thread. Other readers
        // must retain its cause even before this worker reports that outcome.
        primary_ready.send(()).unwrap();
        wait_report.recv_timeout(Duration::from_secs(2)).unwrap();
        result
    });
    wait_primary.recv_timeout(Duration::from_secs(2)).unwrap();
    let before_dispatched_peer = transport.require_rpc_evidence_current();
    release_reply.send(()).unwrap();
    let first_result = first.join().unwrap();
    let after_dispatched_peer = transport.require_rpc_evidence_current();
    report_primary.send(()).unwrap();
    let primary = second.join().unwrap();
    let records = fixture.records("concurrent-cause.jsonl");
    eprintln!(
        "actual_rpc_evidence_concurrent_first_cause {}",
        serde_json::json!({
            "primary":format!("{:?}",primary),"before_peer":format!("{:?}",before_dispatched_peer),
            "peer":format!("{:?}",first_result),"after_peer":format!("{:?}",after_dispatched_peer),"records":records,
        })
    );
    for result in [&primary, &before_dispatched_peer] {
        assert!(
            matches!(result,Err(BicDbError::RpcEvidenceFailure{may_have_dispatched:false,detail}) if detail==FIRST_CAUSE),
            "{result:?}"
        );
    }
    for result in [&first_result, &after_dispatched_peer] {
        assert!(
            matches!(result,Err(BicDbError::RpcEvidenceFailure{may_have_dispatched:true,detail}) if detail==FIRST_CAUSE),
            "{result:?}"
        );
    }
    assert!(records.iter().any(|r| r["request_id"] == 1
        && r["phase"] == "response_frame_before_decode"
        && r["may_have_dispatched"] == true));
    assert!(records
        .iter()
        .filter(|r| r["request_id"] == 2)
        .all(|r| r["may_have_dispatched"] == false));
    assert_eq!(transport.next_request_id.load(Ordering::SeqCst), 3);
}

struct UnwindCause {
    journal: Arc<Journal>,
}
impl ClusterRpcEvidenceObserver for UnwindCause {
    fn record(&self, record: &ClusterRpcEvidence) -> io::Result<()> {
        self.journal.record(record)?;
        panic!("unit observer callback unwinds after actual journal persistence");
    }
}
#[test]
fn actual_rpc_evidence_first_cause_unwind_publishes_cause_before_future_fence() {
    let fixture = Fixture::new();
    let sink = Arc::new(UnwindCause {
        journal: Journal::new(&fixture.root.path().join("unwind-cause.jsonl"), None, false),
    });
    let transport = fixture
        .transport()
        .with_rpc_evidence_observer(sink, ClusterRpcEvidenceLimits::default())
        .unwrap();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        transport.call(&fixture.config.node_id, ClusterDataRequest::Ping)
    }));
    let fenced = transport.require_rpc_evidence_current();
    eprintln!(
        "actual_rpc_evidence_unwind_first_cause {}",
        serde_json::json!({
            "caught_unwind":outcome.is_err(),"future_fence":format!("{:?}",fenced),
            "next_request_id":transport.next_request_id.load(Ordering::SeqCst),
        })
    );
    assert!(outcome.is_err());
    assert!(
        matches!(fenced,Err(BicDbError::RpcEvidenceFailure{may_have_dispatched:false,detail})
        if detail=="evidence sink callback unwound; observed dispatch is fenced")
    );
    assert_eq!(transport.next_request_id.load(Ordering::SeqCst), 1);
}
