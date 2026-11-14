//! Test-only bounded copied facts. Never retain an error, ACK, authority or
//! guard, and never invoke Display/formatting on the normal acquisition path.
use super::*;
const MAX_FACTS: usize = 64;
struct Prefix<const N: usize> {
    bytes: [u8; N],
    len: usize,
    original_len: usize,
}
impl<const N: usize> Prefix<N> {
    fn copy(bytes: &[u8]) -> Self {
        let len = bytes.len().min(N);
        let mut prefix = Self {
            bytes: [0; N],
            len,
            original_len: bytes.len(),
        };
        prefix.bytes[..len].copy_from_slice(&bytes[..len]);
        prefix
    }
    fn json(&self) -> serde_json::Value {
        serde_json::json!({"copied_bytes":&self.bytes[..self.len],
            "utf8_lossy":String::from_utf8_lossy(&self.bytes[..self.len]),
            "original_bytes":self.original_len,"truncated":self.len<self.original_len})
    }
}
enum Outcome {
    Ack {
        accepted: bool,
        context: uuid::Uuid,
        request: uuid::Uuid,
        success: bool,
        matched: u64,
        conflict: u64,
        term: u64,
        applied: u64,
        read_index: u64,
        reply_peer: Prefix<128>,
    },
    Error {
        variant: std::mem::Discriminant<BicDbError>,
        name: &'static str,
        detail: Option<Prefix<512>>,
        io_kind: Option<io::ErrorKind>,
        os_code: Option<i32>,
    },
}
struct Fact {
    peer: Prefix<128>,
    attempt: usize,
    dequeued_elapsed: Duration,
    recorded_elapsed: Duration,
    remaining: Duration,
    outcome: Outcome,
}
pub(super) struct ProbeDiagnostics {
    started: Instant,
    deadline: Instant,
    context: uuid::Uuid,
    request: uuid::Uuid,
    acquisition: uuid::Uuid,
    range: crate::RangeId,
    epoch: u64,
    term: u64,
    index: u64,
    facts: Vec<Fact>,
    omitted: usize,
}
impl ProbeDiagnostics {
    pub(super) fn new(context: &MetadataReadContext, request: &MetadataReadAcquireRequest) -> Self {
        Self {
            started: Instant::now(),
            deadline: context.deadline(),
            context: context.context_id(),
            request: context.read_request_id(),
            acquisition: request.acquisition_id,
            range: request.range_id,
            epoch: request.range_epoch,
            term: context.term(),
            index: context.read_index(),
            facts: Vec::with_capacity(MAX_FACTS),
            omitted: 0,
        }
    }
    fn record(
        &mut self,
        peer: &ClusterNodeId,
        attempt: usize,
        outcome: Outcome,
        dequeued_at: Instant,
    ) {
        if self.facts.len() == MAX_FACTS {
            self.omitted += 1;
            return;
        }
        let at = Instant::now();
        self.facts.push(Fact {
            peer: Prefix::copy(peer.as_str().as_bytes()),
            attempt,
            dequeued_elapsed: dequeued_at.duration_since(self.started),
            recorded_elapsed: at.duration_since(self.started),
            remaining: self.deadline.saturating_duration_since(dequeued_at),
            outcome,
        });
    }
    pub(super) fn record_ack(
        &mut self,
        peer: &ClusterNodeId,
        attempt: usize,
        ack: &MetadataReadAck,
        accepted: bool,
        dequeued_at: Instant,
    ) {
        let actual = ack.test_probe_response_data();
        self.record(
            peer,
            attempt,
            Outcome::Ack {
                accepted,
                context: actual.context_id,
                request: actual.read_request_id,
                success: actual.append.success,
                matched: actual.append.match_index,
                conflict: actual.append.conflict_index,
                term: actual.append.term,
                applied: actual.applied_index,
                read_index: actual.read_index,
                reply_peer: Prefix::copy(actual.append.node_id.as_str().as_bytes()),
            },
            dequeued_at,
        );
    }
    pub(super) fn record_error(
        &mut self,
        peer: &ClusterNodeId,
        attempt: usize,
        error: &BicDbError,
        dequeued_at: Instant,
    ) {
        // Copy already-existing bytes; never call an arbitrary io::Error's
        // custom source/Display or extend its lifetime to obtain a message.
        let (name, bytes, kind, os) = match error {
            BicDbError::Cluster(s) => ("Cluster", Some(s.as_bytes()), None, None),
            BicDbError::Consensus(s) => ("Consensus", Some(s.as_bytes()), None, None),
            BicDbError::Replication(s) => ("Replication", Some(s.as_bytes()), None, None),
            BicDbError::HighAvailability(s) => ("HighAvailability", Some(s.as_bytes()), None, None),
            BicDbError::ResourceGovernance(s) => {
                ("ResourceGovernance", Some(s.as_bytes()), None, None)
            }
            BicDbError::WriteFailed { detail, .. } => {
                ("WriteFailed", Some(detail.as_bytes()), None, None)
            }
            BicDbError::CommitOutcomeUnknown { detail, .. } => {
                ("CommitOutcomeUnknown", Some(detail.as_bytes()), None, None)
            }
            BicDbError::RpcEvidenceFailure { detail, .. } => {
                ("RpcEvidenceFailure", Some(detail.as_bytes()), None, None)
            }
            BicDbError::Io(e) => ("Io", None, Some(e.kind()), e.raw_os_error()),
            BicDbError::Json(_) => ("Json", None, None, None),
            BicDbError::QueryTimedOut => ("QueryTimedOut", None, None, None),
            BicDbError::QueryCanceled => ("QueryCanceled", None, None, None),
            _ => ("Other; actual discriminant retained", None, None, None),
        };
        self.record(
            peer,
            attempt,
            Outcome::Error {
                variant: std::mem::discriminant(error),
                name,
                detail: bytes.map(Prefix::copy),
                io_kind: kind,
                os_code: os,
            },
            dequeued_at,
        );
    }
    pub(super) fn report_exhausted(&self) {
        // Called only at the existing final candidates-exhausted refusal,
        // after the loop's consensus guard has gone out of scope.
        let facts: Vec<_> = self.facts.iter().map(|fact| {
            let outcome = match &fact.outcome {
                Outcome::Ack { accepted, context, request, success, matched, conflict, term, applied, read_index, reply_peer } =>
                    serde_json::json!({"kind":"ack","accepted":accepted,"ack_context":context,
                        "ack_request":request,"append_success":success,"match_index":matched,
                        "conflict_index":conflict,"term":term,"applied_index":applied,
                        "reply_read_index":read_index,"reply_peer":reply_peer.json()}),
                Outcome::Error { variant, name, detail, io_kind, os_code } =>
                    serde_json::json!({"kind":"error","variant":name,"actual_discriminant":format!("{variant:?}"),
                        "copied_existing_detail":detail.as_ref().map(Prefix::json),
                        "io_kind":io_kind.map(|kind|format!("{kind:?}")),"os_code":os_code,
                        "detail_copy_available":detail.is_some(),"original_error_or_source_retained":false,
                        "full_error_display_invoked":false}),
            };
            serde_json::json!({"actual_peer":fact.peer.json(),"attempt":fact.attempt,
                "result_dequeued_elapsed_us":fact.dequeued_elapsed.as_micros(),
                "facts_recorded_elapsed_us":fact.recorded_elapsed.as_micros(),
                "context_remaining_us_at_dequeue":fact.remaining.as_micros(),"outcome":outcome})
        }).collect();
        let report = serde_json::json!({"context":self.context,"read_request":self.request,
            "acquisition":self.acquisition,"range_id":self.range,"range_epoch":self.epoch,
            "term":self.term,"read_index":self.index,"facts":facts,"omitted_facts":self.omitted,
            "fact_limit":MAX_FACTS,"peer_byte_limit":128,"message_byte_limit":512,
            "elapsed_origin":"after real context creation, before probe workers",
            "dequeue_marker":"after original candidate removal, before original ACK validation",
            "trigger":"existing candidates exhausted without fresh quorum",
            "scope":"copied test diagnostics; dequeue times are not network completion times"});
        let _ = std::io::Write::write_fmt(
            &mut std::io::stderr().lock(),
            format_args!("METADATA_PROBE_EXHAUSTED_FACTS {report}\n"),
        );
    }
}
