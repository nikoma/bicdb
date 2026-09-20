//! BicDB core library.
//!
//! v0.1 is intentionally small: durable append-only local storage, collections,
//! JSON metadata, optional f32 vectors, exact vector search, time-series scans,
//! and a durable pending sync log.

mod backup;
mod broker;
mod cancellation;
pub mod consensus;
mod db;
pub mod distribution;
mod distribution_anti_entropy;
mod distribution_anti_entropy_auto;
mod distribution_anti_entropy_repair;
mod distribution_anti_entropy_repair_certificate;
mod distribution_anti_entropy_repair_run;
mod distribution_anti_entropy_run;
mod distribution_anti_entropy_scheduler;
mod distribution_backup;
mod distribution_backup_run;
mod distribution_certification;
mod distribution_consensus;
mod distribution_data;
mod distribution_gateway;
mod distribution_query;
mod distribution_range_consensus;
mod distribution_restore;
mod distribution_restore_activation;
mod distribution_routing;
mod distribution_schema_rollout;
mod distribution_supervisor;
#[cfg(feature = "tls-replication")]
mod distribution_transport;
mod encryption;
mod encryption_rotation;
mod error;
mod event;
mod format;
mod fts_build;
mod fts_cache;
mod fts_filters;
mod fts_format;
mod fts_postings;
mod fts_scoring;
mod fts_segment;
mod geometry;
mod graph;
mod hnsw;
mod large_value;
mod memory;
mod memory_index;
mod mutation;
mod numeric_key;
mod observability;
mod paged_checkpoint_maintenance;
mod paged_collection;
mod paged_integrity_maintenance;
mod paged_maintenance;
pub mod parse_budget;
mod protected_data;
pub mod query_exec;
mod record;
pub mod replication;
mod residency;
mod resource_governor;
// TCP/TLS replication transport; not built for wasm cache builds
// (no sockets there), and it carries the rustls dependency.
pub mod aggregate_projection;
pub mod aggregate_sketch;
#[cfg(feature = "tls-replication")]
pub mod replication_transport;
mod snapshot;
mod spatial_pack_build;
mod spatial_packed;
mod storage;

pub mod storage_mode_migration;
mod sync;
mod sync_mesh;
mod vector;

pub use backup::{
    apply_archived_wal, create_backup, create_backup_from_open_database, create_backup_to_writer,
    create_online_paged_base_backup, create_wal_tail_backup, drill_backup_restore,
    drill_backup_restore_with_limits, prune_archived_wal, restore_backup, restore_backup_to_point,
    restore_backup_to_point_with_limits, verify_backup, verify_backup_chain,
    verify_backup_from_reader, wal_floor_of_base, BackupChainVerifyReport, BackupCreateOptions,
    BackupCreateReport, BackupDrillReport, BackupPitrReplayLimits, BackupPointInTimeRestoreOptions,
    BackupRestoreOptions, BackupRestoreReport, BackupVerifyReport, WalPruneReport,
};
pub use bicdb_page::{
    BTreeNodeViolation, BTreePageExpectation, BTreeVerifyCursor, BTreeVerifyFault,
    BTreeVerifyLimits, BTreeVerifyStepReport, BTreeVerifyStopReason, PageClassIoSnapshot,
    PageIoLatencySnapshot, PageIoSnapshot, PageTypeIoSnapshot, PagedCheckpointCursor,
    PagedCheckpointLimits, PagedCheckpointPhase, PagedCheckpointStepReport,
    PagedCheckpointStopReason, PagedIntegrityReport, PagedStoreSnapshot, ReadAheadLimits,
    ReadAheadStepReport, ReadAheadStopReason, ReadAheadSubmitReport, RecoveryReport,
    TailReclaimLimits, TailReclaimReport, TupleLocator, VacuumCursor, VacuumLimits, VacuumReport,
    VacuumStopReason, VersionChainFaultSample, VersionChainInspection, VersionChainRepairReport,
    VersionChainTerminal, VersionChainVerifyCursor, VersionChainVerifyLimits,
    VersionChainVerifyReport, VersionChainVerifyStepReport, VersionChainVerifyStopReason,
    VersionChainVersion, WalSnapshot, WritebackCursor, WritebackLimits, WritebackStepReport,
    WritebackStopReason, MAX_BTREE_VERIFY_CURSOR_KEY_BYTES,
    MAX_BTREE_VERIFY_DURATION_MILLIS_PER_STEP, MAX_BTREE_VERIFY_ENTRIES_PER_STEP,
    MAX_BTREE_VERIFY_HEIGHT, MAX_BTREE_VERIFY_KEY_BYTES_PER_STEP,
    MAX_BTREE_VERIFY_LEAF_PAGES_PER_STEP, MAX_BTREE_VERIFY_PAGE_BYTES_PER_STEP,
    MAX_CHECKPOINT_FREEZE_XIDS_PER_STEP, MAX_READ_AHEAD_CANDIDATES_PER_STEP,
    MAX_READ_AHEAD_DURATION_MILLIS_PER_STEP, MAX_READ_AHEAD_IO_BYTES_PER_STEP,
    MAX_READ_AHEAD_QUEUE_PAGES, MAX_TAIL_RECLAIM_IO_BYTES, MAX_TAIL_RECLAIM_PAGE_VISITS,
    MAX_VERSION_VERIFY_BYTES_PER_STEP, MAX_VERSION_VERIFY_CURSOR_KEY_BYTES,
    MAX_VERSION_VERIFY_DURATION_MILLIS_PER_STEP, MAX_VERSION_VERIFY_FAULT_SAMPLES,
    MAX_VERSION_VERIFY_FAULT_SAMPLE_BYTES, MAX_VERSION_VERIFY_KEYS_PER_STEP,
    MAX_VERSION_VERIFY_VERSIONS_PER_STEP, MAX_WAL_RECORD_BYTES,
    MAX_WRITEBACK_DURATION_MILLIS_PER_STEP, MAX_WRITEBACK_IO_BYTES_PER_STEP,
    MAX_WRITEBACK_PAGES_PER_STEP, MIN_PAGE_SIZE, PAGED_STORE_SNAPSHOT_FORMAT_VERSION,
    PAGE_IO_LATENCY_BUCKET_COUNT, PAGE_IO_LATENCY_BUCKET_UPPER_BOUNDS_NANOS, VERSION_HEADER_BYTES,
};
pub use broker::{
    Broker, BrokerAccess, BrokerCaller, BrokerMessage, BrokerStats, ConsumeOptions,
    ConsumerGroupInfo, DrainOptions, DrainReport, GroupStats, NackOptions, PeekedMessage,
    PublishOptions, PublishReceipt, QueueConfig, QueueStats, TrimReport, DEAD_LETTER_MESSAGE_EVENT,
    DEFAULT_MAX_ATTEMPTS,
};
pub use cancellation::CancellationToken;
pub use consensus::{
    AppendEntries, AppendResponse, ConsensusConfig, ConsensusLogEntry, ConsensusPeer,
    ConsensusRole, ConsensusState, ConsensusStatus, RequestVote, VoteResponse,
};
pub use db::effective_fts_partitions;
pub use db::AuthenticationStrength;
pub use db::FullTextBuildStep;
pub use db::FullTextStoredTextPage;
pub use db::{
    analyze_sample_limit, estimate_distinct_from_sample, scale_sample_count, AnalyzeSampler,
};
pub use db::{
    decode_fts_posting_payload, decode_fts_posting_payload_into, fts_impact_bucket,
    fts_impact_bucket_upper_edge, fts_rank_conjunctive, fts_rank_single_term,
    full_text_oversized_terms_skipped, full_text_term_is_indexable, paged_tid_hint_stats,
    parse_repair_decimal, BicDb, BlockGate, BypassPolicy, ClusterSchemaActivationAdvance,
    ClusterSchemaActivationLimits, ClusterSchemaActivationPhase, ClusterSchemaActivationState,
    ClusterSchemaBundle, ClusterSchemaCatalogRecord, ClusterSchemaCompatibilityWindow,
    ClusterSchemaFinalizationState, ClusterSchemaOnlinePlan, ClusterSchemaStageLimits,
    ClusterSchemaStageState, CollectionCompactionReport, CollectionStats, ColumnStatistics,
    CompactionOptions, CompactionReport, DbConfig, DbSnapshot, DbStats, EventHorizonReport,
    FtsQueryBudget, FtsQueryLimits, FullTextBuildLifecycleState, FullTextBuildLifecycleStatus,
    FullTextBuildProgress, FullTextBuildRecommendedAction, FullTextBuildReconcileOutcome,
    FullTextBuildReconcileReport, FullTextOpenMetrics, FullTextPublishedGeneration,
    FullTextReadSession, GeofenceTransition, GeofenceTransitionKind, HaApplyReport, HaRole,
    HaState, HaStatus, IncrementalCompactionAdvance, IncrementalCompactionLimits,
    IncrementalCompactionPhase, IncrementalCompactionState, IndexCatalogStats, IndexDefinition,
    IndexExclusion, IndexExclusionElement, IndexField, IndexKind, IndexLockPhaseReport,
    IndexMaintenanceAllReport, IndexMaintenanceReport, IndexPredicate,
    IndexPredicateBinaryOperator, IndexPredicateUnaryOperator, IndexPredicateValueType,
    IndexStatistics, IndexValue, IndexVerifyReport, IntegrityReport, NetworkStatistics,
    OnlineBackupReport, OptimizedRoute, OptimizedRouteStop, OsmImportBbox, OsmImportReport,
    PagedBTreeBuildAdvance, PagedBTreeBuildLimits, PagedBTreeBuildPhase, PagedBTreeBuildState,
    PagedCheckpointMaintenanceHandle, PagedReadAheadHandle, PagedStorageOpsHandle,
    PagedVacuumMaintenanceHandle, PlannerStatsCatalog, ProtectedDataBackfillReport,
    ProtectedDataBlindIndexCoverageReport, ProtectedDataCiphertextSamplingReport,
    ProtectedDataKeyRotationOptions, ProtectedDataKeyRotationReport,
    ProtectedDataSecurityEvidenceReport, RangeStatistics, RawSqlAuditReport, RecordConflict,
    RecordConflictCandidate, RecordSystemMetadata, RepairDelta, RepairPlan, RoadEdge, RoadNode,
    RoutePath, RouteProfile, RowId, SchemaCompatibilityAuthority, SchemaCompatibilityFingerprint,
    SecureBicDb, SecureOperation, SecurityContext, SignedClusterSchemaBundle, SnappedRoadNode,
    SpatialPackReport, SpatialQueryResult, TableStatistics, TenantIsolationReport, Transaction,
    TransactionId, TransactionIsolation, TransactionRollbackMark, TxLogHandle, TxState,
    TypedColumnStatistics, TypedValueFrequency, ValueFrequency, VersionedRecord, VisibleRow,
    CLUSTER_SCHEMA_ACTIVATION_FORMAT_VERSION, CLUSTER_SCHEMA_BUNDLE_FORMAT_VERSION,
    CLUSTER_SCHEMA_CATALOG_COLLECTIONS, CLUSTER_SCHEMA_FINALIZATION_FORMAT_VERSION,
    CLUSTER_SCHEMA_STAGE_FORMAT_VERSION, DEFAULT_CLUSTER_SCHEMA_ACTIVATION,
    DEFAULT_CLUSTER_SCHEMA_FINALIZATION, DEFAULT_CLUSTER_SCHEMA_STAGE, DEFAULT_COLLECTION_CATALOG,
    DEFAULT_COMPACTION_CHECKPOINT, DEFAULT_FORMAT_METADATA, DEFAULT_GRAPH_DIR, DEFAULT_HA_STATE,
    DEFAULT_INCREMENTAL_COMPACTION_STATE, DEFAULT_INDEX_CATALOG, DEFAULT_INDEX_MAINTENANCE,
    DEFAULT_PAGED_BTREE_BUILD_DIR, DEFAULT_PAGED_BUFFER_POOL_BYTES, DEFAULT_PAGED_DIR,
    DEFAULT_PAGED_PAGE_SIZE, DEFAULT_PAGED_READ_AHEAD_QUEUE_PAGES, DEFAULT_PAGED_WAL_MAX_BYTES,
    DEFAULT_PLANNER_STATS, DEFAULT_SEGMENTS_DIR, DEFAULT_TRANSACTION_LOG, DEFAULT_VECTOR_INDEX_DIR,
    INCREMENTAL_COMPACTION_FORMAT_VERSION, MAX_CLUSTER_SCHEMA_BUNDLE_BYTES,
    MAX_CLUSTER_SCHEMA_BUNDLE_RECORDS, MAX_CLUSTER_SCHEMA_SIGNER_KEY_ID_BYTES,
    MAX_FULL_TEXT_TERM_BYTES, PAGED_BTREE_BUILD_FORMAT_VERSION,
    SCHEMA_COMPATIBILITY_FORMAT_VERSION, SIGNED_CLUSTER_SCHEMA_BUNDLE_FORMAT_VERSION,
};
pub use db::{
    CommitAdmission, CommitAdmissionIntent, CommitAdmissionMutation, CommitAdmissionTicket,
};
pub use distribution::{
    build_failure_repair_plan, build_rebalance_plan, distribution_key_token,
    filter_commit_frame_for_range, load_distribution_config, provision_cluster_member_directory,
    save_distribution_config, ClusterBootstrapSnapshot, ClusterId, ClusterNetworkTransportConfig,
    ClusterNode, ClusterNodeId, ClusterNodeLifecycle, ClusterNodeLiveness, ClusterPlacementPlanner,
    ClusterTopology, DistributionConfig, DistributionStore, FailureRepairPlan, MetadataMemberRole,
    MetadataMutationToken, NodeRemovalSafety, PlacementPolicy, PlannedLeaderTransfer,
    PlannedReplicaMove, RangeAvailability, RangeDescriptor, RangeId, RangeRelocation, RangeReplica,
    RangeReplicaRole, RangeSnapshotBatch, RangeSnapshotExport, RangeSnapshotOptions,
    RebalanceOptions, RebalancePlan, RelocationId, RelocationPhase, ReplicaId, ReplicaMoveReason,
    StandardFailureDomain, TopologyChange, UnplacedRange, CLUSTER_DATA_PROTOCOL_VERSION,
    DEFAULT_CLUSTER_TOPOLOGY, DEFAULT_DISTRIBUTION_CONFIG, DISTRIBUTION_FORMAT_VERSION,
    DISTRIBUTION_HASH_VERSION, SCHEMA_BOOTSTRAP_NODE_LABEL, SCHEMA_COMPATIBILITY_NODE_LABEL,
    SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL,
};
pub use distribution_anti_entropy::{
    append_range_digest_bucket_records, calculate_range_digest_root,
    compare_range_digest_manifests, load_range_digest_state, range_digest_bucket_for_record,
    save_range_digest_state, RangeDigestBucket, RangeDigestBucketScanStep, RangeDigestDiff,
    RangeDigestLimits, RangeDigestManifest, RangeDigestState, RangeDigestTransport,
    RANGE_DIGEST_FORMAT_VERSION,
};
pub use distribution_anti_entropy_auto::{
    AutomaticRangeAntiEntropyAdvance, AutomaticRangeAntiEntropyController,
    AutomaticRangeAntiEntropyLimits, AutomaticRangeAntiEntropyState,
    RangeAntiEntropyFenceAuthority, RangeAntiEntropyFencePlan,
    AUTOMATIC_RANGE_ANTI_ENTROPY_FORMAT_VERSION, DEFAULT_AUTOMATIC_RANGE_ANTI_ENTROPY_DIR,
    DEFAULT_AUTOMATIC_RANGE_ANTI_ENTROPY_SCHEDULE, DEFAULT_AUTOMATIC_RANGE_ANTI_ENTROPY_STATE,
    RANGE_ANTI_ENTROPY_FENCE_PLAN_FORMAT_VERSION,
};
pub use distribution_anti_entropy_repair::{
    load_range_digest_repair_state, save_range_digest_repair_state, RangeDigestRepairBatch,
    RangeDigestRepairLimits, RangeDigestRepairState, RangeDigestRepairTransport,
    RANGE_DIGEST_REPAIR_FORMAT_VERSION,
};
pub use distribution_anti_entropy_repair_certificate::{
    load_range_digest_repair_certificate, save_range_digest_repair_certificate,
    RangeDigestRepairCertificate, RangeDigestRepairCertificateLimits, RangeDigestRepairedReplica,
    RangeDigestVerifiedBucketEvidence, DEFAULT_RANGE_DIGEST_REPAIR_CERTIFICATE,
    RANGE_DIGEST_REPAIR_CERTIFICATE_FORMAT_VERSION,
};
pub use distribution_anti_entropy_repair_run::{
    load_range_digest_repair_run, save_range_digest_repair_run, RangeDigestRepairRun,
    RangeDigestRepairRunAdvance, RangeDigestRepairRunLimits, RangeDigestRepairRunPhase,
    DEFAULT_RANGE_DIGEST_REPAIR_RUN_STATE, RANGE_DIGEST_REPAIR_RUN_FORMAT_VERSION,
};
pub use distribution_anti_entropy_run::{
    load_range_digest_run, save_range_digest_run, RangeDigestReplicaProgress,
    RangeDigestRootEvidence, RangeDigestRun, RangeDigestRunAdvance, RangeDigestRunLimits,
    RangeDigestRunOutcome, RangeDigestRunReport, RANGE_DIGEST_RUN_FORMAT_VERSION,
};
pub use distribution_anti_entropy_scheduler::{
    load_range_anti_entropy_schedule, save_range_anti_entropy_schedule, RangeAntiEntropySchedule,
    RangeAntiEntropyScheduleAdvance, RangeAntiEntropyScheduleLimits,
    RANGE_ANTI_ENTROPY_SCHEDULE_FORMAT_VERSION,
};
pub use distribution_backup::{
    load_cluster_backup_certificate, load_cluster_backup_plan, load_cluster_node_backup_artifact,
    save_cluster_backup_certificate, save_cluster_backup_plan, save_cluster_node_backup_artifact,
    ClusterBackupCertificate, ClusterBackupLimits, ClusterBackupPlan, ClusterNodeBackupArtifact,
    RangeBackupBarrier, CLUSTER_BACKUP_CERTIFICATE_FORMAT_VERSION,
    DEFAULT_CLUSTER_BACKUP_CERTIFICATE, DEFAULT_CLUSTER_BACKUP_PLAN,
    DEFAULT_CLUSTER_NODE_BACKUP_ARTIFACT, MAX_CLUSTER_BACKUP_CONTROL_FILE_BYTES,
};
pub use distribution_backup_run::{
    ClusterBackupRun, ClusterBackupRunLimits, ClusterBackupRunPhase, ClusterBackupRunStatus,
    CLUSTER_BACKUP_RUN_FORMAT_VERSION, DEFAULT_CLUSTER_BACKUP_RUN_DIR,
    DEFAULT_CLUSTER_BACKUP_RUN_JOURNAL, DEFAULT_CLUSTER_BACKUP_RUN_MANIFEST,
};
pub use distribution_certification::{
    capture_cluster_certification_artifact, finalize_cluster_certification_bundle,
    initialize_cluster_certification_bundle, load_cluster_certification_plan,
    load_cluster_certification_publication_manifest, load_cluster_certification_report,
    load_cluster_certification_state, record_cluster_certification_observation,
    register_cluster_certification_artifact, save_cluster_certification_plan,
    verify_cluster_certification_bundle, ClusterArtifactEvidence,
    ClusterBackgroundSaturationEvidence, ClusterCertificationArtifactKind,
    ClusterCertificationGates, ClusterCertificationMeasurements, ClusterCertificationObservation,
    ClusterCertificationPlan, ClusterCertificationPreflight, ClusterCertificationProtocolVersions,
    ClusterCertificationPublicationManifest, ClusterCertificationRawArtifactHeader,
    ClusterCertificationRawArtifactPayloadFormat, ClusterCertificationReport,
    ClusterCertificationState, ClusterCertificationVerification, ClusterExpansionEvidence,
    ClusterFailurePoint, ClusterFailureTrialEvidence, ClusterNodeBackgroundSaturationEvidence,
    ClusterNodeHardwareEvidence, ClusterNodeLossMechanism, ClusterNodeRecoveryMode,
    ClusterRestoreEvidence, ClusterScaleProfile, CLUSTER_CERTIFICATION_FORMAT_VERSION,
    CLUSTER_CERTIFICATION_PUBLICATION_FORMAT_VERSION,
    CLUSTER_CERTIFICATION_RAW_ARTIFACT_FORMAT_VERSION, CLUSTER_CERTIFICATION_STATE_FORMAT_VERSION,
    DEFAULT_CLUSTER_CERTIFICATION_MANIFEST, DEFAULT_CLUSTER_CERTIFICATION_PLAN,
    DEFAULT_CLUSTER_CERTIFICATION_REPORT, DEFAULT_CLUSTER_CERTIFICATION_STATE,
};
pub use distribution_consensus::{
    MetadataAppendRequest, MetadataAppendResponse, MetadataConsensusRole, MetadataConsensusStatus,
    MetadataConsensusStore, MetadataLogEntry, MetadataRestoreActivation, MetadataSnapshot,
    MetadataVoteRequest, MetadataVoteResponse, CLUSTER_METADATA_CONSENSUS_FORMAT_VERSION,
    DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE, METADATA_RESTORE_ACTIVATION_FORMAT_VERSION,
};
pub use distribution_data::{
    CatchUpSourceBatch, ClusterDataNodeService, ClusterSchemaActivationReceipt,
    ClusterSchemaFinalizationReceipt, ClusterSchemaStageReceipt,
    InProcessClusterRelocationTransport, RangeDigestAdvance, RangeLearnerApplyState,
    RangeLearnerStore, SnapshotSourceStep, DEFAULT_RANGE_DIGEST_REPAIR_STATE_DIR,
    DEFAULT_RANGE_DIGEST_STATE_DIR, DEFAULT_RANGE_LEARNER_APPLY_STATE,
    RANGE_LEARNER_APPLY_FORMAT_VERSION,
};
pub use distribution_gateway::{
    BoundedClusterConnectionPool, ClusterConnectionPoolStats, ClusterGatewayConfig,
    ClusterPointConnection, ClusterPointConnector, ClusterPointGateway, PointTransportReply,
};
pub use distribution_query::{
    execute_distributed_transaction, execute_scatter_gather, execute_scatter_gather_governed,
    merge_fts_top_k, merge_global_fts_statistics, merge_numeric_aggregates, merge_ordered_top_k,
    plan_point_query, plan_scatter_query, plan_shard_local_write, DistributedCommitDecision,
    DistributedCommitProtocol, DistributedFtsHit, DistributedNumericAggregate,
    DistributedOrderedRow, DistributedQueryFailurePolicy, DistributedQueryLimits,
    DistributedQueryPlan, DistributedQueryScope, DistributedRangeTarget, DistributedScatterResult,
    DistributedShardExecutor, DistributedShardFailure, DistributedShardRows,
    DistributedSortDirection, DistributedTransactionParticipant, DistributedWriteIntent,
    GlobalFtsStatisticsPolicy, GlobalFtsStatisticsSnapshot, ShardFtsStatisticsSnapshot,
    ShardLocalWritePlan, ShardQueryOutput, DISTRIBUTED_FTS_STATISTICS_FORMAT_VERSION,
    DISTRIBUTED_QUERY_PROTOCOL_VERSION,
};
pub use distribution_range_consensus::{
    RangeBackupFenceQuorum, RangeBackupFenceSession, RangeBackupWriteFence, RangeWriteAck,
    RangeWriteCatalogInspection, RangeWriteCommand, RangeWriteCoordinator, RangeWriteLogEntry,
    RangeWriteProbe, RangeWriteProgress, RangeWriteRepairBatch, RangeWriteRepairLimits,
    RangeWriteState, RangeWriteStore, RangeWriteTransport, DEFAULT_RANGE_WRITE_LOG,
    RANGE_WRITE_LOG_FORMAT_VERSION, RANGE_WRITE_PROTOCOL_VERSION,
};
pub use distribution_restore::{
    load_cluster_restore_admission, load_cluster_restore_admission_run_state,
    save_cluster_restore_admission, save_cluster_restore_admission_run_state,
    verify_cluster_restore_admission, verify_cluster_restore_admission_governed,
    ClusterRestoreAdmissionLimits, ClusterRestoreAdmissionReport, ClusterRestoreAdmissionRun,
    ClusterRestoreAdmissionRunAdvance, ClusterRestoreAdmissionRunLimits,
    ClusterRestoreAdmissionRunPhase, ClusterRestoreAdmissionRunState,
    ClusterRestoreCandidateDescriptor, ClusterRestoreNodeCandidate, ClusterRestoreNodeEvidence,
    CLUSTER_RESTORE_ADMISSION_FORMAT_VERSION, CLUSTER_RESTORE_ADMISSION_RUN_FORMAT_VERSION,
    DEFAULT_CLUSTER_RESTORE_ADMISSION, DEFAULT_CLUSTER_RESTORE_ADMISSION_RUN,
    MAX_CLUSTER_RESTORE_ADMISSION_BYTES,
};
pub use distribution_restore_activation::{
    acknowledge_cluster_restore_node, ClusterRestoreActivationLimits,
    ClusterRestoreActivationPhase, ClusterRestoreActivationPlan, ClusterRestoreActivationRun,
    ClusterRestoreActivationStatus, ClusterRestoreNodeAcknowledgement, ClusterRestoreReadiness,
    CLUSTER_RESTORE_ACTIVATION_FORMAT_VERSION, DEFAULT_CLUSTER_RESTORE_ACTIVATION_DIR,
    DEFAULT_CLUSTER_RESTORE_ACTIVATION_PLAN, DEFAULT_CLUSTER_RESTORE_ACTIVATION_STATE,
    DEFAULT_CLUSTER_RESTORE_READINESS,
};
pub use distribution_routing::{
    ClusterRequestRouter, PointOperation, PointReadPolicy, PointRoute, ReplicaRoutingProgress,
    RouteDecision, RouteRetry, RouteRetryCode, RouteTarget, RouteValidation, RoutedRequestHeader,
    TopologyInstall,
};
pub use distribution_schema_rollout::{
    ClusterSchemaActivationRolloutAdvance, ClusterSchemaActivationTransport,
    ClusterSchemaFinalizationRolloutAdvance, ClusterSchemaFinalizationTransport,
    ClusterSchemaRolloutAdvance, ClusterSchemaRolloutLimits, ClusterSchemaRolloutPhase,
    ClusterSchemaRolloutRun, ClusterSchemaRolloutState, ClusterSchemaStageTransport,
    CLUSTER_SCHEMA_ROLLOUT_FORMAT_VERSION, DEFAULT_CLUSTER_SCHEMA_ROLLOUT_STATE,
};
pub use distribution_supervisor::{
    CatchUpProgress, CleanupProgress, ClusterBackupMetadata, ClusterOperationalMetrics,
    ClusterProtocolCapabilities, ClusterRelocationDriver, ClusterRelocationTransport,
    ClusterRelocationTransportConfig, ClusterSupervisor, ClusterSupervisorConfig,
    ClusterSupervisorReport, DeferredClusterRelocationDriver, RelocationDriveOutcome,
    SnapshotCopyProgress, TransportClusterRelocationDriver,
};
#[cfg(feature = "tls-replication")]
pub use distribution_transport::{
    start_cluster_data_server, ClusterDataServerHandle, TcpClusterBootstrapClient,
    TcpClusterRelocationTransport,
};
pub use encryption::{
    DatabaseObjectCipher, EncryptionBinding, EncryptionConfig, EncryptionKdfMetadata,
    EncryptionMetadata, EncryptionMode, EncryptionObjectPurpose, KeyRotationPlan, KeySource,
    ENCRYPTION_METADATA_FILE,
};
pub use encryption_rotation::{
    rotate_bound_database_encryption, BoundEncryptionRotationCheckpoint,
    BoundEncryptionRotationOptions, BoundEncryptionRotationReport,
};
pub use error::{BicDbError, Result};
pub use event::{
    DeviceView, Event, EventProjection, EventQueue, EventStream, StoredEvent, SubscriptionId,
    CLOCK_OBSERVATION_STREAM, DEFAULT_EVENTS_DIR, DEFAULT_EVENTS_SEGMENT, RECORD_AUDIT_STREAM,
    SPATIAL_AUDIT_STREAM,
};
pub use format::{
    ensure_mode_supported, ensure_requested_mode_matches, ensure_storage_mode_compatible,
    persist_current, persist_current_with_mode, storage_mode, FormatMetadata, FormatMigrationPlan,
    FormatMigrationReport, FormatMigrationStep, StorageMode, CURRENT_FORMAT_VERSION,
    FORMAT_METADATA_FILE, FORMAT_MIGRATION_STATE_FILE, MIN_READ_FORMAT_VERSION,
    MIN_WRITE_FORMAT_VERSION, SERVER_PAGED_FEATURE_FLAG,
};
pub use fts_build::FullTextBuildStatus;
pub use fts_filters::FullTextDocumentFilter;
pub use fts_format::{
    full_text_generation_format_is_readable, full_text_query_instrumentation,
    FullTextCollectionStatistics, FullTextDocumentInput, FullTextDocumentStatistics, FullTextField,
    FullTextFieldInput, FullTextFilterInput, FullTextQueryInstrumentation,
    FullTextStorageAccounting, FullTextTermDictionaryEntry, FullTextTermInput,
    FullTextTermStatistics, KeyForensics, StorageNamespaceAccounting,
    FTS_GENERATION_FORMAT_VERSION, FTS_MIN_READ_FORMAT_VERSION, MAX_FULL_TEXT_STORED_TEXT_BYTES,
};
pub use fts_postings::{
    intersect_sorted_document_ids, intersect_sorted_document_ids_into, FullTextConjunctivePosting,
    FullTextRankedPosting,
};
pub use fts_scoring::{
    bm25_inverse_document_frequency, bm25_term_score, bm25f_term_score, Bm25Parameters,
    Bm25fParameters,
};
pub use geometry::{Geometry, WGS84_SRID};
pub use graph::{
    GraphEdge, GraphEdgeSource, GraphEndpoint, GraphEventEdgeSource, GraphEventNodeSource,
    GraphNode, GraphNodeSource, GraphPath, GraphProjection, GraphProjectionData, GraphVerifyReport,
};
pub use hnsw::{HnswIndexConfig, HnswIndexVerifyReport};
pub use large_value::{
    LargeValueIntegrityReport, LargeValueReader, LargeValueRef, DEFAULT_LARGE_VALUES_DIR,
    DEFAULT_LARGE_VALUE_CHUNK_BYTES, DEFAULT_LARGE_VALUE_THRESHOLD_BYTES, LARGE_VALUE_REF_MARKER,
};
pub use memory::{
    AgentWorkspace, AgentWorkspaceSnapshot, ConversationMessage, Memory, MemoryRecallOptions,
    MemoryRecallResult, MemoryScoringWeights, MemoryStore, MemorySummary, MemorySummaryInput,
    MemoryTimeline, MemoryType, DEFAULT_MEMORY_COLLECTION, MEMORY_EVENT_STREAM,
};
pub use memory_index::{
    EmbeddingProviderKind, EmbeddingRuntime, MemoryIndexConsistency, MemoryIndexDefinition,
    MemoryIndexJob, MemoryIndexMode, MemoryIndexProcessReport, MemoryJobStatus, ModelRegistryEntry,
};
pub use mutation::{
    MutationActor, MutationGrantId, MutationGrantSpec, MutationOperation, MutationPolicy,
    NativeCommitValidator, NativeInvariantAggregateFunction, NativeInvariantBinaryOperator,
    NativeInvariantDefinition, NativeInvariantExpression, NativeInvariantKind,
    NativeInvariantUnaryOperator, NativeInvariantValueType,
};
pub use numeric_key::{canonical_numeric_parts, numeric_index_key_from_canonical_text};
pub use observability::{
    append_slow_query_log, append_structured_log, default_sensitive_fields, doctor_bundle_json,
    operational_event_json, redact_bind_parameters, redact_query_text, DoctorReport, HealthReport,
    HealthState, OperationalMetrics, RedactionConfig, SlowQueryLogEntry, StructuredLogEvent,
    DEFAULT_SLOW_QUERY_THRESHOLD_MS,
};
pub use paged_checkpoint_maintenance::{
    PagedCheckpointSchedule, PagedCheckpointScheduleAdvance, PagedCheckpointScheduleLimits,
    PagedCheckpointTotals, DEFAULT_PAGED_CHECKPOINT_SCHEDULE,
    PAGED_CHECKPOINT_SCHEDULE_FORMAT_VERSION,
};
pub use paged_collection::{IndexEntryFormat, IndexEntryRef, PagedRecords, PagedRecordsOptions};
pub use paged_integrity_maintenance::{
    PagedIntegrityPhase, PagedIntegritySchedule, PagedIntegrityScheduleAdvance,
    PagedIntegrityScheduleLimits, PagedIntegrityTotals, DEFAULT_PAGED_INTEGRITY_SCHEDULE,
    PAGED_INTEGRITY_SCHEDULE_FORMAT_VERSION,
};
pub use paged_maintenance::{
    PagedVacuumSchedule, PagedVacuumScheduleAdvance, PagedVacuumScheduleLimits, PagedVacuumTotals,
    DEFAULT_PAGED_MAINTENANCE_DIR, DEFAULT_PAGED_MAINTENANCE_IDENTITY,
    DEFAULT_PAGED_VACUUM_SCHEDULE, PAGED_MAINTENANCE_IDENTITY_FORMAT_VERSION,
    PAGED_VACUUM_SCHEDULE_FORMAT_VERSION,
};
pub use query_exec::{NumericSummary, TimeSeriesFilter};
pub use record::{
    patch_json_object_text, splice_json_object_text, BlindIndexPolicy, CellRef, CellRow,
    CollectionMeta, CollectionMode, CollectionPolicy, ColumnSecurity, OwnedCell, OwnedCellRow,
    Record, RedactionPolicy, StoredRecord, TypedCell, TypedRow,
};
pub use replication::{
    CommitFrame, ReplicationApplyReport, ReplicationApplyState, ReplicationConfig,
    ReplicationFrame, ReplicationMode, ReplicationOperationType, ReplicationRetentionStatus,
    ReplicationTlsConfig, ReplicationWatermark, ReplicationWrite, DEFAULT_REPLICATION_STREAM_ID,
    REPLICATION_PROTOCOL_VERSION,
};
pub use residency::{
    process_resident_bytes, CollectionResidency, HnswResidency, IndexResidency, ResidencyReport,
};
pub use resource_governor::{
    ResourceCapacity, ResourceDemand, ResourceGovernor, ResourceGovernorConfig,
    ResourceGovernorSnapshot, ResourceLane, ResourceLaneLimit, ResourceLaneSnapshot,
    ResourcePermit, ResourceUsage,
};
pub use spatial_packed::SpatialPackStrategy;
pub use storage::{CompressionConfig, SegmentReadMode};
pub use sync::{OpType, SyncOp};
pub use sync_mesh::{
    EventEnvelope, EventId, NodeId, PeerSyncStatus, StreamId, SyncBundle, SyncBundleEvent,
    SyncCheckpoint, SyncExportReport, SyncImportReport, SyncMesh, SyncPendingChanges, SyncStatus,
    SyncVector,
};
pub use vector::{
    cosine_similarity, dot_product, l2_distance, JsonFilter, ProfiledVectorSearch, VectorMetric,
    VectorSearchProfile, VectorSearchResult,
};
