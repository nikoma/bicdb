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
mod distribution_events;
mod distribution_gateway;
#[cfg(feature = "tls-replication")]
mod distribution_io;
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
#[cfg(feature = "tls-replication")]
pub use distribution_events::ClusterEventClient;
pub use distribution_events::{
    CLUSTER_EVENTS_COLLECTION, ClusterEvent, ClusterEventCursor, ClusterEventOperation,
    ClusterEventPage, ClusterEventRequest, ClusterEventResponse, MAX_CLUSTER_EVENT_BYTES,
    MAX_CLUSTER_EVENT_PAGE_ROWS, cluster_event_record_id,
};
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
#[cfg(feature = "lua")]
mod lua;
#[cfg(feature = "lua")]
pub use lua::{LuaError, LuaLimits, LuaReply};
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
pub mod archive_guard;
pub mod log_archive;
pub mod query_budget;
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
    BackupChainVerifyReport, BackupCreateOptions, BackupCreateReport, BackupDrillReport,
    BackupPitrReplayLimits, BackupPointInTimeRestoreOptions, BackupRestoreOptions,
    BackupRestoreReport, BackupVerifyReport, OnlineBackupControl, OnlineBackupLimits,
    OnlineBackupPhase, OnlineBackupProgress, OnlineBackupSnapshot, RepositoryBackupReport,
    RepositoryPruneReport, WalPruneReport, apply_archived_wal, create_backup,
    create_backup_from_open_database, create_backup_to_writer, create_online_paged_base_backup,
    create_wal_tail_backup, drill_backup_restore, drill_backup_restore_with_limits,
    is_repository_backup, prune_archived_wal, prune_backup_repository, restore_backup,
    restore_backup_to_point, restore_backup_to_point_with_limits, verify_backup,
    verify_backup_chain, verify_backup_from_reader, wal_floor_of_base,
};
pub use bicdb_page::{
    BTreeNodeViolation, BTreePageExpectation, BTreeVerifyCursor, BTreeVerifyFault,
    BTreeVerifyLimits, BTreeVerifyStepReport, BTreeVerifyStopReason,
    MAX_BTREE_VERIFY_CURSOR_KEY_BYTES, MAX_BTREE_VERIFY_DURATION_MILLIS_PER_STEP,
    MAX_BTREE_VERIFY_ENTRIES_PER_STEP, MAX_BTREE_VERIFY_HEIGHT,
    MAX_BTREE_VERIFY_KEY_BYTES_PER_STEP, MAX_BTREE_VERIFY_LEAF_PAGES_PER_STEP,
    MAX_BTREE_VERIFY_PAGE_BYTES_PER_STEP, MAX_CHECKPOINT_FREEZE_XIDS_PER_STEP,
    MAX_READ_AHEAD_CANDIDATES_PER_STEP, MAX_READ_AHEAD_DURATION_MILLIS_PER_STEP,
    MAX_READ_AHEAD_IO_BYTES_PER_STEP, MAX_READ_AHEAD_QUEUE_PAGES, MAX_TAIL_RECLAIM_IO_BYTES,
    MAX_TAIL_RECLAIM_PAGE_VISITS, MAX_VERSION_VERIFY_BYTES_PER_STEP,
    MAX_VERSION_VERIFY_CURSOR_KEY_BYTES, MAX_VERSION_VERIFY_DURATION_MILLIS_PER_STEP,
    MAX_VERSION_VERIFY_FAULT_SAMPLE_BYTES, MAX_VERSION_VERIFY_FAULT_SAMPLES,
    MAX_VERSION_VERIFY_KEYS_PER_STEP, MAX_VERSION_VERIFY_VERSIONS_PER_STEP, MAX_WAL_RECORD_BYTES,
    MAX_WRITEBACK_DURATION_MILLIS_PER_STEP, MAX_WRITEBACK_IO_BYTES_PER_STEP,
    MAX_WRITEBACK_PAGES_PER_STEP, MIN_PAGE_SIZE, PAGE_IO_LATENCY_BUCKET_COUNT,
    PAGE_IO_LATENCY_BUCKET_UPPER_BOUNDS_NANOS, PAGED_STORE_SNAPSHOT_FORMAT_VERSION,
    PageClassIoSnapshot, PageIoLatencySnapshot, PageIoSnapshot, PageTypeIoSnapshot,
    PagedCheckpointCursor, PagedCheckpointLimits, PagedCheckpointPhase, PagedCheckpointStepReport,
    PagedCheckpointStopReason, PagedIntegrityReport, PagedStoreSnapshot, ReadAheadLimits,
    ReadAheadStepReport, ReadAheadStopReason, ReadAheadSubmitReport, RecoveryReport,
    TailReclaimLimits, TailReclaimReport, TupleLocator, VERSION_HEADER_BYTES, VacuumCursor,
    VacuumLimits, VacuumReport, VacuumStopReason, VersionChainFaultSample, VersionChainInspection,
    VersionChainRepairReport, VersionChainTerminal, VersionChainVerifyCursor,
    VersionChainVerifyLimits, VersionChainVerifyReport, VersionChainVerifyStepReport,
    VersionChainVerifyStopReason, VersionChainVersion, WalSnapshot, WritebackCursor,
    WritebackLimits, WritebackStepReport, WritebackStopReason,
};
pub use bicdb_page::{DurabilityState, WriteFailure};
pub use broker::{
    Broker, BrokerAccess, BrokerCaller, BrokerDeliverySnapshot, BrokerDeliveryState, BrokerMessage,
    BrokerStats, ConsumeOptions, ConsumerGroupInfo, DEAD_LETTER_MESSAGE_EVENT,
    DEFAULT_MAX_ATTEMPTS, DrainOptions, DrainReport, GroupStats, NackOptions, PeekedMessage,
    PublishOptions, PublishReceipt, QueueConfig, QueueStats, TrimReport,
};
pub use cancellation::{BlockingWaitAdmission, CancellationToken};
pub use consensus::{
    AppendEntries, AppendResponse, ConsensusConfig, ConsensusLogEntry, ConsensusPeer,
    ConsensusRole, ConsensusState, ConsensusStatus, RequestVote, VoteResponse,
};
pub use db::effective_fts_partitions;
pub use db::ensure_frames_within_durable_horizon;
mod oracle_simple_case;
pub use oracle_simple_case::{
    oracle_simple_lower_v1, oracle_simple_national_lower_v1, oracle_simple_national_upper_v1,
    oracle_simple_upper_v1,
};

pub use db::AuthenticationStrength;
pub use db::ChangedRecord;
pub use db::DeferredMaterializeStats;
pub use db::FullTextBuildStep;
pub use db::FullTextStoredTextPage;
pub use db::ReadCommittedRowReservation;
pub use db::RecordReadSnapshot;
pub use db::ReplicationWriterAuthority;
pub use db::UniqueIndexRangeCursor;
pub use db::index_key_lower;
pub use db::index_key_upper;
pub use db::index_predicate_matches;
pub use db::row_lock_waits_active;
pub use db::{
    AnalyzeSampler, TransactionSuspension, analyze_sample_limit, estimate_distinct_from_sample,
    scale_sample_count,
};
pub use db::{
    BicDb, BlockGate, BypassPolicy, CLUSTER_SCHEMA_ACTIVATION_FORMAT_VERSION,
    CLUSTER_SCHEMA_BUNDLE_FORMAT_VERSION, CLUSTER_SCHEMA_CATALOG_COLLECTIONS,
    CLUSTER_SCHEMA_FINALIZATION_FORMAT_VERSION, CLUSTER_SCHEMA_STAGE_FORMAT_VERSION,
    ClusterSchemaActivationAdvance, ClusterSchemaActivationLimits, ClusterSchemaActivationPhase,
    ClusterSchemaActivationState, ClusterSchemaBundle, ClusterSchemaCatalogRecord,
    ClusterSchemaCompatibilityWindow, ClusterSchemaFinalizationState, ClusterSchemaOnlinePlan,
    ClusterSchemaStageLimits, ClusterSchemaStageState, CollectionCompactionReport, CollectionStats,
    ColumnStatistics, CompactionOptions, CompactionReport, DEFAULT_CLUSTER_SCHEMA_ACTIVATION,
    DEFAULT_CLUSTER_SCHEMA_FINALIZATION, DEFAULT_CLUSTER_SCHEMA_STAGE, DEFAULT_COLLECTION_CATALOG,
    DEFAULT_COMPACTION_CHECKPOINT, DEFAULT_FORMAT_METADATA, DEFAULT_GRAPH_DIR, DEFAULT_HA_STATE,
    DEFAULT_INCREMENTAL_COMPACTION_STATE, DEFAULT_INDEX_CATALOG, DEFAULT_INDEX_MAINTENANCE,
    DEFAULT_PAGED_BTREE_BUILD_DIR, DEFAULT_PAGED_BUFFER_POOL_BYTES, DEFAULT_PAGED_DIR,
    DEFAULT_PAGED_PAGE_SIZE, DEFAULT_PAGED_READ_AHEAD_QUEUE_PAGES, DEFAULT_PAGED_WAL_MAX_BYTES,
    DEFAULT_PLANNER_STATS, DEFAULT_SEGMENTS_DIR, DEFAULT_TRANSACTION_LOG, DEFAULT_VECTOR_INDEX_DIR,
    DURABILITY_FAILURE_MARKER, DbConfig, DbSnapshot, DbStats, EventHorizonReport, FtsQueryBudget,
    FtsQueryLimits, FullTextBuildLifecycleState, FullTextBuildLifecycleStatus,
    FullTextBuildProgress, FullTextBuildRecommendedAction, FullTextBuildReconcileOutcome,
    FullTextBuildReconcileReport, FullTextOpenMetrics, FullTextPublishedGeneration,
    FullTextReadSession, GeofenceTransition, GeofenceTransitionKind, HaApplyReport, HaRole,
    HaState, HaStatus, INCREMENTAL_COMPACTION_FORMAT_VERSION, IncrementalCompactionAdvance,
    IncrementalCompactionLimits, IncrementalCompactionPhase, IncrementalCompactionState,
    IndexCatalogStats, IndexDefinition, IndexExclusion, IndexExclusionElement, IndexField,
    IndexKind, IndexLockPhaseReport, IndexMaintenanceAllReport, IndexMaintenanceReport,
    IndexPredicate, IndexPredicateBinaryOperator, IndexPredicateUnaryOperator,
    IndexPredicateValueType, IndexStatistics, IndexValue, IndexVerifyReport, IntegrityReport,
    MAX_CLUSTER_SCHEMA_BUNDLE_BYTES, MAX_CLUSTER_SCHEMA_BUNDLE_RECORDS,
    MAX_CLUSTER_SCHEMA_SIGNER_KEY_ID_BYTES, MAX_FULL_TEXT_TERM_BYTES, NetworkStatistics,
    OnlineBackupReport, OptimizedRoute, OptimizedRouteStop, OsmImportBbox, OsmImportReport,
    PAGED_BTREE_BUILD_FORMAT_VERSION, PagedBTreeBuildAdvance, PagedBTreeBuildLimits,
    PagedBTreeBuildPhase, PagedBTreeBuildState, PagedCheckpointMaintenanceHandle,
    PagedReadAheadHandle, PagedStorageOpsHandle, PagedVacuumMaintenanceHandle, PlannerStatsCatalog,
    ProtectedDataBackfillReport, ProtectedDataBlindIndexCoverageReport,
    ProtectedDataCiphertextSamplingReport, ProtectedDataKeyRotationOptions,
    ProtectedDataKeyRotationReport, ProtectedDataSecurityEvidenceReport, RangeStatistics,
    RawSqlAuditReport, RecordConflict, RecordConflictCandidate, RecordSystemMetadata, RepairDelta,
    RepairPlan, RoadEdge, RoadNode, RoutePath, RouteProfile, RowId,
    SCHEMA_COMPATIBILITY_FORMAT_VERSION, SIGNED_CLUSTER_SCHEMA_BUNDLE_FORMAT_VERSION,
    SchemaCompatibilityAuthority, SchemaCompatibilityFingerprint, SecureBicDb, SecureOperation,
    SecurityContext, SignedClusterSchemaBundle, SnappedRoadNode, SpatialPackReport,
    SpatialQueryResult, TableStatistics, TenantIsolationReport, Transaction, TransactionId,
    TransactionIsolation, TransactionOutcome, TransactionOutcomeObserver, TransactionRollbackMark,
    TransactionWriteReceipt, TransactionWriteRollback, TxLogHandle, TxState, TypedColumnStatistics,
    TypedValueFrequency, ValueFrequency, VersionedRecord, VisibleRow, decode_fts_posting_payload,
    decode_fts_posting_payload_into, fts_impact_bucket, fts_impact_bucket_upper_edge,
    fts_rank_conjunctive, fts_rank_single_term, full_text_oversized_terms_skipped,
    full_text_term_is_indexable, paged_tid_hint_stats, parse_repair_decimal,
};
pub use db::{
    CollectionDiff, DatabaseDiff, HA_NODE_FILE, HaNodeStatus, NodeRole, RecordChange,
    RecordChangeKind, ReplicationCursor, ReplicationFeed, StandbyResume, SwitchoverOffer,
    SyncQuorum, SyncQuorumStatus, SyncTimeoutPolicy, diff_databases, finish_standby_bootstrap,
    read_only_standby_hint, read_persisted_ha_role, stage_standby_bootstrap,
    standby_bootstrap_staging, take_thread_commit_seq,
};
pub use db::{
    CommitAdmission, CommitAdmissionIntent, CommitAdmissionMutation, CommitAdmissionTicket,
};
pub use db::{CommitResolution, INCARNATION_JOURNAL};
pub use db::{
    FLASHBACK_HISTORY_PREFIX, FlashbackConfig, FlashbackOperation, FlashbackPoint,
    FlashbackVersion, flashback_error_sqlstate, flashback_history_collection,
    is_flashback_history_collection, scn_to_unix_micros, unix_micros_to_scn,
};
pub use db::{
    LOG_ARCHIVE_HOLDER, LogReplayReport, RestoreDrillReport, TableChecksum, VerificationManifest,
    restore_from_log_archive, restore_point_in_time, verify_restore_drill,
};
pub use db::{WalArchiveReport, WalArchiveStatus};
pub use db::{timestamp_day_index_inputs_supported, timestamp_day_index_value};
pub use distribution::{
    CLUSTER_DATA_PROTOCOL_VERSION, ClusterBootstrapSnapshot, ClusterId,
    ClusterNetworkTransportConfig, ClusterNode, ClusterNodeId, ClusterNodeLifecycle,
    ClusterNodeLiveness, ClusterPlacementPlanner, ClusterTopology, DEFAULT_CLUSTER_TOPOLOGY,
    DEFAULT_DISTRIBUTION_CONFIG, DISTRIBUTION_FORMAT_VERSION, DISTRIBUTION_HASH_VERSION,
    DistributionConfig, DistributionStore, FailureRepairPlan, MetadataMemberRole,
    MetadataMutationToken, NodeRemovalSafety, PlacementPolicy, PlannedLeaderTransfer,
    PlannedReplicaMove, RangeAvailability, RangeDescriptor, RangeId, RangeRelocation, RangeReplica,
    RangeReplicaRole, RangeSnapshotBatch, RangeSnapshotExport, RangeSnapshotOptions,
    RebalanceOptions, RebalancePlan, RelocationId, RelocationPhase, ReplicaId, ReplicaMoveReason,
    SCHEMA_BOOTSTRAP_NODE_LABEL, SCHEMA_COMPATIBILITY_NODE_LABEL,
    SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL, StandardFailureDomain, TopologyChange, UnplacedRange,
    build_failure_repair_plan, build_rebalance_plan, distribution_key_token,
    filter_commit_frame_for_range, load_distribution_config, provision_cluster_member_directory,
    save_distribution_config,
};
pub use distribution_anti_entropy::{
    RANGE_DIGEST_FORMAT_VERSION, RangeDigestBucket, RangeDigestBucketScanStep, RangeDigestDiff,
    RangeDigestLimits, RangeDigestManifest, RangeDigestState, RangeDigestTransport,
    append_range_digest_bucket_records, calculate_range_digest_root,
    compare_range_digest_manifests, load_range_digest_state, range_digest_bucket_for_record,
    save_range_digest_state,
};
pub use distribution_anti_entropy_auto::{
    AUTOMATIC_RANGE_ANTI_ENTROPY_FORMAT_VERSION, AutomaticRangeAntiEntropyAdvance,
    AutomaticRangeAntiEntropyController, AutomaticRangeAntiEntropyLimits,
    AutomaticRangeAntiEntropyState, DEFAULT_AUTOMATIC_RANGE_ANTI_ENTROPY_DIR,
    DEFAULT_AUTOMATIC_RANGE_ANTI_ENTROPY_SCHEDULE, DEFAULT_AUTOMATIC_RANGE_ANTI_ENTROPY_STATE,
    RANGE_ANTI_ENTROPY_FENCE_PLAN_FORMAT_VERSION, RangeAntiEntropyFenceAuthority,
    RangeAntiEntropyFencePlan,
};
pub use distribution_anti_entropy_repair::{
    RANGE_DIGEST_REPAIR_FORMAT_VERSION, RangeDigestRepairBatch, RangeDigestRepairLimits,
    RangeDigestRepairState, RangeDigestRepairTransport, load_range_digest_repair_state,
    save_range_digest_repair_state,
};
pub use distribution_anti_entropy_repair_certificate::{
    DEFAULT_RANGE_DIGEST_REPAIR_CERTIFICATE, RANGE_DIGEST_REPAIR_CERTIFICATE_FORMAT_VERSION,
    RangeDigestRepairCertificate, RangeDigestRepairCertificateLimits, RangeDigestRepairedReplica,
    RangeDigestVerifiedBucketEvidence, load_range_digest_repair_certificate,
    save_range_digest_repair_certificate,
};
pub use distribution_anti_entropy_repair_run::{
    DEFAULT_RANGE_DIGEST_REPAIR_RUN_STATE, RANGE_DIGEST_REPAIR_RUN_FORMAT_VERSION,
    RangeDigestRepairRun, RangeDigestRepairRunAdvance, RangeDigestRepairRunLimits,
    RangeDigestRepairRunPhase, load_range_digest_repair_run, save_range_digest_repair_run,
};
pub use distribution_anti_entropy_run::{
    RANGE_DIGEST_RUN_FORMAT_VERSION, RangeDigestReplicaProgress, RangeDigestRootEvidence,
    RangeDigestRun, RangeDigestRunAdvance, RangeDigestRunLimits, RangeDigestRunOutcome,
    RangeDigestRunReport, load_range_digest_run, save_range_digest_run,
};
pub use distribution_anti_entropy_scheduler::{
    RANGE_ANTI_ENTROPY_SCHEDULE_FORMAT_VERSION, RangeAntiEntropySchedule,
    RangeAntiEntropyScheduleAdvance, RangeAntiEntropyScheduleLimits,
    load_range_anti_entropy_schedule, save_range_anti_entropy_schedule,
};
pub use distribution_backup::{
    CLUSTER_BACKUP_CERTIFICATE_FORMAT_VERSION, ClusterBackupCertificate, ClusterBackupLimits,
    ClusterBackupPlan, ClusterNodeBackupArtifact, DEFAULT_CLUSTER_BACKUP_CERTIFICATE,
    DEFAULT_CLUSTER_BACKUP_PLAN, DEFAULT_CLUSTER_NODE_BACKUP_ARTIFACT,
    MAX_CLUSTER_BACKUP_CONTROL_FILE_BYTES, RangeBackupBarrier, load_cluster_backup_certificate,
    load_cluster_backup_plan, load_cluster_node_backup_artifact, save_cluster_backup_certificate,
    save_cluster_backup_plan, save_cluster_node_backup_artifact,
};
pub use distribution_backup_run::{
    CLUSTER_BACKUP_RUN_FORMAT_VERSION, ClusterBackupRun, ClusterBackupRunLimits,
    ClusterBackupRunPhase, ClusterBackupRunStatus, DEFAULT_CLUSTER_BACKUP_RUN_DIR,
    DEFAULT_CLUSTER_BACKUP_RUN_JOURNAL, DEFAULT_CLUSTER_BACKUP_RUN_MANIFEST,
};
pub use distribution_certification::{
    CLUSTER_CERTIFICATION_FORMAT_VERSION, CLUSTER_CERTIFICATION_PUBLICATION_FORMAT_VERSION,
    CLUSTER_CERTIFICATION_RAW_ARTIFACT_FORMAT_VERSION, CLUSTER_CERTIFICATION_STATE_FORMAT_VERSION,
    ClusterArtifactEvidence, ClusterBackgroundSaturationEvidence, ClusterCertificationArtifactKind,
    ClusterCertificationGates, ClusterCertificationMeasurements, ClusterCertificationObservation,
    ClusterCertificationPlan, ClusterCertificationPreflight, ClusterCertificationProtocolVersions,
    ClusterCertificationPublicationManifest, ClusterCertificationRawArtifactHeader,
    ClusterCertificationRawArtifactPayloadFormat, ClusterCertificationReport,
    ClusterCertificationState, ClusterCertificationVerification, ClusterExpansionEvidence,
    ClusterFailurePoint, ClusterFailureTrialEvidence, ClusterNodeBackgroundSaturationEvidence,
    ClusterNodeHardwareEvidence, ClusterNodeLossMechanism, ClusterNodeRecoveryMode,
    ClusterRestoreEvidence, ClusterScaleProfile, DEFAULT_CLUSTER_CERTIFICATION_MANIFEST,
    DEFAULT_CLUSTER_CERTIFICATION_PLAN, DEFAULT_CLUSTER_CERTIFICATION_REPORT,
    DEFAULT_CLUSTER_CERTIFICATION_STATE, capture_cluster_certification_artifact,
    finalize_cluster_certification_bundle, initialize_cluster_certification_bundle,
    load_cluster_certification_plan, load_cluster_certification_publication_manifest,
    load_cluster_certification_report, load_cluster_certification_state,
    record_cluster_certification_observation, register_cluster_certification_artifact,
    save_cluster_certification_plan, verify_cluster_certification_bundle,
};
pub use distribution_consensus::{
    CLUSTER_METADATA_CONSENSUS_FORMAT_VERSION, DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE,
    METADATA_READ_AUTHORITY_PROTOCOL_VERSION, METADATA_RESTORE_ACTIVATION_FORMAT_VERSION,
    MetadataAppendRequest, MetadataAppendResponse, MetadataConsensusRole, MetadataConsensusStatus,
    MetadataConsensusStore, MetadataLogEntry, MetadataReadAck, MetadataReadAuthority,
    MetadataReadContext, MetadataReadParticipant, MetadataReadProbeRequest,
    MetadataReadProbeResponse, MetadataRestoreActivation, MetadataSnapshot, MetadataVoteRequest,
    MetadataVoteResponse,
};
pub use distribution_data::{
    CatchUpSourceBatch, ClusterDataNodeService, ClusterSchemaActivationReceipt,
    ClusterSchemaFinalizationReceipt, ClusterSchemaStageReceipt,
    DEFAULT_RANGE_DIGEST_REPAIR_STATE_DIR, DEFAULT_RANGE_DIGEST_STATE_DIR,
    DEFAULT_RANGE_LEARNER_APPLY_STATE, InProcessClusterRelocationTransport,
    RANGE_LEARNER_APPLY_FORMAT_VERSION, RangeDigestAdvance, RangeLearnerApplyState,
    RangeLearnerStore, SnapshotSourceStep,
};
pub use distribution_gateway::{
    BoundedClusterConnectionPool, ClusterConnectionPoolStats, ClusterGatewayConfig,
    ClusterPointConnection, ClusterPointConnector, ClusterPointGateway, PointTransportReply,
};
pub use distribution_query::{
    DISTRIBUTED_FTS_STATISTICS_FORMAT_VERSION, DISTRIBUTED_QUERY_PROTOCOL_VERSION,
    DistributedCommitDecision, DistributedCommitProtocol, DistributedFtsHit,
    DistributedNumericAggregate, DistributedOrderedRow, DistributedQueryFailurePolicy,
    DistributedQueryLimits, DistributedQueryPlan, DistributedQueryScope, DistributedRangeTarget,
    DistributedScatterResult, DistributedShardExecutor, DistributedShardFailure,
    DistributedShardRows, DistributedSortDirection, DistributedTransactionParticipant,
    DistributedWriteIntent, GlobalFtsStatisticsPolicy, GlobalFtsStatisticsSnapshot,
    ShardFtsStatisticsSnapshot, ShardLocalWritePlan, ShardQueryOutput,
    execute_distributed_transaction, execute_scatter_gather, execute_scatter_gather_governed,
    merge_fts_top_k, merge_global_fts_statistics, merge_numeric_aggregates, merge_ordered_top_k,
    plan_point_query, plan_scatter_query, plan_shard_local_write,
};
pub use distribution_range_consensus::{
    DEFAULT_RANGE_WRITE_LOG, RANGE_WRITE_LOG_FORMAT_VERSION, RANGE_WRITE_PROTOCOL_VERSION,
    RangeBackupFenceQuorum, RangeBackupFenceSession, RangeBackupWriteFence, RangeWriteAck,
    RangeWriteCatalogInspection, RangeWriteCommand, RangeWriteCoordinator, RangeWriteLogEntry,
    RangeWriteProbe, RangeWriteProgress, RangeWriteRepairBatch, RangeWriteRepairLimits,
    RangeWriteState, RangeWriteStore, RangeWriteTransport,
};
pub use distribution_restore::{
    CLUSTER_RESTORE_ADMISSION_FORMAT_VERSION, CLUSTER_RESTORE_ADMISSION_RUN_FORMAT_VERSION,
    ClusterRestoreAdmissionLimits, ClusterRestoreAdmissionReport, ClusterRestoreAdmissionRun,
    ClusterRestoreAdmissionRunAdvance, ClusterRestoreAdmissionRunLimits,
    ClusterRestoreAdmissionRunPhase, ClusterRestoreAdmissionRunState,
    ClusterRestoreCandidateDescriptor, ClusterRestoreNodeCandidate, ClusterRestoreNodeEvidence,
    DEFAULT_CLUSTER_RESTORE_ADMISSION, DEFAULT_CLUSTER_RESTORE_ADMISSION_RUN,
    MAX_CLUSTER_RESTORE_ADMISSION_BYTES, load_cluster_restore_admission,
    load_cluster_restore_admission_run_state, save_cluster_restore_admission,
    save_cluster_restore_admission_run_state, verify_cluster_restore_admission,
    verify_cluster_restore_admission_governed,
};
pub use distribution_restore_activation::{
    CLUSTER_RESTORE_ACTIVATION_FORMAT_VERSION, ClusterRestoreActivationLimits,
    ClusterRestoreActivationPhase, ClusterRestoreActivationPlan, ClusterRestoreActivationRun,
    ClusterRestoreActivationStatus, ClusterRestoreNodeAcknowledgement, ClusterRestoreReadiness,
    DEFAULT_CLUSTER_RESTORE_ACTIVATION_DIR, DEFAULT_CLUSTER_RESTORE_ACTIVATION_PLAN,
    DEFAULT_CLUSTER_RESTORE_ACTIVATION_STATE, DEFAULT_CLUSTER_RESTORE_READINESS,
    acknowledge_cluster_restore_node,
};
pub use distribution_routing::{
    ClusterRequestRouter, PointOperation, PointReadPolicy, PointRoute, ReplicaRoutingProgress,
    RouteDecision, RouteRetry, RouteRetryCode, RouteTarget, RouteValidation, RoutedRequestHeader,
    TopologyInstall,
};
pub use distribution_schema_rollout::{
    CLUSTER_SCHEMA_ROLLOUT_FORMAT_VERSION, ClusterSchemaActivationRolloutAdvance,
    ClusterSchemaActivationTransport, ClusterSchemaFinalizationRolloutAdvance,
    ClusterSchemaFinalizationTransport, ClusterSchemaRolloutAdvance, ClusterSchemaRolloutLimits,
    ClusterSchemaRolloutPhase, ClusterSchemaRolloutRun, ClusterSchemaRolloutState,
    ClusterSchemaStageTransport, DEFAULT_CLUSTER_SCHEMA_ROLLOUT_STATE,
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
    ClusterDataServerHandle, TcpClusterBootstrapClient, TcpClusterRelocationTransport,
    start_cluster_data_server,
};
pub use encryption::{
    DatabaseObjectCipher, ENCRYPTION_METADATA_FILE, EncryptionBinding, EncryptionConfig,
    EncryptionKdfMetadata, EncryptionMetadata, EncryptionMode, EncryptionObjectPurpose,
    KeyRotationPlan, KeySource,
};
pub use encryption_rotation::{
    BoundEncryptionRotationCheckpoint, BoundEncryptionRotationOptions,
    BoundEncryptionRotationReport, rotate_bound_database_encryption,
};
pub use error::{BicDbError, Result};
pub use event::{
    CLOCK_OBSERVATION_STREAM, DEFAULT_EVENTS_DIR, DEFAULT_EVENTS_SEGMENT, DeviceView, Event,
    EventProjection, EventQueue, EventStream, RECORD_AUDIT_STREAM, SPATIAL_AUDIT_STREAM,
    StoredEvent, SubscriptionId,
};
pub use format::{
    CURRENT_FORMAT_VERSION, FORMAT_METADATA_FILE, FORMAT_MIGRATION_STATE_FILE, FormatMetadata,
    FormatMigrationPlan, FormatMigrationReport, FormatMigrationStep,
    LOCAL_NAMESPACE_IDENTITY_FEATURE_FLAG, MIN_READ_FORMAT_VERSION, MIN_WRITE_FORMAT_VERSION,
    ORACLE_INVALID_ROUTINES_FEATURE_FLAG, SERVER_PAGED_FEATURE_FLAG, StorageMode,
    ensure_mode_supported, ensure_requested_mode_matches, ensure_storage_mode_compatible,
    persist_current, persist_current_with_mode, storage_mode,
};
pub use fts_build::FullTextBuildStatus;
pub use fts_filters::FullTextDocumentFilter;
pub use fts_format::{
    FTS_GENERATION_FORMAT_VERSION, FTS_MIN_READ_FORMAT_VERSION, FullTextCollectionStatistics,
    FullTextDocumentInput, FullTextDocumentStatistics, FullTextField, FullTextFieldInput,
    FullTextFilterInput, FullTextQueryInstrumentation, FullTextStorageAccounting,
    FullTextTermDictionaryEntry, FullTextTermInput, FullTextTermStatistics, KeyForensics,
    MAX_FULL_TEXT_STORED_TEXT_BYTES, StorageNamespaceAccounting,
    full_text_generation_format_is_readable, full_text_query_instrumentation,
};
pub use fts_postings::{
    FullTextConjunctivePosting, FullTextRankedPosting, intersect_sorted_document_ids,
    intersect_sorted_document_ids_into,
};
pub use fts_scoring::{
    Bm25Parameters, Bm25fParameters, bm25_inverse_document_frequency, bm25_term_score,
    bm25f_term_score,
};
pub use geometry::{Geometry, WGS84_SRID};
pub use graph::{
    GraphEdge, GraphEdgeSource, GraphEndpoint, GraphEventEdgeSource, GraphEventNodeSource,
    GraphNode, GraphNodeSource, GraphPath, GraphProjection, GraphProjectionData, GraphVerifyReport,
};
pub use hnsw::{HnswIndexConfig, HnswIndexVerifyReport};
pub use large_value::{
    DEFAULT_LARGE_VALUE_CHUNK_BYTES, DEFAULT_LARGE_VALUE_THRESHOLD_BYTES, DEFAULT_LARGE_VALUES_DIR,
    LARGE_VALUE_REF_MARKER, LargeValueIntegrityReport, LargeValueReader, LargeValueRef,
};
pub use memory::{
    AgentWorkspace, AgentWorkspaceSnapshot, ConversationMessage, DEFAULT_MEMORY_COLLECTION,
    MEMORY_EVENT_STREAM, Memory, MemoryRecallOptions, MemoryRecallResult, MemoryScoringWeights,
    MemoryStore, MemorySummary, MemorySummaryInput, MemoryTimeline, MemoryType,
};
pub use memory_index::{
    EmbeddingProviderKind, EmbeddingRuntime, MemoryIndexConsistency, MemoryIndexDefinition,
    MemoryIndexJob, MemoryIndexMode, MemoryIndexProcessReport, MemoryJobStatus, ModelRegistryEntry,
};
pub use mutation::{
    MutationActor, MutationGrantId, MutationGrantSpec, MutationOperation, MutationPolicy,
    NativeCommitValidator, NativeFactPublicationPolicy, NativeInvariantAggregateFunction,
    NativeInvariantBinaryOperator, NativeInvariantDefinition, NativeInvariantExpression,
    NativeInvariantKind, NativeInvariantUnaryOperator, NativeInvariantValueType,
};
pub use numeric_key::{canonical_numeric_parts, numeric_index_key_from_canonical_text};
pub use observability::{
    DEFAULT_SLOW_QUERY_THRESHOLD_MS, DoctorReport, HealthReport, HealthState, OperationalMetrics,
    RedactionConfig, SlowQueryLogEntry, StructuredLogEvent, append_slow_query_log,
    append_structured_log, default_sensitive_fields, doctor_bundle_json, operational_event_json,
    redact_bind_parameters, redact_query_text,
};
pub use paged_checkpoint_maintenance::{
    DEFAULT_PAGED_CHECKPOINT_SCHEDULE, PAGED_CHECKPOINT_SCHEDULE_FORMAT_VERSION,
    PagedCheckpointSchedule, PagedCheckpointScheduleAdvance, PagedCheckpointScheduleLimits,
    PagedCheckpointTotals,
};
pub use paged_collection::{
    IndexEntryFormat, IndexEntryRef, PagedRecords, PagedRecordsOptions,
    paged_live_count_walks_on_this_thread, paged_record_gets_on_this_thread,
    paged_record_scans_on_this_thread,
};
pub use paged_integrity_maintenance::{
    DEFAULT_PAGED_INTEGRITY_SCHEDULE, PAGED_INTEGRITY_SCHEDULE_FORMAT_VERSION, PagedIntegrityPhase,
    PagedIntegritySchedule, PagedIntegrityScheduleAdvance, PagedIntegrityScheduleLimits,
    PagedIntegrityTotals,
};
pub use paged_maintenance::{
    DEFAULT_PAGED_MAINTENANCE_DIR, DEFAULT_PAGED_MAINTENANCE_IDENTITY,
    DEFAULT_PAGED_VACUUM_SCHEDULE, PAGED_MAINTENANCE_IDENTITY_FORMAT_VERSION,
    PAGED_VACUUM_SCHEDULE_FORMAT_VERSION, PagedVacuumSchedule, PagedVacuumScheduleAdvance,
    PagedVacuumScheduleLimits, PagedVacuumTotals,
};
pub use query_exec::{NumericSummary, TimeSeriesFilter};
pub use record::{
    BlindIndexPolicy, CellRef, CellRow, CollectionMeta, CollectionMode, CollectionPolicy,
    ColumnSecurity, OwnedCell, OwnedCellRow, Record, RedactionPolicy, StoredRecord, TypedCell,
    TypedRow, patch_json_object_text, splice_json_object_text,
};
pub use replication::{
    CommitFrame, DEFAULT_REPLICATION_STREAM_ID, REPLICATION_PROTOCOL_VERSION,
    ReplicationApplyReport, ReplicationApplyState, ReplicationConfig, ReplicationFrame,
    ReplicationMode, ReplicationOperationType, ReplicationRetentionStatus, ReplicationTlsConfig,
    ReplicationWatermark, ReplicationWrite,
};
pub use residency::{
    CollectionResidency, HnswResidency, IndexResidency, ResidencyReport, process_resident_bytes,
};
pub use resource_governor::{
    ResourceCapacity, ResourceDemand, ResourceGovernor, ResourceGovernorConfig,
    ResourceGovernorSnapshot, ResourceLane, ResourceLaneLimit, ResourceLaneSnapshot,
    ResourcePermit, ResourceUsage,
};
pub use spatial_packed::SpatialPackStrategy;
pub use storage::{CompressionConfig, FrameLogSalvage, SegmentReadMode, salvage_frame_log};
pub use sync::{OpType, SyncOp};
pub use sync_mesh::{
    EventEnvelope, EventId, NodeId, PeerSyncStatus, StreamId, SyncBundle, SyncBundleEvent,
    SyncCheckpoint, SyncExportReport, SyncImportReport, SyncMesh, SyncPendingChanges, SyncStatus,
    SyncVector,
};
pub use vector::{
    JsonFilter, ProfiledVectorSearch, VectorMetric, VectorSearchProfile, VectorSearchResult,
    cosine_similarity, dot_product, l2_distance,
};
