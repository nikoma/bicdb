//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;

impl BicDb {
    pub(crate) fn pitr_clear_collection_batch(
        &self,
        collection: &str,
        max_records: usize,
    ) -> Result<usize> {
        self.ensure_collection(collection)?;
        let mut record_ids = Vec::with_capacity(max_records.min(4_096));
        let streamed =
            self.for_each_record_batch_after(collection, None, max_records, |records| {
                record_ids.extend(records.into_iter().map(|record| record.id));
                Ok(false)
            })?;
        if !streamed {
            let state = self.collection_state(collection)?;
            record_ids.extend(
                state
                    .read_all()
                    .iter()
                    .flat_map(|shard| shard.records.values())
                    .take(max_records)
                    .map(|entry| entry.record.id.clone()),
            );
        }
        if record_ids.is_empty() {
            return Ok(0);
        }
        let removed = record_ids.len();
        let mut transaction = self.begin_transaction()?;
        transaction.bypass_commit_admission = true;
        transaction.disable_memory_jobs_on_commit();
        transaction.delete_many_unchecked(collection, record_ids)?;
        transaction.commit()?;
        Ok(removed)
    }

    pub(crate) fn pitr_apply_actions(&self, actions: Vec<PitrAuditAction>) -> Result<usize> {
        let applied = actions
            .iter()
            .filter(|action| action.operation.is_some())
            .count();
        if applied == 0 {
            return Ok(0);
        }
        let mut transaction = self.begin_transaction()?;
        transaction.bypass_commit_admission = true;
        transaction.disable_memory_jobs_on_commit();
        for action in actions {
            match action.operation {
                Some(PitrAuditOperation::Upsert(record)) => {
                    transaction.write_upserts_unchecked(&action.collection, vec![record])?;
                }
                Some(PitrAuditOperation::Delete(record_id)) => {
                    transaction.delete_many_unchecked(&action.collection, [record_id])?;
                }
                None => {}
            }
        }
        transaction.commit()?;
        Ok(applied)
    }

    pub fn search_vector(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        filter: Option<&JsonFilter>,
    ) -> Result<Vec<VectorSearchResult>> {
        self.search_vector_with_metric(collection, query, top_k, filter, VectorMetric::Cosine)
    }

    pub fn search_vector_exact(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
    ) -> Result<Vec<VectorSearchResult>> {
        self.search_vector_with_metric(collection, query, top_k, None, VectorMetric::Cosine)
    }

    pub fn search_vector_exact_with_metric(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        metric: VectorMetric,
    ) -> Result<Vec<VectorSearchResult>> {
        self.search_vector_with_metric(collection, query, top_k, None, metric)
    }

    pub fn create_vector_index(
        &mut self,
        collection: &str,
        config: HnswIndexConfig,
    ) -> Result<HnswIndexVerifyReport> {
        self.ensure_collection(collection)?;
        if self.hnsw_indexes.read().contains_key(collection) {
            return Err(BicDbError::Index(format!(
                "HNSW vector index for `{collection}` already exists"
            )));
        }
        self.rebuild_vector_index_with_config(collection, config)
    }

    pub fn rebuild_vector_index(&self, collection: &str) -> Result<HnswIndexVerifyReport> {
        let config = self
            .hnsw_indexes
            .read()
            .get(collection)
            .map(HnswIndex::config)
            .unwrap_or_default();
        self.rebuild_vector_index_with_config(collection, config)
    }

    pub fn verify_vector_index(&self, collection: &str) -> Result<HnswIndexVerifyReport> {
        self.ensure_collection(collection)?;
        let hnsw_indexes = self.hnsw_indexes.read();
        let index = hnsw_indexes.get(collection).ok_or_else(|| {
            BicDbError::Index(format!("HNSW vector index for `{collection}` not found"))
        })?;
        Ok(index.verify(
            collection,
            &self.hnsw_vectors_for_collection(collection)?,
            self.hnsw_index_size_bytes(collection)?,
        ))
    }

    pub fn has_vector_index(&self, collection: &str) -> bool {
        self.hnsw_indexes.read().contains_key(collection)
    }

    pub fn vector_index_memory_estimate_bytes(&self, collection: &str) -> Result<u64> {
        self.hnsw_indexes
            .read()
            .get(collection)
            .map(HnswIndex::memory_estimate_bytes)
            .ok_or_else(|| {
                BicDbError::Index(format!("HNSW vector index for `{collection}` not found"))
            })
    }

    pub fn vector_index_size_bytes(&self, collection: &str) -> Result<u64> {
        self.hnsw_index_size_bytes(collection)
    }

    pub fn search_vector_ann(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        ef_search: usize,
    ) -> Result<Vec<VectorSearchResult>> {
        let metric = self
            .hnsw_indexes
            .read()
            .get(collection)
            .map(|index| index.config().distance)
            .ok_or_else(|| {
                BicDbError::Index(format!("HNSW vector index for `{collection}` not found"))
            })?;
        self.search_vector_ann_with_metric(collection, query, top_k, ef_search, metric)
    }

    pub fn search_vector_ann_with_metric(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        ef_search: usize,
        metric: VectorMetric,
    ) -> Result<Vec<VectorSearchResult>> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        self.search_vector_ann_with_metric_unchecked(collection, query, top_k, ef_search, metric)
    }

    pub fn search_vector_ann_with_metric_cancellable(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        ef_search: usize,
        metric: VectorMetric,
        cancellation: &CancellationToken,
    ) -> Result<Vec<VectorSearchResult>> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        self.search_vector_ann_with_metric_unchecked_cancellable(
            collection,
            query,
            top_k,
            ef_search,
            metric,
            cancellation,
        )
    }

    pub(crate) fn search_vector_ann_with_metric_unchecked(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        ef_search: usize,
        metric: VectorMetric,
    ) -> Result<Vec<VectorSearchResult>> {
        self.search_vector_ann_with_metric_unchecked_cancellable(
            collection,
            query,
            top_k,
            ef_search,
            metric,
            &CancellationToken::uncancelable(),
        )
    }

    pub(crate) fn search_vector_ann_with_metric_unchecked_cancellable(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        ef_search: usize,
        metric: VectorMetric,
        cancellation: &CancellationToken,
    ) -> Result<Vec<VectorSearchResult>> {
        let state = self.collection_state(collection)?;
        cancellation.check()?;
        vector::validate_vector(query)?;
        if let Some(expected) = state.meta.vector_dim {
            if expected != query.len() {
                return Err(BicDbError::DimensionMismatch {
                    collection: collection.to_string(),
                    expected,
                    actual: query.len(),
                });
            }
        }
        let hnsw_indexes = self.hnsw_indexes.read();
        let index = hnsw_indexes.get(collection).ok_or_else(|| {
            BicDbError::Index(format!("HNSW vector index for `{collection}` not found"))
        })?;
        let hits =
            index.search_cancellable(collection, query, top_k, ef_search, metric, cancellation)?;
        cancellation.check()?;
        self.vector_hits_to_results(collection, hits)
    }

    pub fn search_vector_with_metric(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        filter: Option<&JsonFilter>,
        metric: VectorMetric,
    ) -> Result<Vec<VectorSearchResult>> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        self.search_vector_with_metric_unchecked(collection, query, top_k, filter, metric)
    }

    pub fn search_vector_with_metric_cancellable(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        filter: Option<&JsonFilter>,
        metric: VectorMetric,
        cancellation: &CancellationToken,
    ) -> Result<Vec<VectorSearchResult>> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        self.search_vector_with_metric_unchecked_cancellable(
            collection,
            query,
            top_k,
            filter,
            metric,
            cancellation,
        )
    }

    pub(crate) fn search_vector_with_metric_unchecked(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        filter: Option<&JsonFilter>,
        metric: VectorMetric,
    ) -> Result<Vec<VectorSearchResult>> {
        self.search_vector_with_metric_unchecked_cancellable(
            collection,
            query,
            top_k,
            filter,
            metric,
            &CancellationToken::uncancelable(),
        )
    }

    pub(crate) fn search_vector_with_metric_unchecked_cancellable(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        filter: Option<&JsonFilter>,
        metric: VectorMetric,
        cancellation: &CancellationToken,
    ) -> Result<Vec<VectorSearchResult>> {
        let state = self.collection_state(collection)?;
        cancellation.check()?;
        vector::validate_vector(query)?;
        if let Some(expected) = state.meta.vector_dim {
            if expected != query.len() {
                return Err(BicDbError::DimensionMismatch {
                    collection: collection.to_string(),
                    expected,
                    actual: query.len(),
                });
            }
        }

        // LAZY PAGED (Slice 3): no resident rows, no resident vector store —
        // exact search streams the page store in batches and keeps a top-k,
        // scored by the SAME vector::search machinery per batch so metric and
        // filter semantics cannot drift. Memory: one batch + k results.
        if state.paged_lazy {
            if let Some(paged) = &self.paged_records {
                let snapshot = paged.latest_snapshot();
                let mut best: Vec<VectorSearchResult> = Vec::new();
                paged.for_each_batch(&snapshot, collection, 4096, |batch| {
                    let stored = batch
                        .iter()
                        .map(StoredRecord::from_record)
                        .collect::<Result<Vec<_>>>()?;
                    let batch_best = vector::search_cancellable(
                        collection,
                        stored.iter(),
                        query,
                        top_k,
                        filter,
                        metric,
                        cancellation,
                    )?;
                    best.extend(batch_best);
                    best.sort_by(|left, right| {
                        right
                            .score
                            .partial_cmp(&left.score)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| left.record.id.cmp(&right.record.id))
                    });
                    best.truncate(top_k);
                    Ok(true)
                })?;
                return Ok(best);
            }
        }
        if filter.is_none() {
            // TODO(3a-review): merged record-map snapshot across shards to satisfy
            // the whole-collection `&RecordMap` vector-store API; behavior-preserving
            // but coarser than a per-shard form (a later stage can revisit).
            let records: RecordByPk = state
                .read_all()
                .iter()
                .flat_map(|shard| {
                    shard
                        .records
                        .values()
                        .map(|v| (v.record.id.clone(), v.clone()))
                })
                .collect();
            return state.vector_store.read().search_untimed_cancellable(
                collection,
                &records,
                query,
                top_k,
                metric,
                cancellation,
            );
        }

        let shards = state.read_all();
        vector::search_cancellable(
            collection,
            shards
                .iter()
                .flat_map(|shard| shard.records.values())
                .map(|entry| entry.record.as_ref()),
            query,
            top_k,
            filter,
            metric,
            cancellation,
        )
    }

    pub fn search_vector_record_scan_with_metric(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        filter: Option<&JsonFilter>,
        metric: VectorMetric,
    ) -> Result<Vec<VectorSearchResult>> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        self.search_vector_record_scan_with_metric_unchecked(
            collection, query, top_k, filter, metric,
        )
    }

    pub(crate) fn search_vector_record_scan_with_metric_unchecked(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        filter: Option<&JsonFilter>,
        metric: VectorMetric,
    ) -> Result<Vec<VectorSearchResult>> {
        let state = self.collection_state(collection)?;
        vector::validate_vector(query)?;
        if let Some(expected) = state.meta.vector_dim {
            if expected != query.len() {
                return Err(BicDbError::DimensionMismatch {
                    collection: collection.to_string(),
                    expected,
                    actual: query.len(),
                });
            }
        }

        let shards = state.read_all();
        vector::search(
            collection,
            shards
                .iter()
                .flat_map(|shard| shard.records.values())
                .map(|entry| entry.record.as_ref()),
            query,
            top_k,
            filter,
            metric,
        )
    }

    pub fn profile_vector_search(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        filter: Option<&JsonFilter>,
    ) -> Result<ProfiledVectorSearch> {
        self.profile_vector_search_with_metric(
            collection,
            query,
            top_k,
            filter,
            VectorMetric::Cosine,
        )
    }

    pub fn profile_vector_search_with_metric(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        filter: Option<&JsonFilter>,
        metric: VectorMetric,
    ) -> Result<ProfiledVectorSearch> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        self.profile_vector_search_with_metric_unchecked(collection, query, top_k, filter, metric)
    }

    pub(crate) fn profile_vector_search_with_metric_unchecked(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        filter: Option<&JsonFilter>,
        metric: VectorMetric,
    ) -> Result<ProfiledVectorSearch> {
        let state = self.collection_state(collection)?;
        vector::validate_vector(query)?;
        if let Some(expected) = state.meta.vector_dim {
            if expected != query.len() {
                return Err(BicDbError::DimensionMismatch {
                    collection: collection.to_string(),
                    expected,
                    actual: query.len(),
                });
            }
        }

        if filter.is_none() {
            // TODO(3a-review): merged record-map snapshot across shards to satisfy
            // the whole-collection `&RecordMap` vector-store API; behavior-preserving
            // but coarser than a per-shard form (a later stage can revisit).
            let records: RecordByPk = state
                .read_all()
                .iter()
                .flat_map(|shard| {
                    shard
                        .records
                        .values()
                        .map(|v| (v.record.id.clone(), v.clone()))
                })
                .collect();
            return state
                .vector_store
                .read()
                .profile_search(collection, &records, query, top_k, metric);
        }

        let shards = state.read_all();
        vector::profile_search(
            collection,
            shards
                .iter()
                .flat_map(|shard| shard.records.values())
                .map(|entry| entry.record.as_ref()),
            query,
            top_k,
            filter,
            metric,
        )
    }

    pub fn profile_vector_search_record_scan_with_metric(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        filter: Option<&JsonFilter>,
        metric: VectorMetric,
    ) -> Result<ProfiledVectorSearch> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        self.profile_vector_search_record_scan_with_metric_unchecked(
            collection, query, top_k, filter, metric,
        )
    }

    pub(crate) fn profile_vector_search_record_scan_with_metric_unchecked(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        filter: Option<&JsonFilter>,
        metric: VectorMetric,
    ) -> Result<ProfiledVectorSearch> {
        let state = self.collection_state(collection)?;
        vector::validate_vector(query)?;
        if let Some(expected) = state.meta.vector_dim {
            if expected != query.len() {
                return Err(BicDbError::DimensionMismatch {
                    collection: collection.to_string(),
                    expected,
                    actual: query.len(),
                });
            }
        }

        let shards = state.read_all();
        vector::profile_search(
            collection,
            shards
                .iter()
                .flat_map(|shard| shard.records.values())
                .map(|entry| entry.record.as_ref()),
            query,
            top_k,
            filter,
            metric,
        )
    }

    pub fn scan_time_range(
        &self,
        collection: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<Record>> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        self.scan_time_range_unchecked(collection, start_ts, end_ts)
    }

    pub(crate) fn scan_time_range_unchecked(
        &self,
        collection: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<Record>> {
        if start_ts > end_ts {
            return Err(BicDbError::InvalidTimeRange { start_ts, end_ts });
        }

        let state = self.collection_state(collection)?;
        let mut records = state
            .read_all()
            .iter()
            .flat_map(|shard| shard.records.values())
            .filter_map(|entry| {
                let timestamp = entry.record.timestamp?;
                if timestamp >= start_ts && timestamp <= end_ts {
                    entry.record.to_record().ok()
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        records.sort_by(|left, right| {
            left.timestamp
                .cmp(&right.timestamp)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(records)
    }

    pub fn latest_value_per_device(
        &self,
        collection: &str,
        device_id: &str,
        metric: &str,
    ) -> Result<Option<Record>> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        let state = self.collection_state(collection)?;
        let mut best: Option<Record> = None;
        for shard in state.read_all() {
            for entry in shard.records.values() {
                if entry.record.timestamp.is_none() {
                    continue;
                }
                let record = entry.record.to_record()?;
                if metadata_string(&record.metadata, "device_id") == Some(device_id)
                    && metadata_string(&record.metadata, "metric") == Some(metric)
                    && best
                        .as_ref()
                        .is_none_or(|best| best.timestamp <= record.timestamp)
                {
                    best = Some(record);
                }
            }
        }
        Ok(best)
    }

    pub fn time_series_summary(
        &self,
        collection: &str,
        filter: &TimeSeriesFilter,
    ) -> Result<NumericSummary> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        if filter.start_ts > filter.end_ts {
            return Err(BicDbError::InvalidTimeRange {
                start_ts: filter.start_ts,
                end_ts: filter.end_ts,
            });
        }

        let state = self.collection_state(collection)?;
        let materialized = state
            .read_all()
            .iter()
            .flat_map(|shard| shard.records.values())
            .map(|entry| entry.record.to_record())
            .collect::<Result<Vec<_>>>()?;
        Ok(query_exec::aggregate(materialized.iter(), filter))
    }

    pub fn pending_sync_ops(&self) -> Vec<SyncOp> {
        self.sync_log.lock().pending_ops()
    }

    pub fn mark_synced(&mut self, op_ids: &[Uuid]) -> Result<()> {
        self.sync_log.lock().mark_synced(op_ids)
    }

    pub fn collections(&self) -> Vec<CollectionMeta> {
        let mut collections = self
            .collections
            .values()
            .map(|state| state.read().meta.clone())
            .collect::<Vec<_>>();
        collections.sort_by(|left, right| left.name.cmp(&right.name));
        collections
    }

    /// Canonical compatibility identity for collection policy, executable
    /// indexes, and the structural SQL catalogs that every range replica must
    /// understand identically. Volatile rows (roles, sequence values,
    /// migration history, notifications) are intentionally excluded.
    pub fn schema_compatibility_fingerprint(&self) -> Result<SchemaCompatibilityFingerprint> {
        #[derive(Serialize)]
        struct StructuralRecord {
            collection: String,
            id: String,
            metadata: Value,
        }

        #[derive(Serialize)]
        struct Payload {
            format_version: u32,
            user_collections: Vec<CollectionMeta>,
            indexes: Vec<IndexDefinition>,
            structural_records: Vec<StructuralRecord>,
        }

        let schema_generation = self.schema_compatibility.generation();
        let user_collections = self
            .collections()
            .into_iter()
            .filter(|meta| !meta.name.starts_with("__bicdb_"))
            .collect::<Vec<_>>();
        let indexes = self.index_definitions();
        let mut structural_records = Vec::new();
        for collection in CLUSTER_SCHEMA_CATALOG_COLLECTIONS {
            let records = match self.scan_collection(collection) {
                Ok(records) => records,
                Err(BicDbError::CollectionNotFound(_)) => continue,
                Err(error) => return Err(error),
            };
            structural_records.extend(records.into_iter().map(|record| StructuralRecord {
                collection: (*collection).to_string(),
                id: record.id,
                metadata: record.metadata,
            }));
        }
        structural_records.sort_by(|left, right| {
            (&left.collection, &left.id).cmp(&(&right.collection, &right.id))
        });
        let payload = Payload {
            format_version: SCHEMA_COMPATIBILITY_FORMAT_VERSION,
            user_collections,
            indexes,
            structural_records,
        };
        let structural_catalog_record_count = payload.structural_records.len();
        let user_collection_count = payload.user_collections.len();
        let index_count = payload.indexes.len();
        let sha256 = hex::encode(sha2::Sha256::digest(serde_json::to_vec(&payload)?));
        if !self
            .schema_compatibility
            .publish_verified(schema_generation, &sha256)
        {
            return Err(BicDbError::Cluster(
                "schema changed while computing compatibility fingerprint; retry after the current schema operation completes"
                    .to_string(),
            ));
        }
        Ok(SchemaCompatibilityFingerprint {
            format_version: SCHEMA_COMPATIBILITY_FORMAT_VERSION,
            sha256,
            user_collection_count,
            index_count,
            structural_catalog_record_count,
        })
    }

    pub fn schema_compatibility_authority(&self) -> SchemaCompatibilityAuthority {
        self.schema_compatibility.clone()
    }

    pub fn cluster_schema_bundle(&self) -> Result<ClusterSchemaBundle> {
        let fingerprint = self.schema_compatibility_fingerprint()?;
        let indexes = self.index_definitions();
        let indexed_collections = indexes
            .iter()
            .map(|index| index.collection.as_str())
            .collect::<BTreeSet<_>>();
        let collections = self
            .collections()
            .into_iter()
            .filter(|meta| {
                !meta.name.starts_with("__bicdb_")
                    || CLUSTER_SCHEMA_CATALOG_COLLECTIONS.contains(&meta.name.as_str())
                    || indexed_collections.contains(meta.name.as_str())
            })
            .collect::<Vec<_>>();
        let mut structural_records = Vec::new();
        for collection in CLUSTER_SCHEMA_CATALOG_COLLECTIONS {
            match self.scan_collection(collection) {
                Ok(records) => {
                    structural_records.extend(records.into_iter().map(|record| {
                        ClusterSchemaCatalogRecord {
                            collection: (*collection).to_string(),
                            record,
                        }
                    }));
                }
                Err(BicDbError::CollectionNotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        structural_records.sort_by(|left, right| {
            (&left.collection, &left.record.id).cmp(&(&right.collection, &right.record.id))
        });
        let mut bundle = ClusterSchemaBundle {
            format_version: CLUSTER_SCHEMA_BUNDLE_FORMAT_VERSION,
            fingerprint,
            collections,
            indexes,
            structural_records,
            checksum_sha256: String::new(),
        };
        bundle.checksum_sha256 = bundle.calculate_checksum()?;
        bundle.verify()?;
        Ok(bundle)
    }

    pub(crate) fn additive_cluster_schema_plan(
        &self,
        desired: &ClusterSchemaBundle,
        limits: ClusterSchemaStageLimits,
    ) -> Result<ClusterSchemaOnlinePlan> {
        limits.validate()?;
        desired.verify()?;
        let current = self.cluster_schema_bundle()?;
        if current.fingerprint.sha256 == desired.fingerprint.sha256 {
            return Err(BicDbError::Cluster(
                "cluster schema bundle is already active".to_string(),
            ));
        }

        let current_collections = current
            .collections
            .iter()
            .map(|meta| (&meta.name, meta))
            .collect::<BTreeMap<_, _>>();
        let desired_collections = desired
            .collections
            .iter()
            .map(|meta| (&meta.name, meta))
            .collect::<BTreeMap<_, _>>();
        for (name, existing) in &current_collections {
            match desired_collections.get(name) {
                Some(candidate) if *candidate == *existing => {}
                Some(_) => {
                    return Err(BicDbError::Cluster(format!(
                        "online cluster schema staging rejects collection metadata replacement `{name}`; use an explicit migration edition"
                    )));
                }
                None => {
                    return Err(BicDbError::Cluster(format!(
                        "online cluster schema staging rejects collection removal `{name}`; use an explicit contraction edition"
                    )));
                }
            }
        }

        let current_indexes = current
            .indexes
            .iter()
            .map(|index| (&index.name, index))
            .collect::<BTreeMap<_, _>>();
        let desired_indexes = desired
            .indexes
            .iter()
            .map(|index| (&index.name, index))
            .collect::<BTreeMap<_, _>>();
        for (name, existing) in &current_indexes {
            match desired_indexes.get(name) {
                Some(candidate) if *candidate == *existing => {}
                Some(_) => {
                    return Err(BicDbError::Cluster(format!(
                        "online cluster schema staging rejects index replacement `{name}`; build a new generation under a new name"
                    )));
                }
                None => {
                    return Err(BicDbError::Cluster(format!(
                        "online cluster schema staging rejects index removal `{name}`; use an explicit contraction edition"
                    )));
                }
            }
        }

        let current_records = current
            .structural_records
            .iter()
            .map(|entry| ((&entry.collection, &entry.record.id), &entry.record))
            .collect::<BTreeMap<_, _>>();
        let desired_records = desired
            .structural_records
            .iter()
            .map(|entry| ((&entry.collection, &entry.record.id), &entry.record))
            .collect::<BTreeMap<_, _>>();
        for (key, existing) in &current_records {
            match desired_records.get(key) {
                Some(candidate) if *candidate == *existing => {}
                Some(_) => {
                    return Err(BicDbError::Cluster(format!(
                        "online cluster schema staging rejects structural record replacement `{}/{}`; use an explicit migration edition",
                        key.0, key.1
                    )));
                }
                None => {
                    return Err(BicDbError::Cluster(format!(
                        "online cluster schema staging rejects structural record removal `{}/{}`; use an explicit contraction edition",
                        key.0, key.1
                    )));
                }
            }
        }

        let plan = ClusterSchemaOnlinePlan {
            base_fingerprint_sha256: current.fingerprint.sha256,
            target_fingerprint_sha256: desired.fingerprint.sha256.clone(),
            collection_additions: desired_collections
                .len()
                .saturating_sub(current_collections.len()),
            index_additions: desired_indexes.len().saturating_sub(current_indexes.len()),
            structural_record_additions: desired_records
                .len()
                .saturating_sub(current_records.len()),
        };
        if plan.total_changes() == 0 || plan.total_changes() > limits.max_changes {
            return Err(BicDbError::Cluster(format!(
                "online cluster schema plan has {} changes outside the configured 1..={} bound",
                plan.total_changes(),
                limits.max_changes
            )));
        }
        Ok(plan)
    }

    /// Verify and durably stage a signed, strictly additive schema bundle.
    /// Staging is intentionally non-mutating: non-empty voters keep serving
    /// their current schema until a later coordinated activation protocol has
    /// fenced and acknowledged every participant.
    pub fn stage_signed_cluster_schema_bundle(
        &mut self,
        signed_bundle: SignedClusterSchemaBundle,
        trusted_keys: &BTreeMap<String, [u8; 32]>,
        limits: ClusterSchemaStageLimits,
        now_ms: u64,
    ) -> Result<ClusterSchemaStageState> {
        self.ensure_writable("stage signed cluster schema bundle")?;
        limits.validate()?;
        signed_bundle.verify(trusted_keys)?;
        let plan = self.additive_cluster_schema_plan(&signed_bundle.bundle, limits)?;
        let path = self.path.join(DEFAULT_CLUSTER_SCHEMA_STAGE);
        if path.exists() {
            let existing = self.load_cluster_schema_stage(trusted_keys, limits)?;
            if existing.signed_bundle == signed_bundle
                && existing.plan == plan
                && existing.plan.base_fingerprint_sha256
                    == self.schema_compatibility_fingerprint()?.sha256
            {
                return Ok(existing);
            }
            return Err(BicDbError::Cluster(
                "another signed cluster schema bundle is already staged".to_string(),
            ));
        }
        let mut state = ClusterSchemaStageState {
            format_version: CLUSTER_SCHEMA_STAGE_FORMAT_VERSION,
            stage_id: Uuid::now_v7(),
            signed_bundle,
            plan,
            created_at_ms: now_ms,
            checksum_sha256: String::new(),
        };
        state.refresh_checksum()?;
        state.verify(trusted_keys, limits)?;
        let bytes = serde_json::to_vec(&state)?;
        if bytes.len() as u64 > limits.max_state_bytes {
            return Err(BicDbError::Cluster(
                "cluster schema stage exceeds its durable byte bound".to_string(),
            ));
        }
        storage::write_atomic(&path, &bytes, self.config.fsync)?;
        Ok(state)
    }

    pub fn load_cluster_schema_stage(
        &self,
        trusted_keys: &BTreeMap<String, [u8; 32]>,
        limits: ClusterSchemaStageLimits,
    ) -> Result<ClusterSchemaStageState> {
        limits.validate()?;
        let path = self.path.join(DEFAULT_CLUSTER_SCHEMA_STAGE);
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() == 0
            || metadata.len() > limits.max_state_bytes
        {
            return Err(BicDbError::Cluster(
                "cluster schema stage is unsafe or outside its byte bound".to_string(),
            ));
        }
        #[cfg(unix)]
        let file = {
            use std::os::unix::fs::OpenOptionsExt;
            fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&path)?
        };
        #[cfg(not(unix))]
        let file = File::open(&path)?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take(limits.max_state_bytes.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 != metadata.len() || bytes.len() as u64 > limits.max_state_bytes {
            return Err(BicDbError::Cluster(
                "cluster schema stage changed or grew while reading".to_string(),
            ));
        }
        let state: ClusterSchemaStageState = serde_json::from_slice(&bytes)?;
        state.verify(trusted_keys, limits)?;
        Ok(state)
    }

    /// Verify the node-local authority used to admit old-schema operations
    /// while a signed additive target is being installed. A non-base live
    /// fingerprint also requires the matching durable activation checkpoint.
    pub fn verify_cluster_schema_compatibility_window(
        &self,
        trusted_keys: &BTreeMap<String, [u8; 32]>,
        limits: ClusterSchemaActivationLimits,
    ) -> Result<ClusterSchemaCompatibilityWindow> {
        limits.validate()?;
        let stage = self.load_cluster_schema_stage(trusted_keys, limits.stage)?;
        let live = self.schema_compatibility_fingerprint()?;
        self.validate_cluster_schema_activation_prefix(&stage, limits.stage)?;
        if live.sha256 != stage.plan.base_fingerprint_sha256 {
            let activation = self.load_cluster_schema_activation(trusted_keys, limits)?;
            if activation.stage_id != stage.stage_id
                || activation.target_fingerprint_sha256 != stage.plan.target_fingerprint_sha256
            {
                return Err(BicDbError::Cluster(
                    "cluster schema compatibility window activation identity is invalid"
                        .to_string(),
                ));
            }
        }
        Ok(ClusterSchemaCompatibilityWindow {
            stage_id: stage.stage_id,
            base_fingerprint_sha256: stage.plan.base_fingerprint_sha256,
            target_fingerprint_sha256: stage.plan.target_fingerprint_sha256,
            verified_live_fingerprint_sha256: live.sha256,
        })
    }

    pub fn discard_cluster_schema_stage(
        &self,
        trusted_keys: &BTreeMap<String, [u8; 32]>,
        limits: ClusterSchemaStageLimits,
    ) -> Result<bool> {
        let path = self.path.join(DEFAULT_CLUSTER_SCHEMA_STAGE);
        if !path.exists() {
            return Ok(false);
        }
        let stage = self.load_cluster_schema_stage(trusted_keys, limits)?;
        if self.path.join(DEFAULT_CLUSTER_SCHEMA_ACTIVATION).exists() {
            return Err(BicDbError::Cluster(
                "cluster schema stage has an activation checkpoint; abort or resume it explicitly"
                    .to_string(),
            ));
        }
        let current = self.schema_compatibility_fingerprint()?;
        let unchanged_plan = if current.sha256 == stage.plan.base_fingerprint_sha256 {
            self.additive_cluster_schema_plan(&stage.signed_bundle.bundle, limits)
                .ok()
        } else {
            None
        };
        if unchanged_plan.as_ref() != Some(&stage.plan) {
            return Err(BicDbError::Cluster(
                "cluster schema stage cannot be discarded after additive activation changed the live schema"
                    .to_string(),
            ));
        }
        fs::remove_file(path)?;
        Ok(true)
    }

    /// Create the durable node-local activation cursor for an exact staged
    /// bundle. The first call is permitted only while the live schema is still
    /// the staged base. Reopening an existing activation accepts only a strict
    /// additive prefix of that same signed target.
    pub fn begin_cluster_schema_activation(
        &mut self,
        trusted_keys: &BTreeMap<String, [u8; 32]>,
        limits: ClusterSchemaActivationLimits,
        now_ms: u64,
    ) -> Result<ClusterSchemaActivationState> {
        self.ensure_writable("begin cluster schema activation")?;
        limits.validate()?;
        let stage = self.load_cluster_schema_stage(trusted_keys, limits.stage)?;
        let path = self.path.join(DEFAULT_CLUSTER_SCHEMA_ACTIVATION);
        if path.exists() {
            let existing = self.load_cluster_schema_activation(trusted_keys, limits)?;
            self.validate_cluster_schema_activation_prefix(&stage, limits.stage)?;
            return Ok(existing);
        }
        let current = self.schema_compatibility_fingerprint()?;
        if current.sha256 != stage.plan.base_fingerprint_sha256 {
            return Err(BicDbError::Cluster(format!(
                "cluster schema activation expected base {}, found {}",
                stage.plan.base_fingerprint_sha256, current.sha256
            )));
        }
        let exact_plan =
            self.additive_cluster_schema_plan(&stage.signed_bundle.bundle, limits.stage)?;
        if exact_plan != stage.plan {
            return Err(BicDbError::Cluster(
                "cluster schema activation stage plan no longer matches the live base".to_string(),
            ));
        }
        self.ensure_no_pending_transactions()?;
        let mut state = ClusterSchemaActivationState {
            format_version: CLUSTER_SCHEMA_ACTIVATION_FORMAT_VERSION,
            activation_id: Uuid::now_v7(),
            stage_id: stage.stage_id,
            phase: ClusterSchemaActivationPhase::Collections,
            collection_cursor: 0,
            structural_record_cursor: 0,
            index_cursor: 0,
            target_fingerprint_sha256: stage.plan.target_fingerprint_sha256.clone(),
            limits,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            checksum_sha256: String::new(),
        };
        state.normalize_phase(&stage.signed_bundle.bundle);
        state.refresh_checksum()?;
        state.verify(&stage, limits)?;
        self.persist_cluster_schema_activation(&state)?;
        Ok(state)
    }

    pub fn load_cluster_schema_activation(
        &self,
        trusted_keys: &BTreeMap<String, [u8; 32]>,
        limits: ClusterSchemaActivationLimits,
    ) -> Result<ClusterSchemaActivationState> {
        limits.validate()?;
        let stage = self.load_cluster_schema_stage(trusted_keys, limits.stage)?;
        let path = self.path.join(DEFAULT_CLUSTER_SCHEMA_ACTIVATION);
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() == 0
            || metadata.len() > limits.max_state_bytes
        {
            return Err(BicDbError::Cluster(
                "cluster schema activation is unsafe or outside its byte bound".to_string(),
            ));
        }
        #[cfg(unix)]
        let file = {
            use std::os::unix::fs::OpenOptionsExt;
            fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&path)?
        };
        #[cfg(not(unix))]
        let file = File::open(&path)?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take(limits.max_state_bytes.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 != metadata.len() || bytes.len() as u64 > limits.max_state_bytes {
            return Err(BicDbError::Cluster(
                "cluster schema activation changed or grew while reading".to_string(),
            ));
        }
        let state: ClusterSchemaActivationState = serde_json::from_slice(&bytes)?;
        state.verify(&stage, limits)?;
        Ok(state)
    }

    /// Apply at most one additive schema change while inspecting no more than
    /// `max_items_per_step` signed bundle entries. A crash after the schema
    /// mutation but before the cursor checkpoint is safe: the next step sees
    /// the exact existing object and checkpoints it without recreating it.
    pub fn advance_cluster_schema_activation(
        &mut self,
        trusted_keys: &BTreeMap<String, [u8; 32]>,
        limits: ClusterSchemaActivationLimits,
        now_ms: u64,
    ) -> Result<ClusterSchemaActivationAdvance> {
        self.ensure_writable("advance cluster schema activation")?;
        limits.validate()?;
        let stage = self.load_cluster_schema_stage(trusted_keys, limits.stage)?;
        let mut state = self.load_cluster_schema_activation(trusted_keys, limits)?;
        if now_ms < state.updated_at_ms {
            return Err(BicDbError::Cluster(
                "cluster schema activation clock regressed".to_string(),
            ));
        }
        let bundle = &stage.signed_bundle.bundle;
        let current = self.schema_compatibility_fingerprint()?;
        if current.sha256 == bundle.fingerprint.sha256 {
            state.collection_cursor = bundle.collections.len();
            state.structural_record_cursor = bundle.structural_records.len();
            state.index_cursor = bundle.indexes.len();
            state.phase = ClusterSchemaActivationPhase::Complete;
            state.updated_at_ms = now_ms;
            state.refresh_checksum()?;
            state.verify(&stage, limits)?;
            self.persist_cluster_schema_activation(&state)?;
            return Ok(ClusterSchemaActivationAdvance {
                state,
                inspected_items: 0,
                applied_change: false,
            });
        }
        if state.phase == ClusterSchemaActivationPhase::Complete {
            return Err(BicDbError::Cluster(format!(
                "completed cluster schema activation expected {}, found {}",
                bundle.fingerprint.sha256, current.sha256
            )));
        }
        self.validate_cluster_schema_activation_prefix(&stage, limits.stage)?;

        let mut inspected_items = 0usize;
        let mut applied_change = false;
        while inspected_items < limits.max_items_per_step
            && !applied_change
            && state.phase != ClusterSchemaActivationPhase::Complete
        {
            match state.phase {
                ClusterSchemaActivationPhase::Collections => {
                    let desired = &bundle.collections[state.collection_cursor];
                    inspected_items += 1;
                    match self.collections.get(&desired.name) {
                        Some(existing) if existing.read().meta == *desired => {}
                        Some(_) => {
                            return Err(BicDbError::Cluster(format!(
                                "cluster schema activation found conflicting collection `{}`",
                                desired.name
                            )));
                        }
                        None => {
                            self.ensure_no_pending_transactions()?;
                            self.collections.insert(
                                desired.name.clone(),
                                RwLock::new(CollectionState::new(desired.clone())),
                            );
                            if let Err(error) = self.persist_catalog() {
                                self.collections.remove(&desired.name);
                                return Err(error);
                            }
                            self.schema_compatibility.invalidate();
                            applied_change = true;
                        }
                    }
                    state.collection_cursor += 1;
                }
                ClusterSchemaActivationPhase::StructuralRecords => {
                    let desired = &bundle.structural_records[state.structural_record_cursor];
                    inspected_items += 1;
                    match self.get(&desired.collection, &desired.record.id)? {
                        Some(existing) if existing.as_ref() == &desired.record => {}
                        Some(_) => {
                            return Err(BicDbError::Cluster(format!(
                                "cluster schema activation found conflicting structural record `{}/{}`",
                                desired.collection, desired.record.id
                            )));
                        }
                        None => {
                            self.ensure_no_pending_transactions()?;
                            let mut tx = self.begin_transaction()?;
                            tx.bypass_commit_admission = true;
                            tx.write_upserts_unchecked(
                                &desired.collection,
                                vec![desired.record.clone()],
                            )?;
                            tx.commit()?;
                            applied_change = true;
                        }
                    }
                    state.structural_record_cursor += 1;
                }
                ClusterSchemaActivationPhase::Indexes => {
                    let desired = &bundle.indexes[state.index_cursor];
                    inspected_items += 1;
                    match self.indexes.get(&desired.name) {
                        Some(existing) if existing.read().definition == *desired => {}
                        Some(_) => {
                            return Err(BicDbError::Cluster(format!(
                                "cluster schema activation found conflicting index `{}`",
                                desired.name
                            )));
                        }
                        None => {
                            self.ensure_no_pending_transactions()?;
                            self.create_index_online(desired.clone())?;
                            applied_change = true;
                        }
                    }
                    state.index_cursor += 1;
                }
                ClusterSchemaActivationPhase::Complete => break,
            }
            state.normalize_phase(bundle);
        }

        if state.phase == ClusterSchemaActivationPhase::Complete {
            let installed = self.schema_compatibility_fingerprint()?;
            if installed.sha256 != bundle.fingerprint.sha256 {
                return Err(BicDbError::Cluster(format!(
                    "cluster schema activation produced {}, expected {}; node remains fenced",
                    installed.sha256, bundle.fingerprint.sha256
                )));
            }
        }
        state.updated_at_ms = now_ms;
        state.refresh_checksum()?;
        state.verify(&stage, limits)?;
        self.persist_cluster_schema_activation(&state)?;
        Ok(ClusterSchemaActivationAdvance {
            state,
            inspected_items,
            applied_change,
        })
    }

    /// Remove an activation checkpoint only while no additive change has
    /// altered the staged base. Once any change is durable, resumption is the
    /// safe recovery path; silently abandoning a prefix is forbidden.
    pub fn abort_cluster_schema_activation(
        &self,
        trusted_keys: &BTreeMap<String, [u8; 32]>,
        limits: ClusterSchemaActivationLimits,
    ) -> Result<bool> {
        let path = self.path.join(DEFAULT_CLUSTER_SCHEMA_ACTIVATION);
        if !path.exists() {
            return Ok(false);
        }
        let stage = self.load_cluster_schema_stage(trusted_keys, limits.stage)?;
        self.load_cluster_schema_activation(trusted_keys, limits)?;
        let current = self.schema_compatibility_fingerprint()?;
        let unchanged_plan = if current.sha256 == stage.plan.base_fingerprint_sha256 {
            self.additive_cluster_schema_plan(&stage.signed_bundle.bundle, limits.stage)
                .ok()
        } else {
            None
        };
        if unchanged_plan.as_ref() != Some(&stage.plan) {
            return Err(BicDbError::Cluster(
                "cluster schema activation changed the live schema and must be resumed, not aborted"
                    .to_string(),
            ));
        }
        fs::remove_file(path)?;
        Ok(true)
    }

    pub(crate) fn validate_cluster_schema_activation_prefix(
        &self,
        stage: &ClusterSchemaStageState,
        limits: ClusterSchemaStageLimits,
    ) -> Result<()> {
        let current = self.schema_compatibility_fingerprint()?;
        if current.sha256 == stage.plan.target_fingerprint_sha256 {
            return Ok(());
        }
        let remaining = self.additive_cluster_schema_plan(&stage.signed_bundle.bundle, limits)?;
        if remaining.target_fingerprint_sha256 != stage.plan.target_fingerprint_sha256 {
            return Err(BicDbError::Cluster(
                "cluster schema activation prefix targets a different fingerprint".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn persist_cluster_schema_activation(
        &self,
        state: &ClusterSchemaActivationState,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(state)?;
        if bytes.len() as u64 > state.limits.max_state_bytes {
            return Err(BicDbError::Cluster(
                "cluster schema activation exceeds its durable byte bound".to_string(),
            ));
        }
        storage::write_atomic(
            &self.path.join(DEFAULT_CLUSTER_SCHEMA_ACTIVATION),
            &bytes,
            self.config.fsync,
        )
    }

    /// Verify an exact completed activation, durably retain a compact proof,
    /// and then remove the large signed stage and activation cursor. The proof
    /// is written first, so interruption can only leave safely retryable
    /// checkpoint files behind. Repeating the exact request returns the same
    /// proof and finishes any interrupted cleanup.
    #[allow(clippy::too_many_arguments)]
    pub fn finalize_cluster_schema_activation(
        &mut self,
        trusted_keys: &BTreeMap<String, [u8; 32]>,
        limits: ClusterSchemaActivationLimits,
        rollout_id: Uuid,
        expected_stage_id: Uuid,
        expected_activation_id: Uuid,
        expected_target_fingerprint_sha256: &str,
        expected_activation_state_checksum_sha256: &str,
        now_ms: u64,
    ) -> Result<ClusterSchemaFinalizationState> {
        self.ensure_writable("finalize cluster schema activation")?;
        limits.validate()?;
        let finalization_path = self.path.join(DEFAULT_CLUSTER_SCHEMA_FINALIZATION);
        if path_entry_exists(&finalization_path)? {
            let existing = self.load_cluster_schema_finalization(limits)?;
            if existing.matches_request(
                rollout_id,
                expected_stage_id,
                expected_activation_id,
                expected_target_fingerprint_sha256,
                expected_activation_state_checksum_sha256,
            ) {
                self.cleanup_finalized_cluster_schema_files(&existing, trusted_keys, limits)?;
                return Ok(existing);
            }
        }

        let stage = self.load_cluster_schema_stage(trusted_keys, limits.stage)?;
        let activation = self.load_cluster_schema_activation(trusted_keys, limits)?;
        let live = self.schema_compatibility_fingerprint()?;
        if rollout_id.is_nil()
            || stage.stage_id != expected_stage_id
            || activation.activation_id != expected_activation_id
            || activation.stage_id != expected_stage_id
            || activation.phase != ClusterSchemaActivationPhase::Complete
            || stage.plan.target_fingerprint_sha256 != expected_target_fingerprint_sha256
            || activation.target_fingerprint_sha256 != expected_target_fingerprint_sha256
            || live.sha256 != expected_target_fingerprint_sha256
            || activation.checksum_sha256 != expected_activation_state_checksum_sha256
            || now_ms < activation.updated_at_ms
        {
            return Err(BicDbError::Cluster(
                "cluster schema finalization does not match an exact completed live activation"
                    .to_string(),
            ));
        }

        let mut state = ClusterSchemaFinalizationState {
            format_version: CLUSTER_SCHEMA_FINALIZATION_FORMAT_VERSION,
            rollout_id,
            stage_id: stage.stage_id,
            activation_id: activation.activation_id,
            target_fingerprint_sha256: expected_target_fingerprint_sha256.to_string(),
            activation_state_checksum_sha256: activation.checksum_sha256,
            finalized_at_ms: now_ms,
            checksum_sha256: String::new(),
        };
        state.refresh_checksum()?;
        state.verify(limits)?;
        storage::write_atomic(
            &finalization_path,
            &serde_json::to_vec(&state)?,
            self.config.fsync,
        )?;
        self.cleanup_finalized_cluster_schema_files(&state, trusted_keys, limits)?;
        Ok(state)
    }

    pub fn load_cluster_schema_finalization(
        &self,
        limits: ClusterSchemaActivationLimits,
    ) -> Result<ClusterSchemaFinalizationState> {
        limits.validate()?;
        let path = self.path.join(DEFAULT_CLUSTER_SCHEMA_FINALIZATION);
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() == 0
            || metadata.len() > limits.max_state_bytes
        {
            return Err(BicDbError::Cluster(
                "cluster schema finalization is unsafe or outside its byte bound".to_string(),
            ));
        }
        #[cfg(unix)]
        let file = {
            use std::os::unix::fs::OpenOptionsExt;
            fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&path)?
        };
        #[cfg(not(unix))]
        let file = File::open(&path)?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take(limits.max_state_bytes.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 != metadata.len() || bytes.len() as u64 > limits.max_state_bytes {
            return Err(BicDbError::Cluster(
                "cluster schema finalization changed or grew while reading".to_string(),
            ));
        }
        let state: ClusterSchemaFinalizationState = serde_json::from_slice(&bytes)?;
        state.verify(limits)?;
        Ok(state)
    }

    pub(crate) fn cleanup_finalized_cluster_schema_files(
        &self,
        state: &ClusterSchemaFinalizationState,
        trusted_keys: &BTreeMap<String, [u8; 32]>,
        limits: ClusterSchemaActivationLimits,
    ) -> Result<()> {
        let activation_path = self.path.join(DEFAULT_CLUSTER_SCHEMA_ACTIVATION);
        let stage_path = self.path.join(DEFAULT_CLUSTER_SCHEMA_STAGE);
        if path_entry_exists(&activation_path)? {
            let activation = self.load_cluster_schema_activation(trusted_keys, limits)?;
            if activation.stage_id != state.stage_id
                || activation.activation_id != state.activation_id
                || activation.phase != ClusterSchemaActivationPhase::Complete
                || activation.target_fingerprint_sha256 != state.target_fingerprint_sha256
                || activation.checksum_sha256 != state.activation_state_checksum_sha256
            {
                return Err(BicDbError::Cluster(
                    "cluster schema cleanup activation differs from durable finalization proof"
                        .to_string(),
                ));
            }
            fs::remove_file(&activation_path)?;
            if self.config.fsync {
                sync_parent_dir(&activation_path)?;
            }
        }
        if path_entry_exists(&stage_path)? {
            let stage = self.load_cluster_schema_stage(trusted_keys, limits.stage)?;
            if stage.stage_id != state.stage_id
                || stage.plan.target_fingerprint_sha256 != state.target_fingerprint_sha256
            {
                return Err(BicDbError::Cluster(
                    "cluster schema cleanup stage differs from durable finalization proof"
                        .to_string(),
                ));
            }
            fs::remove_file(&stage_path)?;
            if self.config.fsync {
                sync_parent_dir(&stage_path)?;
            }
        }
        Ok(())
    }

    /// Install a leader-authenticated schema on an empty prospective voter.
    /// The node remains excluded from placement until the resulting live
    /// fingerprint is advertised through metadata consensus, so interruption
    /// can leave only a quarantined, safely resumable partial installation.
    pub fn install_cluster_schema_bundle(&mut self, bundle: &ClusterSchemaBundle) -> Result<bool> {
        self.ensure_writable("install cluster schema bundle")?;
        bundle.verify()?;
        if self.schema_compatibility_fingerprint()?.sha256 == bundle.fingerprint.sha256 {
            return Ok(false);
        }
        for meta in self
            .collections()
            .into_iter()
            .filter(|meta| !meta.name.starts_with("__bicdb_"))
        {
            if !self.scan_collection_unchecked(&meta.name)?.is_empty() {
                return Err(BicDbError::Cluster(format!(
                    "schema bootstrap rejected: non-empty collection `{}` must be repaired explicitly",
                    meta.name
                )));
            }
        }

        let desired_collections = bundle
            .collections
            .iter()
            .map(|meta| (meta.name.clone(), meta.clone()))
            .collect::<BTreeMap<_, _>>();
        let extra_user_collections = self
            .collections()
            .into_iter()
            .filter(|meta| {
                !meta.name.starts_with("__bicdb_") && !desired_collections.contains_key(&meta.name)
            })
            .map(|meta| meta.name)
            .collect::<Vec<_>>();
        for collection in extra_user_collections {
            self.drop_collection(&collection)?;
        }

        let desired_indexes = bundle
            .indexes
            .iter()
            .map(|index| (index.name.clone(), index.clone()))
            .collect::<BTreeMap<_, _>>();
        for existing in self.index_definitions() {
            if desired_indexes.get(&existing.name) != Some(&existing) {
                self.drop_index(&existing.name)?;
            }
        }

        let mut catalog_changed = false;
        for meta in desired_collections.values() {
            if let Some(state) = self.collections.get_mut(&meta.name) {
                if state.get_mut().meta != *meta {
                    state.get_mut().meta = meta.clone();
                    catalog_changed = true;
                }
            } else {
                self.collections.insert(
                    meta.name.clone(),
                    RwLock::new(CollectionState::new(meta.clone())),
                );
                catalog_changed = true;
            }
        }
        if catalog_changed {
            self.persist_catalog()?;
            self.schema_compatibility.invalidate();
        }

        let desired_records = bundle.structural_records.iter().fold(
            BTreeMap::<String, Vec<Record>>::new(),
            |mut records, entry| {
                records
                    .entry(entry.collection.clone())
                    .or_default()
                    .push(entry.record.clone());
                records
            },
        );
        let mut tx = self.begin_transaction()?;
        tx.bypass_commit_admission = true;
        let mut structural_changed = false;
        for collection in CLUSTER_SCHEMA_CATALOG_COLLECTIONS {
            if !self.collections.contains_key(*collection) {
                continue;
            }
            let mut current = self.scan_collection_unchecked(collection)?;
            current.sort_by(|left, right| left.id.cmp(&right.id));
            let desired = desired_records
                .get(*collection)
                .map(Vec::as_slice)
                .unwrap_or_default();
            if current == desired {
                continue;
            }
            let current_ids = current
                .into_iter()
                .map(|record| record.id)
                .collect::<Vec<_>>();
            if !current_ids.is_empty() {
                tx.delete_many(collection, &current_ids)?;
            }
            if !desired.is_empty() {
                tx.write_upserts_unchecked(collection, desired.to_vec())?;
            }
            structural_changed = true;
        }
        if structural_changed {
            tx.commit()?;
        } else {
            tx.rollback()?;
        }

        for index in desired_indexes.values() {
            if !self.indexes.contains_key(&index.name) {
                self.create_index(index.clone())?;
            }
        }
        let installed = self.schema_compatibility_fingerprint()?;
        if installed.sha256 != bundle.fingerprint.sha256 {
            return Err(BicDbError::Cluster(format!(
                "schema bootstrap produced fingerprint {}, expected {}; node remains quarantined",
                installed.sha256, bundle.fingerprint.sha256
            )));
        }
        Ok(true)
    }

    /// Live row count.
    ///
    /// In paged lazy/registry mode the shards are not a complete view (they
    /// hold only rows touched this session, or none at all), so this counts the
    /// page store's keys instead. That walk touches leaf pages only — no heap
    /// page is read and no value is decoded — which is what makes it affordable
    /// on the query-planning path.
    ///
    /// This is the EXACT count: callers include user-visible ones (RESP
    /// `DBSIZE`), and a wrong count is worse than a slow one. Use
    /// [`Self::estimated_record_count`] on planning paths, which may answer
    /// from bookkeeping instead.
    ///
    /// Returning the resident count in paged mode was silently wrong: a lazy
    /// collection has no resident rows, so every paged table reported zero.
    pub fn collection_record_count(&self, collection: &str) -> Result<usize> {
        self.collection_record_count_cancellable(collection, &CancellationToken::uncancelable())
    }

    /// Exact live row count that observes query cancellation and deadlines
    /// while walking a server-paged collection.
    pub fn collection_record_count_cancellable(
        &self,
        collection: &str,
        cancellation: &CancellationToken,
    ) -> Result<usize> {
        cancellation.check()?;
        let state = self.collection_state(collection)?;
        if !state.paged_lazy {
            return Ok(state.record_count());
        }
        let Some(paged) = &self.paged_records else {
            return Ok(state.record_count());
        };
        paged.count_live_cancellable(&paged.latest_snapshot(), collection, cancellation)
    }

    /// Visit a collection's live rows in bounded batches, stopping early when
    /// `visit` returns `false`.
    ///
    /// Returns `false` if this database cannot stream (embedded mode, or a
    /// paged collection whose resident shards are authoritative), so a caller
    /// can fall back to `scan_collection` rather than silently seeing nothing.
    /// Reporting inability is the whole point: a streaming path that quietly
    /// returned zero rows would look like an empty table.
    ///
    /// Rows touched by the calling session are NOT overlaid here — this is the
    /// non-transactional read path, matching `scan_collection`'s semantics for
    /// a lazy collection.
    pub fn for_each_record_batch(
        &self,
        collection: &str,
        batch_size: usize,
        visit: impl FnMut(Vec<Record>) -> Result<bool>,
    ) -> Result<bool> {
        self.for_each_record_batch_cancellable(
            collection,
            batch_size,
            &CancellationToken::uncancelable(),
            visit,
        )
    }

    /// Cancellation-aware form of [`Self::for_each_record_batch`].
    pub fn for_each_record_batch_cancellable(
        &self,
        collection: &str,
        batch_size: usize,
        cancellation: &CancellationToken,
        visit: impl FnMut(Vec<Record>) -> Result<bool>,
    ) -> Result<bool> {
        cancellation.check()?;
        let state = self.collection_state(collection)?;
        if !state.paged_lazy {
            return Ok(false);
        }
        let touched: usize = state
            .read_all()
            .iter()
            .map(|shard| shard.versions.len())
            .sum();
        // A session that has written to this collection has chains that
        // override the page store per key; streaming would miss those
        // overrides, so defer to the materializing path.
        if touched > 0 {
            return Ok(false);
        }
        let Some(paged) = &self.paged_records else {
            return Ok(false);
        };
        drop(state);
        let snapshot = paged.latest_snapshot();
        paged.for_each_batch_cancellable(
            &snapshot,
            collection,
            batch_size.max(1),
            cancellation,
            visit,
        )?;
        Ok(true)
    }

    /// Resume the bounded paged scan strictly after a primary key.
    pub fn for_each_record_batch_after(
        &self,
        collection: &str,
        after_id: Option<&str>,
        batch_size: usize,
        visit: impl FnMut(Vec<Record>) -> Result<bool>,
    ) -> Result<bool> {
        self.for_each_record_batch_after_cancellable(
            collection,
            after_id,
            batch_size,
            &CancellationToken::uncancelable(),
            visit,
        )
    }

    /// Cancellation-aware bounded paged scan resumed after a primary key.
    pub fn for_each_record_batch_after_cancellable(
        &self,
        collection: &str,
        after_id: Option<&str>,
        batch_size: usize,
        cancellation: &CancellationToken,
        visit: impl FnMut(Vec<Record>) -> Result<bool>,
    ) -> Result<bool> {
        cancellation.check()?;
        let state = self.collection_state(collection)?;
        if self.paged_records.is_none() {
            return Ok(false);
        }
        let Some(paged) = &self.paged_records else {
            return Ok(false);
        };
        drop(state);
        let snapshot = paged.latest_snapshot();
        paged.for_each_batch_after_cancellable(
            &snapshot,
            collection,
            batch_size.max(1),
            after_id,
            cancellation,
            visit,
        )?;
        Ok(true)
    }

    /// Resumable bounded collection scan with heap-page-local candidate reads.
    /// This is intended for full-corpus projections whose UUID key order is
    /// unrelated to physical heap placement.
    pub fn for_each_record_locality_batch_after(
        &self,
        collection: &str,
        after_id: Option<&str>,
        batch_size: usize,
        visit: impl FnMut(Vec<Record>) -> Result<bool>,
    ) -> Result<bool> {
        let state = self.collection_state(collection)?;
        if self.paged_records.is_none() {
            return Ok(false);
        }
        let Some(paged) = &self.paged_records else {
            return Ok(false);
        };
        drop(state);
        let snapshot = paged.latest_snapshot();
        paged.for_each_locality_batch_after_cancellable(
            &snapshot,
            collection,
            batch_size.max(1),
            after_id,
            &CancellationToken::uncancelable(),
            visit,
        )?;
        Ok(true)
    }

    /// Export one range from a stable MVCC snapshot without materializing the
    /// collection. The returned watermark is captured with the read snapshot;
    /// callers then stream filtered replication frames after that sequence.
    pub fn for_each_range_snapshot_batch(
        &self,
        collection: &str,
        range: &RangeDescriptor,
        after_id: Option<&str>,
        options: &RangeSnapshotOptions,
        mut visit: impl FnMut(RangeSnapshotBatch) -> Result<bool>,
    ) -> Result<RangeSnapshotExport> {
        options.validate()?;
        let state = self.collection_state(collection)?;
        if !state.paged_lazy {
            return Err(BicDbError::Cluster(format!(
                "bounded range snapshot export requires a server_paged collection; `{collection}` is resident"
            )));
        }
        drop(state);
        let paged = self.paged_records.as_ref().ok_or_else(|| {
            BicDbError::Cluster(
                "bounded range snapshot export requires the paged storage engine".to_string(),
            )
        })?;

        // A transaction pins both the core visibility watermark and the page
        // store snapshot. Commits that begin or finish afterwards remain
        // outside this export and are recovered from the commit-frame suffix.
        let snapshot_tx = self.begin_transaction()?;
        let snapshot_commit_sequence = snapshot_tx.applied_snapshot;
        let paged_snapshot = snapshot_tx.paged_snapshot.as_ref().ok_or_else(|| {
            BicDbError::Cluster("range snapshot has no paged MVCC snapshot".to_string())
        })?;

        let mut pending = Vec::with_capacity(options.max_records_per_batch.min(4_096));
        let mut pending_bytes = 0_usize;
        let mut records_exported = 0_u64;
        let mut serialized_record_bytes = 0_u64;
        let mut last_scanned_key = after_id.map(ToOwned::to_owned);
        let mut last_emitted_resume = after_id.map(ToOwned::to_owned);
        let mut stopped = false;

        paged.for_each_batch_after(
            paged_snapshot,
            collection,
            options.max_records_per_batch,
            after_id,
            |records| {
                for record in records {
                    let record_id = record.id.clone();
                    let encoded_bytes = serde_json::to_vec(&record)?.len();
                    if encoded_bytes > options.max_record_bytes {
                        return Err(BicDbError::Cluster(format!(
                            "record `{collection}/{record_id}` is {encoded_bytes} bytes, exceeding range snapshot max_record_bytes {}",
                            options.max_record_bytes
                        )));
                    }
                    let belongs =
                        range.contains_token(distribution_key_token(collection, &record_id));
                    if belongs
                        && !pending.is_empty()
                        && (pending.len() >= options.max_records_per_batch
                            || pending_bytes.saturating_add(encoded_bytes)
                                > options.max_bytes_per_batch)
                    {
                        let resume_after_key = last_scanned_key.clone().ok_or_else(|| {
                            BicDbError::Cluster(
                                "range snapshot lost its scan resume key".to_string(),
                            )
                        })?;
                        let batch_records = std::mem::take(&mut pending);
                        let batch_bytes = std::mem::take(&mut pending_bytes);
                        records_exported =
                            records_exported.saturating_add(batch_records.len() as u64);
                        serialized_record_bytes =
                            serialized_record_bytes.saturating_add(batch_bytes as u64);
                        last_emitted_resume = Some(resume_after_key.clone());
                        if !visit(RangeSnapshotBatch {
                            collection: collection.to_string(),
                            range_id: range.id,
                            range_epoch: range.epoch,
                            snapshot_commit_sequence,
                            resume_after_key,
                            serialized_record_bytes: batch_bytes,
                            records: batch_records,
                        })? {
                            stopped = true;
                            return Ok(false);
                        }
                    }
                    if belongs {
                        pending_bytes = pending_bytes.saturating_add(encoded_bytes);
                        pending.push(record);
                    }
                    last_scanned_key = Some(record_id);
                    if pending.len() >= options.max_records_per_batch {
                        let resume_after_key = last_scanned_key.clone().ok_or_else(|| {
                            BicDbError::Cluster(
                                "range snapshot lost its scan resume key".to_string(),
                            )
                        })?;
                        let batch_records = std::mem::take(&mut pending);
                        let batch_bytes = std::mem::take(&mut pending_bytes);
                        records_exported =
                            records_exported.saturating_add(batch_records.len() as u64);
                        serialized_record_bytes =
                            serialized_record_bytes.saturating_add(batch_bytes as u64);
                        last_emitted_resume = Some(resume_after_key.clone());
                        if !visit(RangeSnapshotBatch {
                            collection: collection.to_string(),
                            range_id: range.id,
                            range_epoch: range.epoch,
                            snapshot_commit_sequence,
                            resume_after_key,
                            serialized_record_bytes: batch_bytes,
                            records: batch_records,
                        })? {
                            stopped = true;
                            return Ok(false);
                        }
                    }
                }
                Ok(true)
            },
        )?;

        if !stopped && !pending.is_empty() {
            let resume_after_key = last_scanned_key.clone().ok_or_else(|| {
                BicDbError::Cluster("range snapshot lost its final resume key".to_string())
            })?;
            let batch_records = std::mem::take(&mut pending);
            let batch_bytes = std::mem::take(&mut pending_bytes);
            records_exported = records_exported.saturating_add(batch_records.len() as u64);
            serialized_record_bytes = serialized_record_bytes.saturating_add(batch_bytes as u64);
            last_emitted_resume = Some(resume_after_key.clone());
            if !visit(RangeSnapshotBatch {
                collection: collection.to_string(),
                range_id: range.id,
                range_epoch: range.epoch,
                snapshot_commit_sequence,
                resume_after_key,
                serialized_record_bytes: batch_bytes,
                records: batch_records,
            })? {
                stopped = true;
            }
        }

        Ok(RangeSnapshotExport {
            range_id: range.id,
            range_epoch: range.epoch,
            snapshot_commit_sequence,
            records_exported,
            serialized_record_bytes,
            resume_after_key: if stopped {
                last_emitted_resume
            } else {
                last_scanned_key
            },
            completed: !stopped,
        })
    }

    /// Cheap row-count estimate for query planning.
    ///
    /// In registry mode this is the registry's size — one entry per row,
    /// answered with no I/O at all. It can overcount rows whose keys survive
    /// their last version (a delete's key lives until vacuum), which is
    /// acceptable for costing and is why this is separate from
    /// [`Self::collection_record_count`].
    pub fn estimated_record_count(&self, collection: &str) -> Result<usize> {
        let state = self.collection_state(collection)?;
        if !state.paged_lazy {
            return Ok(state.record_count());
        }
        // The registry answers only for REGISTRY-MODE collections (indexed:
        // one entry per row by construction, `track_reverse` set at open). A
        // plain-lazy collection's pk map holds just the rows touched this
        // session — returning it as the estimate told the planner a 500-row
        // table had 15 rows after 15 deletes.
        let guards = state.read_all();
        if guards.iter().any(|shard| shard.track_reverse) {
            let registered: usize = guards.iter().map(|shard| shard.pk_to_rowid.len()).sum();
            return Ok(registered);
        }
        drop(guards);
        // Seed once from a key-only walk (leaf pages, no heap reads, dead
        // keys included — this is an estimate), then answer from the counter
        // that commits keep nudged. Before this, the fallback was a FULL
        // visibility-checked scan per call — and the planner calls this per
        // query, which put an O(n) floor under every simple statement against
        // a lazy paged table (B3 in IMPORTANT-TODO.md: most of the ~1 s cost
        // of `LIMIT 3` on 2M rows was the plan, not the query).
        let Some(paged) = &self.paged_records else {
            return Ok(state.record_count());
        };
        let estimate = state.paged_row_estimate.get_or_init(|| {
            let mut count: i64 = 0;
            if let Ok(ids) = paged.scan_ids(&paged.latest_snapshot(), collection) {
                for id in ids {
                    if id.is_err() {
                        // A torn walk seeds low; planning tolerates that, and
                        // the exact paths still surface the error properly.
                        break;
                    }
                    count += 1;
                }
            }
            std::sync::atomic::AtomicI64::new(count)
        });
        Ok(estimate.load(AtomicOrdering::Relaxed).max(0) as usize)
    }

    /// Whether rows live in the paged store (server_paged mode).
    pub fn is_server_paged(&self) -> bool {
        self.paged_records.is_some()
    }

    /// Start a new explicitly bounded server-paged vacuum sweep.
    ///
    /// An active operation cannot be replaced. A completed or operator-paused
    /// operation may be superseded by a fresh sweep from the beginning; the
    /// page operation is idempotent, so that is always safe. The returned UUID
    /// fences stale worker tasks from advancing a later schedule.
    pub fn start_paged_vacuum_maintenance(
        &self,
        now_ms: u64,
        limits: crate::PagedVacuumScheduleLimits,
    ) -> Result<crate::PagedVacuumSchedule> {
        let paged = self.paged_records.as_ref().ok_or_else(|| {
            BicDbError::PagedStorage(
                "paged vacuum maintenance requires storage_mode = server_paged".to_string(),
            )
        })?;
        let _supervisor = self.paged_maintenance_lock.lock();
        let (identity_path, schedule_path) =
            crate::paged_maintenance::maintenance_paths(&self.path);
        crate::paged_maintenance::start_schedule(
            &identity_path,
            &schedule_path,
            Uuid::new_v4(),
            now_ms,
            limits,
            paged,
            self.config.fsync,
        )
    }

    /// Load and fully verify the local paged-vacuum operation, if one exists.
    /// Corrupt, oversized, symlinked, or store-mismatched state fails closed.
    pub fn paged_vacuum_maintenance_status(&self) -> Result<Option<crate::PagedVacuumSchedule>> {
        if self.paged_records.is_none() {
            return Err(BicDbError::PagedStorage(
                "paged vacuum maintenance requires storage_mode = server_paged".to_string(),
            ));
        }
        let _supervisor = self.paged_maintenance_lock.lock();
        let (identity_path, schedule_path) =
            crate::paged_maintenance::maintenance_paths(&self.path);
        crate::paged_maintenance::load_active_schedule(&identity_path, &schedule_path)
    }

    /// Advance at most one resource-admitted vacuum step and atomically publish
    /// the resulting exact cursor, retry, pause, or completion state.
    pub fn tick_paged_vacuum_maintenance(
        &self,
        expected_operation_id: Uuid,
        governor: &ResourceGovernor,
        now_ms: u64,
    ) -> Result<crate::PagedVacuumScheduleAdvance> {
        let paged = self.paged_records.as_ref().ok_or_else(|| {
            BicDbError::PagedStorage(
                "paged vacuum maintenance requires storage_mode = server_paged".to_string(),
            )
        })?;
        let _supervisor = self.paged_maintenance_lock.lock();
        let (identity_path, schedule_path) =
            crate::paged_maintenance::maintenance_paths(&self.path);
        let mut schedule =
            crate::paged_maintenance::load_active_schedule(&identity_path, &schedule_path)?
                .ok_or_else(|| {
                    BicDbError::PagedStorage("no paged vacuum operation is active".to_string())
                })?;
        schedule.tick_and_checkpoint(
            &schedule_path,
            expected_operation_id,
            paged.as_ref(),
            governor,
            now_ms,
            self.config.fsync,
        )
    }

    /// Resume a paused operation without changing its immutable resource
    /// envelope. To enlarge an insufficient byte limit, start a new operation;
    /// it safely re-walks from the beginning.
    pub fn resume_paged_vacuum_maintenance(
        &self,
        expected_operation_id: Uuid,
        next_attempt_at_ms: u64,
        now_ms: u64,
    ) -> Result<crate::PagedVacuumSchedule> {
        if self.paged_records.is_none() {
            return Err(BicDbError::PagedStorage(
                "paged vacuum maintenance requires storage_mode = server_paged".to_string(),
            ));
        }
        let _supervisor = self.paged_maintenance_lock.lock();
        let (identity_path, schedule_path) =
            crate::paged_maintenance::maintenance_paths(&self.path);
        let mut schedule =
            crate::paged_maintenance::load_active_schedule(&identity_path, &schedule_path)?
                .ok_or_else(|| {
                    BicDbError::PagedStorage("no paged vacuum operation is active".to_string())
                })?;
        schedule.resume_and_checkpoint(
            &schedule_path,
            expected_operation_id,
            next_attempt_at_ms,
            now_ms,
            self.config.fsync,
        )?;
        Ok(schedule)
    }

    /// Atomically pause an active local vacuum operation with an operator
    /// reason. No page work is performed by this transition.
    pub fn pause_paged_vacuum_maintenance(
        &self,
        expected_operation_id: Uuid,
        reason: impl Into<String>,
        now_ms: u64,
    ) -> Result<crate::PagedVacuumSchedule> {
        if self.paged_records.is_none() {
            return Err(BicDbError::PagedStorage(
                "paged vacuum maintenance requires storage_mode = server_paged".to_string(),
            ));
        }
        let _supervisor = self.paged_maintenance_lock.lock();
        let (identity_path, schedule_path) =
            crate::paged_maintenance::maintenance_paths(&self.path);
        let mut schedule =
            crate::paged_maintenance::load_active_schedule(&identity_path, &schedule_path)?
                .ok_or_else(|| {
                    BicDbError::PagedStorage("no paged vacuum operation is active".to_string())
                })?;
        schedule.pause_by_operator_and_checkpoint(
            &schedule_path,
            expected_operation_id,
            reason,
            now_ms,
            self.config.fsync,
        )?;
        Ok(schedule)
    }

    /// Start one durable, resource-governed server-paged checkpoint.
    ///
    /// The page operation drains, freezes, and finalizes through inclusive
    /// bounded phases. An active operation cannot be silently replaced, and its
    /// UUID fences stale worker tasks after restart.
    /// Narrow capability for the automatic checkpoint worker: everything a
    /// bounded checkpoint tick needs, cloned out so the caller can drop its
    /// database-wide read guard before the step runs. `None` when the
    /// database is not in `server_paged` mode.
    /// Narrow capability for the automatic vacuum worker; `None` unless
    /// `server_paged`.
    pub fn paged_vacuum_maintenance_handle(&self) -> Option<PagedVacuumMaintenanceHandle> {
        self.paged_records
            .as_ref()
            .map(|paged| PagedVacuumMaintenanceHandle {
                paged: Arc::clone(paged),
                supervisor: Arc::clone(&self.paged_maintenance_lock),
                path: self.path.clone(),
                fsync: self.config.fsync,
            })
    }

    /// Synchronous full-store vacuum: bounded cursor-resumable steps until
    /// complete, checking the cancellation token between steps. Backs the
    /// SQL `VACUUM` statement.
    pub fn run_vacuum_to_completion(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<bicdb_page::VacuumReport> {
        let paged = self.paged_records.as_ref().ok_or_else(|| {
            BicDbError::PagedStorage("VACUUM requires storage_mode = server_paged".to_string())
        })?;
        let mut cursor = bicdb_page::VacuumCursor::default();
        let mut totals = bicdb_page::VacuumReport::default();
        loop {
            cancellation.check()?;
            let report = paged.vacuum_step(
                cursor,
                bicdb_page::VacuumLimits {
                    max_pages: bicdb_page::MAX_VACUUM_PAGES_PER_STEP,
                    max_bytes: bicdb_page::MAX_VACUUM_BYTES_PER_STEP,
                    max_duration_millis: bicdb_page::MAX_VACUUM_DURATION_MILLIS_PER_STEP,
                },
            )?;
            totals.pages_scanned += report.pages_scanned;
            totals.bytes_examined += report.bytes_examined;
            totals.versions_examined += report.versions_examined;
            totals.versions_reclaimed += report.versions_reclaimed;
            totals.bytes_reclaimed += report.bytes_reclaimed;
            totals.pages_freed += report.pages_freed;
            totals.oldest_active = report.oldest_active;
            totals.elapsed_millis += report.elapsed_millis;
            totals.next_cursor = report.next_cursor;
            totals.stop_reason = report.stop_reason;
            totals.complete = report.complete;
            if report.complete {
                // Vacuum freed interior pages; hand their bytes back to the
                // filesystem in the same operator action.
                let _ = paged.punch_free_pages(u64::MAX);
                return Ok(totals);
            }
            cursor = report.next_cursor;
        }
    }

    /// Return interior free-page bytes to the filesystem (sparse-file hole
    /// punching). Advisory and crash-trivial; `supported: false` on
    /// platforms/filesystems without punch support. Runs automatically at
    /// the end of every completed vacuum pass.
    pub fn punch_free_space(&self, max_pages: u64) -> Result<bicdb_page::HolePunchReport> {
        let paged = self.paged_records.as_ref().ok_or_else(|| {
            BicDbError::PagedStorage(
                "hole punching requires storage_mode = server_paged".to_string(),
            )
        })?;
        paged.punch_free_pages(max_pages)
    }

    pub fn paged_checkpoint_maintenance_handle(&self) -> Option<PagedCheckpointMaintenanceHandle> {
        self.paged_records
            .as_ref()
            .map(|paged| PagedCheckpointMaintenanceHandle {
                paged: Arc::clone(paged),
                supervisor: Arc::clone(&self.paged_maintenance_lock),
                path: self.path.clone(),
                fsync: self.config.fsync,
            })
    }

    /// Storage-only capability for long-running online storage operations;
    /// see [`PagedStorageOpsHandle`]. `None` unless `server_paged`.
    pub fn paged_storage_ops_handle(&self) -> Option<PagedStorageOpsHandle> {
        self.paged_records
            .as_ref()
            .map(|paged| PagedStorageOpsHandle {
                paged: Arc::clone(paged),
                path: self.path.clone(),
                fsync: self.config.fsync,
            })
    }

    pub(crate) fn require_paged_checkpoint_maintenance_handle(
        &self,
    ) -> Result<PagedCheckpointMaintenanceHandle> {
        self.paged_checkpoint_maintenance_handle().ok_or_else(|| {
            BicDbError::PagedStorage(
                "paged checkpoint maintenance requires storage_mode = server_paged".to_string(),
            )
        })
    }

    pub fn start_paged_checkpoint_maintenance(
        &self,
        now_ms: u64,
        limits: crate::PagedCheckpointScheduleLimits,
    ) -> Result<crate::PagedCheckpointSchedule> {
        self.require_paged_checkpoint_maintenance_handle()?
            .start_checkpoint_maintenance(now_ms, limits)
    }

    /// Load and fully verify the local checkpoint operation, if one exists.
    pub fn paged_checkpoint_maintenance_status(
        &self,
    ) -> Result<Option<crate::PagedCheckpointSchedule>> {
        self.require_paged_checkpoint_maintenance_handle()?
            .checkpoint_maintenance_status()
    }

    /// Advance one compaction-lane-admitted checkpoint phase and atomically
    /// publish the resulting cursor, retry, pause, or terminal state.
    pub fn tick_paged_checkpoint_maintenance(
        &self,
        expected_operation_id: Uuid,
        governor: &ResourceGovernor,
        now_ms: u64,
    ) -> Result<crate::PagedCheckpointScheduleAdvance> {
        self.require_paged_checkpoint_maintenance_handle()?
            .tick_checkpoint_maintenance(expected_operation_id, governor, now_ms)
    }

    pub fn pause_paged_checkpoint_maintenance(
        &self,
        expected_operation_id: Uuid,
        reason: impl Into<String>,
        now_ms: u64,
    ) -> Result<crate::PagedCheckpointSchedule> {
        if self.paged_records.is_none() {
            return Err(BicDbError::PagedStorage(
                "paged checkpoint maintenance requires storage_mode = server_paged".to_string(),
            ));
        }
        let _supervisor = self.paged_maintenance_lock.lock();
        let (identity_path, schedule_path) =
            crate::paged_checkpoint_maintenance::maintenance_paths(&self.path);
        let mut schedule = crate::paged_checkpoint_maintenance::load_active_schedule(
            &identity_path,
            &schedule_path,
        )?
        .ok_or_else(|| {
            BicDbError::PagedStorage("no paged checkpoint operation is active".to_string())
        })?;
        schedule.pause_by_operator_and_checkpoint(
            &schedule_path,
            expected_operation_id,
            reason,
            now_ms,
            self.config.fsync,
        )?;
        Ok(schedule)
    }

    pub fn resume_paged_checkpoint_maintenance(
        &self,
        expected_operation_id: Uuid,
        next_attempt_at_ms: u64,
        now_ms: u64,
    ) -> Result<crate::PagedCheckpointSchedule> {
        if self.paged_records.is_none() {
            return Err(BicDbError::PagedStorage(
                "paged checkpoint maintenance requires storage_mode = server_paged".to_string(),
            ));
        }
        let _supervisor = self.paged_maintenance_lock.lock();
        let (identity_path, schedule_path) =
            crate::paged_checkpoint_maintenance::maintenance_paths(&self.path);
        let mut schedule = crate::paged_checkpoint_maintenance::load_active_schedule(
            &identity_path,
            &schedule_path,
        )?
        .ok_or_else(|| {
            BicDbError::PagedStorage("no paged checkpoint operation is active".to_string())
        })?;
        schedule.resume_and_checkpoint(
            &schedule_path,
            expected_operation_id,
            next_attempt_at_ms,
            now_ms,
            self.config.fsync,
        )?;
        Ok(schedule)
    }

    /// Start one durable structural-then-MVCC integrity sweep.
    ///
    /// Structural completion is an atomic phase boundary: MVCC verification
    /// cannot begin until the complete B+ tree result has been checkpointed.
    pub fn start_paged_integrity_maintenance(
        &self,
        now_ms: u64,
        limits: crate::PagedIntegrityScheduleLimits,
    ) -> Result<crate::PagedIntegritySchedule> {
        let paged = self.paged_records.as_ref().ok_or_else(|| {
            BicDbError::PagedStorage(
                "paged integrity maintenance requires storage_mode = server_paged".to_string(),
            )
        })?;
        let _supervisor = self.paged_maintenance_lock.lock();
        let (identity_path, schedule_path) =
            crate::paged_integrity_maintenance::maintenance_paths(&self.path);
        crate::paged_integrity_maintenance::start_schedule(
            &identity_path,
            &schedule_path,
            Uuid::new_v4(),
            now_ms,
            limits,
            paged,
            self.config.fsync,
        )
    }

    /// Write back one bounded dirty-buffer window under the background
    /// compaction lane. Admission and demand validation happen before the page
    /// engine examines its first candidate.
    pub fn paged_writeback_step_governed(
        &self,
        cursor: crate::WritebackCursor,
        limits: crate::WritebackLimits,
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
    ) -> Result<crate::WritebackStepReport> {
        let paged = self.paged_records.as_ref().ok_or_else(|| {
            BicDbError::PagedStorage(
                "paged writeback requires storage_mode = server_paged".to_string(),
            )
        })?;
        let page_size = paged.page_size();
        limits
            .validate(page_size)
            .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
        demand.validate()?;
        let candidate_bytes = limits.max_candidates.checked_mul(8).ok_or_else(|| {
            BicDbError::ResourceGovernance(
                "paged writeback candidate memory accounting overflowed".to_string(),
            )
        })?;
        let minimum_memory = candidate_bytes.saturating_add(64 * 1024);
        if demand.memory_bytes < minimum_memory
            || demand.io_bytes < limits.max_io_bytes
            || demand.io_charge_bytes < limits.max_io_bytes
        {
            return Err(BicDbError::ResourceGovernance(format!(
                "paged writeback demand must reserve at least {minimum_memory} memory bytes and {} I/O bytes",
                limits.max_io_bytes
            )));
        }

        let _supervisor = self.paged_maintenance_lock.lock();
        let permit = governor.try_admit(ResourceLane::Compaction, demand, now_ms)?;
        let result = paged.writeback_step(cursor, limits);
        drop(permit);
        result
    }

    /// Exact queued speculative work for server-paged storage. `None` means
    /// this database uses a different storage mode; zero means read-ahead is
    /// disabled or currently idle.
    pub fn paged_read_ahead_queue_depth(&self) -> Option<u64> {
        self.paged_read_ahead_handle()
            .map(|handle| handle.queue_depth())
    }

    /// A live handle is what makes the pool ACCEPT speculative requests:
    /// hosts running the worker pattern must hold one for the worker's
    /// lifetime, not construct one per step — while no handle (or other
    /// registered driver) exists, requests are dropped as undriven.
    /// Engine-level paged handle for tests and embedders that drive raw
    /// concurrent load; not a stable API.
    #[doc(hidden)]
    pub fn paged_records_handle(&self) -> Option<Arc<crate::paged_collection::PagedRecords>> {
        self.paged_records.as_ref().cloned()
    }

    pub fn paged_read_ahead_handle(&self) -> Option<PagedReadAheadHandle> {
        self.paged_records.as_ref().cloned().map(|paged| {
            let driver = Arc::new(ReadAheadDriverRegistration::new(Arc::clone(&paged)));
            PagedReadAheadHandle {
                paged,
                _driver: driver,
            }
        })
    }

    /// Drain one bounded speculative-read window under the shared background
    /// compaction lane. The permit is obtained before any queued page is
    /// removed, so saturation delays rather than loses exact read hints.
    pub fn paged_read_ahead_step_governed(
        &self,
        limits: crate::ReadAheadLimits,
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
    ) -> Result<Option<crate::ReadAheadStepReport>> {
        let Some(handle) = self.paged_read_ahead_handle() else {
            return Ok(None);
        };
        handle
            .step_governed(limits, governor, demand, now_ms)
            .map(Some)
    }

    /// Load and verify the checksummed integrity schedule, if present.
    pub fn paged_integrity_maintenance_status(
        &self,
    ) -> Result<Option<crate::PagedIntegritySchedule>> {
        if self.paged_records.is_none() {
            return Err(BicDbError::PagedStorage(
                "paged integrity maintenance requires storage_mode = server_paged".to_string(),
            ));
        }
        let _supervisor = self.paged_maintenance_lock.lock();
        let (identity_path, schedule_path) =
            crate::paged_integrity_maintenance::maintenance_paths(&self.path);
        crate::paged_integrity_maintenance::load_active_schedule(&identity_path, &schedule_path)
    }

    /// Advance at most one anti-entropy-lane-admitted integrity step and
    /// atomically publish its cursor, phase, retry, pause, or terminal result.
    pub fn tick_paged_integrity_maintenance(
        &self,
        expected_operation_id: Uuid,
        governor: &ResourceGovernor,
        now_ms: u64,
    ) -> Result<crate::PagedIntegrityScheduleAdvance> {
        let paged = self.paged_records.as_ref().ok_or_else(|| {
            BicDbError::PagedStorage(
                "paged integrity maintenance requires storage_mode = server_paged".to_string(),
            )
        })?;
        let _supervisor = self.paged_maintenance_lock.lock();
        let (identity_path, schedule_path) =
            crate::paged_integrity_maintenance::maintenance_paths(&self.path);
        let mut schedule = crate::paged_integrity_maintenance::load_active_schedule(
            &identity_path,
            &schedule_path,
        )?
        .ok_or_else(|| {
            BicDbError::PagedStorage("no paged integrity operation is active".to_string())
        })?;
        schedule.tick_and_checkpoint(
            &schedule_path,
            expected_operation_id,
            paged.as_ref(),
            governor,
            now_ms,
            self.config.fsync,
        )
    }

    pub fn pause_paged_integrity_maintenance(
        &self,
        expected_operation_id: Uuid,
        reason: impl Into<String>,
        now_ms: u64,
    ) -> Result<crate::PagedIntegritySchedule> {
        if self.paged_records.is_none() {
            return Err(BicDbError::PagedStorage(
                "paged integrity maintenance requires storage_mode = server_paged".to_string(),
            ));
        }
        let _supervisor = self.paged_maintenance_lock.lock();
        let (identity_path, schedule_path) =
            crate::paged_integrity_maintenance::maintenance_paths(&self.path);
        let mut schedule = crate::paged_integrity_maintenance::load_active_schedule(
            &identity_path,
            &schedule_path,
        )?
        .ok_or_else(|| {
            BicDbError::PagedStorage("no paged integrity operation is active".to_string())
        })?;
        schedule.pause_by_operator_and_checkpoint(
            &schedule_path,
            expected_operation_id,
            reason,
            now_ms,
            self.config.fsync,
        )?;
        Ok(schedule)
    }

    pub fn resume_paged_integrity_maintenance(
        &self,
        expected_operation_id: Uuid,
        next_attempt_at_ms: u64,
        now_ms: u64,
    ) -> Result<crate::PagedIntegritySchedule> {
        if self.paged_records.is_none() {
            return Err(BicDbError::PagedStorage(
                "paged integrity maintenance requires storage_mode = server_paged".to_string(),
            ));
        }
        let _supervisor = self.paged_maintenance_lock.lock();
        let (identity_path, schedule_path) =
            crate::paged_integrity_maintenance::maintenance_paths(&self.path);
        let mut schedule = crate::paged_integrity_maintenance::load_active_schedule(
            &identity_path,
            &schedule_path,
        )?
        .ok_or_else(|| {
            BicDbError::PagedStorage("no paged integrity operation is active".to_string())
        })?;
        schedule.resume_and_checkpoint(
            &schedule_path,
            expected_operation_id,
            next_attempt_at_ms,
            now_ms,
            self.config.fsync,
        )?;
        Ok(schedule)
    }

    /// Inspect one paged record's complete MVCC chain without resolving
    /// visibility. This is a bounded, read-only diagnostic.
    pub fn inspect_paged_record_chain(
        &self,
        collection: &str,
        record_id: &str,
    ) -> Result<crate::VersionChainInspection> {
        self.ensure_collection(collection)?;
        let paged = self.paged_records.as_ref().ok_or_else(|| {
            BicDbError::PagedStorage(
                "MVCC chain inspection requires storage_mode = server_paged".to_string(),
            )
        })?;
        paged.inspect_version_chain(collection, record_id)
    }

    /// Verify the page B-tree and every reachable paged MVCC chain.
    pub fn verify_paged_storage_integrity(
        &self,
        max_fault_samples: usize,
    ) -> Result<crate::PagedIntegrityReport> {
        let paged = self.paged_records.as_ref().ok_or_else(|| {
            BicDbError::PagedStorage(
                "paged integrity verification requires storage_mode = server_paged".to_string(),
            )
        })?;
        paged.verify_integrity(max_fault_samples)
    }

    /// Verify one bounded, restartable slice of the server-paged B+ tree.
    /// The structure read lock is released after the step even when the full
    /// tree contains petabytes of logical data.
    pub fn verify_paged_btree_step(
        &self,
        cursor: crate::BTreeVerifyCursor,
        limits: crate::BTreeVerifyLimits,
    ) -> Result<crate::BTreeVerifyStepReport> {
        let paged = self.paged_records.as_ref().ok_or_else(|| {
            BicDbError::PagedStorage(
                "paged B-tree verification requires storage_mode = server_paged".to_string(),
            )
        })?;
        paged.verify_btree_step(cursor, limits)
    }

    /// Verify one explicitly bounded, restartable slice of reachable MVCC
    /// chains. Unlike `verify_paged_storage_integrity`, this production path
    /// never holds the structure lock across the complete database and never
    /// reads row payload bytes merely to inspect their fixed MVCC headers.
    pub fn verify_paged_version_chains_step(
        &self,
        cursor: crate::VersionChainVerifyCursor,
        limits: crate::VersionChainVerifyLimits,
    ) -> Result<crate::VersionChainVerifyStepReport> {
        let paged = self.paged_records.as_ref().ok_or_else(|| {
            BicDbError::PagedStorage(
                "paged MVCC verification requires storage_mode = server_paged".to_string(),
            )
        })?;
        paged.verify_version_chains_step(cursor, limits)
    }

    /// Offline recovery for one confirmed cyclic or over-limit MVCC chain.
    ///
    /// `expected_head` must come from a fresh inspection. The replacement
    /// record should be reconstructed from an independent durable ledger. The
    /// page engine compares the exact head and re-diagnoses the fault under its
    /// writer lock before atomically publishing a fresh one-version chain.
    pub fn repair_paged_record_chain(
        &mut self,
        collection: &str,
        record_id: &str,
        expected_head: crate::TupleLocator,
        replacement: &Record,
    ) -> Result<crate::VersionChainRepairReport> {
        self.ensure_writable("repair paged MVCC version chain")?;
        self.ensure_collection(collection)?;
        let paged = self.paged_records.as_ref().ok_or_else(|| {
            BicDbError::PagedStorage(
                "MVCC chain repair requires storage_mode = server_paged".to_string(),
            )
        })?;
        paged.replace_faulty_version_chain(collection, record_id, expected_head, replacement)
    }

    pub fn create_index(&mut self, definition: IndexDefinition) -> Result<()> {
        self.create_index_online(definition).map(|_| ())
    }

    pub fn create_index_online(
        &mut self,
        definition: IndexDefinition,
    ) -> Result<IndexMaintenanceReport> {
        self.ensure_writable("create index online")?;
        validate_index_definition(&definition)?;
        self.ensure_collection(&definition.collection)?;
        self.validate_secure_index_definition(&definition)?;
        if self.indexes.contains_key(&definition.name) {
            if self.is_server_paged()
                && definition.kind == IndexKind::BTree
                && definition.exclusion.is_none()
            {
                let started = Instant::now();
                if let Some(state) = self.reconcile_published_paged_btree_build(&definition)? {
                    return Ok(Self::paged_btree_build_report(
                        &definition,
                        &state,
                        started,
                        true,
                    ));
                }
            }
            // An interrupted full-text build leaves its workspace on disk, and
            // the build resumes from that checkpoint when it is re-entered.
            // What was missing was REACHABILITY: the catalog entry survives the
            // crash, so the retry an operator naturally issues was rejected
            // here and the only way forward was DROP, which discards the
            // workspace. A corpus-scale build runs for days; refusing the retry
            // turned a crash in the final phase into a total loss of the run.
            if let Some(report) = self.resume_incomplete_full_text_build(&definition)? {
                return Ok(report);
            }
            return Err(BicDbError::Index(format!(
                "index `{}` already exists",
                definition.name
            )));
        }
        if self.is_server_paged() && definition.kind == IndexKind::FullText {
            return self.create_paged_full_text_index_online(definition);
        }
        // Clean slate for a brand-new spatial index: a crashed DROP can
        // leave [0,0,13] leftovers (nodes/delta, or a meta whose reopen
        // adoption would resurrect the dropped index's corpus). A new index
        // must own its packed namespace outright.
        if definition.kind == IndexKind::Spatial {
            if let Some(paged) = self.paged_records.clone() {
                let physical = paged.resolve_index_name(&definition.name);
                self.remove_raw_prefix(
                    &paged,
                    &crate::paged_collection::index_spatial_prefix(&physical),
                )?;
            }
            if let Some(workspace) =
                crate::spatial_pack_build::SpatialPackWorkspace::load(&self.path, &definition.name)?
            {
                workspace.discard()?;
            }
        }
        if self.is_server_paged()
            && definition.kind == IndexKind::BTree
            && definition.exclusion.is_none()
        {
            let started = Instant::now();
            let limits = self.paged_btree_build_limits();
            let mut now = unix_timestamp_millis().max(0) as u64;
            let governor = ResourceGovernor::new(crate::ResourceGovernorConfig::default(), now)?;
            let demand = ResourceDemand {
                memory_bytes: limits.max_batch_bytes as u64,
                io_bytes: limits.max_batch_bytes as u64,
                cpu_slots: 1,
                io_charge_bytes: limits.max_batch_bytes as u64,
            };
            let initial = self.begin_paged_btree_build(definition.clone(), limits, now)?;
            let interrupted_previous = initial.resume_after_id.is_some()
                || initial.phase != PagedBTreeBuildPhase::Building;
            let state = loop {
                // The compatibility wrapper runs synchronously; advance the
                // governor's logical clock by a refill interval between ticks.
                now = now.saturating_add(1_000);
                match self.advance_paged_btree_build_governed(
                    &definition.name,
                    limits,
                    &governor,
                    demand,
                    now,
                )? {
                    PagedBTreeBuildAdvance::Progress(_) => {}
                    PagedBTreeBuildAdvance::Complete(state) => break state,
                }
            };
            return Ok(Self::paged_btree_build_report(
                &definition,
                &state,
                started,
                interrupted_previous,
            ));
        }
        if definition.exclusion.is_some() {
            let mutations = self
                .scan_collection_unchecked(&definition.collection)?
                .into_iter()
                .map(|record| IndexRecordMutation {
                    old_record: OldImage::none(),
                    new_record: OldImage::from_record(Some(Arc::new(record))),
                    rowid: None,
                    changed: None,
                })
                .collect::<Vec<_>>();
            self.validate_exclusion_record_mutations(&definition, &mutations)?;
        }
        // Index entries carry rowids that must resolve through resident shard
        // entries, so a lazy paged collection is materialized first. Defining
        // an index is precisely how a caller opts a collection into O(n)
        // residency — the cost lands here, visibly, not on every open.
        // Spatial is the exception: its entries are (pk, rect) and stream
        // from identity rows, so the collection stays lazy.
        if !(self.is_server_paged()
            && matches!(definition.kind, IndexKind::BTree | IndexKind::FullText))
            && !self.spatial_index_streams_from_paged(&definition)
        {
            self.materialize_lazy_paged_collection(&definition.collection)?;
        }
        let durable = definition.clone();
        let report = self.rebuild_index_online_from_definition(definition, "created", false)?;
        // Backfill the durable entries for rows that predate the index, so the
        // paged keyspace is complete from the moment the index exists. Rows
        // written from now on maintain their own entries transactionally.
        if let Err(error) = self.backfill_paged_index_entries(&durable) {
            // The logical catalog is itself a publication surface. A failed
            // new build keeps its disk checkpoint/runs for resume, but it must
            // not leave an index definition that blocks a retry or invites a
            // reopen to trust an unpublished generation.
            let sql_prepass = crate::fts_build::FtsBuildWorkspace::open_existing(
                &self.path.join(DEFAULT_FTS_BUILD_DIR),
                &durable.name,
                self.config.fsync,
            )?
            .is_some_and(|workspace| !workspace.source_signature().starts_with("core:"));
            if sql_prepass {
                self.index_generation
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.indexes.remove(&durable.name);
                self.persist_index_catalog()?;
            }
            return Err(error);
        }
        // A BULK-BUILT paged full-text index (rows existed, the direct build
        // wrote posting blocks) serves READ-THROUGH from the moment the
        // backfill completes: its rows carry no in-row projection since
        // 0.9.63, so the resident store is empty and only the durable
        // keyspaces are complete. Index-first flows (empty collection at
        // CREATE) keep the resident path — subsequent inserts maintain it,
        // and statistics/verification read it, exactly as before.
        if durable.kind == IndexKind::FullText {
            if let Some(paged) = &self.paged_records {
                let snapshot = paged.latest_snapshot();
                if paged.index_has_any_posting_blocks(&snapshot, &durable.name)? {
                    if let Some(state) = self.indexes.get(&durable.name) {
                        state.write().paged_read_through = true;
                    }
                }
            }
        }
        self.schema_compatibility.invalidate();
        Ok(report)
    }

    /// Create a server-paged FTS generation without first verifying an empty
    /// resident index against the entire collection. The external builder is
    /// the index build; publishing a placeholder catalog entry only makes its
    /// durable checkpoint reachable after a restart.
    pub(crate) fn create_paged_full_text_index_online(
        &mut self,
        definition: IndexDefinition,
    ) -> Result<IndexMaintenanceReport> {
        let started = Instant::now();
        self.persist_index_maintenance_status(
            &definition,
            IndexMaintenanceStatus {
                collection: definition.collection.clone(),
                kind: definition.kind.clone(),
                status: "building".to_string(),
                progress_percent: 1,
                stale: true,
                updated_unix_ms: unix_timestamp_millis(),
                ..IndexMaintenanceStatus::default()
            },
        )?;

        self.index_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.indexes.insert(
            definition.name.clone(),
            RwLock::new(IndexState {
                definition: definition.clone(),
                store: new_index_store(),
                spatial: None,
                packed_spatial: None,
                spatial_tombstones: FxHashSet::default(),
                spatial_delta_durable: false,
                paged_read_through: false,
                full_text_build_incomplete: false,
            }),
        );
        if let Err(error) = self.persist_index_catalog() {
            self.index_generation
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.indexes.remove(&definition.name);
            return Err(error);
        }

        self.backfill_paged_index_entries(&definition)?;
        if let Some(paged) = &self.paged_records {
            let snapshot = paged.latest_snapshot();
            if paged.index_has_any_posting_blocks(&snapshot, &definition.name)? {
                if let Some(state) = self.indexes.get(&definition.name) {
                    state.write().paged_read_through = true;
                }
            }
        }

        let mut verification = self.verify_index(&definition.name)?;
        verification.last_verified_unix_ms = Some(unix_timestamp_millis());
        let report = IndexMaintenanceReport {
            index_name: definition.name.clone(),
            collection: definition.collection.clone(),
            kind: definition.kind.clone(),
            operation: "created".to_string(),
            status: if verification.valid {
                "complete"
            } else {
                "corrupt"
            }
            .to_string(),
            online: true,
            restartable: true,
            interrupted_previous: false,
            progress_percent: 100,
            records_scanned: verification.expected_records,
            records_indexed: verification.indexed_records,
            size_bytes: verification.size_bytes,
            build_time_ms: started.elapsed().as_millis(),
            lock_phases: vec![IndexLockPhaseReport {
                phase: "external-generation-and-catalog-publish".to_string(),
                bounded: true,
                duration_ms: started.elapsed().as_millis(),
            }],
            stale: !verification.valid,
            corrupt: !verification.valid,
            last_verified_unix_ms: verification.last_verified_unix_ms,
            verification,
        };
        self.persist_index_maintenance_status(
            &definition,
            IndexMaintenanceStatus {
                collection: definition.collection.clone(),
                kind: definition.kind.clone(),
                status: report.status.clone(),
                progress_percent: report.progress_percent,
                size_bytes: report.size_bytes,
                build_time_ms: report.build_time_ms,
                stale: report.stale,
                corrupt: report.corrupt,
                last_verified_unix_ms: report.last_verified_unix_ms,
                updated_unix_ms: unix_timestamp_millis(),
            },
        )?;
        self.schema_compatibility.invalidate();
        Ok(report)
    }

    /// Resource-admitted online index creation. The lane is acquired before
    /// materialization, tokenization, spill generation, merge, or publication.
    pub fn create_index_online_governed(
        &mut self,
        definition: IndexDefinition,
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
    ) -> Result<IndexMaintenanceReport> {
        let permit = self.admit_index_build(governor, demand, now_ms)?;
        let result = self.create_index_online(definition);
        drop(permit);
        result
    }

    pub(crate) fn paged_btree_build_limits(&self) -> PagedBTreeBuildLimits {
        PagedBTreeBuildLimits {
            max_batch_rows: self.config.btree_build_batch_rows,
            max_batch_bytes: self.config.btree_build_batch_bytes,
            ..PagedBTreeBuildLimits::default()
        }
    }

    pub(crate) fn paged_btree_build_report(
        definition: &IndexDefinition,
        state: &PagedBTreeBuildState,
        started: Instant,
        interrupted_previous: bool,
    ) -> IndexMaintenanceReport {
        let indexed = usize::try_from(state.expected_entries).unwrap_or(usize::MAX);
        let verification = IndexVerifyReport {
            index_name: definition.name.clone(),
            collection: definition.collection.clone(),
            kind: IndexKind::BTree,
            indexed_records: indexed,
            expected_records: indexed,
            missing_entries: 0,
            stale_entries: 0,
            duplicate_entries: 0,
            wrong_entries: 0,
            size_bytes: paged_btree_entry_bytes(indexed),
            last_verified_unix_ms: Some(unix_timestamp_millis()),
            valid: true,
        };
        IndexMaintenanceReport {
            index_name: definition.name.clone(),
            collection: definition.collection.clone(),
            kind: IndexKind::BTree,
            operation: "created".to_string(),
            status: "complete".to_string(),
            online: false,
            restartable: true,
            interrupted_previous,
            progress_percent: 100,
            records_scanned: indexed,
            records_indexed: indexed,
            size_bytes: paged_btree_entry_bytes(indexed),
            build_time_ms: started.elapsed().as_millis(),
            lock_phases: vec![IndexLockPhaseReport {
                phase: "bounded-shadow-generation-and-catalog-publish".to_string(),
                bounded: true,
                duration_ms: started.elapsed().as_millis(),
            }],
            stale: false,
            corrupt: false,
            last_verified_unix_ms: verification.last_verified_unix_ms,
            verification,
        }
    }

    /// Complete the checkpoint after a crash that occurred after both durable
    /// publication files were installed but before the build state could be
    /// advanced from `publishing` to `complete`.
    /// Progress of a full-text build, running or interrupted.
    ///
    /// Read from the durable checkpoint rather than from live build state, so
    /// it answers from a different process while the build is in flight —
    /// which is the only time an operator needs it. `None` means no build
    /// workspace exists: either it never started or it finished and was
    /// cleaned up.
    pub fn full_text_build_progress(
        &self,
        index: &str,
    ) -> Result<Option<crate::fts_build::FullTextBuildStatus>> {
        let root = self.path.join(DEFAULT_FTS_BUILD_DIR);
        let Some(workspace) =
            crate::fts_build::FtsBuildWorkspace::open_existing(&root, index, self.config.fsync)?
        else {
            return Ok(None);
        };
        let mut progress = workspace.progress();
        // A denominator exists only while tokenizing: the rows the scan has
        // yet to reach. The later phases work on intermediate runs whose size
        // is not a fraction of anything an operator would recognise.
        if progress.phase == "tokenizing" {
            if let Ok(total) = self.collection_record_count(&progress.collection) {
                let total = total as u64;
                progress.documents_total = Some(total);
                if total > 0 {
                    progress.percent = Some(
                        ((progress.documents_tokenized.min(total) * 100) / total).min(100) as u8,
                    );
                }
            }
        }
        Ok(Some(progress))
    }
}
