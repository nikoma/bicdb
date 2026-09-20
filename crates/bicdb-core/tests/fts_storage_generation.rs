use bicdb_core::{
    BicDb, BlockGate, Bm25fParameters, DbConfig, FullTextBuildLifecycleState,
    FullTextBuildRecommendedAction, FullTextBuildReconcileOutcome, FullTextDocumentInput,
    FullTextField, FullTextFieldInput, FullTextFilterInput, FullTextTermInput, IndexDefinition,
    IndexField, IndexKind, StorageMode,
};

fn term(term: &str, positions: &[u16]) -> FullTextTermInput {
    FullTextTermInput {
        term: term.to_string(),
        positions: positions.to_vec(),
    }
}

#[test]
fn direct_documents_use_native_fields_compressed_source_and_compact_impacts() {
    let directory = tempfile::tempdir().unwrap();
    let config = DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_fts_build_memory_bytes(2 * 1024 * 1024)
        .with_fts_build_workers(2);
    let mut db = BicDb::open_with_config(directory.path(), config).unwrap();
    db.create_collection("documents").unwrap();
    db.create_index(IndexDefinition {
        name: "documents_fts".to_string(),
        collection: "documents".to_string(),
        fields: vec![IndexField::MetadataPath(vec!["unused".to_string()])],
        unique: false,
        kind: IndexKind::FullText,
        predicate: None,
        exclusion: None,
    })
    .unwrap();

    let progress = db
        .prepare_full_text_build("documents_fts", "documents", "direct-fields-v1")
        .unwrap();
    assert!(progress.needs_tokenization);
    let documents = (0..256)
        .map(|number| {
            let field = if number % 2 == 0 {
                FullTextField::Title
            } else {
                FullTextField::Body
            };
            FullTextDocumentInput {
                primary_key: format!("doc-{number:04}"),
                fields: vec![FullTextFieldInput {
                    field,
                    terms: vec![term("bicdb", &[1]), term(&format!("unique{number}"), &[2])],
                }],
                filters: Vec::new(),
                stored_text: Some(
                    format!(
                        "<html><body>BicDB document {number}. {}</body></html>",
                        "retrieval text ".repeat(200)
                    )
                    .into_bytes(),
                ),
            }
        })
        .collect::<Vec<_>>();
    db.append_full_text_documents("documents_fts", &documents)
        .unwrap();
    db.finish_full_text_tokenization("documents_fts").unwrap();
    db.complete_prepared_full_text_build("documents_fts")
        .unwrap();

    assert!(
        db.scan_collection("documents").unwrap().is_empty(),
        "direct ingestion must not retain a duplicate source row"
    );
    assert_eq!(
        db.full_text_term_postings("documents_fts", "bicdb", false)
            .unwrap()
            .len(),
        documents.len()
    );
    assert_eq!(
        db.full_text_stored_text("documents_fts", "doc-0007")
            .unwrap()
            .unwrap(),
        documents[7].stored_text.clone().unwrap()
    );
    let generation = db.full_text_published_generation("documents_fts").unwrap();
    let first = db
        .full_text_stored_text_page("documents_fts", &generation.physical_index, None, 2)
        .unwrap();
    assert_eq!(first.physical_generation, generation.physical_index);
    assert_eq!(
        first.rows,
        vec![
            (0, documents[0].stored_text.clone().unwrap()),
            (1, documents[1].stored_text.clone().unwrap()),
        ]
    );
    assert_eq!(first.next_after_document_id, Some(1));

    let middle = db
        .full_text_stored_text_page("documents_fts", &generation.physical_index, Some(127), 2)
        .unwrap();
    assert_eq!(middle.rows[0].0, 128);
    assert_eq!(middle.rows[1].0, 129);
    assert_eq!(middle.next_after_document_id, Some(129));

    let final_page = db
        .full_text_stored_text_page("documents_fts", &generation.physical_index, Some(254), 2)
        .unwrap();
    assert_eq!(
        final_page.rows,
        vec![(255, documents[255].stored_text.clone().unwrap())]
    );
    assert_eq!(final_page.next_after_document_id, None);

    let exact_boundary = db
        .full_text_stored_text_page("documents_fts", &generation.physical_index, None, 256)
        .unwrap();
    assert_eq!(exact_boundary.rows.len(), 256);
    assert_eq!(exact_boundary.next_after_document_id, None);
    let empty = db
        .full_text_stored_text_page("documents_fts", &generation.physical_index, Some(255), 2)
        .unwrap();
    assert!(empty.rows.is_empty());
    assert_eq!(empty.next_after_document_id, None);
    assert!(db
        .full_text_stored_text_page("documents_fts", &generation.physical_index, None, 0)
        .is_err());
    assert!(db
        .full_text_stored_text_page("documents_fts", &generation.physical_index, None, 4_097)
        .is_err());
    assert!(db
        .full_text_stored_text_page("documents_fts", "wrong-generation", None, 2)
        .is_err());

    let ranked = db
        .full_text_bm25f_top_k(
            "documents_fts",
            &["bicdb"],
            Bm25fParameters::default(),
            16,
            false,
        )
        .unwrap()
        .unwrap();
    assert!(
        ranked
            .iter()
            .all(|posting| posting.primary_key[4..].parse::<usize>().unwrap() % 2 == 0),
        "native title fields should outrank otherwise identical body fields"
    );
    let mut impact_visits = 0usize;
    let mut budget = bicdb_core::FtsQueryBudget::unlimited();
    assert!(db
        .full_text_block_impact_scan(
            "documents_fts",
            "bicdb",
            &mut budget,
            |_, _| BlockGate::Scan,
            |_, _, _, _, _| {
                impact_visits += 1;
                Ok(impact_visits < 16)
            },
        )
        .unwrap());
    assert_eq!(impact_visits, 16);

    let accounting = db.full_text_storage_accounting("documents_fts").unwrap();
    assert_eq!(accounting.row_data.entries, 0);
    assert_eq!(accounting.stored_text.entries, documents.len() as u64);
    let raw_stored_bytes = documents
        .iter()
        .map(|document| document.stored_text.as_ref().unwrap().len() as u64)
        .sum::<u64>();
    assert!(
        accounting.stored_text.value_bytes * 10 < raw_stored_bytes,
        "stored={} raw={raw_stored_bytes}",
        accounting.stored_text.value_bytes
    );
    assert_eq!(
        accounting.document_statistics.entries,
        documents.len() as u64
    );
    assert!(accounting.term_dictionary.entries > documents.len() as u64);
    assert!(accounting.postings.value_bytes > 0);
    if std::env::var("BICDB_FTS_PACKED_SEGMENTS").as_deref() == Ok("0") {
        // v1 posting blocks carry ~5 bytes of per-posting metadata, so the
        // score-only impact sidecar is always the smaller stream.
        assert!(
            accounting.impact_metadata.value_bytes < accounting.postings.value_bytes,
            "impact={} postings={}",
            accounting.impact_metadata.value_bytes,
            accounting.postings.value_bytes
        );
    } else {
        // Slim segment postings shed that metadata; on a tiny-document corpus
        // the impact sidecar can legitimately outweigh them. The invariant
        // that matters is that both streams exist and are accounted.
        assert!(accounting.impact_metadata.value_bytes > 0);
    }
}

#[test]
fn sealed_direct_documents_are_searchable_without_reverse_term_blobs() {
    let directory = tempfile::tempdir().unwrap();
    let config = DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_fts_packed_segments(true)
        .with_fts_build_memory_bytes(2 * 1024 * 1024)
        .with_fts_build_workers(2);
    let mut db = BicDb::open_with_config(directory.path(), config).unwrap();
    db.create_collection("documents").unwrap();
    db.create_index(IndexDefinition {
        name: "documents_fts".to_string(),
        collection: "documents".to_string(),
        fields: vec![IndexField::MetadataPath(vec!["unused".to_string()])],
        unique: false,
        kind: IndexKind::FullText,
        predicate: None,
        exclusion: None,
    })
    .unwrap();
    db.prepare_full_text_build("documents_fts", "documents", "sealed-direct-v1")
        .unwrap();
    let documents = vec![
        FullTextDocumentInput {
            primary_key: "doc-0001".to_string(),
            fields: vec![FullTextFieldInput {
                field: FullTextField::Body,
                terms: vec![term("sealed", &[1]), term("corpus", &[2])],
            }],
            filters: vec![FullTextFilterInput {
                name: "domain".to_string(),
                value: "example.com".to_string(),
            }],
            stored_text: Some(b"first retrieval preview".to_vec()),
        },
        FullTextDocumentInput {
            primary_key: "doc-0002".to_string(),
            fields: vec![FullTextFieldInput {
                field: FullTextField::Body,
                terms: vec![term("sealed", &[1]), term("search", &[2])],
            }],
            filters: vec![FullTextFilterInput {
                name: "domain".to_string(),
                value: "example.org".to_string(),
            }],
            stored_text: Some(b"second retrieval preview".to_vec()),
        },
    ];
    db.append_sealed_full_text_documents("documents_fts", &documents)
        .unwrap();
    db.finish_full_text_tokenization("documents_fts").unwrap();
    db.complete_prepared_full_text_build("documents_fts")
        .unwrap();

    assert_eq!(db.full_text_doc_terms_count("documents_fts").unwrap(), 0);
    assert_eq!(
        db.full_text_term_postings("documents_fts", "sealed", false)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        db.full_text_stored_text("documents_fts", "doc-0002")
            .unwrap()
            .unwrap(),
        b"second retrieval preview"
    );
    let accounting = db.full_text_storage_accounting("documents_fts").unwrap();
    assert_eq!(accounting.document_terms.entries, 0);
    assert_eq!(accounting.stored_text.entries, 2);
    assert!(accounting.postings.value_bytes > 0);

    let filter = db
        .full_text_document_filter_from_sealed_value("documents_fts", "domain", "example.org")
        .unwrap();
    assert_eq!(filter.cardinality(), 1);
    let filtered = db
        .full_text_bm25_top_k_filtered(
            "documents_fts",
            &["sealed"],
            bicdb_core::Bm25Parameters::default(),
            10,
            true,
            Some(&filter),
        )
        .unwrap()
        .unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].primary_key, "doc-0002");
}

#[test]
fn sealed_direct_progressive_documents_are_searchable_before_completion() {
    let directory = tempfile::tempdir().unwrap();
    let config = DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_fts_packed_segments(true)
        .with_fts_progressive(true)
        .with_fts_progressive_interval_docs(2)
        .with_fts_build_memory_bytes(2 * 1024 * 1024)
        .with_fts_build_workers(2);
    let mut db = BicDb::open_with_config(directory.path(), config.clone()).unwrap();
    db.create_collection("documents").unwrap();
    db.create_index(IndexDefinition {
        name: "documents_fts".to_string(),
        collection: "documents".to_string(),
        fields: vec![IndexField::MetadataPath(vec!["unused".to_string()])],
        unique: false,
        kind: IndexKind::FullText,
        predicate: None,
        exclusion: None,
    })
    .unwrap();
    db.prepare_full_text_build("documents_fts", "documents", "sealed-direct-progressive-v1")
        .unwrap();

    let documents = (0..4)
        .map(|number| FullTextDocumentInput {
            primary_key: format!("doc-{number:04}"),
            fields: vec![FullTextFieldInput {
                field: FullTextField::Body,
                terms: vec![
                    term("progressive", &[1]),
                    term(&format!("item{number}"), &[2]),
                ],
            }],
            filters: vec![FullTextFilterInput {
                name: "domain".to_string(),
                value: if number % 2 == 0 {
                    "even.example"
                } else {
                    "odd.example"
                }
                .to_string(),
            }],
            stored_text: Some(format!("progressive document {number}").into_bytes()),
        })
        .collect::<Vec<_>>();

    db.append_sealed_full_text_documents("documents_fts", &documents[..2])
        .unwrap();
    drop(db);

    let mut db = BicDb::open_with_config(directory.path(), config).unwrap();
    let resumed = db
        .prepare_full_text_build("documents_fts", "documents", "sealed-direct-progressive-v1")
        .unwrap();
    assert!(resumed.needs_tokenization);
    let lifecycle = db.full_text_build_lifecycle("documents_fts").unwrap();
    assert_eq!(lifecycle.state, FullTextBuildLifecycleState::AwaitingInput);
    assert_eq!(
        lifecycle.recommended_action,
        FullTextBuildRecommendedAction::AppendDocuments
    );
    assert_eq!(lifecycle.progressive_documents_staged, 2);
    let reconcile = db
        .reconcile_full_text_build("documents_fts", 1_000)
        .unwrap();
    assert_eq!(
        reconcile.outcome,
        FullTextBuildReconcileOutcome::AwaitingInput,
        "reconciliation must not guess that an external producer is finished"
    );
    assert_eq!(
        db.full_text_term_postings("documents_fts", "progressive", false)
            .unwrap()
            .len(),
        2,
        "the first direct progressive interval must be searchable"
    );
    assert_eq!(
        db.full_text_document_filter_from_sealed_value("documents_fts", "domain", "even.example")
            .unwrap()
            .cardinality(),
        1,
        "published progressive filters must cover the same documents as postings"
    );

    db.append_sealed_full_text_documents("documents_fts", &documents[2..])
        .unwrap();
    assert_eq!(
        db.full_text_term_postings("documents_fts", "progressive", false)
            .unwrap()
            .len(),
        4,
        "later direct progressive intervals must extend visible coverage"
    );
    assert_eq!(
        db.full_text_document_filter_from_sealed_value("documents_fts", "domain", "even.example")
            .unwrap()
            .cardinality(),
        2
    );

    db.finish_full_text_tokenization("documents_fts").unwrap();
    db.complete_prepared_full_text_build("documents_fts")
        .unwrap();
    assert_eq!(
        db.full_text_term_postings("documents_fts", "progressive", false)
            .unwrap()
            .len(),
        4,
        "the final segment must preserve the progressive result set"
    );
    assert_eq!(db.full_text_doc_terms_count("documents_fts").unwrap(), 0);
    db.close().unwrap();
}

#[test]
fn progressive_replacement_never_shadows_the_published_generation() {
    let directory = tempfile::tempdir().unwrap();
    let config = DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_fts_packed_segments(true)
        .with_fts_progressive(true)
        .with_fts_progressive_interval_docs(2)
        .with_fts_build_memory_bytes(2 * 1024 * 1024)
        .with_fts_build_workers(2);

    let mut db = BicDb::open_with_config(directory.path(), config.clone()).unwrap();
    db.create_collection("documents").unwrap();
    db.create_index(IndexDefinition {
        name: "documents_fts".to_string(),
        collection: "documents".to_string(),
        fields: vec![IndexField::MetadataPath(vec!["unused".to_string()])],
        unique: false,
        kind: IndexKind::FullText,
        predicate: None,
        exclusion: None,
    })
    .unwrap();
    db.prepare_full_text_build("documents_fts", "documents", "published-v1")
        .unwrap();
    db.append_sealed_full_text_documents(
        "documents_fts",
        &[
            FullTextDocumentInput {
                primary_key: "old-1".to_string(),
                fields: vec![FullTextFieldInput {
                    field: FullTextField::Body,
                    terms: vec![term("published", &[1])],
                }],
                filters: Vec::new(),
                stored_text: Some(b"old published document one".to_vec()),
            },
            FullTextDocumentInput {
                primary_key: "old-2".to_string(),
                fields: vec![FullTextFieldInput {
                    field: FullTextField::Body,
                    terms: vec![term("published", &[1])],
                }],
                filters: Vec::new(),
                stored_text: Some(b"old published document two".to_vec()),
            },
        ],
    )
    .unwrap();
    db.finish_full_text_tokenization("documents_fts").unwrap();
    db.complete_prepared_full_text_build("documents_fts")
        .unwrap();
    assert_eq!(
        db.full_text_term_postings("documents_fts", "published", false)
            .unwrap()
            .len(),
        2
    );
    let published_generation = db.full_text_published_generation("documents_fts").unwrap();
    assert_eq!(published_generation.document_count, 2);
    assert!(published_generation.segment_manifest_sha256.is_some());

    db.prepare_full_text_build("documents_fts", "documents", "replacement-v2")
        .unwrap();
    let pinned_page = db
        .full_text_stored_text_page(
            "documents_fts",
            &published_generation.physical_index,
            None,
            2,
        )
        .unwrap();
    assert_eq!(pinned_page.rows.len(), 2);
    assert_eq!(
        pinned_page.physical_generation,
        published_generation.physical_index
    );
    drop(db);

    let db = BicDb::open_with_config(directory.path(), config.clone()).unwrap();
    assert_eq!(
        db.full_text_published_generation("documents_fts")
            .unwrap()
            .physical_index,
        published_generation.physical_index
    );
    assert_eq!(
        db.full_text_term_postings("documents_fts", "published", false)
            .unwrap()
            .len(),
        2,
        "an empty replacement workspace must not mask the published generation after reopen"
    );
    db.append_sealed_full_text_documents(
        "documents_fts",
        &[
            FullTextDocumentInput {
                primary_key: "new-1".to_string(),
                fields: vec![FullTextFieldInput {
                    field: FullTextField::Body,
                    terms: vec![term("replacement", &[1])],
                }],
                filters: Vec::new(),
                stored_text: Some(b"replacement document one".to_vec()),
            },
            FullTextDocumentInput {
                primary_key: "new-2".to_string(),
                fields: vec![FullTextFieldInput {
                    field: FullTextField::Body,
                    terms: vec![term("replacement", &[1])],
                }],
                filters: Vec::new(),
                stored_text: Some(b"replacement document two".to_vec()),
            },
        ],
    )
    .unwrap();
    assert_eq!(
        db.full_text_term_postings("documents_fts", "published", false)
            .unwrap()
            .len(),
        2,
        "a progressive replacement sub-segment must remain staging state"
    );
    assert!(db
        .full_text_term_postings("documents_fts", "replacement", false)
        .unwrap()
        .is_empty());
    drop(db);

    let db = BicDb::open_with_config(directory.path(), config).unwrap();
    assert_eq!(
        db.full_text_term_postings("documents_fts", "published", false)
            .unwrap()
            .len(),
        2,
        "reopening a progressive replacement must keep serving the old generation"
    );
    db.finish_full_text_tokenization("documents_fts").unwrap();
    db.complete_prepared_full_text_build("documents_fts")
        .unwrap();
    let replacement_generation = db.full_text_published_generation("documents_fts").unwrap();
    assert_ne!(
        replacement_generation.physical_index,
        published_generation.physical_index
    );
    assert!(db
        .full_text_stored_text_page(
            "documents_fts",
            &published_generation.physical_index,
            None,
            2,
        )
        .is_err());
    assert_eq!(replacement_generation.document_count, 2);
    assert_eq!(
        db.full_text_term_postings("documents_fts", "replacement", false)
            .unwrap()
            .len(),
        2
    );
    assert!(db
        .full_text_term_postings("documents_fts", "published", false)
        .unwrap()
        .is_empty());
    db.close().unwrap();
}
