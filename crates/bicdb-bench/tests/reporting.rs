use bicdb_bench::{
    default_bench_path, run_ann_bench, run_event_bench, run_graph_bench, run_index_bench,
    run_insert_bench, run_memory_bench, run_postgres_compat_suite, run_projection_bench,
    run_queue_bench, run_route_bench, run_server_bench, run_server_bench_with_config,
    run_server_certification, run_spatial_bench, run_spatial_nearest_bench, run_sql_bench,
    run_sync_bench, PostgresCompatCoverageState, ServerBenchScenario,
};
#[cfg(feature = "comparison-engines")]
use bicdb_bench::{run_insert_baseline_suite, BaselineEngine, BaselineStatus};

#[test]
fn insert_report_exports_json_and_csv() {
    let path = default_bench_path("export-test");
    let report = run_insert_bench(path, 100, 25).unwrap();

    let json = report.to_json().unwrap();
    assert!(json.contains("\"mode\": \"inserts\""));
    assert!(json.contains("\"records\": 100"));

    let csv = report.to_csv();
    assert!(csv.starts_with("mode,records,batch_size"));
    assert!(csv.contains("inserts,100,25"));
}

#[test]
#[cfg(feature = "comparison-engines")]
fn baseline_suite_reports_embedded_and_external_engines() {
    let path = default_bench_path("baseline-test");
    let reports = run_insert_baseline_suite(path, 100, 25).unwrap();

    let bicdb = find(&reports, BaselineEngine::BicDb);
    let redb = find(&reports, BaselineEngine::Redb);
    let fjall = find(&reports, BaselineEngine::Fjall);
    let sqlite_vec = find(&reports, BaselineEngine::SqliteVec);
    let lancedb = find(&reports, BaselineEngine::LanceDb);
    let qdrant = find(&reports, BaselineEngine::Qdrant);

    assert!(matches!(bicdb.status, BaselineStatus::Completed));
    assert!(matches!(redb.status, BaselineStatus::Completed));
    assert!(matches!(fjall.status, BaselineStatus::Completed));
    assert!(matches!(sqlite_vec.status, BaselineStatus::Skipped));
    assert!(matches!(lancedb.status, BaselineStatus::Skipped));
    assert!(matches!(qdrant.status, BaselineStatus::Skipped));
}

#[test]
fn event_queue_and_projection_reports_export_json_and_csv() {
    let event_report = run_event_bench(default_bench_path("event-report-test"), 25).unwrap();
    assert!(event_report
        .to_json()
        .unwrap()
        .contains("\"mode\": \"events\""));
    assert!(event_report.to_csv().starts_with("mode,events"));

    let queue_report = run_queue_bench(default_bench_path("queue-report-test"), 25, 10).unwrap();
    assert!(queue_report
        .to_json()
        .unwrap()
        .contains("\"mode\": \"queue\""));
    assert!(queue_report.to_csv().starts_with("mode,messages"));

    let projection_report =
        run_projection_bench(default_bench_path("projection-report-test"), 10, 2).unwrap();
    assert!(projection_report
        .to_json()
        .unwrap()
        .contains("\"mode\": \"projections\""));
    assert!(projection_report.to_csv().starts_with("mode,events"));
}

#[test]
fn sql_report_exports_json_and_csv() {
    let report = run_sql_bench(default_bench_path("sql-report-test"), 100).unwrap();

    assert!(report.to_json().unwrap().contains("\"mode\": \"sql\""));
    assert!(report.to_csv().starts_with("mode,records"));
}

#[test]
fn index_report_exports_json_and_csv() {
    let report = run_index_bench(default_bench_path("index-report-test"), 100).unwrap();

    assert!(report.to_json().unwrap().contains("\"mode\": \"indexes\""));
    assert!(report.to_csv().starts_with("mode,records"));
    assert!(report.index_size_bytes > 0);
    assert!(report.index_rebuild_ms >= 0.0);
    assert!(report.peak_memory_estimate_bytes >= report.index_size_bytes);
}

#[test]
fn ann_report_exports_json_and_csv() {
    let report = run_ann_bench(default_bench_path("ann-report-test"), 64, 8, 5).unwrap();

    assert!(report.to_json().unwrap().contains("\"mode\": \"ann\""));
    assert!(report.to_csv().starts_with("mode,records,dim"));
    assert!(report.ann_ef100_recall_at_k > 0.0);
    assert!(report.index_size_bytes > 0);
}

#[test]
fn graph_report_exports_json_and_csv() {
    let report = run_graph_bench(default_bench_path("graph-report-test"), 25, 100).unwrap();

    assert!(report.to_json().unwrap().contains("\"mode\": \"graph\""));
    assert!(report.to_csv().starts_with("mode,entities,edges"));
    assert_eq!(report.edge_count, 100);
    assert!(report.node_count >= 25);
}

#[test]
fn spatial_reports_export_json_and_csv() {
    let report = run_spatial_bench(default_bench_path("spatial-report-test"), 50).unwrap();

    assert!(report.to_json().unwrap().contains("\"mode\": \"spatial\""));
    assert!(report.to_csv().starts_with("mode,points"));
    assert!(report.database_size_bytes > 0);

    let nearest =
        run_spatial_nearest_bench(default_bench_path("spatial-nearest-report-test"), 50, 5)
            .unwrap();

    let json = nearest.to_json().unwrap();
    assert!(json.contains("\"mode\": \"spatial_nearest\""));
    assert!(json.contains("\"nearest_p50_ms\""));
    assert!(json.contains("\"radius_p99_ms\""));
    assert!(nearest.to_csv().starts_with("mode,points,queries"));
}

#[test]
fn route_report_exports_json_and_csv() {
    let report = run_route_bench(default_bench_path("route-report-test"), 25, 75).unwrap();

    let json = report.to_json().unwrap();
    assert!(json.contains("\"mode\": \"route\""));
    assert!(json.contains("\"route_p50_ms\""));
    assert!(report.to_csv().starts_with("mode,nodes,edges"));
    assert!(report.database_size_bytes > 0);
}

#[test]
fn sync_report_exports_json_and_csv() {
    let report = run_sync_bench(default_bench_path("sync-report-test"), 25).unwrap();

    assert!(report.to_json().unwrap().contains("\"mode\": \"sync\""));
    assert!(report.to_csv().starts_with("mode,records_per_node"));
    assert!(report.converged);
}

#[test]
fn memory_report_exports_json_and_csv() {
    let report = run_memory_bench(default_bench_path("memory-report-test"), 25, 8, 5).unwrap();

    assert!(report.to_json().unwrap().contains("\"mode\": \"memory\""));
    assert!(report.to_csv().starts_with("mode,memories"));
    assert_eq!(report.memory_event_count, 25);
}

#[test]
fn server_report_exports_json_and_csv() {
    let report = run_server_bench(default_bench_path("server-report-test"), 2, 10).unwrap();

    assert!(report.to_json().unwrap().contains("\"mode\": \"server\""));
    assert!(report.to_csv().starts_with("mode,scenario,clients"));
    assert!(report.to_markdown().contains("DB lock wait avg/max ms"));
    assert_eq!(report.queries, 10);
    assert_eq!(report.server_failed_queries, 0);
    assert!(report.db_lock_acquisitions > 0);
}

#[test]
fn server_concurrency_report_covers_pooled_read_clients() {
    let report = run_server_bench_with_config(
        default_bench_path("server-concurrency-report-test"),
        4,
        2,
        12,
        ServerBenchScenario::ReadOnly,
    )
    .unwrap();

    assert_eq!(report.scenario, "read-only");
    assert_eq!(report.clients, 4);
    assert_eq!(report.active_query_concurrency, 2);
    assert_eq!(report.select_queries, 12);
    assert_eq!(report.insert_queries, 0);
    assert!(report.active_connections_peak >= 4);
    assert_eq!(report.server_failed_queries, 0);
    assert!(report.db_lock_acquisitions >= 12);
}

#[test]
fn server_certification_ci_profile_covers_required_scenarios() {
    let report = run_server_certification(
        default_bench_path("server-cert-report-test"),
        "ci",
        4,
        2,
        10,
    )
    .unwrap();

    assert_eq!(report.mode, "server-certification");
    assert!(report.passed, "{:?}", report.scenarios);
    assert_eq!(report.scenarios.len(), 5);
    assert!(report
        .scenarios
        .iter()
        .any(|scenario| scenario.scenario == "idle-pooled"));
    assert!(report
        .scenarios
        .iter()
        .any(|scenario| scenario.scenario == "read-only"));
    assert!(report
        .scenarios
        .iter()
        .any(|scenario| scenario.scenario == "mixed"
            && scenario.report.final_patient_count == Some(200 + scenario.report.insert_queries)));
    assert!(report
        .scenarios
        .iter()
        .any(|scenario| scenario.scenario == "cancel-contention"
            && scenario.report.server_canceled_queries == 1));
    assert!(report
        .scenarios
        .iter()
        .any(|scenario| scenario.scenario == "churn"));

    let json = report.to_json().unwrap();
    assert!(json.contains("\"mode\": \"server-certification\""));
    assert!(json.contains("\"budget\""));

    let csv = report.to_csv();
    assert!(csv.starts_with("mode,profile,cert_passed,idle_soak_ms,scenario"));
    assert!(csv.contains("cancel-contention"));

    let markdown = report.to_markdown();
    assert!(markdown.contains("# BicDB Server Certification"));
    assert!(markdown.contains("## Budgets"));
}

#[test]
fn postgres_compat_report_exports_scorecard_json_and_csv() {
    let report =
        run_postgres_compat_suite(default_bench_path("postgres-compat-report-test"), "18.4")
            .unwrap();

    assert_eq!(report.mode, "postgres_compat");
    assert_eq!(report.target_version, "18.4");
    assert!(report.total_cases >= 20);
    assert_eq!(report.failed_cases, 0);
    assert!(report.score_percent >= 99.0);
    assert!(!report.known_gaps.is_empty());
    assert!(report
        .cases
        .iter()
        .any(|case| case.coverage_state == PostgresCompatCoverageState::Supported));
    assert!(report
        .cases
        .iter()
        .any(|case| case.coverage_state == PostgresCompatCoverageState::Unsupported));
    assert!(report
        .cases
        .iter()
        .any(|case| case.coverage_state == PostgresCompatCoverageState::ExpectedDifference));
    assert!(report
        .cases
        .iter()
        .any(|case| case.coverage_state == PostgresCompatCoverageState::NotYetTested));
    assert!(report
        .category_scores
        .iter()
        .any(|category| category.category == "client_matrix" && category.not_yet_tested_cases > 0));

    let json = report.to_json().unwrap();
    assert!(json.contains("\"mode\": \"postgres_compat\""));
    assert!(json.contains("\"target_version\": \"18.4\""));
    assert!(json.contains("\"category_scores\""));
    assert!(json.contains("\"coverage_state\": \"unsupported\""));
    assert!(json.contains("\"coverage_state\": \"not_yet_tested\""));

    let csv = report.to_csv();
    assert!(csv.starts_with("mode,target_version,category,id"));
    assert!(csv.contains("protocol.simple_query.select_1"));
    assert!(csv.contains("unsupported"));
    assert!(csv.contains("expected_difference"));
    assert!(csv.contains("not_yet_tested"));
    assert!(csv.contains("known_gap_category"));

    let markdown = report.to_markdown();
    assert!(markdown.contains("# BicDB PostgreSQL Compatibility Scorecard"));
    assert!(markdown.contains("## Category Scores"));
    assert!(
        markdown.contains("| Category | Case | Expectation | Status | Coverage State | Detail |")
    );
    assert!(markdown.contains("not_yet_tested"));
}

#[cfg(feature = "comparison-engines")]
fn find(
    reports: &[bicdb_bench::BaselineReport],
    engine: BaselineEngine,
) -> &bicdb_bench::BaselineReport {
    reports
        .iter()
        .find(|report| report.engine == engine)
        .unwrap_or_else(|| panic!("missing baseline report for {engine:?}"))
}
