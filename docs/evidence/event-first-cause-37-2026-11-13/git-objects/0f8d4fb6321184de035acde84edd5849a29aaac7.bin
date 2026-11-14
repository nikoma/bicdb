use super::*;

#[test]
fn event_client_query_budget_context_keeps_tight_nested_deadline_and_shared_peak() {
    let outer_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let outer = QueryMemoryScope::with_deadline(64, outer_deadline);
    let inner_deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    {
        let inner = QueryMemoryScope::with_deadline(16, inner_deadline);
        let context = QueryBudgetContext::capture();
        assert_eq!(context.deadline(), Some(inner_deadline));
        std::thread::spawn(move || {
            assert!(!active(), "fresh worker starts without a query envelope");
            {
                let _installed = context.install();
                assert_eq!(limit(1024), 16);
                observe(17);
                assert!(matches!(check(), Err(BicDbError::QueryMemoryLimit(_))));
            }
            assert!(!active(), "local worker guard restores its original stack");
        })
        .join()
        .unwrap();
        assert_eq!(inner.peak_bytes(), 17);
        assert!(matches!(check(), Err(BicDbError::QueryMemoryLimit(_))));
    }
    assert_eq!(
        outer.peak_bytes(),
        17,
        "worker peak also belongs to outer caller"
    );
    assert!(check().is_ok(), "dropping inner scope restores outer limit");
    assert_eq!(
        QueryBudgetContext::capture().deadline(),
        Some(outer_deadline)
    );
}
