//! G7: sketches must be accurate enough to be useful, associatively
//! mergeable, and honest about when they are approximating.

use bicdb_core::aggregate_sketch::{HyperLogLog, Quantiles};

#[test]
fn hll_is_accurate_across_magnitudes() {
    for distinct in [10usize, 500, 10_000, 200_000] {
        let mut sketch = HyperLogLog::default();
        for index in 0..distinct {
            sketch.add(&format!("domain-{index}.example"));
        }
        let estimate = sketch.estimate();
        let error = (estimate - distinct as f64).abs() / distinct as f64;
        assert!(
            error < 0.05,
            "{distinct} distinct estimated {estimate:.0} ({:.1}% error)",
            error * 100.0
        );
    }
}

#[test]
fn hll_ignores_duplicates() {
    let mut sketch = HyperLogLog::default();
    for _ in 0..50 {
        for index in 0..1_000 {
            sketch.add(&format!("host-{index}"));
        }
    }
    let estimate = sketch.estimate();
    assert!(
        (estimate - 1000.0).abs() / 1000.0 < 0.05,
        "50,000 adds of 1,000 distinct values estimated {estimate:.0}"
    );
}

/// The property distributed aggregation depends on: per-shard sketches merge
/// at a coordinator with no re-scan.
#[test]
fn hll_merges_associatively() {
    let total = 30_000usize;
    let mut whole = HyperLogLog::default();
    let mut shard_a = HyperLogLog::default();
    let mut shard_b = HyperLogLog::default();
    let mut shard_c = HyperLogLog::default();
    for index in 0..total {
        let value = format!("domain-{index}");
        whole.add(&value);
        match index % 3 {
            0 => shard_a.add(&value),
            1 => shard_b.add(&value),
            _ => shard_c.add(&value),
        }
    }
    // Overlapping shards must not double-count.
    for index in 0..1_000 {
        shard_a.add(&format!("domain-{index}"));
        shard_b.add(&format!("domain-{index}"));
    }
    let mut merged = shard_a.clone();
    merged.merge(&shard_b);
    merged.merge(&shard_c);

    let difference = (merged.estimate() - whole.estimate()).abs() / whole.estimate();
    assert!(
        difference < 0.02,
        "merged {:.0} vs whole {:.0}",
        merged.estimate(),
        whole.estimate()
    );

    // Merge order must not matter.
    let mut other_order = shard_c.clone();
    other_order.merge(&shard_a);
    other_order.merge(&shard_b);
    assert!((other_order.estimate() - merged.estimate()).abs() < 1.0);
}

#[test]
fn quantiles_are_exact_while_the_reservoir_holds_everything() {
    let mut sketch = Quantiles::default();
    for value in 1..=100 {
        sketch.add(value as f64);
    }
    assert!(
        sketch.is_exact(),
        "100 values must fit the reservoir exactly"
    );
    assert_eq!(sketch.percentile(0.5), Some(50.0));
    assert_eq!(sketch.percentile(0.95), Some(95.0));
    assert_eq!(sketch.percentile(0.0), Some(1.0));
    assert_eq!(sketch.percentile(1.0), Some(100.0));
    assert_eq!(sketch.count(), 100);
}

#[test]
fn quantiles_stay_bounded_and_admit_approximation() {
    let mut sketch = Quantiles::default();
    for value in 1..=50_000 {
        sketch.add(value as f64);
    }
    assert!(
        !sketch.is_exact(),
        "beyond the reservoir the sketch must ADMIT it is sampling"
    );
    assert_eq!(sketch.count(), 50_000, "the true count is still tracked");

    let median = sketch.percentile(0.5).unwrap();
    assert!(
        (median - 25_000.0).abs() / 25_000.0 < 0.15,
        "median {median:.0} too far from 25,000"
    );
    // Tails are preserved by construction.
    // A uniform sample will not hold the literal extremes; it must bracket
    // the distribution rather than reproduce its endpoints.
    assert!(sketch.percentile(0.05).unwrap() < 10_000.0);
    assert!(sketch.percentile(0.95).unwrap() > 40_000.0);
}

#[test]
fn quantiles_merge() {
    let mut whole = Quantiles::default();
    let mut left = Quantiles::default();
    let mut right = Quantiles::default();
    for value in 1..=2_000 {
        whole.add(value as f64);
        if value % 2 == 0 {
            left.add(value as f64);
        } else {
            right.add(value as f64);
        }
    }
    let mut merged = left.clone();
    merged.merge(&right);
    assert_eq!(merged.count(), 2_000, "merged count must be the true total");
    let difference = (merged.percentile(0.5).unwrap() - whole.percentile(0.5).unwrap()).abs();
    assert!(difference < 200.0, "merged median drifted by {difference}");
}

#[test]
fn empty_sketches_report_absence_not_zero() {
    let hll = HyperLogLog::default();
    assert!(hll.is_empty());
    assert!(hll.estimate() < 1.0);
    let quantiles = Quantiles::default();
    assert_eq!(
        quantiles.percentile(0.5),
        None,
        "no data is not a percentile of 0"
    );
}
