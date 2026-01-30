//! End-to-end tests for Signal Boosts in the Oneiron retrieval system.
//!
//! These tests verify that signal boosts (salience, confidence, recency) properly
//! affect score calculations and result ordering.
//!
//! Ground truth formula for recency decay:
//! boost = 0.5^(age_days / half_life_days)
//!
//! Example with half_life=30 days:
//! - age=0 days: boost = 0.5^0 = 1.0
//! - age=30 days: boost = 0.5^1 = 0.5
//! - age=60 days: boost = 0.5^2 = 0.25
//! - age=90 days: boost = 0.5^3 = 0.125

use bumpalo::Bump;

use crate::{
    helix_engine::{
        reranker::fusion::signal_boost::{
            apply_signal_boosts, confidence_boost, recency_boost, salience_boost,
            SignalBoostConfig,
        },
        traversal_core::traversal_value::TraversalValue,
        vector_core::vector::HVector,
    },
    protocol::value::Value,
};

const MS_PER_DAY: u64 = 1000 * 60 * 60 * 24;
const TEST_BASE_TIME: u64 = 1_000_000_000_000;

fn ts_days_ago(days: u64) -> u64 {
    TEST_BASE_TIME - (days * MS_PER_DAY)
}

fn create_test_vector<'arena>(arena: &'arena Bump, id: u128, score: f64) -> TraversalValue<'arena> {
    let data = arena.alloc_slice_copy(&[1.0, 2.0, 3.0]);
    let mut v = HVector::from_slice("test", 0, data);
    v.id = id;
    v.distance = Some(score);
    TraversalValue::Vector(v)
}

fn create_vector_with_props<'arena>(
    arena: &'arena Bump,
    id: u128,
    score: f64,
    salience: Option<f64>,
    confidence: Option<f64>,
    recency_ts: Option<u64>,
) -> TraversalValue<'arena> {
    let data = arena.alloc_slice_copy(&[1.0, 2.0, 3.0]);
    let mut v = HVector::from_slice("test", 0, data);
    v.id = id;
    v.distance = Some(score);

    let mut props = Vec::new();
    if let Some(sal) = salience {
        props.push(("salience", Value::F64(sal)));
    }
    if let Some(conf) = confidence {
        props.push(("confidence", Value::F64(conf)));
    }
    if let Some(ts) = recency_ts {
        props.push(("recencyTs", Value::U64(ts)));
    }

    if !props.is_empty() {
        let properties = crate::utils::properties::ImmutablePropertiesMap::new(
            props.len(),
            props.into_iter().map(|(k, val)| {
                let key: &'arena str = arena.alloc_str(k);
                (key, val)
            }),
            arena,
        );
        v.properties = Some(properties);
    }

    TraversalValue::Vector(v)
}

fn extract_score(item: &TraversalValue<'_>) -> f64 {
    match item {
        TraversalValue::Vector(v) => v.distance.unwrap_or(0.0),
        TraversalValue::NodeWithScore { score, .. } => *score,
        _ => 0.0,
    }
}

fn assert_approx_eq(actual: f64, expected: f64, epsilon: f64, msg: &str) {
    assert!(
        (actual - expected).abs() < epsilon,
        "{}: expected {}, got {}, diff={}",
        msg,
        expected,
        actual,
        (actual - expected).abs()
    );
}

/// Test signal boosts with real node properties.
///
/// Creates vectors with salience, confidence, and recencyTs properties,
/// then applies signal boosts and verifies scores are properly modified.
#[test]
fn test_signal_boost_with_real_nodes() {
    let arena = Bump::new();

    let config = SignalBoostConfig::default()
        .with_half_life_days(30.0)
        .with_base_time(TEST_BASE_TIME);

    let items = vec![
        create_vector_with_props(&arena, 1, 1.0, Some(0.8), Some(0.9), Some(TEST_BASE_TIME)),
        create_vector_with_props(&arena, 2, 1.0, Some(0.5), Some(1.0), Some(ts_days_ago(30))),
        create_vector_with_props(&arena, 3, 1.0, Some(1.0), Some(0.5), Some(ts_days_ago(60))),
    ];

    let result = apply_signal_boosts(items, &config).unwrap();

    assert_eq!(result.len(), 3);

    let score1 = extract_score(&result.iter().find(|v| v.id() == 1).unwrap());
    let score2 = extract_score(&result.iter().find(|v| v.id() == 2).unwrap());
    let score3 = extract_score(&result.iter().find(|v| v.id() == 3).unwrap());

    let expected1 = 1.0 * 0.8 * 0.9 * 1.0;
    let expected2 = 1.0 * 0.5 * 1.0 * 0.5;
    let expected3 = 1.0 * 1.0 * 0.5 * 0.25;

    println!("=== Signal Boost with Real Nodes ===");
    println!("Item 1: score={:.4}, expected={:.4}", score1, expected1);
    println!("Item 2: score={:.4}, expected={:.4}", score2, expected2);
    println!("Item 3: score={:.4}, expected={:.4}", score3, expected3);

    assert_approx_eq(score1, expected1, 1e-6, "Item 1 score");
    assert_approx_eq(score2, expected2, 1e-6, "Item 2 score");
    assert_approx_eq(score3, expected3, 1e-6, "Item 3 score");
}

/// Test that signal boosts can reorder search results.
///
/// Creates items where a lower-ranked item (by base score) has higher salience,
/// and verifies it gets promoted after applying boosts.
#[test]
fn test_signal_boost_reorders_search_results() {
    let arena = Bump::new();

    let config = SignalBoostConfig::default()
        .with_salience(true)
        .with_recency(false)
        .with_confidence(false);

    let items = vec![
        create_vector_with_props(&arena, 1, 1.0, Some(0.3), None, None),
        create_vector_with_props(&arena, 2, 0.8, Some(0.9), None, None),
        create_vector_with_props(&arena, 3, 0.6, Some(0.5), None, None),
    ];

    let original_order: Vec<u128> = items.iter().map(|v| v.id()).collect();
    assert_eq!(
        original_order,
        vec![1, 2, 3],
        "Original order should be by base score"
    );

    let result = apply_signal_boosts(items, &config).unwrap();

    let new_order: Vec<u128> = result.iter().map(|v| v.id()).collect();

    let score_id2 = extract_score(&result.iter().find(|v| v.id() == 2).unwrap());
    let score_id1 = extract_score(&result.iter().find(|v| v.id() == 1).unwrap());
    let score_id3 = extract_score(&result.iter().find(|v| v.id() == 3).unwrap());

    println!("=== Signal Boost Reorders Results ===");
    println!("Boosted scores:");
    println!("  ID 1: base=1.0, salience=0.3, final={:.3}", score_id1);
    println!("  ID 2: base=0.8, salience=0.9, final={:.3}", score_id2);
    println!("  ID 3: base=0.6, salience=0.5, final={:.3}", score_id3);
    println!("New order: {:?}", new_order);

    assert!(
        score_id2 > score_id1,
        "ID 2 (0.8 * 0.9 = 0.72) should be higher than ID 1 (1.0 * 0.3 = 0.3)"
    );

    assert_eq!(
        new_order[0], 2,
        "ID 2 should be first after reordering (highest boosted score)"
    );
}

/// Test recency boost with real timestamps.
///
/// Ground truth formula verification:
/// boost = 0.5^(age_days / half_life_days)
///
/// Example with half_life=30 days:
/// - age=0 days: boost = 0.5^0 = 1.0
/// - age=30 days: boost = 0.5^1 = 0.5
/// - age=60 days: boost = 0.5^2 = 0.25
/// - age=90 days: boost = 0.5^3 = 0.125
#[test]
fn test_recency_boost_with_real_timestamps() {
    let config = SignalBoostConfig::default()
        .with_half_life_days(30.0)
        .with_base_time(TEST_BASE_TIME);

    let boost_0_days = recency_boost(Some(TEST_BASE_TIME), &config);
    let boost_30_days = recency_boost(Some(ts_days_ago(30)), &config);
    let boost_60_days = recency_boost(Some(ts_days_ago(60)), &config);
    let boost_90_days = recency_boost(Some(ts_days_ago(90)), &config);
    let boost_15_days = recency_boost(Some(ts_days_ago(15)), &config);

    println!("=== Recency Boost Ground Truth Verification ===");
    println!("Half-life: 30 days");
    println!();
    println!("Age 0 days:  boost = {:.6}, expected = 1.0", boost_0_days);
    println!("Age 15 days: boost = {:.6}, expected = {:.6}", boost_15_days, 0.5_f64.powf(0.5));
    println!("Age 30 days: boost = {:.6}, expected = 0.5", boost_30_days);
    println!("Age 60 days: boost = {:.6}, expected = 0.25", boost_60_days);
    println!("Age 90 days: boost = {:.6}, expected = 0.125", boost_90_days);

    assert_approx_eq(boost_0_days, 1.0, 1e-10, "Age 0 days");
    assert_approx_eq(boost_30_days, 0.5, 1e-10, "Age 30 days (1 half-life)");
    assert_approx_eq(boost_60_days, 0.25, 1e-10, "Age 60 days (2 half-lives)");
    assert_approx_eq(boost_90_days, 0.125, 1e-10, "Age 90 days (3 half-lives)");
    assert_approx_eq(
        boost_15_days,
        0.5_f64.powf(0.5),
        1e-10,
        "Age 15 days (0.5 half-life)"
    );

    assert!(
        boost_0_days > boost_30_days,
        "More recent should have higher boost"
    );
    assert!(
        boost_30_days > boost_60_days,
        "Older should have lower boost"
    );
    assert!(
        boost_60_days > boost_90_days,
        "Even older should have even lower boost"
    );
}

/// Test the full pipeline: search results -> apply_signal_boosts -> verify ordering.
///
/// Simulates a realistic search scenario where vector search returns results,
/// and signal boosts are applied to rerank them based on salience, confidence,
/// and recency.
#[test]
fn test_full_pipeline_search_boost_filter() {
    let arena = Bump::new();

    let config = SignalBoostConfig::default()
        .with_half_life_days(30.0)
        .with_base_time(TEST_BASE_TIME)
        .with_salience(true)
        .with_confidence(true)
        .with_recency(true);

    let search_results = vec![
        create_vector_with_props(
            &arena,
            1,
            0.95,
            Some(0.5),
            Some(0.8),
            Some(ts_days_ago(60)),
        ),
        create_vector_with_props(
            &arena,
            2,
            0.90,
            Some(0.9),
            Some(0.9),
            Some(ts_days_ago(10)),
        ),
        create_vector_with_props(
            &arena,
            3,
            0.85,
            Some(0.7),
            Some(1.0),
            Some(TEST_BASE_TIME),
        ),
        create_vector_with_props(
            &arena,
            4,
            0.80,
            Some(0.3),
            Some(0.5),
            Some(ts_days_ago(90)),
        ),
    ];

    let recency_10 = recency_boost(Some(ts_days_ago(10)), &config);
    let recency_60 = recency_boost(Some(ts_days_ago(60)), &config);
    let recency_90 = recency_boost(Some(ts_days_ago(90)), &config);

    let expected_1 = 0.95 * 0.5 * 0.8 * recency_60;
    let expected_2 = 0.90 * 0.9 * 0.9 * recency_10;
    let expected_3 = 0.85 * 0.7 * 1.0 * 1.0;
    let expected_4 = 0.80 * 0.3 * 0.5 * recency_90;

    println!("=== Full Pipeline Test ===");
    println!("Search results -> Signal boosts -> Verify ordering");
    println!();
    println!("Expected scores:");
    println!("  ID 1: 0.95 * 0.5 * 0.8 * {:.4} = {:.6}", recency_60, expected_1);
    println!("  ID 2: 0.90 * 0.9 * 0.9 * {:.4} = {:.6}", recency_10, expected_2);
    println!("  ID 3: 0.85 * 0.7 * 1.0 * 1.0 = {:.6}", expected_3);
    println!("  ID 4: 0.80 * 0.3 * 0.5 * {:.4} = {:.6}", recency_90, expected_4);

    let result = apply_signal_boosts(search_results, &config).unwrap();

    let mut expected_order: Vec<(u128, f64)> = vec![
        (1, expected_1),
        (2, expected_2),
        (3, expected_3),
        (4, expected_4),
    ];
    expected_order.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    let result_order: Vec<u128> = result.iter().map(|v| v.id()).collect();
    let expected_id_order: Vec<u128> = expected_order.iter().map(|(id, _)| *id).collect();

    println!();
    println!("Expected order: {:?}", expected_id_order);
    println!("Actual order:   {:?}", result_order);

    for (idx, item) in result.iter().enumerate() {
        let actual_score = extract_score(item);
        let expected = expected_order
            .iter()
            .find(|(id, _)| *id == item.id())
            .map(|(_, s)| *s)
            .unwrap();

        assert_approx_eq(
            actual_score,
            expected,
            1e-6,
            &format!("Item {} at position {}", item.id(), idx),
        );
    }

    assert_eq!(
        result_order, expected_id_order,
        "Result ordering should match expected"
    );

    for i in 0..result.len() - 1 {
        let score_i = extract_score(&result[i]);
        let score_j = extract_score(&result[i + 1]);
        assert!(
            score_i >= score_j,
            "Results should be sorted descending by score"
        );
    }

    println!();
    println!("=== Test passed! ===");
}

/// Test salience boost values.
#[test]
fn test_salience_boost_values() {
    assert_approx_eq(salience_boost(Some(0.8)), 0.8, 1e-10, "salience 0.8");
    assert_approx_eq(salience_boost(Some(1.0)), 1.0, 1e-10, "salience 1.0");
    assert_approx_eq(salience_boost(Some(0.0)), 0.0, 1e-10, "salience 0.0");
    assert_approx_eq(salience_boost(Some(0.5)), 0.5, 1e-10, "salience 0.5");
    assert_approx_eq(salience_boost(None), 1.0, 1e-10, "salience None defaults to 1.0");
}

/// Test confidence boost values.
#[test]
fn test_confidence_boost_values() {
    assert_approx_eq(confidence_boost(Some(0.9)), 0.9, 1e-10, "confidence 0.9");
    assert_approx_eq(confidence_boost(Some(1.0)), 1.0, 1e-10, "confidence 1.0");
    assert_approx_eq(confidence_boost(Some(0.0)), 0.0, 1e-10, "confidence 0.0");
    assert_approx_eq(
        confidence_boost(None),
        1.0,
        1e-10,
        "confidence None defaults to 1.0",
    );
}

/// Test recency boost edge cases.
#[test]
fn test_recency_boost_edge_cases() {
    let config = SignalBoostConfig::default()
        .with_half_life_days(30.0)
        .with_base_time(TEST_BASE_TIME);

    assert_approx_eq(
        recency_boost(None, &config),
        1.0,
        1e-10,
        "None timestamp defaults to 1.0",
    );

    let future_ts = TEST_BASE_TIME + MS_PER_DAY;
    assert_approx_eq(
        recency_boost(Some(future_ts), &config),
        1.0,
        1e-10,
        "Future timestamp should give 1.0",
    );

    let zero_half_life = SignalBoostConfig::default()
        .with_half_life_days(0.0)
        .with_base_time(TEST_BASE_TIME);
    assert_approx_eq(
        recency_boost(Some(ts_days_ago(30)), &zero_half_life),
        1.0,
        1e-10,
        "Zero half-life should give 1.0",
    );

    let negative_half_life = SignalBoostConfig::default()
        .with_half_life_days(-10.0)
        .with_base_time(TEST_BASE_TIME);
    assert_approx_eq(
        recency_boost(Some(ts_days_ago(30)), &negative_half_life),
        1.0,
        1e-10,
        "Negative half-life should give 1.0",
    );
}

/// Test the combined boost formula.
///
/// Final score = base_score * salience * confidence * recency_boost
#[test]
fn test_combined_boost_formula() {
    let config = SignalBoostConfig::default()
        .with_half_life_days(30.0)
        .with_base_time(TEST_BASE_TIME);

    let base_score = 0.9;
    let sal = 0.8;
    let conf = 0.9;
    let age_30_recency = recency_boost(Some(ts_days_ago(30)), &config);

    let expected_final = base_score * sal * conf * age_30_recency;
    let expected_final_numeric = 0.9 * 0.8 * 0.9 * 0.5;

    println!("=== Combined Boost Formula Test ===");
    println!("base_score: {}", base_score);
    println!("salience: {}", sal);
    println!("confidence: {}", conf);
    println!("recency (30 days): {}", age_30_recency);
    println!();
    println!("Expected: {} * {} * {} * {} = {}", base_score, sal, conf, age_30_recency, expected_final);
    println!("Numeric:  0.9 * 0.8 * 0.9 * 0.5 = {}", expected_final_numeric);

    assert_approx_eq(expected_final, expected_final_numeric, 1e-10, "Combined formula");
}

/// Test that disabled boosts do not affect scores.
#[test]
fn test_disabled_boosts_preserve_scores() {
    let arena = Bump::new();

    let config = SignalBoostConfig::default()
        .with_salience(false)
        .with_confidence(false)
        .with_recency(false);

    let items = vec![
        create_vector_with_props(&arena, 1, 0.9, Some(0.5), Some(0.3), Some(ts_days_ago(60))),
        create_vector_with_props(&arena, 2, 0.7, Some(0.1), Some(0.1), Some(ts_days_ago(90))),
    ];

    let result = apply_signal_boosts(items, &config).unwrap();

    let score1 = extract_score(&result.iter().find(|v| v.id() == 1).unwrap());
    let score2 = extract_score(&result.iter().find(|v| v.id() == 2).unwrap());

    assert_approx_eq(score1, 0.9, 1e-10, "Score 1 should be unchanged");
    assert_approx_eq(score2, 0.7, 1e-10, "Score 2 should be unchanged");
}

/// Test that missing properties default to boost of 1.0.
#[test]
fn test_missing_properties_default_boost() {
    let arena = Bump::new();

    let config = SignalBoostConfig::default()
        .with_half_life_days(30.0)
        .with_base_time(TEST_BASE_TIME);

    let items = vec![create_test_vector(&arena, 1, 0.8)];

    let result = apply_signal_boosts(items, &config).unwrap();
    let score = extract_score(&result[0]);

    assert_approx_eq(
        score,
        0.8,
        1e-10,
        "Missing properties should default to 1.0 boost, preserving score",
    );
}

/// Test empty input handling.
#[test]
fn test_empty_input() {
    let config = SignalBoostConfig::default();
    let items: Vec<TraversalValue> = vec![];

    let result = apply_signal_boosts(items, &config).unwrap();
    assert!(result.is_empty(), "Empty input should produce empty output");
}

/// Test with varying half-life values.
#[test]
fn test_varying_half_life() {
    let ages_days = [0, 7, 14, 30, 60, 90];

    for half_life in [7.0, 14.0, 30.0, 60.0] {
        let config = SignalBoostConfig::default()
            .with_half_life_days(half_life)
            .with_base_time(TEST_BASE_TIME);

        println!("Half-life: {} days", half_life);
        for age in ages_days {
            let ts = if age == 0 {
                TEST_BASE_TIME
            } else {
                ts_days_ago(age)
            };
            let boost = recency_boost(Some(ts), &config);
            let expected = 0.5_f64.powf(age as f64 / half_life);
            println!("  Age {} days: boost = {:.6}, expected = {:.6}", age, boost, expected);
            assert_approx_eq(
                boost,
                expected,
                1e-10,
                &format!("half_life={}, age={}", half_life, age),
            );
        }
        println!();
    }
}
