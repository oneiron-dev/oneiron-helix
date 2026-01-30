// Copyright 2025 HelixDB Inc.
// SPDX-License-Identifier: AGPL-3.0

//! Ranking signal boosts for the Oneiron retrieval system.
//!
//! This module implements the signal boost component of the final score formula:
//! `Final Score = RRF(vector, FTS, PPR) * salience_boost * recency_boost * confidence_boost`
//!
//! Signal boosts are extracted from node properties and applied as multiplicative factors.

use crate::helix_engine::{
    reranker::{
        errors::RerankerResult,
        reranker::{extract_score, update_score},
    },
    traversal_core::traversal_value::TraversalValue,
};
use crate::protocol::value::Value;
use std::time::{SystemTime, UNIX_EPOCH};

type Timestamp = u64;
type BoostFactor = f64;

const MS_PER_DAY: f64 = 1000.0 * 60.0 * 60.0 * 24.0;
const DEFAULT_BOOST: BoostFactor = 1.0;

#[derive(Debug, Clone)]
pub struct SignalBoostConfig {
    pub enable_salience: bool,
    pub enable_recency: bool,
    pub enable_confidence: bool,
    pub recency_half_life_days: f64,
    pub recency_base_time: Option<u64>,
}

impl Default for SignalBoostConfig {
    fn default() -> Self {
        Self {
            enable_salience: true,
            enable_recency: true,
            enable_confidence: true,
            recency_half_life_days: 30.0,
            recency_base_time: None,
        }
    }
}

impl SignalBoostConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_salience(mut self, enable: bool) -> Self {
        self.enable_salience = enable;
        self
    }

    pub fn with_recency(mut self, enable: bool) -> Self {
        self.enable_recency = enable;
        self
    }

    pub fn with_confidence(mut self, enable: bool) -> Self {
        self.enable_confidence = enable;
        self
    }

    pub fn with_half_life_days(mut self, days: f64) -> Self {
        self.recency_half_life_days = days;
        self
    }

    pub fn with_base_time(mut self, base_time: u64) -> Self {
        self.recency_base_time = Some(base_time);
        self
    }
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::F64(f) => Some(*f),
        Value::F32(f) => Some(*f as f64),
        Value::I8(i) => Some(*i as f64),
        Value::I16(i) => Some(*i as f64),
        Value::I32(i) => Some(*i as f64),
        Value::I64(i) => Some(*i as f64),
        Value::U8(u) => Some(*u as f64),
        Value::U16(u) => Some(*u as f64),
        Value::U32(u) => Some(*u as f64),
        Value::U64(u) => Some(*u as f64),
        Value::U128(u) => Some(*u as f64),
        _ => None,
    }
}

fn value_to_u64(value: &Value) -> Option<u64> {
    match value {
        Value::U64(u) => Some(*u),
        Value::U32(u) => Some(*u as u64),
        Value::U16(u) => Some(*u as u64),
        Value::U8(u) => Some(*u as u64),
        Value::I64(i) => u64::try_from(*i).ok(),
        Value::I32(i) => u64::try_from(*i).ok(),
        Value::I16(i) => u64::try_from(*i).ok(),
        Value::I8(i) => u64::try_from(*i).ok(),
        Value::U128(u) => u64::try_from(*u).ok(),
        _ => None,
    }
}

pub fn salience_boost(salience: Option<f64>) -> BoostFactor {
    salience.unwrap_or(DEFAULT_BOOST)
}

pub fn confidence_boost(confidence: Option<f64>) -> BoostFactor {
    confidence.unwrap_or(DEFAULT_BOOST)
}

fn current_time_ms() -> Timestamp {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as Timestamp)
        .unwrap_or(0)
}

pub fn recency_boost(recency_ts: Option<Timestamp>, config: &SignalBoostConfig) -> BoostFactor {
    let Some(ts) = recency_ts else {
        return DEFAULT_BOOST;
    };

    let base_time = config.recency_base_time.unwrap_or_else(current_time_ms);

    if ts >= base_time || config.recency_half_life_days <= 0.0 {
        return DEFAULT_BOOST;
    }

    let age_days = (base_time - ts) as f64 / MS_PER_DAY;
    0.5_f64.powf(age_days / config.recency_half_life_days)
}

fn extract_boost_signals(
    item: &TraversalValue<'_>,
    config: &SignalBoostConfig,
) -> (Option<f64>, Option<f64>, Option<Timestamp>) {
    let salience = config
        .enable_salience
        .then(|| item.get_property("salience").and_then(value_to_f64))
        .flatten();

    let confidence = config
        .enable_confidence
        .then(|| item.get_property("confidence").and_then(value_to_f64))
        .flatten();

    let recency_ts = config
        .enable_recency
        .then(|| item.get_property("recencyTs").and_then(value_to_u64))
        .flatten();

    (salience, confidence, recency_ts)
}

fn compute_combined_boost(
    salience: Option<f64>,
    confidence: Option<f64>,
    recency_ts: Option<Timestamp>,
    config: &SignalBoostConfig,
) -> BoostFactor {
    salience_boost(salience) * confidence_boost(confidence) * recency_boost(recency_ts, config)
}

pub fn apply_signal_boosts<'arena>(
    items: Vec<TraversalValue<'arena>>,
    config: &SignalBoostConfig,
) -> RerankerResult<Vec<TraversalValue<'arena>>> {
    let mut boosted_items: Vec<(TraversalValue<'arena>, f64)> = Vec::with_capacity(items.len());

    for mut item in items {
        let original_score = extract_score(&item)?;
        let (salience, confidence, recency_ts) = extract_boost_signals(&item, config);
        let combined_boost = compute_combined_boost(salience, confidence, recency_ts, config);
        let new_score = original_score * combined_boost;

        update_score(&mut item, new_score)?;
        boosted_items.push((item, new_score));
    }

    boosted_items.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    Ok(boosted_items.into_iter().map(|(item, _)| item).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helix_engine::vector_core::vector::HVector;
    use bumpalo::Bump;

    const TEST_BASE_TIME: Timestamp = 1_000_000_000_000;
    const TEST_MS_PER_DAY: Timestamp = 1000 * 60 * 60 * 24;

    fn create_test_vector<'a>(arena: &'a Bump, id: u128, score: f64) -> TraversalValue<'a> {
        let data = arena.alloc_slice_copy(&[1.0, 2.0, 3.0]);
        let mut v = HVector::from_slice("test", 0, data);
        v.id = id;
        v.distance = Some(score);
        TraversalValue::Vector(v)
    }

    fn assert_approx_eq(actual: f64, expected: f64, msg: &str) {
        assert!(
            (actual - expected).abs() < 1e-10,
            "{}: expected {}, got {}",
            msg,
            expected,
            actual
        );
    }

    fn ts_days_ago(days: u64) -> Timestamp {
        TEST_BASE_TIME - (days * TEST_MS_PER_DAY)
    }

    #[test]
    fn test_salience_boost_values() {
        assert_approx_eq(salience_boost(Some(0.8)), 0.8, "salience 0.8");
        assert_approx_eq(salience_boost(Some(1.0)), 1.0, "salience 1.0");
        assert_approx_eq(salience_boost(Some(0.0)), 0.0, "salience 0.0");
        assert_approx_eq(salience_boost(Some(0.5)), 0.5, "salience 0.5");
        assert_approx_eq(salience_boost(None), 1.0, "salience None");
    }

    #[test]
    fn test_confidence_boost_values() {
        assert_approx_eq(confidence_boost(Some(0.9)), 0.9, "confidence 0.9");
        assert_approx_eq(confidence_boost(Some(1.0)), 1.0, "confidence 1.0");
        assert_approx_eq(confidence_boost(Some(0.0)), 0.0, "confidence 0.0");
        assert_approx_eq(confidence_boost(Some(0.3)), 0.3, "confidence 0.3");
        assert_approx_eq(confidence_boost(None), 1.0, "confidence None");
    }

    #[test]
    fn test_recency_boost_decay() {
        let config = SignalBoostConfig::default()
            .with_half_life_days(30.0)
            .with_base_time(TEST_BASE_TIME);

        assert_approx_eq(
            recency_boost(Some(TEST_BASE_TIME), &config),
            1.0,
            "Age 0 days",
        );
        assert_approx_eq(
            recency_boost(Some(ts_days_ago(30)), &config),
            0.5,
            "Age 30 days (half-life)",
        );
        assert_approx_eq(
            recency_boost(Some(ts_days_ago(60)), &config),
            0.25,
            "Age 60 days (2x half-life)",
        );
        assert_approx_eq(
            recency_boost(Some(ts_days_ago(15)), &config),
            0.5_f64.powf(0.5),
            "Age 15 days (half of half-life)",
        );
    }

    #[test]
    fn test_recency_boost_none() {
        let config = SignalBoostConfig::default();
        assert_approx_eq(recency_boost(None, &config), 1.0, "recency None");
    }

    #[test]
    fn test_recency_boost_future_timestamp() {
        let config = SignalBoostConfig::default().with_base_time(TEST_BASE_TIME);
        let future_ts = TEST_BASE_TIME + TEST_MS_PER_DAY;
        assert_approx_eq(
            recency_boost(Some(future_ts), &config),
            1.0,
            "future timestamp",
        );
    }

    fn disabled_boosts_config() -> SignalBoostConfig {
        SignalBoostConfig::default()
            .with_salience(false)
            .with_recency(false)
            .with_confidence(false)
    }

    fn extract_score_from_result(item: &TraversalValue<'_>) -> f64 {
        match item {
            TraversalValue::Vector(v) => v.distance.unwrap(),
            _ => panic!("Expected Vector"),
        }
    }

    #[test]
    fn test_apply_boosts_multiplies_scores() {
        let arena = Bump::new();
        let config = disabled_boosts_config();

        let items = vec![
            create_test_vector(&arena, 1, 1.0),
            create_test_vector(&arena, 2, 0.8),
            create_test_vector(&arena, 3, 0.6),
        ];

        let result = apply_signal_boosts(items, &config).unwrap();

        assert_eq!(result.len(), 3);
        assert_approx_eq(extract_score_from_result(&result[0]), 1.0, "first score");
        assert_approx_eq(extract_score_from_result(&result[1]), 0.8, "second score");
        assert_approx_eq(extract_score_from_result(&result[2]), 0.6, "third score");
    }

    #[test]
    fn test_disabled_boosts() {
        let arena = Bump::new();
        let config = disabled_boosts_config();

        let items = vec![create_test_vector(&arena, 1, 1.0)];
        let result = apply_signal_boosts(items, &config).unwrap();

        assert_approx_eq(extract_score_from_result(&result[0]), 1.0, "disabled boosts");
    }

    #[test]
    fn test_missing_properties() {
        let arena = Bump::new();
        let config = SignalBoostConfig::default().with_base_time(TEST_BASE_TIME);

        let items = vec![create_test_vector(&arena, 1, 0.9)];
        let result = apply_signal_boosts(items, &config).unwrap();

        assert_approx_eq(
            extract_score_from_result(&result[0]),
            0.9,
            "Missing properties should default to 1.0 boost",
        );
    }

    #[test]
    fn test_combined_boost_formula() {
        let config = SignalBoostConfig::default()
            .with_half_life_days(30.0)
            .with_base_time(TEST_BASE_TIME);

        let sal = salience_boost(Some(0.8));
        let conf = confidence_boost(Some(0.9));
        let rec = recency_boost(Some(ts_days_ago(30)), &config);

        let combined = sal * conf * rec;
        let expected = 0.8 * 0.9 * 0.5;

        assert_approx_eq(combined, expected, "combined boost formula");
    }

    #[test]
    fn test_resorting_after_boosts() {
        let arena = Bump::new();
        let config = disabled_boosts_config();

        let items = vec![
            create_test_vector(&arena, 1, 0.3),
            create_test_vector(&arena, 2, 0.9),
            create_test_vector(&arena, 3, 0.6),
        ];

        let result = apply_signal_boosts(items, &config).unwrap();

        let scores: Vec<f64> = result.iter().map(extract_score_from_result).collect();

        for i in 0..scores.len() - 1 {
            assert!(
                scores[i] >= scores[i + 1],
                "Results should be sorted descending by score"
            );
        }

        if let TraversalValue::Vector(v) = &result[0] {
            assert_eq!(v.id, 2, "Highest scored item should be first");
        }
    }

    #[test]
    fn test_zero_half_life() {
        let config = SignalBoostConfig::default()
            .with_half_life_days(0.0)
            .with_base_time(TEST_BASE_TIME);

        assert_approx_eq(
            recency_boost(Some(ts_days_ago(30)), &config),
            1.0,
            "Zero half-life",
        );
    }

    #[test]
    fn test_negative_half_life() {
        let config = SignalBoostConfig::default()
            .with_half_life_days(-10.0)
            .with_base_time(TEST_BASE_TIME);

        assert_approx_eq(
            recency_boost(Some(ts_days_ago(30)), &config),
            1.0,
            "Negative half-life",
        );
    }

    #[test]
    fn test_config_builder() {
        let config = SignalBoostConfig::new()
            .with_salience(false)
            .with_recency(true)
            .with_confidence(false)
            .with_half_life_days(7.0)
            .with_base_time(12345);

        assert!(!config.enable_salience);
        assert!(config.enable_recency);
        assert!(!config.enable_confidence);
        assert_approx_eq(config.recency_half_life_days, 7.0, "half_life_days");
        assert_eq!(config.recency_base_time, Some(12345));
    }

    #[test]
    fn test_empty_input() {
        let config = SignalBoostConfig::default();
        let items: Vec<TraversalValue> = vec![];

        let result = apply_signal_boosts(items, &config).unwrap();
        assert!(result.is_empty());
    }
}
