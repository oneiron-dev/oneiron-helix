// Copyright 2025 HelixDB Inc.
// SPDX-License-Identifier: AGPL-3.0

//! Fusion-based reranking algorithms.

pub mod mmr;
pub mod rrf;
pub mod score_normalizer;
pub mod signal_boost;

pub use mmr::{DistanceMethod, MMRReranker};
pub use rrf::RRFReranker;
pub use score_normalizer::{NormalizationMethod, normalize_scores};
pub use signal_boost::{
    SignalBoostConfig, apply_signal_boosts, confidence_boost, recency_boost, salience_boost,
};
