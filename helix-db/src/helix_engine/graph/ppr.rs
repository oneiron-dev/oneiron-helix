use std::collections::{HashMap, HashSet};

/// Edge weights for Oneiron PPR propagation
pub const EDGE_WEIGHTS: &[(&str, f64)] = &[
    ("belongs_to", 1.0),
    ("participates_in", 1.0),
    ("attached", 0.8),
    ("authored_by", 0.9),
    ("mentions", 0.6),
    ("about", 0.5),
    ("supports", 1.0),
    ("opposes", 0.0),
    ("claim_of", 1.0),
    ("scoped_to", 0.7),
    ("supersedes", 0.3),
    ("derived_from", 0.2),
    ("part_of", 0.8),
];

/// Local PPR with candidate-set gating (both-endpoints-readable)
pub fn ppr(
    universe_ids: &HashSet<u128>,
    seed_ids: &[u128],
    _edge_weights: &HashMap<String, f64>,
    _max_depth: usize,
    _damping: f64,
    limit: usize,
) -> Vec<(u128, f64)> {
    let mut scores: HashMap<u128, f64> = HashMap::new();
    let initial_score = 1.0 / seed_ids.len().max(1) as f64;

    for &seed in seed_ids {
        if universe_ids.contains(&seed) {
            scores.insert(seed, initial_score);
        }
    }

    // TODO: Implement neighbor iteration by walking each frontier node's edges,
    // filter to neighbors where both endpoints are in universe_ids, apply
    // edge_weights to transition mass with damping, and stop after max_depth.
    // For now, return seeds with scores
    let mut result: Vec<_> = scores.into_iter().collect();
    result.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    result.truncate(limit);
    result
}
