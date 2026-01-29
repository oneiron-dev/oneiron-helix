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

/// Default edge weight for unknown edge labels
const DEFAULT_EDGE_WEIGHT: f64 = 0.5;

/// Minimum score threshold to continue propagation (prevents infinite loops on tiny scores)
const SCORE_THRESHOLD: f64 = 1e-10;

/// Local PPR with candidate-set gating (both-endpoints-readable)
///
/// Personalized PageRank propagates influence from seed nodes through the graph,
/// respecting edge weights and the candidate-set constraint (both endpoints must
/// be in universe_ids for an edge to be traversable).
///
/// # Algorithm
/// 1. Initialize seed nodes with equal scores (1.0 / num_seeds)
/// 2. For each iteration up to max_depth:
///    - For each node in the current frontier with score > threshold:
///      - Get outgoing edges from storage
///      - For each edge where target is in universe_ids:
///        - Look up edge weight by label (opposes=0 blocks propagation)
///        - Propagate score * weight * damping to target
/// 3. Accumulate scores for nodes reached multiple times
/// 4. Sort by score descending and truncate to limit
///
/// # Arguments
/// * `universe_ids` - Set of node IDs that form the candidate set (both endpoints must be readable)
/// * `seed_ids` - Starting nodes for PPR propagation
/// * `edge_weights` - Map of edge label to weight (overrides EDGE_WEIGHTS constant)
/// * `max_depth` - Maximum number of hops from seed nodes
/// * `damping` - Damping factor (typically 0.85), controls score decay per hop
/// * `limit` - Maximum number of results to return
///
/// # Returns
/// Vector of (node_id, score) tuples sorted by score descending
pub fn ppr(
    universe_ids: &HashSet<u128>,
    seed_ids: &[u128],
    edge_weights: &HashMap<String, f64>,
    max_depth: usize,
    damping: f64,
    limit: usize,
) -> Vec<(u128, f64)> {
    if seed_ids.is_empty() {
        return Vec::new();
    }

    let mut scores: HashMap<u128, f64> = HashMap::new();
    let initial_score = 1.0 / seed_ids.len() as f64;

    let mut frontier: HashMap<u128, f64> = HashMap::new();
    for &seed in seed_ids {
        if universe_ids.contains(&seed) {
            *scores.entry(seed).or_insert(0.0) += initial_score;
            *frontier.entry(seed).or_insert(0.0) += initial_score;
        }
    }

    // TODO: Full PPR implementation requires storage access for neighbor iteration.
    // The function signature needs to be extended to include:
    //   - storage: &HelixGraphStorage
    //   - txn: &RoTxn
    //   - arena: &bumpalo::Bump
    //
    // Once storage is available, implement the following iteration loop:
    //
    // for _depth in 0..max_depth {
    //     let mut next_frontier: HashMap<u128, f64> = HashMap::new();
    //
    //     for (&node_id, &node_score) in &frontier {
    //         if node_score < SCORE_THRESHOLD {
    //             continue;
    //         }
    //
    //         // Get outgoing edges for this node (see paths.rs for pattern):
    //         // let out_prefix = node_id.to_be_bytes().to_vec();
    //         // let iter = storage.out_edges_db.prefix_iter(txn, &out_prefix).unwrap();
    //         //
    //         // for result in iter {
    //         //     let (key, value) = result.unwrap();
    //         //     let (edge_id, target_node) = HelixGraphStorage::unpack_adj_edge_data(value).unwrap();
    //         //
    //         //     // Candidate-set gating: target must be in universe_ids
    //         //     if !universe_ids.contains(&target_node) {
    //         //         continue;
    //         //     }
    //         //
    //         //     // Get edge to check its label for weight lookup
    //         //     let edge = storage.get_edge(txn, &edge_id, arena).unwrap();
    //         //     let edge_label = edge.label;
    //         //
    //         //     // Look up edge weight (user-provided overrides, then EDGE_WEIGHTS constant)
    //         //     let weight = edge_weights
    //         //         .get(edge_label)
    //         //         .copied()
    //         //         .or_else(|| EDGE_WEIGHTS.iter().find(|(l, _)| *l == edge_label).map(|(_, w)| *w))
    //         //         .unwrap_or(DEFAULT_EDGE_WEIGHT);
    //         //
    //         //     // opposes=0.0 blocks propagation entirely
    //         //     if weight <= 0.0 {
    //         //         continue;
    //         //     }
    //         //
    //         //     // Propagate score with damping
    //         //     let propagated_score = node_score * weight * damping;
    //         //     *scores.entry(target_node).or_insert(0.0) += propagated_score;
    //         //     *next_frontier.entry(target_node).or_insert(0.0) += propagated_score;
    //         // }
    //     }
    //
    //     if next_frontier.is_empty() {
    //         break; // No more nodes to explore
    //     }
    //     frontier = next_frontier;
    // }
    //
    // For now, this stub returns only seed nodes with their initial scores.
    // The full implementation will propagate scores through the graph.

    let _ = (
        edge_weights,
        max_depth,
        damping,
        SCORE_THRESHOLD,
        DEFAULT_EDGE_WEIGHT,
    );

    let mut result: Vec<_> = scores.into_iter().collect();
    result.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    result.truncate(limit);
    result
}

/// Helper function to get edge weight from either user-provided map or EDGE_WEIGHTS constant
#[inline]
pub fn get_edge_weight(edge_label: &str, edge_weights: &HashMap<String, f64>) -> f64 {
    edge_weights
        .get(edge_label)
        .copied()
        .or_else(|| {
            EDGE_WEIGHTS
                .iter()
                .find(|(l, _)| *l == edge_label)
                .map(|(_, w)| *w)
        })
        .unwrap_or(DEFAULT_EDGE_WEIGHT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ppr_empty_seeds() {
        let universe: HashSet<u128> = [1, 2, 3].into_iter().collect();
        let seeds: Vec<u128> = vec![];
        let edge_weights = HashMap::new();

        let result = ppr(&universe, &seeds, &edge_weights, 3, 0.85, 10);
        assert!(result.is_empty());
    }

    #[test]
    fn test_ppr_seeds_not_in_universe() {
        let universe: HashSet<u128> = [1, 2, 3].into_iter().collect();
        let seeds = vec![100, 200];
        let edge_weights = HashMap::new();

        let result = ppr(&universe, &seeds, &edge_weights, 3, 0.85, 10);
        assert!(result.is_empty());
    }

    #[test]
    fn test_ppr_single_seed() {
        let universe: HashSet<u128> = [1, 2, 3].into_iter().collect();
        let seeds = vec![1];
        let edge_weights = HashMap::new();

        let result = ppr(&universe, &seeds, &edge_weights, 3, 0.85, 10);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, 1);
        assert!((result[0].1 - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_ppr_multiple_seeds() {
        let universe: HashSet<u128> = [1, 2, 3, 4, 5].into_iter().collect();
        let seeds = vec![1, 2, 3];
        let edge_weights = HashMap::new();

        let result = ppr(&universe, &seeds, &edge_weights, 3, 0.85, 10);
        assert_eq!(result.len(), 3);

        let expected_score = 1.0 / 3.0;
        for (_, score) in &result {
            assert!((score - expected_score).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_ppr_limit() {
        let universe: HashSet<u128> = [1, 2, 3, 4, 5].into_iter().collect();
        let seeds = vec![1, 2, 3, 4, 5];
        let edge_weights = HashMap::new();

        let result = ppr(&universe, &seeds, &edge_weights, 3, 0.85, 2);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_ppr_partial_seeds_in_universe() {
        let universe: HashSet<u128> = [1, 2].into_iter().collect();
        let seeds = vec![1, 2, 100];
        let edge_weights = HashMap::new();

        let result = ppr(&universe, &seeds, &edge_weights, 3, 0.85, 10);
        assert_eq!(result.len(), 2);

        let expected_score = 1.0 / 3.0;
        for (_, score) in &result {
            assert!((score - expected_score).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_get_edge_weight_from_constant() {
        let edge_weights = HashMap::new();

        assert!((get_edge_weight("belongs_to", &edge_weights) - 1.0).abs() < f64::EPSILON);
        assert!((get_edge_weight("opposes", &edge_weights) - 0.0).abs() < f64::EPSILON);
        assert!((get_edge_weight("mentions", &edge_weights) - 0.6).abs() < f64::EPSILON);
    }

    #[test]
    fn test_get_edge_weight_user_override() {
        let mut edge_weights = HashMap::new();
        edge_weights.insert("belongs_to".to_string(), 0.5);
        edge_weights.insert("custom_edge".to_string(), 0.9);

        assert!((get_edge_weight("belongs_to", &edge_weights) - 0.5).abs() < f64::EPSILON);
        assert!((get_edge_weight("custom_edge", &edge_weights) - 0.9).abs() < f64::EPSILON);
    }

    #[test]
    fn test_get_edge_weight_unknown() {
        let edge_weights = HashMap::new();
        assert!(
            (get_edge_weight("unknown_edge", &edge_weights) - DEFAULT_EDGE_WEIGHT).abs()
                < f64::EPSILON
        );
    }
}
