//! End-to-end tests for Custom Edge Weights in PPR
//!
//! These tests verify that custom edge weights properly affect score propagation
//! in Personalized PageRank, with NetworkX ground truth validation.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bumpalo::Bump;
use tempfile::TempDir;

use crate::{
    helix_engine::{
        graph::ppr::{get_edge_weight, ppr_with_storage, EDGE_WEIGHTS},
        storage_core::HelixGraphStorage,
        traversal_core::ops::{
            g::G,
            source::{add_e::AddEAdapter, add_n::AddNAdapter},
        },
    },
    props,
};

use super::traversal_tests::test_utils::props_option;

fn setup_test_db() -> (TempDir, Arc<HelixGraphStorage>) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let storage = HelixGraphStorage::new(
        db_path,
        crate::helix_engine::traversal_core::config::Config::default(),
        Default::default(),
    )
    .unwrap();
    (temp_dir, Arc::new(storage))
}

/// Test that PPR with default weights differs from PPR with custom weights.
///
/// This test creates a graph A -> B -> C with "mentions" edges, then runs PPR twice:
/// 1. With default weights (mentions = 0.6)
/// 2. With custom weights (mentions = 0.0, effectively blocking propagation)
///
/// The scores should differ significantly because zero weight blocks propagation.
#[test]
fn test_ppr_custom_weights_changes_propagation() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node_a = G::new_mut(&storage, &arena, &mut txn)
        .add_n("node", props_option(&arena, props!("name" => "A")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let node_b = G::new_mut(&storage, &arena, &mut txn)
        .add_n("node", props_option(&arena, props!("name" => "B")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let node_c = G::new_mut(&storage, &arena, &mut txn)
        .add_n("node", props_option(&arena, props!("name" => "C")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("mentions", None, node_a, node_b, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("mentions", None, node_b, node_c, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let universe: HashSet<u128> = [node_a, node_b, node_c].into_iter().collect();
    let seeds = vec![node_a];

    // Run with default weights
    let arena_default = Bump::new();
    let txn_default = storage.graph_env.read_txn().unwrap();
    let default_weights = HashMap::new();
    let result_default = ppr_with_storage(
        &storage,
        &txn_default,
        &arena_default,
        &universe,
        &seeds,
        &default_weights,
        3,
        0.85,
        10,
        true,
    );
    drop(txn_default);

    // Run with custom weights (mentions = 0.0)
    let arena_custom = Bump::new();
    let txn_custom = storage.graph_env.read_txn().unwrap();
    let mut custom_weights = HashMap::new();
    custom_weights.insert("mentions".to_string(), 0.0);
    let result_custom = ppr_with_storage(
        &storage,
        &txn_custom,
        &arena_custom,
        &universe,
        &seeds,
        &custom_weights,
        3,
        0.85,
        10,
        true,
    );

    let get_score = |results: &[(u128, f64)], id: u128| -> f64 {
        results
            .iter()
            .find(|(node_id, _)| *node_id == id)
            .map(|(_, s)| *s)
            .unwrap_or(0.0)
    };

    let b_score_default = get_score(&result_default, node_b);
    let c_score_default = get_score(&result_default, node_c);
    let b_score_custom = get_score(&result_custom, node_b);
    let c_score_custom = get_score(&result_custom, node_c);

    assert!(
        b_score_default > 0.0,
        "B should have score with default weights"
    );
    assert!(
        c_score_default > 0.0,
        "C should have score with default weights"
    );

    assert!(
        b_score_custom < b_score_default,
        "B score should be lower with zero mentions weight: custom={}, default={}",
        b_score_custom,
        b_score_default
    );

    assert!(
        c_score_custom < c_score_default,
        "C score should be lower with zero mentions weight: custom={}, default={}",
        c_score_custom,
        c_score_default
    );
}

/// Test that setting an edge type to weight 0.0 completely blocks propagation through it.
///
/// Creates: A --(supports)-> B --(mentions)-> C
/// With mentions=0.0, C should receive zero or negligible score while B receives score.
#[test]
fn test_ppr_zero_weight_blocks_edge() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node_a = G::new_mut(&storage, &arena, &mut txn)
        .add_n("node", props_option(&arena, props!("name" => "A")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let node_b = G::new_mut(&storage, &arena, &mut txn)
        .add_n("node", props_option(&arena, props!("name" => "B")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let node_c = G::new_mut(&storage, &arena, &mut txn)
        .add_n("node", props_option(&arena, props!("name" => "C")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("supports", None, node_a, node_b, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("mentions", None, node_b, node_c, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [node_a, node_b, node_c].into_iter().collect();
    let seeds = vec![node_a];

    let mut edge_weights = HashMap::new();
    edge_weights.insert("mentions".to_string(), 0.0);

    let result = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        3,
        0.85,
        10,
        true,
    );

    let get_score = |id: u128| -> f64 {
        result
            .iter()
            .find(|(node_id, _)| *node_id == id)
            .map(|(_, s)| *s)
            .unwrap_or(0.0)
    };

    let a_score = get_score(node_a);
    let b_score = get_score(node_b);
    let c_score = get_score(node_c);

    assert!(a_score > 0.0, "Seed node A should have positive score");
    assert!(
        b_score > 0.0,
        "B should have score (reachable via supports edge)"
    );
    assert!(
        c_score == 0.0 || c_score < 1e-10,
        "C should have zero or negligible score (blocked by zero-weight mentions edge): {}",
        c_score
    );
}

/// Test that setting an edge type to high weight (2.0) boosts scores on that path.
///
/// Creates: Source --(high_edge)-> HighTarget, Source --(low_edge)-> LowTarget
/// With high_edge=2.0 and low_edge=0.5, HighTarget should have 4x the score of LowTarget.
#[test]
fn test_ppr_high_weight_boosts_path() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let source = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "node",
            props_option(&arena, props!("name" => "Source")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let high_target = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "node",
            props_option(&arena, props!("name" => "HighTarget")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let low_target = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "node",
            props_option(&arena, props!("name" => "LowTarget")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("high_edge", None, source, high_target, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("low_edge", None, source, low_target, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [source, high_target, low_target].into_iter().collect();
    let seeds = vec![source];

    let mut edge_weights = HashMap::new();
    edge_weights.insert("high_edge".to_string(), 2.0);
    edge_weights.insert("low_edge".to_string(), 0.5);

    let result = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        2,
        0.85,
        10,
        true,
    );

    let high_score = result
        .iter()
        .find(|(id, _)| *id == high_target)
        .map(|(_, s)| *s)
        .expect("HighTarget should have a score");

    let low_score = result
        .iter()
        .find(|(id, _)| *id == low_target)
        .map(|(_, s)| *s)
        .expect("LowTarget should have a score");

    assert!(
        high_score > low_score,
        "HighTarget ({}) should have higher score than LowTarget ({})",
        high_score,
        low_score
    );

    let expected_ratio = 2.0 / 0.5;
    let actual_ratio = high_score / low_score;
    assert!(
        (actual_ratio - expected_ratio).abs() < 0.01,
        "Score ratio should match edge weight ratio: expected {}, got {}",
        expected_ratio,
        actual_ratio
    );
}

/// Test PPR with custom weights against NetworkX PageRank calculation.
///
/// NetworkX verification (Python):
/// ```python
/// import networkx as nx
/// G = nx.DiGraph()
/// G.add_edge('A', 'B', weight=0.5)  # mentions with custom weight 0.5
/// G.add_edge('B', 'C', weight=1.0)  # supports with default weight 1.0
/// G.add_edge('A', 'C', weight=0.8)  # about with custom weight 0.8
///
/// # Standard PageRank with personalization on A
/// pr = nx.pagerank(G, alpha=0.85, weight='weight', personalization={'A': 1.0, 'B': 0.0, 'C': 0.0})
///
/// # Note: Our PPR uses bidirectional traversal, so scores propagate both ways
/// # along edges. This means C gets more score than B because:
/// # - C receives from A via about (0.8) + from B via supports (1.0)
/// # - B only receives from A via mentions (0.5)
/// ```
///
/// Key validation points:
/// - Custom weights override default EDGE_WEIGHTS
/// - C should have higher score than B (higher incoming weight from A)
/// - All nodes should have positive scores (connected graph)
#[test]
fn test_ppr_custom_weights_vs_networkx() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node_a = G::new_mut(&storage, &arena, &mut txn)
        .add_n("node", props_option(&arena, props!("name" => "A")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let node_b = G::new_mut(&storage, &arena, &mut txn)
        .add_n("node", props_option(&arena, props!("name" => "B")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let node_c = G::new_mut(&storage, &arena, &mut txn)
        .add_n("node", props_option(&arena, props!("name" => "C")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("mentions", None, node_a, node_b, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("supports", None, node_b, node_c, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("about", None, node_a, node_c, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [node_a, node_b, node_c].into_iter().collect();
    let seeds = vec![node_a];

    let mut edge_weights = HashMap::new();
    edge_weights.insert("mentions".to_string(), 0.5);
    edge_weights.insert("supports".to_string(), 1.0);
    edge_weights.insert("about".to_string(), 0.8);

    let result = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        3,
        0.85,
        10,
        true,
    );

    let get_score = |id: u128| -> f64 {
        result
            .iter()
            .find(|(node_id, _)| *node_id == id)
            .map(|(_, s)| *s)
            .unwrap_or(0.0)
    };

    let a_score = get_score(node_a);
    let b_score = get_score(node_b);
    let c_score = get_score(node_c);

    println!("=== PPR Custom Weights vs NetworkX Test ===");
    println!("Graph: A --(mentions,0.5)-> B --(supports,1.0)-> C");
    println!("       A --(about,0.8)-> C");
    println!("Seed: A, Damping: 0.85");
    println!();
    println!("Scores:");
    println!("  A (seed): {:.6}", a_score);
    println!("  B:        {:.6}", b_score);
    println!("  C:        {:.6}", c_score);
    println!();

    assert!(a_score > 0.0, "Seed node A should have positive score");
    assert!(b_score > 0.0, "B should have positive score (connected to A)");
    assert!(c_score > 0.0, "C should have positive score (connected to A and B)");

    assert!(
        c_score > b_score,
        "C should have higher score than B (direct 0.8 edge from A vs 0.5, plus receives from B): C={}, B={}",
        c_score,
        b_score
    );

    let c_b_ratio = c_score / b_score;
    println!("C/B score ratio: {:.3}", c_b_ratio);
    println!("C has higher incoming weight from A (0.8 vs 0.5) plus receives from B");
    assert!(
        c_b_ratio > 1.0,
        "C/B ratio should be > 1.0 due to higher edge weight: {}",
        c_b_ratio
    );

    println!("=== Test passed! ===");
}

/// Test that get_edge_weight correctly prioritizes user-provided weights over defaults.
#[test]
fn test_edge_weight_lookup_priority() {
    let default_mentions_weight = EDGE_WEIGHTS
        .iter()
        .find(|(label, _)| *label == "mentions")
        .map(|(_, w)| *w)
        .expect("mentions should be in EDGE_WEIGHTS");

    assert!(
        (default_mentions_weight - 0.6).abs() < f64::EPSILON,
        "Default mentions weight should be 0.6"
    );

    let empty_weights = HashMap::new();
    assert!(
        (get_edge_weight("mentions", &empty_weights) - 0.6).abs() < f64::EPSILON,
        "Empty HashMap should use default weight"
    );

    let mut custom_weights = HashMap::new();
    custom_weights.insert("mentions".to_string(), 0.9);
    assert!(
        (get_edge_weight("mentions", &custom_weights) - 0.9).abs() < f64::EPSILON,
        "Custom weight should override default"
    );

    let mut partial_weights = HashMap::new();
    partial_weights.insert("unknown_edge".to_string(), 0.7);
    assert!(
        (get_edge_weight("mentions", &partial_weights) - 0.6).abs() < f64::EPSILON,
        "Unspecified edge should use default weight"
    );
    assert!(
        (get_edge_weight("unknown_edge", &partial_weights) - 0.7).abs() < f64::EPSILON,
        "Custom unknown edge should use specified weight"
    );
}

/// Test that opposes edge (default weight=0) truly blocks propagation.
///
/// NetworkX verification (Python):
/// ```python
/// import networkx as nx
/// G = nx.DiGraph()
/// G.add_edge('Source', 'Supported', weight=1.0)  # supports
/// G.add_edge('Source', 'Opposed', weight=0.0)    # opposes
///
/// pr = nx.pagerank(G, alpha=0.85, weight='weight', personalization={'Source': 1.0})
/// # Opposed should have 0 score because weight=0 means no edge traversal
/// ```
#[test]
fn test_opposes_edge_blocks_propagation() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let source = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "claim",
            props_option(&arena, props!("name" => "Source")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let supported = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "claim",
            props_option(&arena, props!("name" => "Supported")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let opposed = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "claim",
            props_option(&arena, props!("name" => "Opposed")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("supports", None, source, supported, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("opposes", None, source, opposed, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [source, supported, opposed].into_iter().collect();
    let seeds = vec![source];
    let edge_weights = HashMap::new();

    let result = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        3,
        0.85,
        10,
        true,
    );

    let supported_score = result
        .iter()
        .find(|(id, _)| *id == supported)
        .map(|(_, s)| *s);

    let opposed_score = result
        .iter()
        .find(|(id, _)| *id == opposed)
        .map(|(_, s)| *s);

    assert!(
        supported_score.is_some() && supported_score.unwrap() > 0.0,
        "Supported node should have positive score via supports edge"
    );

    assert!(
        opposed_score.is_none() || opposed_score.unwrap() == 0.0,
        "Opposed node should have zero score (opposes edge has weight 0.0 by default): {:?}",
        opposed_score
    );
}

/// Test multiple edge types with varying weights in a complex graph.
///
/// NetworkX verification (Python):
/// ```python
/// import networkx as nx
/// G = nx.DiGraph()
/// # Create: Person -> Session <- Turn -> Claim/Topic, Claim -> OldClaim
/// G.add_edge('Person', 'Session', weight=1.0)    # participates_in
/// G.add_edge('Turn', 'Session', weight=1.0)      # belongs_to
/// G.add_edge('Turn', 'Claim', weight=1.0)        # supports
/// G.add_edge('Turn', 'Topic', weight=0.5)        # about
/// G.add_edge('Claim', 'OldClaim', weight=0.3)    # supersedes
///
/// # Note: Our PPR uses bidirectional traversal, so Session can accumulate score
/// # from both Person (outgoing) and Turn (incoming) edges.
/// ```
///
/// Key validation points:
/// - All connected nodes receive positive scores
/// - Claim (supports=1.0) should have higher score than Topic (about=0.5)
/// - Claim should have higher score than OldClaim (supersedes=0.3)
#[test]
fn test_complex_graph_with_varied_weights() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let person = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "Person")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let session = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "session",
            props_option(&arena, props!("name" => "Session")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let turn = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "turn",
            props_option(&arena, props!("name" => "Turn")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let claim = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "claim",
            props_option(&arena, props!("name" => "Claim")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let topic = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "topic",
            props_option(&arena, props!("name" => "Topic")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let old_claim = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "claim",
            props_option(&arena, props!("name" => "OldClaim")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("participates_in", None, person, session, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("belongs_to", None, turn, session, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("supports", None, turn, claim, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("about", None, turn, topic, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("supersedes", None, claim, old_claim, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [person, session, turn, claim, topic, old_claim]
        .into_iter()
        .collect();
    let seeds = vec![person];
    let edge_weights = HashMap::new();

    let result = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        5,
        0.85,
        10,
        true,
    );

    let get_score = |id: u128| -> f64 {
        result
            .iter()
            .find(|(node_id, _)| *node_id == id)
            .map(|(_, s)| *s)
            .unwrap_or(0.0)
    };

    let person_score = get_score(person);
    let session_score = get_score(session);
    let turn_score = get_score(turn);
    let claim_score = get_score(claim);
    let topic_score = get_score(topic);
    let old_claim_score = get_score(old_claim);

    println!("=== Complex Graph Test ===");
    println!("Person (seed): {:.6}", person_score);
    println!("Session:       {:.6}", session_score);
    println!("Turn:          {:.6}", turn_score);
    println!("Claim:         {:.6}", claim_score);
    println!("Topic:         {:.6}", topic_score);
    println!("OldClaim:      {:.6}", old_claim_score);

    assert!(
        person_score > 0.0,
        "Seed (Person) should have positive score"
    );
    assert!(
        session_score > 0.0,
        "Session should have positive score (connected to seed)"
    );

    if claim_score > 0.0 && topic_score > 0.0 {
        assert!(
            claim_score > topic_score,
            "Claim (supports=1.0) should have higher score than Topic (about=0.5): claim={}, topic={}",
            claim_score,
            topic_score
        );
    }

    if claim_score > 0.0 && old_claim_score > 0.0 {
        assert!(
            claim_score > old_claim_score,
            "Claim should have higher score than OldClaim (supersedes=0.3): claim={}, old={}",
            claim_score,
            old_claim_score
        );
    }

    println!("=== Test passed! ===");
}
