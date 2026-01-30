use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bumpalo::Bump;
use tempfile::TempDir;

use super::test_utils::props_option;
use crate::{
    helix_engine::{
        graph::ppr::ppr_with_storage,
        storage_core::HelixGraphStorage,
        traversal_core::ops::{
            g::G,
            source::{add_e::AddEAdapter, add_n::AddNAdapter},
        },
    },
    props,
};

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

#[test]
fn test_ppr_single_seed_propagation() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let alice = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "Alice")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let bob = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "Bob")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let carol = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "Carol")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("belongs_to", None, alice, bob, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("belongs_to", None, bob, carol, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [alice, bob, carol].into_iter().collect();
    let seeds = vec![alice];
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
    );

    assert!(!result.is_empty());

    let alice_score = result.iter().find(|(id, _)| *id == alice).map(|(_, s)| *s);
    let bob_score = result.iter().find(|(id, _)| *id == bob).map(|(_, s)| *s);
    let carol_score = result.iter().find(|(id, _)| *id == carol).map(|(_, s)| *s);

    assert!(alice_score.is_some(), "Alice (seed) should have a score");
    assert!(bob_score.is_some(), "Bob (1 hop) should have a score");
    assert!(carol_score.is_some(), "Carol (2 hops) should have a score");

    let alice_s = alice_score.unwrap();
    let bob_s = bob_score.unwrap();
    let carol_s = carol_score.unwrap();

    assert!(
        alice_s >= 1.0,
        "Seed node should retain at least its initial score"
    );
    assert!(
        bob_s > 0.0,
        "Directly connected node should receive propagated score"
    );
    assert!(carol_s > 0.0, "2-hop node should receive propagated score");
}

#[test]
fn test_ppr_multiple_seeds_distribution() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let topic1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "topic",
            props_option(&arena, props!("name" => "Topic1")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let topic2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "topic",
            props_option(&arena, props!("name" => "Topic2")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let doc1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "document",
            props_option(&arena, props!("name" => "Doc1")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let doc2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "document",
            props_option(&arena, props!("name" => "Doc2")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("belongs_to", None, topic1, doc1, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("belongs_to", None, topic2, doc2, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [topic1, topic2, doc1, doc2].into_iter().collect();
    let seeds = vec![topic1, topic2];
    let edge_weights = HashMap::new();

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
    );

    let topic1_score = result.iter().find(|(id, _)| *id == topic1).map(|(_, s)| *s);
    let topic2_score = result.iter().find(|(id, _)| *id == topic2).map(|(_, s)| *s);

    assert!(topic1_score.is_some());
    assert!(topic2_score.is_some());

    let t1s = topic1_score.unwrap();
    let t2s = topic2_score.unwrap();
    let expected_seed_score = 0.5;
    assert!(
        t1s >= expected_seed_score,
        "Each seed should keep at least its initial score"
    );
    assert!(
        t2s >= expected_seed_score,
        "Each seed should keep at least its initial score"
    );
}

#[test]
fn test_ppr_candidate_set_gating() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let in_universe = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "node",
            props_option(&arena, props!("name" => "InUniverse")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let connected_in = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "node",
            props_option(&arena, props!("name" => "ConnectedIn")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let outside_universe = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "node",
            props_option(&arena, props!("name" => "OutsideUniverse")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("belongs_to", None, in_universe, connected_in, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "belongs_to",
            None,
            in_universe,
            outside_universe,
            false,
            false,
        )
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [in_universe, connected_in].into_iter().collect();
    let seeds = vec![in_universe];
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
    );

    let outside_score = result
        .iter()
        .find(|(id, _)| *id == outside_universe)
        .map(|(_, s)| *s);

    assert!(
        outside_score.is_none() || outside_score.unwrap() == 0.0,
        "Nodes outside universe should not receive any score"
    );

    let connected_score = result
        .iter()
        .find(|(id, _)| *id == connected_in)
        .map(|(_, s)| *s);

    assert!(
        connected_score.is_some() && connected_score.unwrap() > 0.0,
        "Nodes inside universe should receive score"
    );
}

#[test]
fn test_ppr_bidirectional_traversal() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let source = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "Source")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let target = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "Target")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("mentions", None, source, target, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [source, target].into_iter().collect();
    let seeds = vec![target];
    let edge_weights = HashMap::new();

    let result = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        1,
        0.85,
        10,
    );

    let source_score = result.iter().find(|(id, _)| *id == source).map(|(_, s)| *s);

    assert!(
        source_score.is_some() && source_score.unwrap() > 0.0,
        "Inbound edge traversal should propagate score to source node"
    );
}

#[test]
fn test_ppr_part_of_hop_limit() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node_a = G::new_mut(&storage, &arena, &mut txn)
        .add_n("place", props_option(&arena, props!("name" => "A")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let node_b = G::new_mut(&storage, &arena, &mut txn)
        .add_n("place", props_option(&arena, props!("name" => "B")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let node_c = G::new_mut(&storage, &arena, &mut txn)
        .add_n("place", props_option(&arena, props!("name" => "C")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let node_d = G::new_mut(&storage, &arena, &mut txn)
        .add_n("place", props_option(&arena, props!("name" => "D")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("part_of", None, node_a, node_b, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("part_of", None, node_b, node_c, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("part_of", None, node_c, node_d, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [node_a, node_b, node_c, node_d].into_iter().collect();
    let seeds = vec![node_a];
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
    );

    let b_score = result.iter().find(|(id, _)| *id == node_b).map(|(_, s)| *s);
    let c_score = result.iter().find(|(id, _)| *id == node_c).map(|(_, s)| *s);
    let d_score = result.iter().find(|(id, _)| *id == node_d).map(|(_, s)| *s);

    assert!(
        b_score.is_some() && b_score.unwrap() > 0.0,
        "B (1 hop) should have a score"
    );
    assert!(
        c_score.is_some() && c_score.unwrap() > 0.0,
        "C (2 hops) should have a score"
    );
    assert!(
        d_score.is_none() || d_score.unwrap() == 0.0,
        "D (3 hops) should not receive score beyond part_of hop limit"
    );
}

#[test]
fn test_ppr_opposes_edge_blocks_propagation() {
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
        "Nodes connected via 'supports' edge should receive score"
    );

    assert!(
        opposed_score.is_none() || opposed_score.unwrap() == 0.0,
        "Nodes connected via 'opposes' edge should NOT receive score (weight=0)"
    );
}

#[test]
fn test_ppr_custom_edge_weights() {
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

    let high_weight_target = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "node",
            props_option(&arena, props!("name" => "HighWeight")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let low_weight_target = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "node",
            props_option(&arena, props!("name" => "LowWeight")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "high_weight_edge",
            None,
            source,
            high_weight_target,
            false,
            false,
        )
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "low_weight_edge",
            None,
            source,
            low_weight_target,
            false,
            false,
        )
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [source, high_weight_target, low_weight_target]
        .into_iter()
        .collect();
    let seeds = vec![source];

    let mut edge_weights = HashMap::new();
    edge_weights.insert("high_weight_edge".to_string(), 1.0);
    edge_weights.insert("low_weight_edge".to_string(), 0.1);

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
    );

    let high_score = result
        .iter()
        .find(|(id, _)| *id == high_weight_target)
        .map(|(_, s)| *s);

    let low_score = result
        .iter()
        .find(|(id, _)| *id == low_weight_target)
        .map(|(_, s)| *s);

    assert!(high_score.is_some());
    assert!(low_score.is_some());

    let hs = high_score.unwrap();
    let ls = low_score.unwrap();

    assert!(
        hs > ls,
        "Node via high-weight edge ({}) should have higher score than low-weight edge ({})",
        hs,
        ls
    );

    let expected_ratio = 1.0 / 0.1;
    let actual_ratio = hs / ls;
    assert!(
        (actual_ratio - expected_ratio).abs() < 0.01,
        "Score ratio should match edge weight ratio"
    );
}

#[test]
fn test_ppr_disconnected_nodes_zero_score() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let connected1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "node",
            props_option(&arena, props!("name" => "Connected1")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let connected2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "node",
            props_option(&arena, props!("name" => "Connected2")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let disconnected = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "node",
            props_option(&arena, props!("name" => "Disconnected")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("belongs_to", None, connected1, connected2, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [connected1, connected2, disconnected].into_iter().collect();
    let seeds = vec![connected1];
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
    );

    let disconnected_score = result
        .iter()
        .find(|(id, _)| *id == disconnected)
        .map(|(_, s)| *s);

    assert!(
        disconnected_score.is_none() || disconnected_score.unwrap() == 0.0,
        "Disconnected nodes should not receive any score"
    );
}

#[test]
fn test_ppr_damping_factor_effect() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "node",
            props_option(&arena, props!("name" => "Node1")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let node2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "node",
            props_option(&arena, props!("name" => "Node2")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("belongs_to", None, node1, node2, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let universe: HashSet<u128> = [node1, node2].into_iter().collect();
    let seeds = vec![node1];
    let edge_weights = HashMap::new();

    let arena_high = Bump::new();
    let txn_high = storage.graph_env.read_txn().unwrap();
    let result_high_damping = ppr_with_storage(
        &storage,
        &txn_high,
        &arena_high,
        &universe,
        &seeds,
        &edge_weights,
        2,
        0.9,
        10,
    );
    drop(txn_high);

    let arena_low = Bump::new();
    let txn_low = storage.graph_env.read_txn().unwrap();
    let result_low_damping = ppr_with_storage(
        &storage,
        &txn_low,
        &arena_low,
        &universe,
        &seeds,
        &edge_weights,
        2,
        0.5,
        10,
    );

    let high_node2_score = result_high_damping
        .iter()
        .find(|(id, _)| *id == node2)
        .map(|(_, s)| *s)
        .unwrap_or(0.0);

    let low_node2_score = result_low_damping
        .iter()
        .find(|(id, _)| *id == node2)
        .map(|(_, s)| *s)
        .unwrap_or(0.0);

    assert!(
        high_node2_score > low_node2_score,
        "Higher damping factor should result in more score propagation"
    );
}

#[test]
fn test_ppr_teleport_probability() {
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
        .add_edge("belongs_to", None, node_a, node_b, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("belongs_to", None, node_b, node_c, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let universe: HashSet<u128> = [node_a, node_b, node_c].into_iter().collect();
    let seeds = vec![node_a];
    let edge_weights = HashMap::new();

    let arena_teleport = Bump::new();
    let txn_teleport = storage.graph_env.read_txn().unwrap();
    let result_teleport = ppr_with_storage(
        &storage,
        &txn_teleport,
        &arena_teleport,
        &universe,
        &seeds,
        &edge_weights,
        3,
        0.5,
        10,
    );
    drop(txn_teleport);

    let arena_no_teleport = Bump::new();
    let txn_no_teleport = storage.graph_env.read_txn().unwrap();
    let result_no_teleport = ppr_with_storage(
        &storage,
        &txn_no_teleport,
        &arena_no_teleport,
        &universe,
        &seeds,
        &edge_weights,
        3,
        1.0,
        10,
    );

    let a_teleport = result_teleport
        .iter()
        .find(|(id, _)| *id == node_a)
        .map(|(_, s)| *s)
        .expect("Seed node A should have a score with teleport");

    let a_no_teleport = result_no_teleport
        .iter()
        .find(|(id, _)| *id == node_a)
        .map(|(_, s)| *s)
        .expect("Seed node A should have a score without teleport");

    assert!(
        a_teleport > a_no_teleport,
        "Teleport should increase seed score ({} > {})",
        a_teleport,
        a_no_teleport
    );
}

#[test]
fn test_ppr_limit_results() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let seed = G::new_mut(&storage, &arena, &mut txn)
        .add_n("node", props_option(&arena, props!("name" => "Seed")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let mut node_ids = vec![seed];
    for i in 1..=5 {
        let node = G::new_mut(&storage, &arena, &mut txn)
            .add_n(
                "node",
                props_option(&arena, props!("name" => format!("Node{}", i))),
                None,
            )
            .collect::<Result<Vec<_>, _>>()
            .unwrap()[0]
            .id();
        node_ids.push(node);

        G::new_mut(&storage, &arena, &mut txn)
            .add_edge("belongs_to", None, seed, node, false, false)
            .collect_to_obj()
            .unwrap();
    }

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = node_ids.iter().copied().collect();
    let seeds = vec![seed];
    let edge_weights = HashMap::new();

    let result = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        2,
        0.85,
        3,
    );

    assert_eq!(result.len(), 3, "Result should be limited to 3 entries");
}
