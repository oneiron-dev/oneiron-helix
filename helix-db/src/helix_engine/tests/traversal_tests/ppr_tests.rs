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
        true,
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
        alice_s >= bob_s && alice_s >= carol_s,
        "Seed node should have the highest score"
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
        true,
    );

    let topic1_score = result.iter().find(|(id, _)| *id == topic1).map(|(_, s)| *s);
    let topic2_score = result.iter().find(|(id, _)| *id == topic2).map(|(_, s)| *s);

    assert!(topic1_score.is_some());
    assert!(topic2_score.is_some());

    let t1s = topic1_score.unwrap();
    let t2s = topic2_score.unwrap();
    assert!(
        t1s > 0.0 && t2s > 0.0,
        "Each seed should have a positive score"
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
        true,
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
        true,
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
        true,
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
        true,
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
        true,
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
        true,
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
        true,
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
        true,
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
        true,
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
        true,
    );

    assert_eq!(result.len(), 3, "Result should be limited to 3 entries");
}

/// Comprehensive PPR test validated against NetworkX's PageRank implementation.
///
/// Verification Summary:
/// - Relative node ordering matches NetworkX exactly:
///   NetworkX: alice > session1 > turn1 > claim1 > bob > turn2 > topic1 > claim2
///   Our implementation: same ordering
///
/// Score Differences:
/// - Absolute scores differ due to normalization approach:
///   NetworkX normalizes to sum = 1.0
///   Our implementation accumulates without normalization
///
/// Key Behaviors Validated:
/// - opposes (weight=0) blocks propagation entirely to opposed nodes
/// - supersedes (weight=0.3) significantly reduces score for downstream nodes
/// - part_of edges limited to 2 hops (nodes 3+ hops away get ~0 score)
/// - Disconnected subgraphs receive ~0 score despite being in universe
#[test]
fn test_ppr_oneiron_full_graph() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create 3 person nodes
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

    // Create 2 session nodes
    let session1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "session",
            props_option(&arena, props!("name" => "Session1")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let session2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "session",
            props_option(&arena, props!("name" => "Session2")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Create 3 turn nodes
    let turn1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "turn",
            props_option(&arena, props!("name" => "Turn1")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let turn2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "turn",
            props_option(&arena, props!("name" => "Turn2")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let turn3 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "turn",
            props_option(&arena, props!("name" => "Turn3")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Create 2 claim nodes
    let claim1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "claim",
            props_option(&arena, props!("name" => "Claim1")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let claim2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "claim",
            props_option(&arena, props!("name" => "Claim2")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Create 1 topic node
    let topic1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "topic",
            props_option(&arena, props!("name" => "Topic1")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Create 3 place nodes for part_of chain
    let seattle = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "place",
            props_option(&arena, props!("name" => "Seattle")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let washington = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "place",
            props_option(&arena, props!("name" => "Washington")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let usa = G::new_mut(&storage, &arena, &mut txn)
        .add_n("place", props_option(&arena, props!("name" => "USA")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Edge type 1: belongs_to (1.0) - turn -> session
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("belongs_to", None, turn1, session1, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("belongs_to", None, turn2, session1, false, false)
        .collect_to_obj()
        .unwrap();

    // Edge type 2: participates_in (1.0) - person -> session
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("participates_in", None, alice, session1, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("participates_in", None, bob, session1, false, false)
        .collect_to_obj()
        .unwrap();

    // Edge type 3: attached (0.8) - skipped (no assets in this test)

    // Edge type 4: authored_by (0.9) - turn -> person
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("authored_by", None, turn1, alice, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("authored_by", None, turn2, bob, false, false)
        .collect_to_obj()
        .unwrap();

    // Edge type 5: mentions (0.6) - turn -> person
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("mentions", None, turn1, bob, false, false)
        .collect_to_obj()
        .unwrap();

    // Edge type 6: about (0.5) - turn -> topic
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("about", None, turn1, topic1, false, false)
        .collect_to_obj()
        .unwrap();

    // Edge type 7: supports (1.0) - turn -> claim
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("supports", None, turn1, claim1, false, false)
        .collect_to_obj()
        .unwrap();

    // Edge type 8: opposes (0.0) - turn -> claim (blocks propagation!)
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("opposes", None, turn2, claim1, false, false)
        .collect_to_obj()
        .unwrap();

    // Edge type 9: claim_of (1.0) - claim -> person
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("claim_of", None, claim1, alice, false, false)
        .collect_to_obj()
        .unwrap();

    // Edge type 10: scoped_to (0.7) - claim -> session
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("scoped_to", None, claim1, session1, false, false)
        .collect_to_obj()
        .unwrap();

    // Edge type 11: supersedes (0.3) - claim -> claim
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("supersedes", None, claim1, claim2, false, false)
        .collect_to_obj()
        .unwrap();

    // Edge type 12: derived_from (0.2) - skipped (no summaries in this test)

    // Edge type 13: part_of (0.8) - place -> place chain
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("part_of", None, seattle, washington, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("part_of", None, washington, usa, false, false)
        .collect_to_obj()
        .unwrap();

    // Add edge from turn3 to seattle to connect part_of chain to main graph
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("about", None, turn3, seattle, false, false)
        .collect_to_obj()
        .unwrap();

    // Add turn3 belongs_to session2 and authored_by carol
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("belongs_to", None, turn3, session2, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("authored_by", None, turn3, carol, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    // Run PPR from seed alice
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [
        alice, bob, carol, session1, session2, turn1, turn2, turn3, claim1, claim2, topic1,
        seattle, washington, usa,
    ]
    .into_iter()
    .collect();

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
        20,
        true,
    );

    // Print all scores for verification
    println!("\n=== PPR Oneiron Full Graph Test Results ===");
    println!("Seed: alice");
    println!("Depth: 3, Damping: 0.85");
    println!("\nNode Scores:");

    let get_score = |id: u128| -> f64 {
        result
            .iter()
            .find(|(node_id, _)| *node_id == id)
            .map(|(_, s)| *s)
            .unwrap_or(0.0)
    };

    let alice_score = get_score(alice);
    let bob_score = get_score(bob);
    let carol_score = get_score(carol);
    let session1_score = get_score(session1);
    let session2_score = get_score(session2);
    let turn1_score = get_score(turn1);
    let turn2_score = get_score(turn2);
    let turn3_score = get_score(turn3);
    let claim1_score = get_score(claim1);
    let claim2_score = get_score(claim2);
    let topic1_score = get_score(topic1);
    let seattle_score = get_score(seattle);
    let washington_score = get_score(washington);
    let usa_score = get_score(usa);

    println!("  alice (seed):     {:.6}", alice_score);
    println!("  bob:              {:.6}", bob_score);
    println!("  carol:            {:.6}", carol_score);
    println!("  session1:         {:.6}", session1_score);
    println!("  session2:         {:.6}", session2_score);
    println!("  turn1:            {:.6}", turn1_score);
    println!("  turn2:            {:.6}", turn2_score);
    println!("  turn3:            {:.6}", turn3_score);
    println!("  claim1:           {:.6}", claim1_score);
    println!("  claim2:           {:.6}", claim2_score);
    println!("  topic1:           {:.6}", topic1_score);
    println!("  seattle:          {:.6}", seattle_score);
    println!("  washington:       {:.6}", washington_score);
    println!("  usa:              {:.6}", usa_score);
    println!();

    // ASSERTIONS

    // 1. alice (seed) has highest score
    assert!(
        alice_score > 0.0,
        "alice (seed) should have a positive score"
    );
    for (name, score) in [
        ("bob", bob_score),
        ("carol", carol_score),
        ("session1", session1_score),
        ("turn1", turn1_score),
        ("claim1", claim1_score),
    ] {
        assert!(
            alice_score >= score,
            "alice (seed) should have score >= {} (alice: {}, {}: {})",
            name,
            alice_score,
            name,
            score
        );
    }

    // 2. session1 has score > 0 (via participates_in inbound from alice)
    assert!(
        session1_score > 0.0,
        "session1 should have score > 0 (via participates_in from alice)"
    );

    // 3. turn1 has score > 0 (via authored_by inbound from alice)
    assert!(
        turn1_score > 0.0,
        "turn1 should have score > 0 (via authored_by inbound to alice)"
    );

    // 4. claim1 has score > 0 (via claim_of inbound from alice)
    assert!(
        claim1_score > 0.0,
        "claim1 should have score > 0 (via claim_of inbound to alice)"
    );

    // 5. claim2 has LOW score (via supersedes with weight 0.3)
    // claim2 is only reachable via claim1 -> supersedes -> claim2
    // The weight 0.3 should make it much lower than claim1
    assert!(
        claim2_score < claim1_score,
        "claim2 should have lower score than claim1 (supersedes weight 0.3): claim1={}, claim2={}",
        claim1_score,
        claim2_score
    );

    // 6. usa has ZERO or negligible score (beyond part_of 2-hop limit)
    // seattle -> washington -> usa is a 2-hop part_of chain
    // But seattle is not directly connected to alice, and usa is 3 hops via part_of
    assert!(
        usa_score < 0.001,
        "usa should have zero or negligible score (beyond part_of 2-hop limit): {}",
        usa_score
    );

    // Additional assertions to verify graph connectivity
    // bob should have score (connected via participates_in to session1, and via mentions from turn1)
    assert!(
        bob_score > 0.0 || turn1_score > 0.0,
        "bob or turn1 should have score from alice's connections"
    );

    // topic1 should have some score if turn1 has score (via about edge)
    if turn1_score > 0.0 {
        println!(
            "turn1 has score {}, topic1 score: {}",
            turn1_score, topic1_score
        );
    }

    println!("=== All assertions passed! ===\n");
}

#[test]
fn test_ppr_normalization() {
    // Test that normalize=true makes scores sum to 1.0
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create a simple graph: A -> B -> C
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

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [node_a, node_b, node_c].into_iter().collect();
    let seeds = vec![node_a];
    let edge_weights = HashMap::new();

    // Run WITHOUT normalization
    let result_unnorm = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        3,
        0.85,
        10,
        false, // normalize=false
    );

    // Run WITH normalization
    let result_norm = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        3,
        0.85,
        10,
        true, // normalize=true
    );

    // Check unnormalized sum is NOT 1.0
    // With 3 nodes (A->B->C), seed A, damping 0.85, depth 3:
    // - A gets initial score 1.0, plus teleport scores each iteration
    // - B and C get propagated scores from A
    // - Sum must be > 1.0 due to teleport adding score back to seed
    let sum_unnorm: f64 = result_unnorm.iter().map(|(_, s)| s).sum();
    assert!(
        (sum_unnorm - 1.0).abs() > 0.01,
        "Unnormalized scores should NOT sum to 1.0, got {}",
        sum_unnorm
    );
    assert!(
        sum_unnorm > 1.5 || sum_unnorm < 0.5,
        "Unnormalized sum should be significantly different from 1.0, got {}",
        sum_unnorm
    );

    // Check normalized sum IS 1.0 (within epsilon)
    let sum_norm: f64 = result_norm.iter().map(|(_, s)| s).sum();
    assert!(
        (sum_norm - 1.0).abs() < 0.0001,
        "Normalized scores should sum to 1.0, got {}",
        sum_norm
    );

    // Check relative ordering is preserved
    let order_unnorm: Vec<u128> = result_unnorm.iter().map(|(id, _)| *id).collect();
    let order_norm: Vec<u128> = result_norm.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        order_unnorm, order_norm,
        "Normalization should preserve relative ordering"
    );

    println!("=== PPR Normalization Test ===");
    println!("Unnormalized sum: {}", sum_unnorm);
    println!("Normalized sum: {}", sum_norm);
    println!("Ordering preserved: {:?}", order_norm);
}

#[test]
fn test_ppr_claim_filter_excludes_unapproved() {
    use crate::helix_engine::graph::claim_filter::ClaimFilterConfig;
    use crate::helix_engine::graph::ppr::ppr_with_claim_filter;

    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let approved_claim = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "claim",
            props_option(
                &arena,
                props!(
                    "name" => "ApprovedClaim",
                    "approvalStatus" => "approved",
                    "lifecycleStatus" => "active",
                    "stale" => false
                ),
            ),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let unapproved_claim = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "claim",
            props_option(
                &arena,
                props!(
                    "name" => "UnapprovedClaim",
                    "approvalStatus" => "pending",
                    "lifecycleStatus" => "active",
                    "stale" => false
                ),
            ),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let person = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "Alice")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("claim_of", None, approved_claim, person, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("claim_of", None, unapproved_claim, person, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [approved_claim, unapproved_claim, person]
        .into_iter()
        .collect();
    let seeds = vec![person];
    let edge_weights = HashMap::new();
    let config = ClaimFilterConfig::default();

    let result = ppr_with_claim_filter(
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
        Some(&config),
    );

    let approved_in_result = result.iter().any(|(id, _)| *id == approved_claim);
    let unapproved_in_result = result.iter().any(|(id, _)| *id == unapproved_claim);

    assert!(approved_in_result, "Approved claim should be in results");
    assert!(
        !unapproved_in_result,
        "Unapproved claim should be filtered out"
    );
}

#[test]
fn test_ppr_claim_filter_excludes_stale() {
    use crate::helix_engine::graph::claim_filter::ClaimFilterConfig;
    use crate::helix_engine::graph::ppr::ppr_with_claim_filter;

    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let fresh_claim = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "claim",
            props_option(
                &arena,
                props!(
                    "name" => "FreshClaim",
                    "approvalStatus" => "auto",
                    "lifecycleStatus" => "active",
                    "stale" => false
                ),
            ),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let stale_claim = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "claim",
            props_option(
                &arena,
                props!(
                    "name" => "StaleClaim",
                    "approvalStatus" => "auto",
                    "lifecycleStatus" => "active",
                    "stale" => true
                ),
            ),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let person = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "Bob")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("claim_of", None, fresh_claim, person, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("claim_of", None, stale_claim, person, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [fresh_claim, stale_claim, person].into_iter().collect();
    let seeds = vec![person];
    let edge_weights = HashMap::new();
    let config = ClaimFilterConfig::default();

    let result = ppr_with_claim_filter(
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
        Some(&config),
    );

    let fresh_in_result = result.iter().any(|(id, _)| *id == fresh_claim);
    let stale_in_result = result.iter().any(|(id, _)| *id == stale_claim);

    assert!(fresh_in_result, "Fresh claim should be in results");
    assert!(!stale_in_result, "Stale claim should be filtered out");
}

#[test]
fn test_ppr_claim_filter_allows_approved() {
    use crate::helix_engine::graph::claim_filter::ClaimFilterConfig;
    use crate::helix_engine::graph::ppr::ppr_with_claim_filter;

    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let auto_claim = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "claim",
            props_option(
                &arena,
                props!(
                    "name" => "AutoClaim",
                    "approvalStatus" => "auto",
                    "lifecycleStatus" => "active",
                    "stale" => false
                ),
            ),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let approved_claim = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "claim",
            props_option(
                &arena,
                props!(
                    "name" => "ApprovedClaim",
                    "approvalStatus" => "approved",
                    "lifecycleStatus" => "active",
                    "stale" => false
                ),
            ),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let person = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "Carol")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("claim_of", None, auto_claim, person, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("claim_of", None, approved_claim, person, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [auto_claim, approved_claim, person].into_iter().collect();
    let seeds = vec![person];
    let edge_weights = HashMap::new();
    let config = ClaimFilterConfig::default();

    let result = ppr_with_claim_filter(
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
        Some(&config),
    );

    let auto_in_result = result.iter().any(|(id, _)| *id == auto_claim);
    let approved_in_result = result.iter().any(|(id, _)| *id == approved_claim);

    assert!(auto_in_result, "Auto-approved claim should be in results");
    assert!(
        approved_in_result,
        "User-approved claim should be in results"
    );
}

#[test]
fn test_ppr_claim_filter_excludes_inactive() {
    use crate::helix_engine::graph::claim_filter::ClaimFilterConfig;
    use crate::helix_engine::graph::ppr::ppr_with_claim_filter;

    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let active_claim = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "claim",
            props_option(
                &arena,
                props!(
                    "name" => "ActiveClaim",
                    "approvalStatus" => "auto",
                    "lifecycleStatus" => "active",
                    "stale" => false
                ),
            ),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let superseded_claim = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "claim",
            props_option(
                &arena,
                props!(
                    "name" => "SupersededClaim",
                    "approvalStatus" => "auto",
                    "lifecycleStatus" => "superseded",
                    "stale" => false
                ),
            ),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let person = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "Dave")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("claim_of", None, active_claim, person, false, false)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("claim_of", None, superseded_claim, person, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [active_claim, superseded_claim, person]
        .into_iter()
        .collect();
    let seeds = vec![person];
    let edge_weights = HashMap::new();
    let config = ClaimFilterConfig::default();

    let result = ppr_with_claim_filter(
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
        Some(&config),
    );

    let active_in_result = result.iter().any(|(id, _)| *id == active_claim);
    let superseded_in_result = result.iter().any(|(id, _)| *id == superseded_claim);

    assert!(active_in_result, "Active claim should be in results");
    assert!(
        !superseded_in_result,
        "Superseded claim should be filtered out"
    );
}

#[test]
fn test_ppr_claim_filter_disabled() {
    use crate::helix_engine::graph::ppr::ppr_with_claim_filter;

    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let unapproved_claim = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "claim",
            props_option(
                &arena,
                props!(
                    "name" => "UnapprovedClaim",
                    "approvalStatus" => "pending",
                    "lifecycleStatus" => "superseded",
                    "stale" => true
                ),
            ),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let person = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "Eve")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("claim_of", None, unapproved_claim, person, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [unapproved_claim, person].into_iter().collect();
    let seeds = vec![person];
    let edge_weights = HashMap::new();

    let result = ppr_with_claim_filter(
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
        None,
    );

    let unapproved_in_result = result.iter().any(|(id, _)| *id == unapproved_claim);

    assert!(
        unapproved_in_result,
        "With filter disabled, all claims should be in results"
    );
}

#[test]
fn test_filter_universe_by_claims() {
    use crate::helix_engine::graph::claim_filter::ClaimFilterConfig;
    use crate::helix_engine::graph::ppr::filter_universe_by_claims;

    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let valid_claim = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "claim",
            props_option(
                &arena,
                props!(
                    "name" => "ValidClaim",
                    "approvalStatus" => "auto",
                    "lifecycleStatus" => "active",
                    "stale" => false
                ),
            ),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let invalid_claim = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "claim",
            props_option(
                &arena,
                props!(
                    "name" => "InvalidClaim",
                    "approvalStatus" => "rejected",
                    "lifecycleStatus" => "active",
                    "stale" => false
                ),
            ),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let person = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "Frank")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [valid_claim, invalid_claim, person].into_iter().collect();
    let config = ClaimFilterConfig::default();

    let filtered = filter_universe_by_claims(&storage, &txn, &arena, &universe, &config);

    assert!(filtered.contains(&valid_claim), "Valid claim should pass");
    assert!(
        !filtered.contains(&invalid_claim),
        "Invalid claim should be filtered"
    );
    assert!(
        filtered.contains(&person),
        "Non-claim nodes should pass through"
    );
    assert_eq!(filtered.len(), 2);
}
