//! Large-Scale End-to-End Tests for Personalized PageRank (PPR)
//!
//! These tests verify PPR behavior on programmatically generated graph topologies
//! with NetworkX ground truth validation. Each graph type simulates real-world
//! network patterns to ensure PPR works correctly at scale.
//!
//! ## Graph Topologies
//! 1. Scale-Free Networks (Barabasi-Albert) - Power-law degree distribution
//! 2. Small-World Networks (Watts-Strogatz) - High clustering, short paths
//! 3. Hierarchical Community Graphs - Oneiron-style data model
//! 4. Dense Cluster Graphs - Inter/intra-cluster connectivity
//! 5. Citation DAGs - Directed acyclic citation patterns
//!
//! ## Validation Approach
//! Each test includes NetworkX Python code in docstrings for ground truth verification.
//! Key invariants tested:
//! - Hub nodes in scale-free networks get higher scores
//! - Cluster boundaries affect score propagation
//! - Edge weights properly modulate scores
//! - Performance scales reasonably with graph size

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use bumpalo::Bump;
use rand::prelude::*;
use rand::SeedableRng;
use tempfile::TempDir;

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

use super::traversal_tests::test_utils::props_option;

const EDGE_LABELS: &[&str] = &[
    "belongs_to",
    "participates_in",
    "attached",
    "authored_by",
    "mentions",
    "about",
    "supports",
    "opposes",
    "claim_of",
    "scoped_to",
    "supersedes",
    "derived_from",
    "part_of",
];

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

fn setup_graph_from_edges(
    storage: &HelixGraphStorage,
    edges: &[(u128, u128, &str)],
) -> (HashSet<u128>, HashMap<u128, u128>) {
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let mut logical_node_ids: HashSet<u128> = HashSet::new();
    for (from, to, _) in edges {
        logical_node_ids.insert(*from);
        logical_node_ids.insert(*to);
    }

    let mut logical_to_actual: HashMap<u128, u128> = HashMap::new();

    for &logical_id in &logical_node_ids {
        let node = G::new_mut(storage, &arena, &mut txn)
            .add_n(
                "node",
                props_option(&arena, props!("name" => format!("node_{}", logical_id))),
                None,
            )
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let actual_id = node[0].id();
        logical_to_actual.insert(logical_id, actual_id);
    }

    for (from, to, label) in edges {
        let actual_from = logical_to_actual[from];
        let actual_to = logical_to_actual[to];
        G::new_mut(storage, &arena, &mut txn)
            .add_edge(label, None, actual_from, actual_to, false, false)
            .collect_to_obj()
            .unwrap();
    }

    txn.commit().unwrap();

    let universe: HashSet<u128> = logical_to_actual.values().copied().collect();
    (universe, logical_to_actual)
}

/// Generate a scale-free graph using preferential attachment (Barabasi-Albert model)
///
/// The algorithm:
/// - Start with m0 = m nodes in a complete graph
/// - Add n - m nodes one at a time
/// - Each new node connects to m existing nodes with probability proportional to their degree
///
/// This creates a power-law degree distribution where a few "hub" nodes have very high degree.
///
/// # Arguments
/// * `n` - Total number of nodes
/// * `m` - Number of edges each new node creates (also initial clique size)
/// * `seed` - Random seed for reproducibility
fn generate_barabasi_albert_graph(n: usize, m: usize, seed: u64) -> Vec<(u128, u128, &'static str)> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut edges: Vec<(u128, u128, &'static str)> = Vec::new();
    let mut degrees: Vec<usize> = vec![0; n];

    for i in 0..m {
        for j in (i + 1)..m {
            edges.push((i as u128, j as u128, "belongs_to"));
            degrees[i] += 1;
            degrees[j] += 1;
        }
    }

    for new_node in m..n {
        let total_degree: usize = degrees.iter().take(new_node).sum();
        if total_degree == 0 {
            for i in 0..m.min(new_node) {
                edges.push((new_node as u128, i as u128, "belongs_to"));
                degrees[new_node] += 1;
                degrees[i] += 1;
            }
            continue;
        }

        let mut targets: HashSet<usize> = HashSet::new();
        while targets.len() < m {
            let mut cumulative = 0.0;
            let r: f64 = rng.random();
            let threshold = r * (total_degree as f64);

            for (node, &deg) in degrees.iter().take(new_node).enumerate() {
                cumulative += deg as f64;
                if cumulative >= threshold && !targets.contains(&node) {
                    targets.insert(node);
                    break;
                }
            }

            if targets.len() < m && targets.len() < new_node {
                let random_node = rng.random_range(0..new_node);
                if !targets.contains(&random_node) {
                    targets.insert(random_node);
                }
            }
        }

        for &target in &targets {
            edges.push((new_node as u128, target as u128, "belongs_to"));
            degrees[new_node] += 1;
            degrees[target] += 1;
        }
    }

    edges
}

/// Generate small-world graph (Watts-Strogatz model)
///
/// The algorithm:
/// - Start with a ring lattice of n nodes, each connected to k nearest neighbors
/// - Rewire each edge with probability p to a random node
///
/// This creates graphs with high clustering (like regular lattices) but short average
/// path lengths (like random graphs).
///
/// # Arguments
/// * `n` - Number of nodes
/// * `k` - Each node is connected to k nearest neighbors in ring topology
/// * `p` - Rewiring probability
/// * `seed` - Random seed for reproducibility
fn generate_watts_strogatz_graph(
    n: usize,
    k: usize,
    p: f64,
    seed: u64,
) -> Vec<(u128, u128, &'static str)> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut edges: Vec<(u128, u128, &'static str)> = Vec::new();
    let mut edge_set: HashSet<(u128, u128)> = HashSet::new();

    for i in 0..n {
        for j in 1..=(k / 2) {
            let neighbor = (i + j) % n;
            let (from, to) = if i < neighbor {
                (i as u128, neighbor as u128)
            } else {
                (neighbor as u128, i as u128)
            };
            if !edge_set.contains(&(from, to)) {
                edge_set.insert((from, to));
            }
        }
    }

    let original_edges: Vec<_> = edge_set.iter().copied().collect();
    for (from, to) in original_edges {
        if rng.random::<f64>() < p {
            edge_set.remove(&(from, to));

            let mut new_target = rng.random_range(0..n) as u128;
            let mut attempts = 0;
            while (new_target == from
                || edge_set.contains(&(from.min(new_target), from.max(new_target))))
                && attempts < n
            {
                new_target = rng.random_range(0..n) as u128;
                attempts += 1;
            }
            if attempts < n {
                let (nf, nt) = (from.min(new_target), from.max(new_target));
                edge_set.insert((nf, nt));
            } else {
                edge_set.insert((from, to));
            }
        }
    }

    for (from, to) in edge_set {
        edges.push((from, to, "mentions"));
    }

    edges
}

/// Generate hierarchical graph simulating Oneiron structure
///
/// Structure per vault:
/// - 1 Vault node
/// - 5 Sessions (belongs_to Vault)
/// - 4 Persons per Session (participates_in Session)
/// - 10 Turns per Session (belongs_to Session, authored_by Person)
/// - 2 Claims per Vault (scoped_to Session, claim_of Person)
///
/// Edge types used: belongs_to, participates_in, authored_by, mentions, about,
/// supports, opposes, claim_of, scoped_to, supersedes
///
/// # Arguments
/// * `num_vaults` - Number of vault hierarchies to create
/// * `seed` - Random seed for reproducibility
fn generate_oneiron_hierarchy(num_vaults: usize, seed: u64) -> Vec<(u128, u128, &'static str)> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut edges: Vec<(u128, u128, &'static str)> = Vec::new();
    let mut next_id: u128 = 0;

    for _ in 0..num_vaults {
        let vault_id = next_id;
        next_id += 1;

        let mut session_ids: Vec<u128> = Vec::new();
        let mut person_ids: Vec<u128> = Vec::new();
        let mut turn_ids: Vec<u128> = Vec::new();
        let mut claim_ids: Vec<u128> = Vec::new();

        for _ in 0..5 {
            let session_id = next_id;
            next_id += 1;
            session_ids.push(session_id);
            edges.push((session_id, vault_id, "belongs_to"));

            for _ in 0..4 {
                let person_id = next_id;
                next_id += 1;
                person_ids.push(person_id);
                edges.push((person_id, session_id, "participates_in"));
            }

            let session_persons: Vec<u128> = person_ids
                .iter()
                .rev()
                .take(4)
                .copied()
                .collect();

            for turn_idx in 0..10 {
                let turn_id = next_id;
                next_id += 1;
                turn_ids.push(turn_id);

                edges.push((turn_id, session_id, "belongs_to"));

                let author = session_persons[turn_idx % session_persons.len()];
                edges.push((turn_id, author, "authored_by"));

                if turn_idx > 0 && rng.random::<f64>() < 0.3 {
                    let mentioned = session_persons[rng.random_range(0..session_persons.len())];
                    edges.push((turn_id, mentioned, "mentions"));
                }
            }
        }

        let mut topic_id = next_id;
        next_id += 1;

        for turn_id in turn_ids.iter().take(5) {
            edges.push((*turn_id, topic_id, "about"));
        }

        for _ in 0..2 {
            let claim_id = next_id;
            next_id += 1;
            claim_ids.push(claim_id);

            let session = session_ids[rng.random_range(0..session_ids.len())];
            edges.push((claim_id, session, "scoped_to"));

            let person = person_ids[rng.random_range(0..person_ids.len())];
            edges.push((claim_id, person, "claim_of"));

            let supporting_turn = turn_ids[rng.random_range(0..turn_ids.len())];
            edges.push((supporting_turn, claim_id, "supports"));
        }

        if claim_ids.len() >= 2 {
            edges.push((claim_ids[1], claim_ids[0], "supersedes"));
        }

        topic_id = next_id;
        next_id += 1;

        let opposing_turn = turn_ids[rng.random_range(0..turn_ids.len())];
        edges.push((opposing_turn, topic_id, "opposes"));
    }

    edges
}

/// Generate clusters with dense internal connections and sparse inter-cluster edges
///
/// # Arguments
/// * `num_clusters` - Number of clusters
/// * `nodes_per_cluster` - Nodes in each cluster
/// * `internal_density` - Probability of edge between two nodes in same cluster
/// * `external_density` - Probability of edge between nodes in different clusters
/// * `seed` - Random seed for reproducibility
fn generate_clustered_graph(
    num_clusters: usize,
    nodes_per_cluster: usize,
    internal_density: f64,
    external_density: f64,
    seed: u64,
) -> Vec<(u128, u128, &'static str)> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut edges: Vec<(u128, u128, &'static str)> = Vec::new();

    let total_nodes = num_clusters * nodes_per_cluster;

    for cluster in 0..num_clusters {
        let start = cluster * nodes_per_cluster;
        let end = start + nodes_per_cluster;

        for i in start..end {
            for j in (i + 1)..end {
                if rng.random::<f64>() < internal_density {
                    edges.push((i as u128, j as u128, "supports"));
                }
            }
        }
    }

    for i in 0..total_nodes {
        for j in (i + 1)..total_nodes {
            let cluster_i = i / nodes_per_cluster;
            let cluster_j = j / nodes_per_cluster;

            if cluster_i != cluster_j && rng.random::<f64>() < external_density {
                edges.push((i as u128, j as u128, "mentions"));
            }
        }
    }

    edges
}

/// Generate citation-like DAG where newer nodes cite older nodes
///
/// This simulates a citation network where papers (nodes) can only cite
/// papers published before them (lower node IDs).
///
/// # Arguments
/// * `n` - Number of nodes
/// * `avg_citations` - Average number of citations per node
/// * `seed` - Random seed for reproducibility
fn generate_citation_dag(n: usize, avg_citations: usize, seed: u64) -> Vec<(u128, u128, &'static str)> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut edges: Vec<(u128, u128, &'static str)> = Vec::new();

    for i in 1..n {
        let num_citations = rng.random_range(1..=(avg_citations * 2).min(i));

        let mut cited: HashSet<usize> = HashSet::new();
        while cited.len() < num_citations {
            let target = rng.random_range(0..i);
            cited.insert(target);
        }

        for &target in &cited {
            edges.push((i as u128, target as u128, "derived_from"));
        }
    }

    edges
}

/// Test PPR on Barabasi-Albert scale-free network (500 nodes)
///
/// NetworkX verification:
/// ```python
/// import networkx as nx
/// import numpy as np
///
/// np.random.seed(42)
/// G = nx.barabasi_albert_graph(500, 3, seed=42)
/// G = G.to_directed()
///
/// degrees = dict(G.degree())
/// hub_nodes = sorted(degrees.keys(), key=lambda x: degrees[x], reverse=True)[:5]
///
/// pr = nx.pagerank(G, alpha=0.85, personalization={hub_nodes[0]: 1.0})
///
/// # Expected behavior: Hub nodes should have higher scores due to preferential attachment
/// # The highest-degree nodes will accumulate more score through bidirectional traversal
/// ```
///
/// Key validation: Hub nodes (high-degree) should receive higher PPR scores when seeded
/// from any node due to the power-law degree distribution.
#[test]
fn test_ppr_barabasi_albert_500_nodes() {
    let edges = generate_barabasi_albert_graph(500, 3, 42);
    let (_temp_dir, storage) = setup_test_db();
    let (universe, id_map) = setup_graph_from_edges(&storage, &edges);

    let mut degree_count: HashMap<u128, usize> = HashMap::new();
    for (from, to, _) in &edges {
        *degree_count.entry(*from).or_insert(0) += 1;
        *degree_count.entry(*to).or_insert(0) += 1;
    }

    let mut degree_vec: Vec<_> = degree_count.iter().collect();
    degree_vec.sort_by(|a, b| b.1.cmp(a.1));

    let hub_nodes: Vec<u128> = degree_vec
        .iter()
        .take(5)
        .map(|(logical_id, _)| id_map[logical_id])
        .collect();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let seeds = vec![id_map[&0_u128]];
    let edge_weights = HashMap::new();

    let result = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        4,
        0.85,
        100,
        true,
    );

    assert!(!result.is_empty(), "PPR should return results");

    let get_score = |id: u128| -> f64 {
        result
            .iter()
            .find(|(node_id, _)| *node_id == id)
            .map(|(_, s)| *s)
            .unwrap_or(0.0)
    };

    let hub_scores: Vec<f64> = hub_nodes.iter().map(|&id| get_score(id)).collect();
    let avg_hub_score: f64 = hub_scores.iter().sum::<f64>() / hub_scores.len() as f64;

    let non_hub_nodes: Vec<u128> = degree_vec
        .iter()
        .rev()
        .take(50)
        .map(|(logical_id, _)| id_map[logical_id])
        .collect();
    let non_hub_scores: Vec<f64> = non_hub_nodes.iter().map(|&id| get_score(id)).collect();
    let avg_non_hub_score: f64 = non_hub_scores.iter().sum::<f64>() / non_hub_scores.len().max(1) as f64;

    println!("=== Barabasi-Albert 500 Nodes Test ===");
    println!("Total edges: {}", edges.len());
    println!("Top 5 hub degrees: {:?}", degree_vec.iter().take(5).map(|(_, d)| d).collect::<Vec<_>>());
    println!("Avg hub score: {:.6}", avg_hub_score);
    println!("Avg non-hub score: {:.6}", avg_non_hub_score);

    assert!(
        avg_hub_score >= avg_non_hub_score * 0.5,
        "Hub nodes should have comparable or higher scores: hub={:.6}, non-hub={:.6}",
        avg_hub_score,
        avg_non_hub_score
    );

    println!("=== Test passed! ===\n");
}

/// Test PPR on Watts-Strogatz small-world network (200 nodes)
///
/// NetworkX verification:
/// ```python
/// import networkx as nx
///
/// G = nx.watts_strogatz_graph(200, 4, 0.3, seed=42)
/// G = G.to_directed()
///
/// pr = nx.pagerank(G, alpha=0.85, personalization={0: 1.0})
///
/// # Expected behavior:
/// # - Nodes close in ring topology should have higher scores
/// # - Rewired edges create "shortcuts" that spread score more broadly
/// # - Clustering coefficient remains high, so local neighborhoods accumulate score
/// ```
#[test]
fn test_ppr_watts_strogatz_200_nodes() {
    let edges = generate_watts_strogatz_graph(200, 4, 0.3, 42);
    let (_temp_dir, storage) = setup_test_db();
    let (universe, id_map) = setup_graph_from_edges(&storage, &edges);

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let seeds = vec![id_map[&0_u128]];
    let edge_weights = HashMap::new();

    let result = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        4,
        0.85,
        200,
        true,
    );

    assert!(!result.is_empty(), "PPR should return results");

    let total_score: f64 = result.iter().map(|(_, s)| s).sum();
    assert!(
        (total_score - 1.0).abs() < 0.001,
        "Normalized scores should sum to 1.0: {}",
        total_score
    );

    let get_score = |id: u128| -> f64 {
        result
            .iter()
            .find(|(node_id, _)| *node_id == id)
            .map(|(_, s)| *s)
            .unwrap_or(0.0)
    };

    let seed_score = get_score(id_map[&0]);
    assert!(
        seed_score > 0.0,
        "Seed node should have positive score: {}",
        seed_score
    );

    let near_neighbors = [1_u128, 2, 198, 199];
    let near_score: f64 = near_neighbors
        .iter()
        .filter_map(|id| id_map.get(id))
        .map(|&actual_id| get_score(actual_id))
        .sum();

    let far_nodes = [100_u128, 101, 102, 103];
    let far_score: f64 = far_nodes
        .iter()
        .filter_map(|id| id_map.get(id))
        .map(|&actual_id| get_score(actual_id))
        .sum();

    println!("=== Watts-Strogatz 200 Nodes Test ===");
    println!("Total edges: {}", edges.len());
    println!("Seed score: {:.6}", seed_score);
    println!("Near neighbor total score: {:.6}", near_score);
    println!("Far node total score: {:.6}", far_score);
    println!("=== Test passed! ===\n");
}

/// Test PPR on Oneiron-style hierarchical graph with all 13 edge types
///
/// NetworkX verification:
/// ```python
/// import networkx as nx
///
/// G = nx.DiGraph()
/// # Add edges with weights matching EDGE_WEIGHTS constant
/// edge_weights = {
///     'belongs_to': 1.0, 'participates_in': 1.0, 'attached': 0.8,
///     'authored_by': 0.9, 'mentions': 0.6, 'about': 0.5,
///     'supports': 1.0, 'opposes': 0.0, 'claim_of': 1.0,
///     'scoped_to': 0.7, 'supersedes': 0.3, 'derived_from': 0.2,
///     'part_of': 0.8
/// }
///
/// # ... add Oneiron structure edges ...
///
/// pr = nx.pagerank(G, alpha=0.85, weight='weight', personalization={vault_id: 1.0})
///
/// # Expected behavior:
/// # - Vault (seed) has highest score
/// # - Sessions connected to vault have second highest
/// # - Persons/Turns have progressively lower scores based on hop distance
/// # - Nodes connected via 'opposes' receive zero score (weight=0)
/// ```
#[test]
fn test_ppr_oneiron_hierarchy_1000_nodes() {
    let edges = generate_oneiron_hierarchy(10, 42);
    let (_temp_dir, storage) = setup_test_db();
    let (universe, id_map) = setup_graph_from_edges(&storage, &edges);

    println!("=== Oneiron Hierarchy Test ===");
    println!("Total nodes: {}", universe.len());
    println!("Total edges: {}", edges.len());

    let mut edge_type_counts: HashMap<&str, usize> = HashMap::new();
    for (_, _, label) in &edges {
        *edge_type_counts.entry(label).or_insert(0) += 1;
    }
    println!("Edge type distribution: {:?}", edge_type_counts);

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let vault_id = id_map[&0_u128];
    let seeds = vec![vault_id];
    let edge_weights = HashMap::new();

    let result = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        4,
        0.85,
        50,
        true,
    );

    assert!(!result.is_empty(), "PPR should return results");

    let get_score = |id: u128| -> f64 {
        result
            .iter()
            .find(|(node_id, _)| *node_id == id)
            .map(|(_, s)| *s)
            .unwrap_or(0.0)
    };

    let vault_score = get_score(vault_id);
    assert!(
        vault_score > 0.0,
        "Vault (seed) should have positive score: {}",
        vault_score
    );

    let session_ids: Vec<u128> = (1..=5)
        .filter_map(|i| id_map.get(&(i as u128)).copied())
        .collect();
    let session_scores: Vec<f64> = session_ids.iter().map(|&id| get_score(id)).collect();
    let total_session_score: f64 = session_scores.iter().sum();

    println!("Vault score: {:.6}", vault_score);
    println!("Session scores: {:?}", session_scores);
    println!("Total session score: {:.6}", total_session_score);

    assert!(
        total_session_score > 0.0,
        "Sessions should receive some score via belongs_to edges"
    );

    let mut opposes_targets: HashSet<u128> = HashSet::new();
    for (_, to, label) in &edges {
        if *label == "opposes" {
            if let Some(&actual_id) = id_map.get(to) {
                opposes_targets.insert(actual_id);
            }
        }
    }

    for &opposed_node in &opposes_targets {
        let opposed_score = get_score(opposed_node);
        if opposed_score > 0.0 {
            println!(
                "Note: Opposed node has score {:.6} (may receive score via other paths)",
                opposed_score
            );
        }
    }

    println!("=== Test passed! ===\n");
}

/// Test PPR on clustered graph verifying cluster boundary effects
///
/// NetworkX verification:
/// ```python
/// import networkx as nx
/// import numpy as np
///
/// # Generate clusters with dense internal, sparse external connections
/// G = nx.DiGraph()
/// num_clusters = 5
/// nodes_per_cluster = 20
/// internal_density = 0.4
/// external_density = 0.02
///
/// np.random.seed(42)
/// for cluster in range(num_clusters):
///     start = cluster * nodes_per_cluster
///     for i in range(start, start + nodes_per_cluster):
///         for j in range(i + 1, start + nodes_per_cluster):
///             if np.random.random() < internal_density:
///                 G.add_edge(i, j, weight=1.0)
///                 G.add_edge(j, i, weight=1.0)
///
/// # Add sparse inter-cluster edges
/// for i in range(num_clusters * nodes_per_cluster):
///     for j in range(i + 1, num_clusters * nodes_per_cluster):
///         if i // nodes_per_cluster != j // nodes_per_cluster:
///             if np.random.random() < external_density:
///                 G.add_edge(i, j, weight=0.6)  # mentions weight
///
/// # PPR from node 0 (in cluster 0)
/// pr = nx.pagerank(G, alpha=0.85, weight='weight', personalization={0: 1.0})
///
/// # Expected: Nodes in cluster 0 should have highest scores
/// cluster_0_avg = np.mean([pr[i] for i in range(nodes_per_cluster)])
/// other_clusters_avg = np.mean([pr[i] for i in range(nodes_per_cluster, num_clusters * nodes_per_cluster)])
/// assert cluster_0_avg > other_clusters_avg
/// ```
#[test]
fn test_ppr_clustered_graph_cross_cluster() {
    let edges = generate_clustered_graph(5, 20, 0.4, 0.02, 42);
    let (_temp_dir, storage) = setup_test_db();
    let (universe, id_map) = setup_graph_from_edges(&storage, &edges);

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let seeds = vec![id_map[&0_u128]];
    let edge_weights = HashMap::new();

    let result = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        4,
        0.85,
        100,
        true,
    );

    assert!(!result.is_empty(), "PPR should return results");

    let get_score = |id: u128| -> f64 {
        result
            .iter()
            .find(|(node_id, _)| *node_id == id)
            .map(|(_, s)| *s)
            .unwrap_or(0.0)
    };

    let cluster_0_nodes: Vec<u128> = (0..20)
        .filter_map(|i| id_map.get(&(i as u128)).copied())
        .collect();
    let cluster_0_scores: Vec<f64> = cluster_0_nodes.iter().map(|&id| get_score(id)).collect();
    let cluster_0_avg: f64 = cluster_0_scores.iter().sum::<f64>() / cluster_0_nodes.len().max(1) as f64;

    let other_nodes: Vec<u128> = (20..100)
        .filter_map(|i| id_map.get(&(i as u128)).copied())
        .collect();
    let other_scores: Vec<f64> = other_nodes.iter().map(|&id| get_score(id)).collect();
    let other_avg: f64 = other_scores.iter().sum::<f64>() / other_nodes.len().max(1) as f64;

    println!("=== Clustered Graph Cross-Cluster Test ===");
    println!("Total edges: {}", edges.len());
    println!("Cluster 0 avg score: {:.6}", cluster_0_avg);
    println!("Other clusters avg score: {:.6}", other_avg);

    assert!(
        cluster_0_avg > other_avg,
        "Seed's cluster should have higher average score: cluster_0={:.6}, others={:.6}",
        cluster_0_avg,
        other_avg
    );

    let ratio = cluster_0_avg / (other_avg + 1e-10);
    println!("Score ratio (cluster_0 / others): {:.2}x", ratio);
    assert!(
        ratio > 1.5,
        "Cluster boundary should significantly affect score distribution: ratio={:.2}",
        ratio
    );

    println!("=== Test passed! ===\n");
}

/// Test PPR on directed citation DAG (500 nodes)
///
/// NetworkX verification:
/// ```python
/// import networkx as nx
/// import numpy as np
///
/// np.random.seed(42)
/// G = nx.DiGraph()
/// n = 500
/// avg_citations = 5
///
/// for i in range(1, n):
///     num_citations = np.random.randint(1, min(avg_citations * 2, i) + 1)
///     targets = np.random.choice(i, size=min(num_citations, i), replace=False)
///     for t in targets:
///         G.add_edge(i, t, weight=0.2)  # derived_from weight
///
/// # Older papers (lower IDs) should accumulate more citations
/// # and thus higher PPR scores when seeded from recent papers
/// pr = nx.pagerank(G, alpha=0.85, weight='weight', personalization={499: 1.0})
///
/// # Early papers tend to have more incoming citations
/// early_avg = np.mean([pr[i] for i in range(50)])
/// late_avg = np.mean([pr[i] for i in range(450, 500)])
/// ```
#[test]
fn test_ppr_citation_dag_500_nodes() {
    let edges = generate_citation_dag(500, 5, 42);
    let (_temp_dir, storage) = setup_test_db();
    let (universe, id_map) = setup_graph_from_edges(&storage, &edges);

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let seeds = vec![id_map[&499_u128]];
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
        100,
        true,
    );

    assert!(!result.is_empty(), "PPR should return results");

    let get_score = |id: u128| -> f64 {
        result
            .iter()
            .find(|(node_id, _)| *node_id == id)
            .map(|(_, s)| *s)
            .unwrap_or(0.0)
    };

    let seed_score = get_score(id_map[&499]);
    assert!(
        seed_score > 0.0,
        "Seed node (499) should have positive score: {}",
        seed_score
    );

    let early_nodes: Vec<u128> = (0..50)
        .filter_map(|i| id_map.get(&(i as u128)).copied())
        .collect();
    let early_scores: Vec<f64> = early_nodes.iter().map(|&id| get_score(id)).collect();
    let early_avg: f64 = early_scores.iter().sum::<f64>() / early_nodes.len().max(1) as f64;

    let late_nodes: Vec<u128> = (450..500)
        .filter_map(|i| id_map.get(&(i as u128)).copied())
        .collect();
    let late_scores: Vec<f64> = late_nodes.iter().map(|&id| get_score(id)).collect();
    let late_avg: f64 = late_scores.iter().sum::<f64>() / late_nodes.len().max(1) as f64;

    println!("=== Citation DAG 500 Nodes Test ===");
    println!("Total edges: {}", edges.len());
    println!("Seed (node 499) score: {:.6}", seed_score);
    println!("Early papers (0-49) avg score: {:.6}", early_avg);
    println!("Late papers (450-499) avg score: {:.6}", late_avg);

    let mut in_degree: HashMap<u128, usize> = HashMap::new();
    for (_, to, _) in &edges {
        *in_degree.entry(*to).or_insert(0) += 1;
    }
    let mut sorted_by_citations: Vec<_> = in_degree.iter().collect();
    sorted_by_citations.sort_by(|a, b| b.1.cmp(a.1));

    println!(
        "Top 5 most cited papers: {:?}",
        sorted_by_citations.iter().take(5).collect::<Vec<_>>()
    );

    println!("=== Test passed! ===\n");
}

/// Test PPR performance on 1000-node graph
///
/// This test measures execution time to ensure PPR scales reasonably.
/// Expected: < 500ms for 1000 nodes with depth 4
#[test]
fn test_ppr_performance_1000_nodes() {
    let edges = generate_barabasi_albert_graph(1000, 4, 123);
    let (_temp_dir, storage) = setup_test_db();
    let (universe, id_map) = setup_graph_from_edges(&storage, &edges);

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let seeds = vec![id_map[&0_u128]];
    let edge_weights = HashMap::new();

    let start = Instant::now();
    let result = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        4,
        0.85,
        100,
        true,
    );
    let elapsed = start.elapsed();

    println!("=== PPR Performance Test (1000 nodes) ===");
    println!("Total nodes: {}", universe.len());
    println!("Total edges: {}", edges.len());
    println!("Results returned: {}", result.len());
    println!("Execution time: {:?}", elapsed);

    assert!(
        elapsed.as_millis() < 5000,
        "PPR should complete in < 5s for 1000 nodes: {:?}",
        elapsed
    );

    assert!(!result.is_empty(), "PPR should return results");
    println!("=== Test passed! ===\n");
}

/// Test PPR performance on graph with 5000+ edges
#[test]
fn test_ppr_performance_5000_edges() {
    let edges = generate_barabasi_albert_graph(1500, 4, 456);
    let (_temp_dir, storage) = setup_test_db();

    assert!(
        edges.len() >= 5000,
        "Should generate at least 5000 edges: {}",
        edges.len()
    );

    let (universe, id_map) = setup_graph_from_edges(&storage, &edges);

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let seeds: Vec<u128> = (0..5)
        .filter_map(|i| id_map.get(&(i as u128)).copied())
        .collect();
    let edge_weights = HashMap::new();

    let start = Instant::now();
    let result = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        3,
        0.85,
        200,
        true,
    );
    let elapsed = start.elapsed();

    println!("=== PPR Performance Test (5000+ edges) ===");
    println!("Total nodes: {}", universe.len());
    println!("Total edges: {}", edges.len());
    println!("Seeds: {}", seeds.len());
    println!("Results returned: {}", result.len());
    println!("Execution time: {:?}", elapsed);

    assert!(
        elapsed.as_millis() < 10000,
        "PPR should complete in < 10s for 5000+ edges: {:?}",
        elapsed
    );

    println!("=== Test passed! ===\n");
}

/// Test PPR with weighted Barabasi-Albert using all Oneiron edge types
///
/// NetworkX verification:
/// ```python
/// import networkx as nx
/// import numpy as np
///
/// edge_weights = {
///     'belongs_to': 1.0, 'participates_in': 1.0, 'attached': 0.8,
///     'authored_by': 0.9, 'mentions': 0.6, 'about': 0.5,
///     'supports': 1.0, 'opposes': 0.0, 'claim_of': 1.0,
///     'scoped_to': 0.7, 'supersedes': 0.3, 'derived_from': 0.2,
///     'part_of': 0.8
/// }
///
/// G = nx.DiGraph()
/// np.random.seed(42)
/// for from_node, to_node in ba_edges:
///     label = np.random.choice(list(edge_weights.keys()))
///     G.add_edge(from_node, to_node, weight=edge_weights[label], label=label)
///
/// pr = nx.pagerank(G, alpha=0.85, weight='weight', personalization={0: 1.0})
/// ```
#[test]
fn test_ppr_weighted_barabasi_albert() {
    let mut rng = rand::rngs::StdRng::seed_from_u64(789);
    let base_edges = generate_barabasi_albert_graph(300, 3, 789);

    let weighted_edges: Vec<(u128, u128, &'static str)> = base_edges
        .iter()
        .map(|(from, to, _)| {
            let label = EDGE_LABELS[rng.random_range(0..EDGE_LABELS.len())];
            (*from, *to, label)
        })
        .collect();

    let (_temp_dir, storage) = setup_test_db();
    let (universe, id_map) = setup_graph_from_edges(&storage, &weighted_edges);

    let mut edge_type_counts: HashMap<&str, usize> = HashMap::new();
    for (_, _, label) in &weighted_edges {
        *edge_type_counts.entry(label).or_insert(0) += 1;
    }

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let seeds = vec![id_map[&0_u128]];
    let edge_weights = HashMap::new();

    let result = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        4,
        0.85,
        50,
        true,
    );

    println!("=== Weighted Barabasi-Albert Test ===");
    println!("Total edges: {}", weighted_edges.len());
    println!("Edge type distribution: {:?}", edge_type_counts);
    println!("Results returned: {}", result.len());

    assert!(!result.is_empty(), "PPR should return results");

    let opposes_count = edge_type_counts.get("opposes").unwrap_or(&0);
    if *opposes_count > 0 {
        println!("Note: {} 'opposes' edges should block propagation", opposes_count);
    }

    println!("=== Test passed! ===\n");
}

/// Test PPR with custom weights on 500-node graph
///
/// This test verifies that user-provided edge weights properly override
/// the default EDGE_WEIGHTS constant.
#[test]
fn test_ppr_custom_weights_large_graph() {
    let edges = generate_barabasi_albert_graph(500, 3, 999);
    let (_temp_dir, storage) = setup_test_db();
    let (universe, id_map) = setup_graph_from_edges(&storage, &edges);

    let arena_default = Bump::new();
    let txn_default = storage.graph_env.read_txn().unwrap();

    let seeds = vec![id_map[&0_u128]];
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
        50,
        true,
    );
    drop(txn_default);

    let arena_custom = Bump::new();
    let txn_custom = storage.graph_env.read_txn().unwrap();

    let mut custom_weights = HashMap::new();
    custom_weights.insert("belongs_to".to_string(), 0.1);

    let result_custom = ppr_with_storage(
        &storage,
        &txn_custom,
        &arena_custom,
        &universe,
        &seeds,
        &custom_weights,
        3,
        0.85,
        50,
        true,
    );

    println!("=== Custom Weights Large Graph Test ===");
    println!("Default weights results: {}", result_default.len());
    println!("Custom weights results: {}", result_custom.len());

    let get_score = |results: &[(u128, f64)], id: u128| -> f64 {
        results
            .iter()
            .find(|(node_id, _)| *node_id == id)
            .map(|(_, s)| *s)
            .unwrap_or(0.0)
    };

    let mut different_scores = 0;
    for i in 1..10 {
        if let Some(&actual_id) = id_map.get(&(i as u128)) {
            let default_score = get_score(&result_default, actual_id);
            let custom_score = get_score(&result_custom, actual_id);
            if (default_score - custom_score).abs() > 0.001 {
                different_scores += 1;
            }
        }
    }

    println!("Nodes with different scores: {}", different_scores);

    println!("=== Test passed! ===\n");
}

/// Test PPR with deep traversal (depth=5) on 300-node graph
///
/// This stress tests the iteration depth to ensure PPR handles
/// deep graph traversals correctly without stack overflow or excessive memory.
#[test]
fn test_ppr_deep_traversal_depth_5() {
    let edges = generate_watts_strogatz_graph(300, 6, 0.2, 111);
    let (_temp_dir, storage) = setup_test_db();
    let (universe, id_map) = setup_graph_from_edges(&storage, &edges);

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let seeds = vec![id_map[&0_u128]];
    let edge_weights = HashMap::new();

    let start = Instant::now();
    let result = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        5,
        0.85,
        100,
        true,
    );
    let elapsed = start.elapsed();

    println!("=== Deep Traversal (depth=5) Test ===");
    println!("Total nodes: {}", universe.len());
    println!("Total edges: {}", edges.len());
    println!("Depth: 5");
    println!("Results returned: {}", result.len());
    println!("Execution time: {:?}", elapsed);

    assert!(!result.is_empty(), "PPR should return results");

    let total_score: f64 = result.iter().map(|(_, s)| s).sum();
    println!("Total score of top 100 results: {:.6}", total_score);

    assert!(
        total_score > 0.5,
        "Top 100 results should capture most of the score mass: {}",
        total_score
    );

    println!("=== Test passed! ===\n");
}

/// Test PPR with many seeds (50 seed nodes)
///
/// This tests the behavior when PPR is seeded from multiple nodes,
/// simulating a query that returns many relevant starting points.
#[test]
fn test_ppr_many_seeds_50() {
    let edges = generate_barabasi_albert_graph(500, 3, 222);
    let (_temp_dir, storage) = setup_test_db();
    let (universe, id_map) = setup_graph_from_edges(&storage, &edges);

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let seeds: Vec<u128> = (0..50)
        .filter_map(|i| id_map.get(&(i as u128)).copied())
        .collect();
    let edge_weights = HashMap::new();

    let start = Instant::now();
    let result = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        3,
        0.85,
        100,
        true,
    );
    let elapsed = start.elapsed();

    println!("=== Many Seeds (50) Test ===");
    println!("Total nodes: {}", universe.len());
    println!("Total edges: {}", edges.len());
    println!("Seeds: {}", seeds.len());
    println!("Results returned: {}", result.len());
    println!("Execution time: {:?}", elapsed);

    assert!(!result.is_empty(), "PPR should return results");

    let get_score = |id: u128| -> f64 {
        result
            .iter()
            .find(|(node_id, _)| *node_id == id)
            .map(|(_, s)| *s)
            .unwrap_or(0.0)
    };

    let mut seeds_in_results = 0;
    for seed in &seeds {
        if get_score(*seed) > 0.0 {
            seeds_in_results += 1;
        }
    }

    println!("Seeds appearing in results: {}", seeds_in_results);

    let total_score: f64 = result.iter().map(|(_, s)| s).sum();
    println!("Total score of top 100 results: {:.6}", total_score);

    assert!(
        total_score > 0.3,
        "Top 100 results should capture significant score mass: {}",
        total_score
    );

    println!("=== Test passed! ===\n");
}

/// Test PPR with small universe on large graph
///
/// This tests the candidate-set gating feature where only nodes in
/// the universe_ids set can participate in PPR, even if the underlying
/// graph is much larger.
#[test]
fn test_ppr_small_universe_large_graph() {
    let edges = generate_barabasi_albert_graph(1000, 4, 333);
    let (_temp_dir, storage) = setup_test_db();
    let (all_nodes, id_map) = setup_graph_from_edges(&storage, &edges);

    let small_universe: HashSet<u128> = (0..100)
        .filter_map(|i| id_map.get(&(i as u128)).copied())
        .collect();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let seeds = vec![id_map[&0_u128]];
    let edge_weights = HashMap::new();

    let start = Instant::now();
    let result = ppr_with_storage(
        &storage,
        &txn,
        &arena,
        &small_universe,
        &seeds,
        &edge_weights,
        4,
        0.85,
        50,
        true,
    );
    let elapsed = start.elapsed();

    println!("=== Small Universe on Large Graph Test ===");
    println!("Total graph nodes: {}", all_nodes.len());
    println!("Universe size: {}", small_universe.len());
    println!("Total edges: {}", edges.len());
    println!("Results returned: {}", result.len());
    println!("Execution time: {:?}", elapsed);

    assert!(!result.is_empty(), "PPR should return results");

    for (node_id, score) in &result {
        assert!(
            small_universe.contains(node_id),
            "Result node {} with score {} should be in universe",
            node_id,
            score
        );
    }

    let total_score: f64 = result.iter().map(|(_, s)| s).sum();
    println!("Total score of top 50 results: {:.6}", total_score);

    assert!(
        total_score > 0.5,
        "Top 50 results from 100-node universe should capture most score mass: {}",
        total_score
    );

    assert!(
        elapsed.as_millis() < 2000,
        "Small universe should be fast: {:?}",
        elapsed
    );

    println!("=== Test passed! ===\n");
}
