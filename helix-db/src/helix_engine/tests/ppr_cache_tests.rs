use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bumpalo::Bump;
use tempfile::TempDir;

use crate::{
    helix_engine::{
        graph::ppr_cache::{
            PPRCache, PPRSource, StaleReason, populate_cache_entry, ppr_with_cache,
        },
        storage_core::HelixGraphStorage,
        traversal_core::ops::{
            g::G,
            source::{add_e::AddEAdapter, add_n::AddNAdapter},
        },
    },
    props,
};

use super::traversal_tests::test_utils::props_option;

fn setup_test_db_with_cache() -> (TempDir, Arc<HelixGraphStorage>, PPRCache) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let storage = HelixGraphStorage::new(
        db_path,
        crate::helix_engine::traversal_core::config::Config::default(),
        Default::default(),
    )
    .unwrap();

    let mut wtxn = storage.graph_env.write_txn().unwrap();
    let cache = PPRCache::new(&storage.graph_env, &mut wtxn).unwrap();
    wtxn.commit().unwrap();

    (temp_dir, Arc::new(storage), cache)
}

#[test]
fn test_cache_miss_computes_ppr() {
    let (_temp_dir, storage, cache) = setup_test_db_with_cache();
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

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("belongs_to", None, alice, bob, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [alice, bob].into_iter().collect();
    let seeds = vec![alice];
    let edge_weights = HashMap::new();

    assert_eq!(cache.metrics.get_misses(), 0);

    let result = ppr_with_cache(
        &storage,
        &cache,
        &txn,
        &arena,
        &universe,
        &seeds,
        &edge_weights,
        3,
        0.85,
        10,
        true,
        "vault_test",
        "person",
    );

    assert!(matches!(result.source, PPRSource::Live));
    assert!(!result.scores.is_empty());
    assert_eq!(cache.metrics.get_misses(), 1);
    assert_eq!(cache.metrics.get_hits(), 0);

    let alice_score = result.scores.iter().find(|(id, _)| *id == alice);
    assert!(alice_score.is_some(), "Alice (seed) should have a score");

    let bob_score = result.scores.iter().find(|(id, _)| *id == bob);
    assert!(bob_score.is_some(), "Bob (connected) should have a score");
}

#[test]
fn test_cache_hit_returns_cached() {
    let (_temp_dir, storage, cache) = setup_test_db_with_cache();
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

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("belongs_to", None, alice, bob, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let universe: HashSet<u128> = [alice, bob].into_iter().collect();
    let edge_weights = HashMap::new();

    let mut wtxn = storage.graph_env.write_txn().unwrap();
    let entry = populate_cache_entry(
        &storage,
        &cache,
        &mut wtxn,
        &arena,
        &universe,
        alice,
        &edge_weights,
        3,
        0.85,
        100,
        "vault_test",
        "person",
    )
    .unwrap();
    wtxn.commit().unwrap();

    assert_eq!(entry.seed_id, alice);
    assert!(!entry.stale);

    let arena2 = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    cache.metrics.reset();

    let result = ppr_with_cache(
        &storage,
        &cache,
        &txn,
        &arena2,
        &universe,
        &[alice],
        &edge_weights,
        3,
        0.85,
        10,
        true,
        "vault_test",
        "person",
    );

    assert!(matches!(result.source, PPRSource::Cache));
    assert_eq!(cache.metrics.get_hits(), 1);
    assert_eq!(cache.metrics.get_misses(), 0);
    assert!(!result.scores.is_empty());
}

#[test]
fn test_cache_invalidation() {
    let (_temp_dir, storage, cache) = setup_test_db_with_cache();
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

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("belongs_to", None, alice, bob, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let universe: HashSet<u128> = [alice, bob].into_iter().collect();
    let edge_weights = HashMap::new();

    let mut wtxn = storage.graph_env.write_txn().unwrap();
    populate_cache_entry(
        &storage,
        &cache,
        &mut wtxn,
        &arena,
        &universe,
        alice,
        &edge_weights,
        3,
        0.85,
        100,
        "vault_test",
        "person",
    )
    .unwrap();
    wtxn.commit().unwrap();

    let cache_key = PPRCache::make_cache_key("vault_test", "person", alice, 3);
    let rtxn = storage.graph_env.read_txn().unwrap();
    let entry = cache.get_cached_ppr(&rtxn, &cache_key).unwrap();
    assert!(entry.is_some());
    assert!(!entry.unwrap().stale);
    drop(rtxn);

    let mut wtxn = storage.graph_env.write_txn().unwrap();
    let deleted = cache.invalidate_cache_entry(&mut wtxn, &cache_key).unwrap();
    assert!(deleted);
    wtxn.commit().unwrap();

    let rtxn = storage.graph_env.read_txn().unwrap();
    let entry = cache.get_cached_ppr(&rtxn, &cache_key).unwrap();
    assert!(entry.is_none());
}

#[test]
fn test_stale_cache_fallback() {
    let (_temp_dir, storage, cache) = setup_test_db_with_cache();
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

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("belongs_to", None, alice, bob, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let universe: HashSet<u128> = [alice, bob].into_iter().collect();
    let edge_weights = HashMap::new();

    let mut wtxn = storage.graph_env.write_txn().unwrap();
    populate_cache_entry(
        &storage,
        &cache,
        &mut wtxn,
        &arena,
        &universe,
        alice,
        &edge_weights,
        3,
        0.85,
        100,
        "vault_test",
        "person",
    )
    .unwrap();
    wtxn.commit().unwrap();

    let cache_key = PPRCache::make_cache_key("vault_test", "person", alice, 3);
    let mut wtxn = storage.graph_env.write_txn().unwrap();
    cache
        .mark_stale(&mut wtxn, &cache_key, StaleReason::EntityUpdated)
        .unwrap();
    wtxn.commit().unwrap();

    cache.metrics.reset();

    let arena2 = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let result = ppr_with_cache(
        &storage,
        &cache,
        &txn,
        &arena2,
        &universe,
        &[alice],
        &edge_weights,
        3,
        0.85,
        10,
        true,
        "vault_test",
        "person",
    );

    assert!(matches!(result.source, PPRSource::StaleCacheFallback));
    assert_eq!(cache.metrics.get_stale_hits(), 1);
    assert_eq!(cache.metrics.get_hits(), 0);
    assert_eq!(cache.metrics.get_misses(), 0);
    assert!(!result.scores.is_empty());
}

#[test]
fn test_multiple_seeds_bypass_cache() {
    let (_temp_dir, storage, cache) = setup_test_db_with_cache();
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

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let universe: HashSet<u128> = [alice, bob].into_iter().collect();
    let edge_weights = HashMap::new();

    let result = ppr_with_cache(
        &storage,
        &cache,
        &txn,
        &arena,
        &universe,
        &[alice, bob],
        &edge_weights,
        3,
        0.85,
        10,
        true,
        "vault_test",
        "person",
    );

    assert!(matches!(result.source, PPRSource::Live));
    assert_eq!(cache.metrics.get_misses(), 1);
}

#[test]
fn test_cache_hit_rate_calculation() {
    let (_temp_dir, _storage, cache) = setup_test_db_with_cache();

    assert_eq!(cache.metrics.hit_rate(), 0.0);

    cache.metrics.record_hit();
    cache.metrics.record_hit();
    cache.metrics.record_hit();
    cache.metrics.record_miss();

    assert_eq!(cache.metrics.hit_rate(), 0.75);

    cache.metrics.record_stale_hit();

    assert_eq!(cache.metrics.hit_rate(), 0.6);
}

#[test]
fn test_invalidate_for_entity() {
    let (_temp_dir, storage, cache) = setup_test_db_with_cache();
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

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("belongs_to", None, alice, bob, false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let universe: HashSet<u128> = [alice, bob].into_iter().collect();
    let edge_weights = HashMap::new();

    let mut wtxn = storage.graph_env.write_txn().unwrap();
    populate_cache_entry(
        &storage,
        &cache,
        &mut wtxn,
        &arena,
        &universe,
        alice,
        &edge_weights,
        3,
        0.85,
        100,
        "vault_test",
        "person",
    )
    .unwrap();
    wtxn.commit().unwrap();

    let mut wtxn = storage.graph_env.write_txn().unwrap();
    let invalidated = cache
        .invalidate_for_entity(&mut wtxn, "vault_test", alice, StaleReason::EntityUpdated)
        .unwrap();
    wtxn.commit().unwrap();

    assert!(invalidated >= 1);

    let cache_key = PPRCache::make_cache_key("vault_test", "person", alice, 3);
    let rtxn = storage.graph_env.read_txn().unwrap();
    let entry = cache.get_cached_ppr(&rtxn, &cache_key).unwrap();
    assert!(entry.is_some());
    assert!(entry.unwrap().stale);
}
