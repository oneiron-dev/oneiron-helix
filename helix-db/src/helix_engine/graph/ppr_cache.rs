use crate::helix_engine::{
    graph::ppr::ppr_with_storage, storage_core::HelixGraphStorage, types::GraphError,
};
use heed3::{Database, Env, RoTxn, RwTxn, types::*};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::atomic::{AtomicU64, Ordering},
};

const DB_PPR_CACHE: &str = "ppr_cache";

type Timestamp = u64;
type ScoreVec = Vec<(u128, f64)>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpansionScore {
    pub entity_id: u128,
    pub entity_type: String,
    pub score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PPRCacheEntry {
    pub seed_id: u128,
    pub vault_id: String,
    pub expansion_scores: Vec<ExpansionScore>,
    pub computed_at: Timestamp,
    pub depth: usize,
    pub stale: bool,
    pub stale_reason: Option<StaleReason>,
    pub stale_since: Option<Timestamp>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StaleReason {
    EntityUpdated,
    EdgeAdded,
    EdgeRemoved,
    Expired,
}

#[derive(Debug, Default)]
pub struct PPRCacheMetrics {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub stale_hits: AtomicU64,
}

impl PPRCacheMetrics {
    pub fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            stale_hits: AtomicU64::new(0),
        }
    }

    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_stale_hit(&self) {
        self.stale_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn get_misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    pub fn get_stale_hits(&self) -> u64 {
        self.stale_hits.load(Ordering::Relaxed)
    }

    pub fn hit_rate(&self) -> f64 {
        let hits = self.get_hits();
        let misses = self.get_misses();
        let stale = self.get_stale_hits();
        let total = hits + misses + stale;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    pub fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.stale_hits.store(0, Ordering::Relaxed);
    }
}

pub struct PPRCache {
    pub db: Database<Bytes, Bytes>,
    pub metrics: PPRCacheMetrics,
}

impl PPRCache {
    pub fn new(env: &Env, wtxn: &mut RwTxn) -> Result<Self, GraphError> {
        let db = env
            .database_options()
            .types::<Bytes, Bytes>()
            .name(DB_PPR_CACHE)
            .create(wtxn)?;

        Ok(Self {
            db,
            metrics: PPRCacheMetrics::new(),
        })
    }

    pub fn open(env: &Env, rtxn: &RoTxn) -> Result<Option<Self>, GraphError> {
        let db = env
            .database_options()
            .types::<Bytes, Bytes>()
            .name(DB_PPR_CACHE)
            .open(rtxn)?;

        Ok(db.map(|db| Self {
            db,
            metrics: PPRCacheMetrics::new(),
        }))
    }

    pub fn make_cache_key(
        vault_id: &str,
        entity_type: &str,
        entity_id: u128,
        depth: usize,
    ) -> String {
        format!("ppr:{}:{}:{}:{}", vault_id, entity_type, entity_id, depth)
    }

    pub fn get(
        &self,
        txn: &RoTxn,
        cache_key: &str,
    ) -> Result<Option<PPRCacheEntry>, GraphError> {
        let Some(data) = self.db.get(txn, cache_key.as_bytes())? else {
            return Ok(None);
        };
        let entry: PPRCacheEntry = bincode::deserialize(data)?;
        Ok(Some(entry))
    }

    pub fn set(
        &self,
        txn: &mut RwTxn,
        cache_key: &str,
        entry: &PPRCacheEntry,
    ) -> Result<(), GraphError> {
        let value_bytes = bincode::serialize(entry)?;
        self.db.put(txn, cache_key.as_bytes(), &value_bytes)?;
        Ok(())
    }

    pub fn delete(&self, txn: &mut RwTxn, cache_key: &str) -> Result<bool, GraphError> {
        Ok(self.db.delete(txn, cache_key.as_bytes())?)
    }

    #[inline]
    pub fn get_cached_ppr(
        &self,
        txn: &RoTxn,
        cache_key: &str,
    ) -> Result<Option<PPRCacheEntry>, GraphError> {
        self.get(txn, cache_key)
    }

    #[inline]
    pub fn set_cached_ppr(
        &self,
        txn: &mut RwTxn,
        cache_key: &str,
        entry: &PPRCacheEntry,
    ) -> Result<(), GraphError> {
        self.set(txn, cache_key, entry)
    }

    #[inline]
    pub fn invalidate_cache_entry(
        &self,
        txn: &mut RwTxn,
        cache_key: &str,
    ) -> Result<bool, GraphError> {
        self.delete(txn, cache_key)
    }

    pub fn mark_stale(
        &self,
        txn: &mut RwTxn,
        cache_key: &str,
        reason: StaleReason,
    ) -> Result<bool, GraphError> {
        let Some(data) = self.db.get(txn, cache_key.as_bytes())? else {
            return Ok(false);
        };

        let mut entry: PPRCacheEntry = bincode::deserialize(data)?;
        entry.stale = true;
        entry.stale_reason = Some(reason);
        entry.stale_since = Some(current_timestamp());

        let value_bytes = bincode::serialize(&entry)?;
        self.db.put(txn, cache_key.as_bytes(), &value_bytes)?;
        Ok(true)
    }

    pub fn invalidate_for_entity(
        &self,
        txn: &mut RwTxn,
        vault_id: &str,
        entity_id: u128,
        reason: StaleReason,
    ) -> Result<usize, GraphError> {
        let prefix = format!("ppr:{}:", vault_id);
        let entity_id_str = entity_id.to_string();

        let keys_to_update: Vec<String> = self
            .db
            .prefix_iter(txn, prefix.as_bytes())?
            .filter_map(|result| {
                let (key, _) = result.ok()?;
                let key_str = std::str::from_utf8(key).ok()?;
                if key_str.contains(&entity_id_str) {
                    Some(key_str.to_string())
                } else {
                    None
                }
            })
            .collect();

        let mut invalidated_count = 0;
        for key in keys_to_update {
            if self.mark_stale(txn, &key, reason.clone())? {
                invalidated_count += 1;
            }
        }

        Ok(invalidated_count)
    }

    pub fn clear_all(&self, txn: &mut RwTxn) -> Result<(), GraphError> {
        self.db.clear(txn)?;
        Ok(())
    }
}

pub enum PPRSource {
    Cache,
    Live,
    StaleCacheFallback,
}

pub struct PPRResult {
    pub scores: ScoreVec,
    pub source: PPRSource,
}

#[allow(clippy::too_many_arguments)]
pub fn ppr_with_cache(
    storage: &HelixGraphStorage,
    ppr_cache: &PPRCache,
    txn: &RoTxn,
    arena: &bumpalo::Bump,
    universe_ids: &HashSet<u128>,
    seed_ids: &[u128],
    edge_weights: &HashMap<String, f64>,
    max_depth: usize,
    damping: f64,
    limit: usize,
    normalize: bool,
    vault_id: &str,
    entity_type: &str,
) -> PPRResult {
    let compute_live = || {
        ppr_with_storage(
            storage,
            txn,
            arena,
            universe_ids,
            seed_ids,
            edge_weights,
            max_depth,
            damping,
            limit,
            normalize,
        )
    };

    if seed_ids.len() != 1 {
        ppr_cache.metrics.record_miss();
        return PPRResult {
            scores: compute_live(),
            source: PPRSource::Live,
        };
    }

    let seed_id = seed_ids[0];
    let cache_key = PPRCache::make_cache_key(vault_id, entity_type, seed_id, max_depth);

    match ppr_cache.get(txn, &cache_key) {
        Ok(Some(entry)) if !entry.stale => {
            ppr_cache.metrics.record_hit();
            let mut scores: ScoreVec = entry
                .expansion_scores
                .iter()
                .filter(|es| universe_ids.contains(&es.entity_id))
                .map(|es| (es.entity_id, es.score))
                .collect();
            scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            scores.truncate(limit);
            PPRResult {
                scores,
                source: PPRSource::Cache,
            }
        }
        Ok(Some(_)) => {
            ppr_cache.metrics.record_stale_hit();
            PPRResult {
                scores: compute_live(),
                source: PPRSource::StaleCacheFallback,
            }
        }
        Ok(None) | Err(_) => {
            ppr_cache.metrics.record_miss();
            PPRResult {
                scores: compute_live(),
                source: PPRSource::Live,
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn populate_cache_entry(
    storage: &HelixGraphStorage,
    ppr_cache: &PPRCache,
    wtxn: &mut RwTxn,
    arena: &bumpalo::Bump,
    universe_ids: &HashSet<u128>,
    seed_id: u128,
    edge_weights: &HashMap<String, f64>,
    max_depth: usize,
    damping: f64,
    max_expansion: usize,
    vault_id: &str,
    entity_type: &str,
) -> Result<PPRCacheEntry, GraphError> {
    let rtxn = storage.graph_env.read_txn()?;
    let scores = ppr_with_storage(
        storage,
        &rtxn,
        arena,
        universe_ids,
        &[seed_id],
        edge_weights,
        max_depth,
        damping,
        max_expansion,
        true,
    );
    drop(rtxn);

    let expansion_scores: Vec<ExpansionScore> = scores
        .into_iter()
        .map(|(entity_id, score)| ExpansionScore {
            entity_id,
            entity_type: entity_type.to_string(),
            score,
        })
        .collect();

    let entry = PPRCacheEntry {
        seed_id,
        vault_id: vault_id.to_string(),
        expansion_scores,
        computed_at: current_timestamp(),
        depth: max_depth,
        stale: false,
        stale_reason: None,
        stale_since: None,
    };

    let cache_key = PPRCache::make_cache_key(vault_id, entity_type, seed_id, max_depth);
    ppr_cache.set(wtxn, &cache_key, &entry)?;

    Ok(entry)
}

fn current_timestamp() -> Timestamp {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_cache() -> (TempDir, Env, PPRCache) {
        let temp_dir = TempDir::new().unwrap();
        let env = unsafe {
            heed3::EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024)
                .max_dbs(10)
                .open(temp_dir.path())
                .unwrap()
        };
        let mut wtxn = env.write_txn().unwrap();
        let cache = PPRCache::new(&env, &mut wtxn).unwrap();
        wtxn.commit().unwrap();
        (temp_dir, env, cache)
    }

    #[test]
    fn test_cache_key_format() {
        let key = PPRCache::make_cache_key("vault_abc", "PERSON", 123, 3);
        assert_eq!(key, "ppr:vault_abc:PERSON:123:3");
    }

    #[test]
    fn test_cache_set_and_get() {
        let (_temp_dir, env, cache) = setup_cache();

        let entry = PPRCacheEntry {
            seed_id: 123,
            vault_id: "vault_abc".to_string(),
            expansion_scores: vec![
                ExpansionScore {
                    entity_id: 456,
                    entity_type: "PERSON".to_string(),
                    score: 0.8,
                },
                ExpansionScore {
                    entity_id: 789,
                    entity_type: "DOCUMENT".to_string(),
                    score: 0.5,
                },
            ],
            computed_at: 1234567890,
            depth: 3,
            stale: false,
            stale_reason: None,
            stale_since: None,
        };

        let cache_key = PPRCache::make_cache_key("vault_abc", "PERSON", 123, 3);

        let mut wtxn = env.write_txn().unwrap();
        cache.set(&mut wtxn, &cache_key, &entry).unwrap();
        wtxn.commit().unwrap();

        let rtxn = env.read_txn().unwrap();
        let retrieved = cache.get(&rtxn, &cache_key).unwrap();
        assert!(retrieved.is_some());

        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.seed_id, 123);
        assert_eq!(retrieved.vault_id, "vault_abc");
        assert_eq!(retrieved.expansion_scores.len(), 2);
        assert!(!retrieved.stale);
    }

    #[test]
    fn test_cache_miss() {
        let (_temp_dir, env, cache) = setup_cache();

        let rtxn = env.read_txn().unwrap();
        let result = cache.get(&rtxn, "ppr:nonexistent:PERSON:999:3").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_mark_stale() {
        let (_temp_dir, env, cache) = setup_cache();

        let entry = PPRCacheEntry {
            seed_id: 123,
            vault_id: "vault_abc".to_string(),
            expansion_scores: vec![],
            computed_at: 1234567890,
            depth: 3,
            stale: false,
            stale_reason: None,
            stale_since: None,
        };

        let cache_key = PPRCache::make_cache_key("vault_abc", "PERSON", 123, 3);

        let mut wtxn = env.write_txn().unwrap();
        cache.set(&mut wtxn, &cache_key, &entry).unwrap();
        wtxn.commit().unwrap();

        let mut wtxn = env.write_txn().unwrap();
        let marked = cache
            .mark_stale(&mut wtxn, &cache_key, StaleReason::EntityUpdated)
            .unwrap();
        assert!(marked);
        wtxn.commit().unwrap();

        let rtxn = env.read_txn().unwrap();
        let retrieved = cache.get(&rtxn, &cache_key).unwrap().unwrap();
        assert!(retrieved.stale);
        assert!(matches!(
            retrieved.stale_reason,
            Some(StaleReason::EntityUpdated)
        ));
        assert!(retrieved.stale_since.is_some());
    }

    #[test]
    fn test_delete_cache_entry() {
        let (_temp_dir, env, cache) = setup_cache();

        let entry = PPRCacheEntry {
            seed_id: 123,
            vault_id: "vault_abc".to_string(),
            expansion_scores: vec![],
            computed_at: 1234567890,
            depth: 3,
            stale: false,
            stale_reason: None,
            stale_since: None,
        };

        let cache_key = PPRCache::make_cache_key("vault_abc", "PERSON", 123, 3);

        let mut wtxn = env.write_txn().unwrap();
        cache.set(&mut wtxn, &cache_key, &entry).unwrap();
        wtxn.commit().unwrap();

        let mut wtxn = env.write_txn().unwrap();
        let deleted = cache.delete(&mut wtxn, &cache_key).unwrap();
        assert!(deleted);
        wtxn.commit().unwrap();

        let rtxn = env.read_txn().unwrap();
        let result = cache.get(&rtxn, &cache_key).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_metrics() {
        let metrics = PPRCacheMetrics::new();

        assert_eq!(metrics.get_hits(), 0);
        assert_eq!(metrics.get_misses(), 0);
        assert_eq!(metrics.get_stale_hits(), 0);
        assert_eq!(metrics.hit_rate(), 0.0);

        metrics.record_hit();
        metrics.record_hit();
        metrics.record_miss();
        metrics.record_stale_hit();

        assert_eq!(metrics.get_hits(), 2);
        assert_eq!(metrics.get_misses(), 1);
        assert_eq!(metrics.get_stale_hits(), 1);
        assert_eq!(metrics.hit_rate(), 0.5);

        metrics.reset();
        assert_eq!(metrics.get_hits(), 0);
        assert_eq!(metrics.get_misses(), 0);
        assert_eq!(metrics.get_stale_hits(), 0);
    }

    #[test]
    fn test_clear_all() {
        let (_temp_dir, env, cache) = setup_cache();

        let entry1 = PPRCacheEntry {
            seed_id: 123,
            vault_id: "vault_abc".to_string(),
            expansion_scores: vec![],
            computed_at: 1234567890,
            depth: 3,
            stale: false,
            stale_reason: None,
            stale_since: None,
        };

        let entry2 = PPRCacheEntry {
            seed_id: 456,
            vault_id: "vault_abc".to_string(),
            expansion_scores: vec![],
            computed_at: 1234567890,
            depth: 3,
            stale: false,
            stale_reason: None,
            stale_since: None,
        };

        let cache_key1 = PPRCache::make_cache_key("vault_abc", "PERSON", 123, 3);
        let cache_key2 = PPRCache::make_cache_key("vault_abc", "PERSON", 456, 3);

        let mut wtxn = env.write_txn().unwrap();
        cache.set(&mut wtxn, &cache_key1, &entry1).unwrap();
        cache.set(&mut wtxn, &cache_key2, &entry2).unwrap();
        wtxn.commit().unwrap();

        let mut wtxn = env.write_txn().unwrap();
        cache.clear_all(&mut wtxn).unwrap();
        wtxn.commit().unwrap();

        let rtxn = env.read_txn().unwrap();
        assert!(cache.get(&rtxn, &cache_key1).unwrap().is_none());
        assert!(cache.get(&rtxn, &cache_key2).unwrap().is_none());
    }
}
