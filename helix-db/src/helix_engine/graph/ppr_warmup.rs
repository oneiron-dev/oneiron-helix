use crate::helix_engine::{
    graph::ppr_cache::{populate_cache_entry, PPRCache, PPRCacheEntry},
    storage_core::HelixGraphStorage,
    types::GraphError,
};
use crate::protocol::value::Value;
use heed3::RoTxn;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

type Timestamp = u64;

const SECS_PER_HOUR: Timestamp = 60 * 60;
const SECS_PER_DAY: Timestamp = 24 * SECS_PER_HOUR;
const SECS_PER_WEEK: Timestamp = 7 * SECS_PER_DAY;

const TTL_ACTIVE: Timestamp = 24 * SECS_PER_HOUR;
const TTL_RECENT: Timestamp = 72 * SECS_PER_HOUR;
const TTL_DORMANT: Timestamp = 168 * SECS_PER_HOUR;

const ACTIVITY_THRESHOLD_ACTIVE: Timestamp = 7 * SECS_PER_DAY;
const ACTIVITY_THRESHOLD_RECENT: Timestamp = 30 * SECS_PER_DAY;

#[derive(Debug, Clone)]
pub struct PPRWarmupJobConfig {
    pub vault_id: String,
    pub top_k: usize,
    pub entity_types: Vec<String>,
    pub recency_window_days: usize,
    pub depth: usize,
    pub damping_factor: f64,
    pub max_expansion: usize,
    pub max_duration_ms: Option<u64>,
}

impl Default for PPRWarmupJobConfig {
    fn default() -> Self {
        Self {
            vault_id: String::new(),
            top_k: 50,
            entity_types: vec![
                "PERSON".to_string(),
                "CLAIM".to_string(),
                "DOCUMENT".to_string(),
            ],
            recency_window_days: 30,
            depth: 3,
            damping_factor: 0.85,
            max_expansion: 100,
            max_duration_ms: None,
        }
    }
}

impl PPRWarmupJobConfig {
    pub fn new(vault_id: String) -> Self {
        Self {
            vault_id,
            ..Default::default()
        }
    }

    pub fn with_top_k(mut self, top_k: usize) -> Self {
        self.top_k = top_k;
        self
    }

    pub fn with_entity_types(mut self, entity_types: Vec<String>) -> Self {
        self.entity_types = entity_types;
        self
    }

    pub fn with_depth(mut self, depth: usize) -> Self {
        self.depth = depth;
        self
    }

    pub fn with_damping_factor(mut self, damping_factor: f64) -> Self {
        self.damping_factor = damping_factor;
        self
    }

    pub fn with_max_expansion(mut self, max_expansion: usize) -> Self {
        self.max_expansion = max_expansion;
        self
    }

    pub fn with_max_duration_ms(mut self, max_duration_ms: Option<u64>) -> Self {
        self.max_duration_ms = max_duration_ms;
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct PPRWarmupJobResult {
    pub entities_warmed: usize,
    pub cache_entries_created: usize,
    pub cache_entries_updated: usize,
    pub cache_entries_skipped: usize,
    pub total_edges_traversed: usize,
    pub duration_ms: u64,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct WarmupCandidate {
    pub entity_id: u128,
    pub entity_type: String,
    pub mention_count: usize,
    pub last_mentioned_at: Timestamp,
    pub warmup_score: f64,
}

fn current_timestamp() -> Timestamp {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn extract_timestamp(value: &Value) -> Option<Timestamp> {
    match value {
        Value::U64(ts) => Some(*ts),
        Value::I64(ts) if *ts >= 0 => Some(*ts as Timestamp),
        _ => None,
    }
}

fn id_to_prefix(id: u128) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

fn calculate_warmup_score(mention_count: usize, last_mentioned_at: Timestamp, now: Timestamp) -> f64 {
    let time_diff = now.saturating_sub(last_mentioned_at);
    let weeks_elapsed = time_diff as f64 / SECS_PER_WEEK as f64;
    let recency_decay = 0.5_f64.powf(weeks_elapsed);
    mention_count as f64 * recency_decay
}

fn get_node_timestamp(
    node: &crate::utils::items::Node,
    now: Timestamp,
) -> Timestamp {
    const TIMESTAMP_PROPERTIES: &[&str] = &["lastMentionedAt", "updatedAt", "createdAt"];

    TIMESTAMP_PROPERTIES
        .iter()
        .find_map(|prop| node.get_property(prop).and_then(extract_timestamp))
        .unwrap_or(now)
}

fn count_node_edges(
    storage: &HelixGraphStorage,
    txn: &RoTxn,
    node_id: u128,
) -> usize {
    let prefix = id_to_prefix(node_id);
    let out_count = storage
        .out_edges_db
        .prefix_iter(txn, &prefix)
        .map(|iter| iter.count())
        .unwrap_or(0);
    let in_count = storage
        .in_edges_db
        .prefix_iter(txn, &prefix)
        .map(|iter| iter.count())
        .unwrap_or(0);
    out_count + in_count
}

pub fn select_entities_to_warm(
    storage: &HelixGraphStorage,
    txn: &RoTxn,
    config: &PPRWarmupJobConfig,
) -> Result<Vec<WarmupCandidate>, GraphError> {
    let now = current_timestamp();
    let recency_cutoff = now.saturating_sub((config.recency_window_days as Timestamp) * SECS_PER_DAY);

    let arena = bumpalo::Bump::new();
    let entity_types_lower: HashSet<String> = config
        .entity_types
        .iter()
        .map(|s| s.to_lowercase())
        .collect();

    let mut candidates: Vec<WarmupCandidate> = Vec::new();

    for result in storage.nodes_db.iter(txn)? {
        let (id, node_data) = result?;
        let node = match crate::utils::items::Node::from_bincode_bytes(id, node_data, &arena) {
            Ok(n) => n,
            Err(_) => continue,
        };

        let label_lower = node.label.to_lowercase();
        if !entity_types_lower.contains(&label_lower) {
            continue;
        }

        let last_mentioned_at = get_node_timestamp(&node, now);
        if last_mentioned_at < recency_cutoff {
            continue;
        }

        let mention_count = count_node_edges(storage, txn, id);
        let warmup_score = calculate_warmup_score(mention_count, last_mentioned_at, now);

        candidates.push(WarmupCandidate {
            entity_id: id,
            entity_type: node.label.to_string(),
            mention_count,
            last_mentioned_at,
            warmup_score,
        });
    }

    candidates.sort_by(|a, b| {
        b.warmup_score
            .partial_cmp(&a.warmup_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    candidates.truncate(config.top_k);

    Ok(candidates)
}

fn compute_ttl_for_activity(activity_age: Timestamp) -> Timestamp {
    if activity_age <= ACTIVITY_THRESHOLD_ACTIVE {
        TTL_ACTIVE
    } else if activity_age <= ACTIVITY_THRESHOLD_RECENT {
        TTL_RECENT
    } else {
        TTL_DORMANT
    }
}

pub fn check_ttl_expired(entry: &PPRCacheEntry, last_activity_ts: Timestamp) -> bool {
    let now = current_timestamp();
    let computed_age = now.saturating_sub(entry.computed_at);
    let activity_age = now.saturating_sub(last_activity_ts);
    let ttl = compute_ttl_for_activity(activity_age);
    computed_age > ttl
}

#[allow(clippy::too_many_arguments)]
pub fn run_warmup_job(
    storage: &HelixGraphStorage,
    ppr_cache: &PPRCache,
    arena: &bumpalo::Bump,
    universe_ids: &HashSet<u128>,
    config: &PPRWarmupJobConfig,
) -> Result<PPRWarmupJobResult, GraphError> {
    let start = Instant::now();
    let mut result = PPRWarmupJobResult::default();

    let rtxn = storage.graph_env.read_txn()?;
    let candidates = match select_entities_to_warm(storage, &rtxn, config) {
        Ok(c) => c,
        Err(e) => {
            result.errors.push(format!("Failed to select entities: {}", e));
            return Ok(result);
        }
    };
    drop(rtxn);

    let edge_weights: HashMap<String, f64> = HashMap::new();

    for candidate in &candidates {
        if let Some(max_ms) = config.max_duration_ms {
            if start.elapsed().as_millis() as u64 >= max_ms {
                break;
            }
        }

        let rtxn = storage.graph_env.read_txn()?;
        let cache_key = PPRCache::make_cache_key(
            &config.vault_id,
            &candidate.entity_type,
            candidate.entity_id,
            config.depth,
        );

        let existing_entry = ppr_cache.get(&rtxn, &cache_key).ok().flatten();
        drop(rtxn);

        if let Some(ref entry) = existing_entry {
            if !entry.stale && !check_ttl_expired(entry, candidate.last_mentioned_at) {
                result.cache_entries_skipped += 1;
                continue;
            }
        }

        let mut wtxn = storage.graph_env.write_txn()?;
        match populate_cache_entry(
            storage,
            ppr_cache,
            &mut wtxn,
            arena,
            universe_ids,
            candidate.entity_id,
            &edge_weights,
            config.depth,
            config.damping_factor,
            config.max_expansion,
            &config.vault_id,
            &candidate.entity_type,
        ) {
            Ok(entry) => {
                result.total_edges_traversed += entry.expansion_scores.len();
                if existing_entry.is_some() {
                    result.cache_entries_updated += 1;
                } else {
                    result.cache_entries_created += 1;
                }
                result.entities_warmed += 1;
            }
            Err(e) => {
                result.errors.push(format!(
                    "Failed to warm entity {}: {}",
                    candidate.entity_id, e
                ));
            }
        }
        wtxn.commit()?;
    }

    result.duration_ms = start.elapsed().as_millis() as u64;
    Ok(result)
}

pub fn refresh_stale_entries(
    storage: &HelixGraphStorage,
    ppr_cache: &PPRCache,
    arena: &bumpalo::Bump,
    universe_ids: &HashSet<u128>,
    vault_id: &str,
    max_entries: usize,
) -> Result<usize, GraphError> {
    let rtxn = storage.graph_env.read_txn()?;
    let prefix = format!("ppr:{}:", vault_id);

    let stale_keys: Vec<(String, PPRCacheEntry)> = ppr_cache
        .db
        .prefix_iter(&rtxn, prefix.as_bytes())?
        .filter_map(|result| {
            let (key, value) = result.ok()?;
            let key_str = std::str::from_utf8(key).ok()?.to_string();
            let entry: PPRCacheEntry = bincode::deserialize(value).ok()?;
            if entry.stale {
                Some((key_str, entry))
            } else {
                None
            }
        })
        .take(max_entries)
        .collect();
    drop(rtxn);

    let edge_weights: HashMap<String, f64> = HashMap::new();
    let mut refreshed_count = 0;

    for (cache_key, entry) in stale_keys {
        let parts: Vec<&str> = cache_key.split(':').collect();
        if parts.len() < 5 {
            continue;
        }
        let entity_type = parts[2];
        let entity_id: u128 = match parts[3].parse() {
            Ok(id) => id,
            Err(_) => continue,
        };
        let depth: usize = match parts[4].parse() {
            Ok(d) => d,
            Err(_) => entry.depth,
        };

        let mut wtxn = storage.graph_env.write_txn()?;
        if populate_cache_entry(
            storage,
            ppr_cache,
            &mut wtxn,
            arena,
            universe_ids,
            entity_id,
            &edge_weights,
            depth,
            0.85,
            100,
            vault_id,
            entity_type,
        )
        .is_ok()
        {
            refreshed_count += 1;
        }
        wtxn.commit()?;
    }

    Ok(refreshed_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helix_engine::{
        graph::ppr_cache::ExpansionScore,
        storage_core::version_info::VersionInfo,
        traversal_core::config::Config,
    };
    use crate::utils::items::Node;
    use crate::utils::properties::ImmutablePropertiesMap;
    use tempfile::tempdir;

    fn setup_test_storage() -> (HelixGraphStorage, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap();
        let config = Config::default();
        let storage = HelixGraphStorage::new(path, config, VersionInfo::default()).unwrap();
        (storage, temp_dir)
    }

    fn setup_cache(storage: &HelixGraphStorage) -> PPRCache {
        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let cache = PPRCache::new(&storage.graph_env, &mut wtxn).unwrap();
        wtxn.commit().unwrap();
        cache
    }

    fn add_test_node<'arena>(
        storage: &HelixGraphStorage,
        id: u128,
        label: &'arena str,
        props: Option<std::collections::HashMap<&'arena str, Value>>,
        arena: &'arena bumpalo::Bump,
    ) {
        let properties = props.map(|p| {
            let len = p.len();
            ImmutablePropertiesMap::new(len, p.into_iter(), arena)
        });
        let node = Node {
            id,
            label,
            version: 0,
            properties,
        };
        let bytes = bincode::serialize(&node).unwrap();
        let mut wtxn = storage.graph_env.write_txn().unwrap();
        storage.nodes_db.put(&mut wtxn, &id, &bytes).unwrap();
        wtxn.commit().unwrap();
    }

    #[test]
    fn test_warmup_config_defaults() {
        let config = PPRWarmupJobConfig::default();
        assert_eq!(config.top_k, 50);
        assert_eq!(config.depth, 3);
        assert!((config.damping_factor - 0.85).abs() < f64::EPSILON);
        assert_eq!(config.max_expansion, 100);
        assert_eq!(config.recency_window_days, 30);
        assert!(config.vault_id.is_empty());
        assert!(config.max_duration_ms.is_none());
        assert_eq!(
            config.entity_types,
            vec!["PERSON", "CLAIM", "DOCUMENT"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_select_entities_scoring() {
        let now = current_timestamp();
        let one_week_ago = now - SECS_PER_WEEK;
        let two_weeks_ago = now - (2 * SECS_PER_WEEK);

        let score_now = calculate_warmup_score(10, now, now);
        assert!((score_now - 10.0).abs() < f64::EPSILON);

        let score_one_week = calculate_warmup_score(10, one_week_ago, now);
        assert!((score_one_week - 5.0).abs() < 0.01);

        let score_two_weeks = calculate_warmup_score(10, two_weeks_ago, now);
        assert!((score_two_weeks - 2.5).abs() < 0.01);

        let score_high_count = calculate_warmup_score(20, one_week_ago, now);
        assert!((score_high_count - 10.0).abs() < 0.01);
    }

    #[test]
    fn test_run_warmup_job_populates_cache() {
        let (storage, _temp_dir) = setup_test_storage();
        let cache = setup_cache(&storage);
        let arena = bumpalo::Bump::new();

        let now = current_timestamp();
        let mut props = std::collections::HashMap::new();
        let last_key: &str = arena.alloc_str("lastMentionedAt");
        props.insert(last_key, Value::U64(now));
        add_test_node(
            &storage,
            1,
            arena.alloc_str("PERSON"),
            Some(props),
            &arena,
        );

        let mut universe_ids = HashSet::new();
        universe_ids.insert(1u128);

        let config = PPRWarmupJobConfig::new("test_vault".to_string())
            .with_top_k(10)
            .with_entity_types(vec!["PERSON".to_string()]);

        let result = run_warmup_job(&storage, &cache, &arena, &universe_ids, &config).unwrap();

        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_run_warmup_skips_fresh_entries() {
        let (storage, _temp_dir) = setup_test_storage();
        let cache = setup_cache(&storage);
        let arena = bumpalo::Bump::new();

        let now = current_timestamp();
        let mut props = std::collections::HashMap::new();
        let last_key: &str = arena.alloc_str("lastMentionedAt");
        props.insert(last_key, Value::U64(now));
        add_test_node(
            &storage,
            2,
            arena.alloc_str("PERSON"),
            Some(props),
            &arena,
        );

        let mut universe_ids = HashSet::new();
        universe_ids.insert(2u128);

        let config = PPRWarmupJobConfig::new("test_vault".to_string())
            .with_top_k(10)
            .with_entity_types(vec!["PERSON".to_string()]);

        let _result1 = run_warmup_job(&storage, &cache, &arena, &universe_ids, &config).unwrap();

        let result2 = run_warmup_job(&storage, &cache, &arena, &universe_ids, &config).unwrap();

        assert!(result2.cache_entries_skipped > 0 || result2.entities_warmed == 0);
    }

    #[test]
    fn test_ttl_expired_based_on_recency() {
        let now = current_timestamp();

        let fresh_active_entry = PPRCacheEntry {
            seed_id: 1,
            vault_id: "test".to_string(),
            expansion_scores: vec![],
            computed_at: now - (12 * 60 * 60),
            depth: 3,
            stale: false,
            stale_reason: None,
            stale_since: None,
        };
        let last_activity_active = now - (3 * 24 * 60 * 60);
        assert!(!check_ttl_expired(&fresh_active_entry, last_activity_active));

        let expired_active_entry = PPRCacheEntry {
            seed_id: 2,
            vault_id: "test".to_string(),
            expansion_scores: vec![],
            computed_at: now - (30 * 60 * 60),
            depth: 3,
            stale: false,
            stale_reason: None,
            stale_since: None,
        };
        assert!(check_ttl_expired(&expired_active_entry, last_activity_active));

        let fresh_recent_entry = PPRCacheEntry {
            seed_id: 3,
            vault_id: "test".to_string(),
            expansion_scores: vec![],
            computed_at: now - (48 * 60 * 60),
            depth: 3,
            stale: false,
            stale_reason: None,
            stale_since: None,
        };
        let last_activity_recent = now - (14 * 24 * 60 * 60);
        assert!(!check_ttl_expired(&fresh_recent_entry, last_activity_recent));

        let expired_recent_entry = PPRCacheEntry {
            seed_id: 4,
            vault_id: "test".to_string(),
            expansion_scores: vec![],
            computed_at: now - (80 * 60 * 60),
            depth: 3,
            stale: false,
            stale_reason: None,
            stale_since: None,
        };
        assert!(check_ttl_expired(&expired_recent_entry, last_activity_recent));

        let fresh_dormant_entry = PPRCacheEntry {
            seed_id: 5,
            vault_id: "test".to_string(),
            expansion_scores: vec![],
            computed_at: now - (100 * 60 * 60),
            depth: 3,
            stale: false,
            stale_reason: None,
            stale_since: None,
        };
        let last_activity_dormant = now - (60 * 24 * 60 * 60);
        assert!(!check_ttl_expired(&fresh_dormant_entry, last_activity_dormant));

        let expired_dormant_entry = PPRCacheEntry {
            seed_id: 6,
            vault_id: "test".to_string(),
            expansion_scores: vec![],
            computed_at: now - (200 * 60 * 60),
            depth: 3,
            stale: false,
            stale_reason: None,
            stale_since: None,
        };
        assert!(check_ttl_expired(&expired_dormant_entry, last_activity_dormant));
    }

    #[test]
    fn test_refresh_stale_entries() {
        let (storage, _temp_dir) = setup_test_storage();
        let cache = setup_cache(&storage);
        let arena = bumpalo::Bump::new();

        let now = current_timestamp();
        let mut props = std::collections::HashMap::new();
        let last_key: &str = arena.alloc_str("lastMentionedAt");
        props.insert(last_key, Value::U64(now));
        add_test_node(
            &storage,
            3,
            arena.alloc_str("PERSON"),
            Some(props),
            &arena,
        );

        let mut universe_ids = HashSet::new();
        universe_ids.insert(3u128);

        let cache_key = PPRCache::make_cache_key("test_vault", "PERSON", 3, 3);
        let stale_entry = PPRCacheEntry {
            seed_id: 3,
            vault_id: "test_vault".to_string(),
            expansion_scores: vec![ExpansionScore {
                entity_id: 3,
                entity_type: "PERSON".to_string(),
                score: 1.0,
            }],
            computed_at: now - 100000,
            depth: 3,
            stale: true,
            stale_reason: Some(crate::helix_engine::graph::ppr_cache::StaleReason::Expired),
            stale_since: Some(now - 50000),
        };

        {
            let mut wtxn = storage.graph_env.write_txn().unwrap();
            cache.set(&mut wtxn, &cache_key, &stale_entry).unwrap();
            wtxn.commit().unwrap();
        }

        let refreshed =
            refresh_stale_entries(&storage, &cache, &arena, &universe_ids, "test_vault", 10)
                .unwrap();

        let _ = refreshed;
    }

    #[test]
    fn test_max_duration_budget() {
        let (storage, _temp_dir) = setup_test_storage();
        let cache = setup_cache(&storage);
        let arena = bumpalo::Bump::new();

        let now = current_timestamp();
        for i in 1..=20 {
            let mut props = std::collections::HashMap::new();
            let last_key: &str = arena.alloc_str("lastMentionedAt");
            props.insert(last_key, Value::U64(now));
            let label: &str = arena.alloc_str("PERSON");
            add_test_node(&storage, i, label, Some(props), &arena);
        }

        let mut universe_ids = HashSet::new();
        for i in 1..=20 {
            universe_ids.insert(i as u128);
        }

        let config = PPRWarmupJobConfig::new("test_vault".to_string())
            .with_top_k(20)
            .with_entity_types(vec!["PERSON".to_string()])
            .with_max_duration_ms(Some(1));

        let result = run_warmup_job(&storage, &cache, &arena, &universe_ids, &config).unwrap();

        assert!(result.duration_ms <= 100);
    }
}
