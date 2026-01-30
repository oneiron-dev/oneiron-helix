# Oneiron Feature Completeness Report

**Last Updated:** 2026-01-30
**Spec Sources:**
- ONEIRON-ARCH-004-retrieval-graph.md (Retrieval & Graph Engine v6)
- ONEIRON-ARCH-014-ppr-warmcache-v1.md (PPR Warm Cache)
- HELIX-ONEIRON-SPEC-v1.1.1.md (Implementation Spec)

---

## Fully Implemented ✅

### PPR Algorithm (Complete)
- **Location:** `helix-db/src/helix_engine/graph/ppr.rs`
- **Features:**
  - All 13 Oneiron edge types with correct weights
  - Bidirectional traversal (in_edges_db + out_edges_db)
  - Teleport probability: `(1 - damping)` back to seeds
  - part_of max 2 hops (PART_OF_MAX_HOPS constant)
  - Score normalization (optional, default=true)
  - Candidate-set gating (both-endpoints-readable)
  - opposes=0 blocks propagation

### Edge Weights (All 13 Types)
```rust
("belongs_to", 1.0), ("participates_in", 1.0), ("attached", 0.8),
("authored_by", 0.9), ("mentions", 0.6), ("about", 0.5),
("supports", 1.0), ("opposes", 0.0), ("claim_of", 1.0),
("scoped_to", 0.7), ("supersedes", 0.3), ("derived_from", 0.2),
("part_of", 0.8)
```

### Custom Edge Weights in HQL (Complete)
- **Location:** `helix-db/src/grammar.pest`, `helix-db/src/helixc/parser/expression_parse_methods.rs`
- **Syntax:** `PPR<Type>(seeds: ids, universe: ids, weights: { mentions: 0.3, supports: 0.8 })`
- **Features:**
  - Full grammar support for inline weight overrides
  - Weights merged with defaults at runtime
  - 21 parser tests + 7 E2E tests with NetworkX validation

### RRF Fusion Implementation
- **Location:** `helix-db/src/helix_engine/reranker/fusion/rrf.rs`
- `RRFReranker::fuse_lists()` with configurable k parameter (default 60)

### SearchHybrid Operator
- **Location:** `helix-db/src/helixc/generator/source_steps.rs`
- Combines SearchV + SearchBM25 with RRF fusion
- Grammar support with optional prefilter

### PPR HQL Operator
- **Location:** `helix-db/src/helixc/generator/source_steps.rs`
- Syntax: `PPR<Type>(seeds: ids, universe: ids, depth: 2, damping: 0.85, limit: 50)`
- Calls `ppr_with_storage()` with proper storage/txn/arena access

### PPR Warm Cache (Complete - Phase 1 + 2)
- **Phase 1 Location:** `helix-db/src/helix_engine/graph/ppr_cache.rs`
- **Phase 2 Location:** `helix-db/src/helix_engine/graph/ppr_warmup.rs`
- **Phase 1 Features:**
  - `PPRCacheEntry` storage structure with LMDB backend
  - Cache key format: `ppr:{vault_id}:{entity_type}:{entity_id}:{depth}`
  - `ppr_with_cache()` for query-time cache lookup with live fallback
  - `populate_cache_entry()` for cache warming
  - Cache invalidation: `mark_stale()`, `invalidate_for_entity()`
  - `PPRCacheMetrics` with hits/misses/stale_hits tracking
- **Phase 2 Features:**
  - `PPRWarmupJobConfig` with configurable top_k, entity_types, recency_window, depth
  - `run_warmup_job()` to execute warmup with time budget
  - `select_entities_to_warm()` with recency-weighted scoring
  - `check_ttl_expired()` with tiered TTL (24h/72h/168h based on activity)
  - `refresh_stale_entries()` for batch stale entry refresh

### Claim Filtering (Complete)
- **Location:** `helix-db/src/helix_engine/graph/claim_filter.rs`
- **Features:**
  - `ClaimFilterConfig` with configurable filters
  - `approvalStatus IN ("auto", "approved")` filter
  - `lifecycleStatus = "active"` filter
  - `stale = false` filter (auto-inject)
  - `passes_claim_filter()` for individual node checks
  - `filter_universe_by_claims()` for bulk universe filtering
  - `ppr_with_claim_filter()` for integrated PPR + claim filtering
- **Tests:** 19 claim filter tests + 5 PPR integration tests

### Ranking Signal Boosts (Complete)
- **Location:** `helix-db/src/helix_engine/reranker/fusion/signal_boost.rs`
- **Formula:** `Final Score = RRF(...) × salience × recency × confidence`
- **Features:**
  - `SignalBoostConfig` with configurable enable flags and half-life
  - `salience_boost()` - returns salience value or 1.0 if None
  - `confidence_boost()` - returns confidence value or 1.0 if None
  - `recency_boost()` - exponential decay: `0.5^(age_days / half_life_days)`
  - `apply_signal_boosts()` - applies all boosts and re-sorts results
- **Tests:** 14 unit tests + 12 E2E tests with ground truth validation

### Prefilter Grammar Support
- **Location:** `helix-db/src/grammar.pest`
- Enabled for both SearchV and SearchHybrid

---

## Test Coverage Summary

| Feature | Unit Tests | E2E Tests | Total |
|---------|------------|-----------|-------|
| PPR Algorithm | 10 | 19 | 29 |
| PPR Warm Cache | 8 | 7 | 15 |
| PPR Warmup Jobs | 7 | 0 | 7 |
| Claim Filtering | 13 | 5 | 18 |
| Custom Edge Weights | 14 | 7 | 21 |
| Signal Boosts | 14 | 12 | 26 |
| SearchHybrid | 0 | 19 | 19 |
| Stress Tests | 0 | 5 | 5 |
| Large-Scale Graph Tests | 0 | 12 | 12 |
| **Total** | **66** | **86** | **152** |

All tests include NetworkX-validated ground truth verification where applicable.

### Large-Scale Graph Test Coverage

The `ppr_large_scale_tests.rs` module tests PPR on programmatically generated graph topologies:

| Graph Type | Nodes | Edges | Test Focus |
|------------|-------|-------|------------|
| Barabasi-Albert | 500 | 1,494 | Hub node score accumulation |
| Watts-Strogatz | 200 | 400 | Small-world clustering effects |
| Oneiron Hierarchy | 800 | 1,522 | All 13 edge types, hierarchical structure |
| Clustered Graph | 100 | 451 | Cross-cluster propagation (8.69x ratio) |
| Citation DAG | 500 | 2,657 | Directed acyclic traversal |
| Performance (1K) | 1,000 | 3,990 | ~100ms execution time |
| Performance (5K edges) | 1,500 | 5,990 | Scalability verification |
| Weighted BA | 300 | 894 | Random edge type distribution |
| Custom Weights | 500 | 1,494 | Weight override verification |
| Deep Traversal | 300 | 900 | Depth=5 stability |
| Many Seeds | 500 | 1,494 | 50-seed handling |
| Small Universe | 1,000 | 3,990 | Candidate-set gating |

---

## All Features Complete ✅

All features from ONEIRON-ARCH-004 and ONEIRON-ARCH-014 have been implemented.

---

## Files Reference

| Component | Path |
|-----------|------|
| PPR Core | `helix-db/src/helix_engine/graph/ppr.rs` |
| PPR Cache | `helix-db/src/helix_engine/graph/ppr_cache.rs` |
| PPR Warmup | `helix-db/src/helix_engine/graph/ppr_warmup.rs` |
| Claim Filter | `helix-db/src/helix_engine/graph/claim_filter.rs` |
| Signal Boosts | `helix-db/src/helix_engine/reranker/fusion/signal_boost.rs` |
| RRF Reranker | `helix-db/src/helix_engine/reranker/fusion/rrf.rs` |
| Source Steps | `helix-db/src/helixc/generator/source_steps.rs` |
| Grammar | `helix-db/src/grammar.pest` |
| PPR Tests | `helix-db/src/helix_engine/tests/traversal_tests/ppr_tests.rs` |
| PPR Large-Scale Tests | `helix-db/src/helix_engine/tests/ppr_large_scale_tests.rs` |
| Edge Weights E2E | `helix-db/src/helix_engine/tests/edge_weights_e2e_tests.rs` |
| Signal Boost E2E | `helix-db/src/helix_engine/tests/signal_boost_e2e_tests.rs` |
| Hybrid Tests | `helix-db/src/helix_engine/tests/hybrid_search_tests.rs` |

---

## Recent Commits

```
[pending] Add large-scale PPR tests with graph topologies (12 E2E tests)
ae54afa8 Merge upstream/main: sync with HelixDB/helix-db (44 commits)
767e19b3 Implement PPR warm cache Phase 2: background warmup jobs
82526a31 Implement ranking signal boosts with E2E tests
04eb3751 Implement custom edge weights, PPR warm cache, claim filtering
a56893b3 Fix stress test crashes caused by LMDB reader slot issues
```
