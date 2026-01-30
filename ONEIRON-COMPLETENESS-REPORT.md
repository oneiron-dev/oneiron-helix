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
  - 21 parser tests validating all weight patterns

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

### PPR Warm Cache (Phase 1 Complete)
- **Location:** `helix-db/src/helix_engine/graph/ppr_cache.rs`
- **Features:**
  - `PPRCacheEntry` storage structure with LMDB backend
  - Cache key format: `ppr:{vault_id}:{entity_type}:{entity_id}:{depth}`
  - `ppr_with_cache()` for query-time cache lookup with live fallback
  - `populate_cache_entry()` for cache warming
  - Cache invalidation: `mark_stale()`, `invalidate_for_entity()`
  - `PPRCacheMetrics` with hits/misses/stale_hits tracking
  - Type aliases and simplified serialization
- **Remaining for Phase 2:**
  - Background `ppr_warmup` job scheduler

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

### Prefilter Grammar Support
- **Location:** `helix-db/src/grammar.pest`
- Enabled for both SearchV and SearchHybrid

### Test Coverage
- 54 PPR tests (unit + integration + claim filter)
- 19 hybrid search tests
- 19 claim filter unit tests
- 5 stress tests (all passing)
- NetworkX-validated normalization

---

## Not Implemented ❌

### Ranking Signal Boosts
- **Priority:** Medium
- **Spec:** ONEIRON-ARCH-004 Section 4.2
- **Formula:** `Final Score = RRF(...) × salience × recency × confidence`
- **What's needed:**
  - Post-RRF score adjustment layer
  - Configurable boost functions

### PPR Warm Cache Phase 2
- **Priority:** Low (Phase 1 complete)
- **What's needed:**
  - Background scheduler for nightly `ppr_warmup` job
  - Automatic cache warming on startup

---

## Files Reference

| Component | Path |
|-----------|------|
| PPR Core | `helix-db/src/helix_engine/graph/ppr.rs` |
| PPR Cache | `helix-db/src/helix_engine/graph/ppr_cache.rs` |
| Claim Filter | `helix-db/src/helix_engine/graph/claim_filter.rs` |
| RRF Reranker | `helix-db/src/helix_engine/reranker/fusion/rrf.rs` |
| Source Steps | `helix-db/src/helixc/generator/source_steps.rs` |
| Grammar | `helix-db/src/grammar.pest` |
| PPR Tests | `helix-db/src/helix_engine/tests/traversal_tests/ppr_tests.rs` |
| Claim Tests | `helix-db/src/helix_engine/graph/claim_filter.rs` (inline) |
| Hybrid Tests | `helix-db/src/helix_engine/tests/hybrid_search_tests.rs` |

---

## Recent Commits

```
[pending] Implement custom edge weights, PPR warm cache, claim filtering
a56893b3 Fix stress test crashes caused by LMDB reader slot issues
abcbe2b1 Simplify PPR and source_steps code
b0c8d022 Change PPR default to normalize=true
9706d0a6 Add optional normalization to PPR scores
bf0735a4 Implement PPR enhancements: bidirectional, part_of limits, teleport
255e611b Add comprehensive E2E tests for PPR and SearchHybrid
```
