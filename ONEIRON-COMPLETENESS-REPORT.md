# Oneiron Feature Completeness Report

**Generated:** 2026-01-29
**Spec Sources:**
- ONEIRON-ARCH-004-retrieval-graph.md (Retrieval & Graph Engine v6)
- ONEIRON-ARCH-014-ppr-warmcache-v1.md (PPR Warm Cache)
- HELIX-ONEIRON-SPEC-v1.1.1.md (Implementation Spec)

---

## Fully Implemented ✅

### PPR Edge Weights (All 13 Edge Types)
- **Location:** `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/helix_engine/graph/ppr.rs:6-20`
- **Status:** All 13 Oneiron edge types with correct weights as per spec:
  ```rust
  ("belongs_to", 1.0), ("participates_in", 1.0), ("attached", 0.8),
  ("authored_by", 0.9), ("mentions", 0.6), ("about", 0.5),
  ("supports", 1.0), ("opposes", 0.0), ("claim_of", 1.0),
  ("scoped_to", 0.7), ("supersedes", 0.3), ("derived_from", 0.2),
  ("part_of", 0.8)
  ```

### opposes=0 Blocking Propagation
- **Location:** `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/helix_engine/graph/ppr.rs:233-235`
- **Status:** Correctly implemented - edges with weight <= 0.0 are skipped:
  ```rust
  if weight <= 0.0 {
      continue;
  }
  ```

### Both-Endpoints-Readable Constraint (Candidate-Set Gating)
- **Location:** `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/helix_engine/graph/ppr.rs:221-223`
- **Status:** Fully implemented in `ppr_with_storage()`:
  ```rust
  if !universe_ids.contains(&target_node) {
      continue;
  }
  ```
- Seeds are also validated against universe: line 191-194

### RRF Fusion Implementation
- **Location:** `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/helix_engine/reranker/fusion/rrf.rs`
- **Status:** Fully implemented with:
  - `RRFReranker::fuse_lists()` for multi-source fusion (lines 48-106)
  - Configurable k parameter (default 60)
  - Standard RRF formula: `1/(k + rank + 1)`

### SearchHybrid Operator
- **Location:** `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/helixc/generator/source_steps.rs:466-525`
- **Grammar:** `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/grammar.pest:230`
- **Status:** Fully implemented - combines SearchV + SearchBM25 with RRF fusion:
  ```rust
  RRFReranker::fuse_lists(
      vec![__hybrid_vec_results.into_iter(), __hybrid_bm25_results.into_iter()],
      60.0
  )
  ```

### PPR HQL Operator
- **Location:** `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/helixc/generator/source_steps.rs:527-567`
- **Grammar:** `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/grammar.pest:231-235`
- **Status:** Fully integrated with HQL:
  - Syntax: `PPR<Type>(seeds: seed_ids, universe: universe_ids, depth: 2, damping: 0.85, limit: 50)`
  - Calls `ppr_with_storage()` with proper storage/txn/arena access

### Prefilter Grammar Support
- **Location:** `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/grammar.pest:228,230,236`
- **Status:** Grammar enabled for both SearchV and SearchHybrid:
  ```pest
  search_vector = { ... ~ ("::" ~ pre_filter)? }
  search_hybrid = { ... ~ ("::" ~ pre_filter)? }
  pre_filter = { "PREFILTER" ~ "(" ~ (evaluates_to_bool | anonymous_traversal) ~ ")" }
  ```

### PPR Depth Configuration (Retrieval Modes)
- **Location:** `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/helix_engine/graph/ppr.rs:59` and grammar:233
- **Status:** `max_depth` parameter exposed:
  - Default: 3 (deep mode) when not specified
  - Supports spec modes: fast (no PPR), standard (2), deep (3)

---

## Partially Implemented ⚠️

### Prefilter Code Generation
- **Location:** `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/helixc/generator/source_steps.rs:441-462`
- **What's missing:**
  - Prefilter closure generation exists for SearchVector but the generated code passes filter functions that may not cover all Oneiron-required fields
  - No automatic injection of `stale=false` filter
  - Vault isolation (vault_id) prefilter requires manual specification by caller

### Custom Edge Weights Override
- **Location:** `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/helixc/generator/source_steps.rs:557`
- **What's missing:**
  - HQL generator passes empty HashMap: `&std::collections::HashMap::new()`
  - No grammar support for custom weight specification in PPR syntax
  - Spec shows: `weights: { mentions: 0.6, supports: 1.0, opposes: 0.0 }` - not implemented

### PPR Algorithm Completeness
- **Location:** `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/helix_engine/graph/ppr.rs`
- **What's missing:**
  - **Bidirectional traversal:** Current implementation only follows outgoing edges; spec requires both `getOutgoing(srcId)` and `getIncoming(dstId)` for proper PPR expansion (e.g., finding TURNs that mention a PERSON requires inbound edge traversal)
  - **Teleport probability:** Standard PPR includes teleport back to seeds with probability `(1 - damping)`; current implementation lacks this

---

## Not Implemented ❌

### PPR Warm Cache Infrastructure
- **Priority:** High
- **Spec:** ONEIRON-ARCH-014-ppr-warmcache-v1.md
- **What's needed:**
  - `PPRCacheEntry` storage structure
  - Cache key format: `ppr:{vaultId}:{entityType}:{entityId}:{depth}`
  - Nightly `ppr_warmup` job
  - Dependency index for cache invalidation
  - Graph version tracking for staleness detection
  - Query-time cache lookup with fallback to live PPR
  - Metrics: hit/miss rate, latency tracking

### part_of Traversal Limits
- **Priority:** High
- **Spec:** ONEIRON-ARCH-004 Section 1.6
- **What's needed:**
  - Limit `part_of` edges to 1-2 hops during PPR expansion
  - Track hop count per edge type
  - Prevent geographic hierarchy over-expansion (city -> state -> country -> continent)
- **Current state:** No special handling for `part_of` edges despite weight being set to 0.8

### Claim Filtering
- **Priority:** High
- **Spec:** ONEIRON-ARCH-004 Section 4.3
- **What's needed:**
  - `approvalStatus IN ("auto", "approved")` filter
  - `lifecycleStatus = "active"` filter
  - `worldTag = "real"` filter (unless roleplay context)
  - `stale = false` filter
  - Deleted revision contamination check
- **Current state:** No claim-specific filtering infrastructure in Helix

### Ranking Signal Boosts
- **Priority:** Medium
- **Spec:** ONEIRON-ARCH-004 Section 4.2
- **Formula:** `Final Score = RRF(vector, FTS, PPR) × salience_boost × recency_boost × confidence_boost`
- **What's needed:**
  - Salience boost application
  - Recency decay function
  - Confidence multiplier
  - Post-RRF score adjustment
- **Current state:** RRF fusion implemented but no boost multipliers

### Access Control Preprocessor
- **Priority:** Medium
- **Spec:** HELIX-ONEIRON-SPEC-v1.1.1 Section 3.2/Bead 3.2
- **What's needed:**
  - Query preprocessor to compute `universe_ids` from AccessContext
  - Automatic prefilter injection for vault_id, space, sensitivity
  - Admin bypass configuration
  - Cross-vault access blocking
- **Current state:** Manual candidate-set specification required

### predicateNamespace Schema Field
- **Priority:** Medium
- **Spec:** HELIX-ONEIRON-SPEC-v1.1.1 Section 3.5/Bead 2.4
- **What's needed:**
  - Indexed field for efficient namespace filtering
  - Derive namespace from predicate (e.g., "goal.career" -> "goal")
  - Enable `goal.*` queries via `predicateNamespace == "goal"`
- **Current state:** No predicate pattern matching support

### Retrieval Mode Fast Path
- **Priority:** Low
- **Spec:** ONEIRON-ARCH-004 Section 4.1
- **What's needed:**
  - `mode: "fast"` option that skips PPR entirely
  - Target latency ~50ms for fast mode
- **Current state:** Always runs PPR when PPR operator is used

---

## Recommendations for Optimal Performance

### Immediate (High Priority)

1. **Implement part_of Traversal Limits**
   - Add `part_of_max_hops` parameter to PPR function (default: 2)
   - Track per-edge-type hop counts during traversal
   - Prevents runaway expansion through geographic hierarchies
   - ~2-4 hours implementation

2. **Add Bidirectional PPR Traversal**
   - Current: only outgoing edges
   - Required: both incoming and outgoing edges
   - Essential for queries like "What do we know about Alice?" (find content that mentions Alice via inbound edges)
   - ~4-6 hours implementation

3. **Implement PPR Teleport Probability**
   - Add `newScores.set(seedId, score * (1 - damping))` teleport
   - Standard PageRank behavior, improves convergence
   - ~1-2 hours implementation

### Short-term (Medium Priority)

4. **PPR Warm Cache - Phase 1**
   - Add `ppr_cache` table/structure
   - Implement cache lookup in retrieval path
   - Fall back to live PPR on miss
   - Track basic metrics (hit/miss)
   - ~1 week implementation per spec

5. **Claim Filtering Infrastructure**
   - Add claim status fields to schema
   - Implement prefilter extensions for claim fields
   - Integrate with existing PREFILTER syntax
   - ~3-5 days implementation

6. **Custom Edge Weights in HQL**
   - Extend grammar: `PPR<Type>(..., weights: { edge: weight })`
   - Parse and pass to `ppr_with_storage()`
   - ~2-3 days implementation

### Longer-term (Lower Priority)

7. **Access Control Preprocessor**
   - Automatic universe_ids computation
   - Prefilter injection
   - Config-driven access rules
   - ~1 week implementation

8. **Ranking Signal Boosts**
   - Post-RRF score adjustment layer
   - Configurable boost functions
   - ~3-5 days implementation

9. **PPR Warm Cache - Phase 2-4**
   - Entity selection algorithm
   - Dependency index
   - Lazy invalidation
   - Metrics dashboard
   - ~2-3 weeks total

---

## Performance Considerations

| Feature | Current State | Spec Target | Gap |
|---------|--------------|-------------|-----|
| SearchHybrid + RRF | Implemented | <10ms | Needs benchmarking |
| PPR depth 2 | Implemented | <50ms | Needs benchmarking |
| PPR depth 3 | Implemented | <500ms | Warm cache needed for production |
| Combined retrieval p99 | Unknown | <100ms | Needs benchmarking |
| PPR cache hit | Not implemented | ~50ms | Full cache infrastructure needed |

---

## Files Reference

| Component | Path |
|-----------|------|
| PPR Core | `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/helix_engine/graph/ppr.rs` |
| RRF Reranker | `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/helix_engine/reranker/fusion/rrf.rs` |
| Source Steps (SearchHybrid, PPR HQL) | `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/helixc/generator/source_steps.rs` |
| Grammar | `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/grammar.pest` |
