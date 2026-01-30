# Test Validation Report

**Date:** 2026-01-29
**Validator:** Automated Analysis
**Purpose:** Validate E2E tests for PPR and SearchHybrid are legitimate tests (not reward hacking)

---

## Executive Summary

The tests are **legitimate** and test real functionality. However, there are **significant coverage gaps** that need attention before the implementation can be considered complete for Oneiron requirements.

---

## PPR Integration Tests (ppr_tests.rs)

**File:** `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/helix_engine/tests/traversal_tests/ppr_tests.rs`

### test_ppr_single_seed_propagation
- **Status:** ✅ Valid
- **What it tests:** PPR propagates scores from a single seed through a 3-node chain (Alice -> Bob -> Carol)
- **Validation status:** Tests real graph traversal with actual storage layer; verifies score decay (seed >= 1-hop >= 2-hop)
- **Notes:** Uses real LMDB storage via `HelixGraphStorage`. Validates core PPR behavior.

### test_ppr_multiple_seeds_distribution
- **Status:** ✅ Valid
- **What it tests:** Multiple seeds receive equal initial scores (1/num_seeds each)
- **Validation status:** Tests score initialization; verifies each seed gets 0.5 when 2 seeds provided
- **Notes:** Important for seed-based personalization behavior.

### test_ppr_candidate_set_gating
- **Status:** ✅ Valid (Critical Test)
- **What it tests:** `both-endpoints-readable` requirement - nodes outside universe get no score
- **Validation status:** Creates 3 nodes, puts only 2 in universe, verifies the third gets no/zero score
- **Notes:** **This directly validates Oneiron invariant #1** (candidate-set gating). Edge traversal respects universe constraint.

### test_ppr_opposes_edge_blocks_propagation
- **Status:** ✅ Valid (Critical Test)
- **What it tests:** Edges labeled "opposes" have weight 0.0 and block score propagation
- **Validation status:** Creates "supports" and "opposes" edges from same source; verifies supported node gets score, opposed node gets zero
- **Notes:** **Directly validates Oneiron spec requirement** that `opposes: 0.0` blocks contradiction spread.

### test_ppr_custom_edge_weights
- **Status:** ✅ Valid
- **What it tests:** Custom edge weights affect score proportionally
- **Validation status:** Uses 1.0 vs 0.1 weights, verifies 10:1 score ratio
- **Notes:** Important for Oneiron's 13 weighted edge types.

### test_ppr_disconnected_nodes_zero_score
- **Status:** ✅ Valid
- **What it tests:** Nodes in universe but disconnected from seeds get no score
- **Validation status:** Verifies PPR only spreads through connected graph
- **Notes:** Ensures no spurious scores.

### test_ppr_damping_factor_effect
- **Status:** ✅ Valid
- **What it tests:** Higher damping (0.9 vs 0.5) results in more score propagation to neighbors
- **Validation status:** Compares same graph with different damping factors
- **Notes:** Validates damping parameter works correctly.

### test_ppr_limit_results
- **Status:** ✅ Valid
- **What it tests:** Result limit parameter truncates output
- **Validation status:** Creates 6 nodes, requests limit=3, verifies only 3 returned
- **Notes:** Basic pagination/limit functionality.

---

## PPR Unit Tests (in ppr.rs)

**File:** `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/helix_engine/graph/ppr.rs`

### test_ppr_empty_seeds / test_ppr_seeds_not_in_universe / test_ppr_single_seed / test_ppr_multiple_seeds / test_ppr_limit / test_ppr_partial_seeds_in_universe
- **Status:** ⚠️ Partial (Tests stub function only)
- **What they test:** The `ppr()` stub function (without storage access)
- **Validation status:** These test edge cases for seed handling, but `ppr()` is a **stub** that only returns seed scores (no propagation)
- **Notes:** The comment in code (lines 78-137) explicitly states this is a stub: "For now, this stub returns only seed nodes with their initial scores."

### test_get_edge_weight_*
- **Status:** ✅ Valid
- **What they test:** Edge weight lookup from EDGE_WEIGHTS constant and user overrides
- **Validation status:** Validates weight lookup mechanism works correctly
- **Notes:** Important for Oneiron's edge weighting system.

### test_ppr_with_storage_signature_compiles
- **Status:** ⚠️ Concern (Weak test)
- **What it tests:** That `ppr_with_storage` function signature compiles
- **Validation status:** This is a compile-time test, not a runtime behavior test
- **Notes:** Useful for CI but doesn't test actual behavior.

---

## HQL E2E Tests

### ppr_e2e

**File:** `/home/ubuntu/projects/oneiron-helixdb/helix/hql-tests/tests/ppr_e2e/queries.hx`

**Schema:**
- Topic nodes with name/category
- RelatesTo, Supports, Opposes edges

**Queries Defined:**
1. `CreateTopic` - Creates Topic nodes
2. `LinkTopics` - Creates RelatesTo edges
3. `RankTopics` - Calls `PPR<Topic>(seeds, universe, limit)`
4. `RankTopicsCustom` - Calls PPR with custom depth/damping/limit

**Assessment:** ✅ Valid but **incomplete**
- **Strengths:**
  - Tests real HQL syntax compilation
  - Tests grammar rule parsing for PPR
  - Schema includes Supports/Opposes edges matching Oneiron spec
- **Weaknesses:**
  - No test harness asserting behavior (only query definitions)
  - No test cases exercising opposes=0 blocking through HQL
  - No test for candidate-set gating through HQL interface
  - Relies on integration tests in ppr_tests.rs for behavior validation

### search_hybrid_e2e

**File:** `/home/ubuntu/projects/oneiron-helixdb/helix/hql-tests/tests/search_hybrid_e2e/queries.hx`

**Schema:**
- Document vector type with content/title

**Queries Defined:**
1. `CreateDocument` - Creates Document vectors with embeddings
2. `HybridSearch` - Calls `SearchHybrid<Document>(query_vec, query_text, limit)`

**Assessment:** ✅ Valid but **minimal**
- **Strengths:**
  - Tests real HQL syntax for SearchHybrid
  - Tests vector type with embedding + text fields
  - Generated code correctly calls both search_v and search_bm25, then fuses with RRFReranker::fuse_lists
- **Weaknesses:**
  - No test harness with assertions
  - No test for PREFILTER functionality
  - No test comparing hybrid results vs individual search types

---

## Implementation Analysis

### PPR Implementation (`ppr_with_storage`)
**File:** `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/helix_engine/graph/ppr.rs`

**Correctness Assessment:** ✅ Implements spec correctly
- Uses BFS-style frontier propagation (not power iteration, but valid for local PPR)
- Respects `universe_ids` constraint (line 221: `if !universe_ids.contains(&target_node)`)
- Uses EDGE_WEIGHTS constant with `opposes: 0.0` (line 14)
- Blocks propagation when weight <= 0.0 (lines 233-235)
- Applies damping factor per hop (line 237)
- Supports custom edge weights override (line 231: `get_edge_weight()`)

**Deviation from Spec:**
- ⚠️ **Missing `part_of` guardrail**: Spec requires limiting `part_of` edges to 1-2 hops. Implementation uses same depth limit for all edges.

### SearchHybrid Implementation
**File:** `/home/ubuntu/projects/oneiron-helixdb/helix/helix-db/src/helixc/generator/source_steps.rs` (lines 466-524)

**Correctness Assessment:** ✅ Implements spec correctly
- Runs SearchV for vector results
- Runs SearchBM25 for text results
- Fuses using `RRFReranker::fuse_lists()` with k=60
- Supports PREFILTER parameter for vector search

---

## Coverage Gaps

### Critical Gaps (Oneiron Requirements)

1. **`part_of` traversal guardrail** - Not implemented
   - Spec: "Limit `part_of` traversal to 1-2 hops during PPR expansion"
   - Implementation: No special handling for `part_of` edge label
   - Risk: Runaway expansion through deep hierarchies

2. **No E2E test assertions** - ppr_e2e and search_hybrid_e2e only define queries
   - No test runner executing queries and validating outputs
   - No behavioral assertions (e.g., "opposes blocks propagation")

3. **Access control integration** - Not tested
   - Spec requires: "RetrievalIndex rows with `stale=true` excluded"
   - No test for stale filtering through PPR universe
   - No test for vault isolation

4. **Prefilter recall test** - Not implemented
   - Spec: "Test proves recall >= 99% vs post-filter + 10x overfetch"
   - No comparative test between prefilter and post-filter approaches

### Missing Test Scenarios

| Scenario | Status | Location |
|----------|--------|----------|
| PPR with `part_of` edges (guardrail) | ❌ Missing | ppr_tests.rs |
| PPR with all 13 Oneiron edge types | ❌ Missing | ppr_tests.rs |
| SearchHybrid with PREFILTER | ❌ Missing | search_hybrid_e2e |
| SearchHybrid result ordering | ❌ Missing | search_hybrid_e2e |
| PPR + SearchHybrid combined retrieval | ❌ Missing | N/A |
| Stale=false filtering through universe | ❌ Missing | N/A |
| Cross-vault access blocking | ❌ Missing | N/A |
| PPR depth=2 vs depth=3 performance | ❌ Missing | N/A |

---

## Recommendations

### High Priority (Blocking for Oneiron)

1. **Implement `part_of` guardrail in ppr.rs**
   ```rust
   // In the iteration loop:
   if edge_label == "part_of" && current_depth >= PART_OF_MAX_HOPS {
       continue;
   }
   ```

2. **Add E2E test harness with assertions**
   - Create test driver that populates data, runs queries, validates outputs
   - Test `opposes` blocking through HQL interface
   - Test candidate-set gating through HQL interface

3. **Add all 13 Oneiron edge types to tests**
   - Schema already supports Supports/Opposes
   - Add: belongs_to, participates_in, attached, authored_by, mentions, about, claim_of, scoped_to, supersedes, derived_from, part_of

### Medium Priority

4. **Add SearchHybrid PREFILTER test**
   - Test with vault_id filter
   - Verify correct results returned

5. **Add combined retrieval test (SearchHybrid + PPR)**
   - Match Oneiron retrieval pipeline: `RRF(vector, FTS, PPR)`

6. **Add performance regression tests**
   - PPR depth=2 < 50ms target
   - SearchHybrid + RRF < 10ms target

### Low Priority

7. **Improve stub function tests**
   - Either remove `ppr()` stub or add note that `ppr_with_storage()` is canonical

8. **Add edge weight documentation**
   - Document why each edge has its weight value

---

## Conclusion

The existing tests are **legitimate** and test **real functionality** - they are not reward hacking. The PPR implementation correctly implements:
- Both-endpoints-readable via candidate-set gating ✅
- opposes=0 blocks propagation ✅
- Custom edge weights ✅
- Damping factor ✅

However, the test suite is **incomplete** for Oneiron production use. The main gaps are:
1. Missing `part_of` guardrail implementation
2. No E2E test assertions (only query definitions)
3. Missing tests for all 13 Oneiron edge types
4. No access control integration tests

**Overall Assessment:** Tests are valid but coverage is ~60% of Oneiron requirements.
