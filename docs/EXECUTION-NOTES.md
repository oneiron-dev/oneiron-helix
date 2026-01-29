# Execution Notes: Query Pipeline

This document traces how vector search (SearchV), BM25 search (SearchBM25), and RRF reranking execute in the current helix engine code, and calls out where prefilter logic must be inserted to satisfy HELIX-ONEIRON-SPEC-v1.1.1.

Relevant files:
- `helix-db/src/helix_engine/traversal_core/ops/vectors/search.rs`
- `helix-db/src/helix_engine/vector_core/vector_core.rs`
- `helix-db/src/helix_engine/vector_core/utils.rs`
- `helix-db/src/helix_engine/traversal_core/ops/bm25/search_bm25.rs`
- `helix-db/src/helix_engine/bm25/bm25.rs`
- `helix-db/src/helix_engine/reranker/fusion/rrf.rs`

## 1) SearchV execution (vector_core + traversal_core/ops/vectors)

Entry point:
- `RoTraversalIterator::search_v` in `traversal_core/ops/vectors/search.rs`.

Flow:
1) `search_v` calls `self.storage.vectors.search(...)` (HNSW) with:
   - `query` slice, `k`, and `label`
   - `filter` passed through
   - `should_trickle = false`
   - `arena` and `txn`
2) `VectorCore::search` (impl of `HNSW::search`) in `vector_core/vector_core.rs`:
   - Wraps the query as an `HVector` (label + vector).
   - Loads the HNSW entry point from `vectors_db` via `ENTRY_POINT_KEY`.
   - Walks down from the entry point's top layer to layer 1 using `search_level`.
     - Each `search_level` uses a candidate heap + results heap, expands neighbors,
       and keeps at most `ef` closest items.
     - `get_neighbors` can apply the `filter` to neighbor expansion, but only if
       `should_trickle` is true (see below).
   - Runs `search_level` again on level 0 to get a candidate heap.
   - Converts the candidate heap to final results using
     `BinaryHeap::to_vec_with_filter` (in `vector_core/utils.rs`), which:
       - Pulls candidates in score order
       - Loads `VectorWithoutData` from `vector_properties_db`
       - Skips deleted vectors (SHOULD_CHECK_DELETED is true)
       - Enforces label match and `filter` predicate
       - Expands `HVector` with stored properties
3) `search_v` maps each result `HVector` into `TraversalValue::Vector`.
   - Errors from `VectorCore::search` are mapped to `GraphError` variants.

Important behavior details:
- `should_trickle` is currently set to `false` by `search_v`, so the `filter`
  is only applied at the final `to_vec_with_filter` stage.
- The HNSW traversal itself uses raw neighbor expansion without prefiltering
  unless `should_trickle` is enabled.

Related optional path:
- `brute_force_search_v` in `traversal_core/ops/vectors/brute_force_search.rs` is a
  full-scan cosine similarity path over an existing vector stream. It is separate
  from `search_v` and is only used if explicitly invoked.

## 2) SearchBM25 execution

Entry point:
- `RoTraversalIterator::search_bm25` in `traversal_core/ops/bm25/search_bm25.rs`.

Flow:
1) `search_bm25` fetches the configured BM25 engine (`HBM25Config`) from storage
   and calls `HBM25Config::search` in `bm25/bm25.rs`.
2) `HBM25Config::search`:
   - Tokenizes the query (lowercase, split on non-alphanumerics, length > 2).
   - Loads `BM25Metadata` (total docs + avgdl) from `bm25_metadata`.
   - For each query term:
     - Reads the term document frequency from `term_frequencies_db`.
     - Iterates the term's postings in `inverted_index_db`.
     - For each posting, loads doc length from `doc_lengths_db` and
       accumulates BM25 score into `doc_scores`.
   - Sorts results by score desc, truncates to `limit`.
3) `search_bm25` adapter post-processes the `(doc_id, score)` list:
   - Reads raw node bytes from `nodes_db` for each doc id.
   - Extracts the label from the LMDB header and only keeps nodes whose
     label matches the requested label.
   - Deserializes the node and returns `TraversalValue::NodeWithScore { node, score }`.

Important behavior details:
- The label filter is applied *after* BM25 scoring (post-filter).
- No other prefiltering is performed in BM25 search today.

## 3) RerankRRF (Reciprocal Rank Fusion)

Implementation:
- `helix_engine/reranker/fusion/rrf.rs`

Flow:
- `RRFReranker::fuse_lists` (multi-list):
  1) Iterate each ranked list and each item with its rank index.
  2) Compute `rr_score = 1.0 / (k + rank + 1)` and accumulate per id.
  3) Keep the first instance of each item in an `items_map`.
  4) Sort by total RRF score desc, update each item's score via
     `update_score`, and return the fused list.
- `Reranker::rerank` (single-list):
  - Converts each item rank to an RRF score and updates in-place.

Score handling:
- `update_score` only updates `TraversalValue::Vector` (distance field) and
  `TraversalValue::NodeWithScore`. Other variants error on update.

## 4) Prefilter insertion points (required by HELIX-ONEIRON-SPEC-v1.1.1)

Spec requirement (section 2.6):
- `vaultId`, `space`, `relationshipId`, `sensitivity`, and `stale=false` must be
  applied *before* vector/BM25 ranking.

Current state:
- Vector search applies the filter only during final result materialization
  (`to_vec_with_filter`) because `search_v` sets `should_trickle = false`.
- BM25 applies only label filtering after scoring.

Where to insert prefilter logic:

Vector (SearchV / HNSW):
- Primary insertion points:
  - `traversal_core/ops/vectors/search.rs` should call
    `self.storage.vectors.search(..., filter, true, ...)` so the filter is used
    in HNSW traversal (`get_neighbors`) and candidate expansion.
  - `vector_core/vector_core.rs::search_level` and/or `get_neighbors` can be
    extended to accept a stronger allowlist (candidate set) and to skip the
    entry point if it fails the filter, ensuring candidate-set gating happens
    *before* distances are ranked.
- Optional stronger path:
  - Pass an explicit allowlist of ids into `VectorCore::search` (or a new
    `search_prefiltered`) and enforce allowlist checks before distance
    computation and before pushing candidates into heaps.

BM25 (SearchBM25):
- Primary insertion point:
  - Modify `HBM25Config::search` to accept a candidate set or predicate.
    Only accumulate scores for doc_ids that pass prefilter.
- Secondary insertion point:
  - Move label filtering into the BM25 search itself or maintain
    label-specific BM25 indexes to avoid scoring docs that are filtered out.
- The adapter (`search_bm25`) is currently post-filter and should not be relied
  upon for prefilter correctness.

Reranking (RRF):
- RRF assumes prefiltering already happened. It only combines ranked lists and
  updates scores; it is not a place to enforce access gating.

Notes for invariants:
- The candidate-set gating requirement suggests storing or passing an explicit
  allowlist derived from the RetrievalIndex (stale=false + ACL filters) into
  both vector and BM25 search paths, prior to scoring.
