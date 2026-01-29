# AGENTS.md - oneiron-helix

## Rust Workflow
For any Rust changes, run:
1. cargo fmt
2. cargo clippy
3. cargo test

## Do Not
- Never modify vendored dependencies.

## Test Locations
- HQL features: hql-tests/
- Oneiron extensions: oneiron-tests/

## Oneiron Invariants (HELIX-ONEIRON-SPEC-v1.1.1.md section 2)
1. Both-endpoints-readable traversal: Graph traversal must be enforced via candidate-set gating. Only traverse edges where BOTH endpoints are in the accessible candidate set.
2. Stale exclusion by default: RetrievalIndex rows with stale=true are excluded from all queries unless explicitly requested.
3. Retrieval scoring pipeline: `Final Score = RRF(vector, FTS, PPR) * salience * recency * confidence`, plus claim filters (predicate patterns, lifecycle status).
4. PPR modes:
   - fast: No PPR (vector + FTS only)
   - standard: 2 hops (default)
   - deep: 3 hops (expensive, needs caching)
5. part_of traversal guardrails: limit to 1-2 hops to prevent runaway expansion.
6. Prefilter correctness: vaultId, space, relationshipId, sensitivity, and stale=false MUST be applied BEFORE vector/BM25 ranking.

## Rust Coding Guidelines
Follow ../.agents/skills/coding-guidelines/SKILL.md. Key rules:
- No get_ prefix for accessors; use conversion naming (as_/to_/into_).
- Use newtypes for domain semantics; pre-allocate collections with with_capacity.
- Use ? for error propagation; prefer expect() over unwrap() when guaranteed.
- Use meaningful lifetime names (e.g., 'src, 'ctx); do not hold locks across await.
- Prefer std::sync::OnceLock/LazyLock over lazy_static!/once_cell::Lazy.
