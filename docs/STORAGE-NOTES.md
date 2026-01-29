# Helix Graph Storage Notes

This document summarizes how the graph storage layer is implemented in LMDB and how traversal ops iterate neighbors. It is based on `helix-db/src/helix_engine/storage_core/mod.rs` and the traversal ops in `helix-db/src/helix_engine/traversal_core/ops/out/out.rs` and `helix-db/src/helix_engine/traversal_core/ops/in_/in_.rs`.

## 1) LMDB layout for nodes and edges

Helix uses LMDB via `heed3::Env` and creates named databases (tables) in `HelixGraphStorage::new`.

- `nodes` (`DB_NODES`)
  - Type: `Database<U128<BE>, Bytes>`
  - Key: node id as big-endian `u128` (16 bytes)
  - Value: bincode bytes for the node payload (dynamic length)

- `edges` (`DB_EDGES`)
  - Type: `Database<U128<BE>, Bytes>`
  - Key: edge id as big-endian `u128` (16 bytes)
  - Value: bincode bytes for the edge payload (dynamic length)

- `out_edges` (`DB_OUT_EDGES`)
  - Type: `Database<Bytes, Bytes>` with `DUP_SORT | DUP_FIXED`
  - Key: 20 bytes (`from_node_id` + `label_hash`)
  - Value: 32 bytes (`edge_id` + `to_node_id`)

- `in_edges` (`DB_IN_EDGES`)
  - Type: `Database<Bytes, Bytes>` with `DUP_SORT | DUP_FIXED`
  - Key: 20 bytes (`to_node_id` + `label_hash`)
  - Value: 32 bytes (`edge_id` + `from_node_id`)

`DUP_SORT` stores multiple adjacency values under the same key (one per edge), and `DUP_FIXED` enforces a fixed-size value so LMDB can store the duplicates compactly.

## 2) Neighbor iteration (out and in)

Traversal ops use `RoTraversalIterator` adapters to expand neighbors.

- Outgoing neighbors (`OutAdapter::out_node` / `out_vec` in `ops/out/out.rs`)
  1. For each traversal item, compute a label hash from the requested edge label.
  2. Build the 20-byte out-edge key using `HelixGraphStorage::out_edge_key(from_id, label_hash)`.
  3. Call `out_edges_db.get_duplicates(txn, &key)` to get the duplicate values for that key.
  4. For each 32-byte value, unpack `(edge_id, to_node_id)` via `unpack_adj_edge_data`.
  5. Load the neighbor node (`get_node`) or vector (`get_full_vector` / `get_vector_properties`) based on the method.

- Incoming neighbors (`InAdapter::in_node` / `in_vec` in `ops/in_/in_.rs`)
  1. Compute the label hash.
  2. Build the 20-byte in-edge key using `HelixGraphStorage::in_edge_key(to_id, label_hash)`.
  3. Call `in_edges_db.get_duplicates(txn, &key)`.
  4. Unpack `(edge_id, from_node_id)` from each 32-byte value.
  5. Load the neighbor node or vector as above.

## 3) Filtering by edge type during iteration

Filtering by edge type (label) is done by key construction, not by scanning values.

- The traversal methods (`out_node`, `out_vec`, `in_node`, `in_vec`) take an `edge_label: &str`.
- That label is hashed with `hash_label(edge_label, None)` into a 4-byte label id.
- The 20-byte adjacency key is built as `[node_id(16) | label_hash(4)]`.
- Because the key includes the label hash, `get_duplicates` returns only edges of that label.

This means edge type filtering is a direct LMDB lookup by the composite key, not a post-filter.

## 4) Edge index storage structure (20-byte keys, 32-byte values)

The adjacency index uses a fixed layout defined in `storage_core/mod.rs`:

- **Key (20 bytes)**
  - `out_edges`: `from_node_id(16) | label_hash(4)`
  - `in_edges`: `to_node_id(16) | label_hash(4)`

- **Value (32 bytes, DUP_FIXED)**
  - `edge_id(16) | other_node_id(16)`
  - For `out_edges`, other node is `to_node_id`.
  - For `in_edges`, other node is `from_node_id`.

Helpers used by traversal and storage:

- `out_edge_key(from_node_id, label_hash)` -> `[u8; 20]`
- `in_edge_key(to_node_id, label_hash)` -> `[u8; 20]`
- `pack_edge_data(edge_id, node_id)` -> `[u8; 32]`
- `unpack_adj_edge_data(&[u8])` -> `(edge_id, node_id)`

This layout allows all edges of a given label for a given node to be fetched with a single LMDB duplicate lookup.
