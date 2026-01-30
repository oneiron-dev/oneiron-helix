# Stress Test Deep Diagnostic

**Date:** 2026-01-30
**Test:** `test_stress_memory_stability`
**Error:** `free(): invalid size` / SIGABRT

## Root Cause: Arena-Transaction Lifetime Mismatch

The crash is caused by a **use-after-free** due to the arena being deallocated before transaction cleanup completes.

### The Problematic Pattern (lines 469-487)

```rust
while start.elapsed() < duration {
    {
        let arena = Bump::new();         // ← Inner block scope
        let mut wtxn = storage.graph_env.write_txn().unwrap();

        G::new_mut(&storage, &arena, &mut wtxn)
            .add_n(&label, None, None)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        wtxn.commit().unwrap();
    }  // ← ARENA DEALLOCATED HERE, but transaction cleanup still references it!

    {
        let rtxn = storage.graph_env.read_txn().unwrap();
        let _count = storage.nodes_db.len(&rtxn).unwrap();
    }
}
```

### Why This Fails

The `G::new_mut()` signature enforces: `'db: 'arena: 'txn`

This means the arena should outlive the transaction. But in this test:
1. Arena created in inner block
2. Transaction commits
3. **Arena drops at block end**
4. Transaction cleanup (implicit RwTxn drop) still accesses arena-allocated memory
5. **Use-after-free → heap corruption → SIGABRT**

### Comparison: Passing vs Failing Tests

| Test | Arena Scope | Result |
|------|-------------|--------|
| test_stress_mixed_read_write_operations | Loop level | ✅ PASS |
| test_stress_rapid_graph_growth | Loop level | ✅ PASS |
| test_stress_transaction_contention | Loop level | ✅ PASS |
| test_stress_long_running_transactions | Block level (aligned) | ✅ PASS |
| **test_stress_memory_stability** | **Inner block** | ❌ FAIL |

### Fix

Move arena to loop scope (matching other passing tests):

```rust
while start.elapsed() < duration {
    let arena = Bump::new();  // ← Move to loop scope

    {
        let mut wtxn = storage.graph_env.write_txn().unwrap();
        // ... operations ...
        wtxn.commit().unwrap();
    }

    {
        let rtxn = storage.graph_env.read_txn().unwrap();
        let _count = storage.nodes_db.len(&rtxn).unwrap();
    }
}  // ← Arena drops here, after all transactions are fully cleaned up
```
