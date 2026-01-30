# Stress Test Crash Diagnostic Report

**Date:** 2026-01-30
**Test:** `test_stress_memory_stability`
**Error:** `double free or corruption (out)` / `signal: 6, SIGABRT`

## Executive Summary

The crash is caused by a **lifetime safety bug introduced in commit 56e1dc44** ("updating tests to avoid concurrency bug"). The root cause is improper handling of the TempDir lifetime in relation to LMDB's memory-mapped state.

## Root Cause Analysis

### The Critical Issue

Commit 56e1dc44 changed the test setup pattern from:
- **Before**: `fn setup_stress_storage() -> (Arc<HelixGraphStorage>, TempDir)` - TempDir returned and held by test
- **After**: `fn setup_stress_storage(temp_dir: &TempDir) -> Arc<HelixGraphStorage>` - TempDir is local to caller

### Why This Causes a Crash

1. **LMDB Memory-Mapped State**: The `HelixGraphStorage` wraps an LMDB `Env` which maintains:
   - Memory-mapped file pointers to database files
   - Internal state referencing the database directory
   - Active transaction handles with references to memory-mapped regions

2. **TempDir Cleanup Race**: When `tempfile::TempDir` is dropped:
   - It deletes the temporary directory and all files
   - LMDB `Env` may still hold references to the now-deleted memory region
   - This creates a use-after-free condition

3. **Multi-Threaded Vulnerability**: The `test_stress_memory_stability` test:
   - Creates 4 concurrent worker threads
   - Runs 3 iterations (3 seconds each)
   - Each thread performs write and read transactions continuously

4. **The Race Condition**:
   - Worker threads complete but LMDB internal structures still hold mmap references
   - Test function exits and `temp_dir` goes out of scope
   - TempDir destructor deletes database files
   - LMDB/allocator accesses deleted mmap region → memory corruption → SIGABRT

## Suggested Fix

**Restore safe lifetime management** - TempDir must outlive all LMDB references:

```rust
fn setup_stress_storage() -> (Arc<HelixGraphStorage>, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap();
    let mut config = Config::default();
    config.db_max_size_gb = Some(20);
    let storage = HelixGraphStorage::new(path, config, Default::default()).unwrap();
    (Arc::new(storage), temp_dir)
}

fn test_stress_memory_stability() {
    let (storage, _temp_dir) = setup_stress_storage();  // Hold TempDir
    // ... test runs ...
    // _temp_dir dropped at END of test, after all threads complete
}
```

## Impact Assessment

- **Severity**: HIGH - Test crashes consistently, indicates memory safety violation
- **Scope**: Affects stress testing; potential risk to concurrent LMDB usage
- **Fix Complexity**: LOW - Simple pattern restoration
