# Oneiron Deployment Architecture

## Overview

Oneiron uses a **per-vault isolation model**: each user's knowledge graph runs as an independent HelixDB instance. This provides strong data isolation, predictable performance, and independent scaling.

## Per-Vault Isolation Model

Each vault is a standalone HelixDB process with its own:
- LMDB storage directory (data.mdb)
- HNSW vector index (768-dim f64)
- BM25 inverted index
- PPR graph traversal state
- PPR warm cache

Vaults share nothing. A crashed or slow vault cannot affect other users.

## Memory Footprint per Vault

| Vault Size | Raw Vectors (768-dim f64) | Total (with 2.5x index overhead + 100MB OS) |
|------------|---------------------------|----------------------------------------------|
| 1K docs    | 6 MB                      | ~115 MB                                      |
| 5K docs    | 30 MB                     | ~175 MB                                      |
| 10K docs   | 60 MB                     | ~250 MB                                      |
| 50K docs   | 300 MB                    | ~850 MB                                      |
| 100K docs  | 600 MB                    | ~1.6 GB                                      |

Formula: `raw = docs × 768 × 8 bytes`, total = `raw × 2.5 + 100MB`

## Recommended Fly.io Presets per Vault Size

| Vault Size | Fly.io Preset         | vCPU | RAM    | Cost/month |
|------------|-----------------------|------|--------|------------|
| 1K docs    | shared-cpu-1x         | 1    | 256 MB | $2.02      |
| 5K docs    | shared-cpu-1x         | 1    | 256 MB | $2.02      |
| 10K docs   | shared-cpu-1x + 256MB | 1    | 512 MB | $3.32      |
| 50K docs   | shared-cpu-4x         | 4    | 1 GB   | $8.08      |
| 100K docs  | shared-cpu-8x         | 8    | 2 GB   | $16.15     |

## API Routing Layer Design

### Colocated API + Vault per Region

```
User (Tokyo) ──→ Fly Edge (NRT) ──→ API + Vault (NRT)
                                      │
                                      ├─ HelixDB process
                                      ├─ LMDB volume
                                      └─ API handler
```

Each Fly.io machine runs both the API handler and the HelixDB vault process. This eliminates network hops between API and database — internal communication is sub-1ms via local IPC.

### Request Flow

```
1. User sends request
2. Fly.io edge routes to nearest region
3. Request hits colocated API handler
4. API handler calls local HelixDB vault
   a. Vector search (HNSW)
   b. BM25 full-text search
   c. RRF fusion
   d. PPR graph re-ranking
   e. Signal boosts (salience, recency, confidence)
5. Response returned to user
```

Total expected latency at 10K docs: **< 5ms** for full pipeline.

## Scale-to-Zero Strategy

Fly.io supports machine auto-stop and auto-start:

```toml
# fly.toml
[http_service]
  auto_stop_machines = true
  auto_start_machines = true
  min_machines_running = 0

[http_service.concurrency]
  type = "requests"
  hard_limit = 25
  soft_limit = 20
```

### Idle Timeout
- Machines auto-stop after 5 minutes of no requests
- First request after idle: **< 1s cold start** (Fly.io resume is fast)
- LMDB data persists on Fly.io volumes — no re-indexing needed
- Idle vaults cost **$0** (only billed for compute time)

### Warm-Up Behavior
On resume, HelixDB:
1. Opens existing LMDB environment (instant — memory-mapped)
2. HNSW index is ready immediately (stored in LMDB)
3. BM25 index is ready immediately (stored in LMDB)
4. PPR cache may need warming (runs in background)

## Region Coverage

### Priority Regions (Asia + US focus)

| Region Code | Location          | Use Case            |
|-------------|-------------------|---------------------|
| nrt         | Tokyo, Japan      | Asia-Pacific users  |
| sin         | Singapore         | Southeast Asia      |
| sjc         | San Jose, CA      | US West Coast       |
| iad         | Ashburn, VA       | US East Coast       |
| ord         | Chicago, IL       | US Central          |

### Secondary Regions (as demand grows)

| Region Code | Location          |
|-------------|-------------------|
| lhr         | London, UK        |
| fra         | Frankfurt, Germany|
| syd         | Sydney, Australia |
| gru         | São Paulo, Brazil |

Fly.io supports 30+ regions. New regions can be added with a single command:
```bash
fly scale count 1 --region lhr
```

## Hosting Cost Comparison

| Platform                   | Cost/vault/mo | 100 vaults | Scale-to-zero | Notes                        |
|----------------------------|---------------|------------|---------------|------------------------------|
| Hetzner Dedicated (packed) | $0.44-0.60    | $44-60     | No            | Cheapest, manual mgmt        |
| k3s on Hetzner Cloud       | $0.70-1.00    | $70-100    | Yes (1-3s)    | Best balance cost+isolation   |
| Coolify on Hetzner         | $0.60-0.90    | $60-90     | Partial       | Self-hosted PaaS, easy Docker |
| **Fly.io (recommended)**   | $2.02-3.32    | $200-332   | Yes (<1s)     | Best DX, 30+ regions         |
| Google Cloud Run           | $2.50-4.00    | $250-400   | Yes (0.5-3s)  | Storage limitations           |
| Railway                    | $6.50-8.00    | $650-800   | Yes (10min)   | Simple, expensive             |

### Why Fly.io

1. **Sub-second cold start** — machines resume from suspended state, not from container image
2. **Colocated API + vault** — no inter-service network hop
3. **30+ global regions** — users get single-digit ms latency
4. **Scale-to-zero** — idle vaults cost nothing
5. **Persistent volumes** — LMDB data survives machine restarts
6. **Single platform** — one billing account, one CLI, one deployment workflow

## Growth Path

### Phase 1: Fly.io (0-500 vaults)
- All vaults on Fly.io
- Simple ops: `fly deploy`, `fly scale`, `fly logs`
- Cost: ~$2-8/vault/month active, ~$0 idle
- Total at 100 active vaults: ~$200-332/month

### Phase 2: Hybrid (500-5000 vaults)
- Hot vaults (daily active): remain on Fly.io for latency
- Warm vaults (weekly active): migrate to k3s on Hetzner Cloud
- Cold vaults (monthly active): scale-to-zero on either platform
- Cost optimization: Hetzner vaults at $0.70-1.00/vault/month

### Phase 3: k3s on Hetzner (5000+ vaults)
- Primary compute on Hetzner dedicated servers
- Fly.io used only for edge routing and regional presence
- Full k3s cluster with auto-scaling
- Cost: $0.44-0.60/vault/month at scale

### Migration Strategy
- Vaults are portable: LMDB data directory can be moved between hosts
- No vendor lock-in: HelixDB runs anywhere with a filesystem
- Gradual migration: move vaults one at a time based on activity patterns

## Deployment Commands

### Create a new vault
```bash
fly apps create vault-${USER_ID}
fly volumes create helix_data --size 1 --region nrt --app vault-${USER_ID}
fly deploy --app vault-${USER_ID} \
  --vm-cpu-kind shared --vm-cpus 1 --vm-memory 256
```

### Scale a vault up (growing user)
```bash
fly scale vm shared-cpu-4x --memory 1024 --app vault-${USER_ID}
```

### Destroy a vault
```bash
fly apps destroy vault-${USER_ID} --yes
```

### Run benchmarks on Fly.io hardware
```bash
fly deploy --build-only --push --dockerfile Dockerfile.bench

# Test on shared-cpu-1x (256MB) — matches 1K-5K vault tier
fly machine run registry.fly.io/oneiron-helix:bench \
  --vm-cpu-kind shared --vm-cpus 1 --vm-memory 256

# Test on shared-cpu-1x (512MB) — matches 10K vault tier
fly machine run registry.fly.io/oneiron-helix:bench \
  --vm-cpu-kind shared --vm-cpus 1 --vm-memory 512

# Test on shared-cpu-4x (1GB) — matches 50K vault tier
fly machine run registry.fly.io/oneiron-helix:bench \
  --vm-cpu-kind shared --vm-cpus 4 --vm-memory 1024
```
