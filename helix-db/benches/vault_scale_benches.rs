/// HelixDB Vault-Scale Performance Benchmarks
///
/// Validates HelixDB latency at vault-realistic scales (1K-100K docs)
/// against turbopuffer's published benchmarks at 1M docs.
///
/// Run with:
///   cargo test --test vault_scale_benches --features bench --release -- --nocapture
#[cfg(feature = "bench")]
mod tests {
    use bumpalo::Bump;
    use heed3::{Env, EnvOpenOptions};
    use helix_db::{
        helix_engine::{
            bm25::bm25::{BM25, HBM25Config},
            vector_core::{
                hnsw::HNSW,
                vector::HVector,
                vector_core::{HNSWConfig, VectorCore},
                vector_distance::cosine_similarity,
            },
        },
        utils::id::v6_uuid,
    };
    use rand::prelude::*;
    use std::{
        collections::{HashMap, HashSet},
        fs,
        sync::{Arc, Mutex},
        thread,
        time::{Duration, Instant},
    };

    // ─── Filter type alias (needed for HNSW generics) ────────────────────────

    type Filter = fn(&HVector, &heed3::RoTxn) -> bool;

    // ─── Progress logging ────────────────────────────────────────────────────

    fn phase(test: &str, msg: &str) {
        let rss = process_rss_mb();
        eprintln!("[{test}] {msg} (RSS: {rss:.0}MB)");
    }

    // ─── Latency collector ───────────────────────────────────────────────────

    struct LatencyCollector {
        samples: Vec<Duration>,
    }

    impl LatencyCollector {
        fn new() -> Self {
            Self {
                samples: Vec::new(),
            }
        }

        fn with_capacity(cap: usize) -> Self {
            Self {
                samples: Vec::with_capacity(cap),
            }
        }

        fn record(&mut self, d: Duration) {
            self.samples.push(d);
        }

        fn sorted(&self) -> Vec<Duration> {
            let mut s = self.samples.clone();
            s.sort();
            s
        }

        fn percentile(&self, p: f64) -> Duration {
            let s = self.sorted();
            if s.is_empty() {
                return Duration::ZERO;
            }
            let idx = ((p * s.len() as f64).floor() as usize).min(s.len() - 1);
            s[idx]
        }

        fn p50(&self) -> Duration {
            self.percentile(0.50)
        }
        fn p90(&self) -> Duration {
            self.percentile(0.90)
        }
        fn p99(&self) -> Duration {
            self.percentile(0.99)
        }

        fn mean(&self) -> Duration {
            if self.samples.is_empty() {
                return Duration::ZERO;
            }
            let total: Duration = self.samples.iter().sum();
            total / self.samples.len() as u32
        }

        fn qps(&self) -> f64 {
            if self.samples.is_empty() {
                return 0.0;
            }
            let total: Duration = self.samples.iter().sum();
            self.samples.len() as f64 / total.as_secs_f64()
        }
    }

    // ─── Data generation ─────────────────────────────────────────────────────

    fn gen_sim_vecs(n: usize, dim: usize, similarity: f64) -> Vec<Vec<f64>> {
        let mut rng = rand::rng();
        let similarity = 1.0 - similarity;
        let base: Vec<f64> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();

        (0..n)
            .map(|_| {
                base.iter()
                    .map(|&v| (v + rng.random_range(-similarity..similarity)).clamp(-1.0, 1.0))
                    .collect()
            })
            .collect()
    }

    fn gen_query_vecs(base_vecs: &[Vec<f64>], n_queries: usize, noise: f64) -> Vec<Vec<f64>> {
        let mut rng = rand::rng();
        (0..n_queries)
            .map(|_| {
                let base = &base_vecs[rng.random_range(0..base_vecs.len())];
                base.iter()
                    .map(|&v| (v + rng.random_range(-noise..noise)).clamp(-1.0, 1.0))
                    .collect()
            })
            .collect()
    }

    const VOCABULARY: &[&str] = &[
        "neural", "network", "transformer", "attention", "embedding",
        "gradient", "descent", "backpropagation", "convolution", "recurrent",
        "encoder", "decoder", "tokenizer", "vocabulary", "inference",
        "training", "validation", "optimization", "regularization", "dropout",
        "activation", "softmax", "sigmoid", "relu", "batch",
        "normalization", "residual", "connection", "layer", "hidden",
        "weights", "bias", "learning", "rate", "epoch",
        "loss", "function", "cross", "entropy", "accuracy",
        "precision", "recall", "model", "architecture", "parameter",
        "hyperparameter", "tuning", "fine", "pretrained", "transfer",
        "knowledge", "distillation", "pruning", "quantization", "deployment",
        "latency", "throughput", "scalability", "distributed", "parallel",
        "computation", "memory", "bandwidth", "cache", "storage",
        "database", "index", "query", "retrieval", "augmented",
        "generation", "context", "window", "sequence", "length",
        "position", "encoding", "multi", "head", "self",
        "supervised", "unsupervised", "reinforcement", "reward", "policy",
        "agent", "environment", "state", "action", "observation",
        "feature", "extraction", "representation", "clustering", "classification",
        "regression", "segmentation", "detection", "recognition", "synthesis",
        "generative", "adversarial", "variational", "autoencoder", "diffusion",
        "sampling", "temperature", "probability", "distribution", "likelihood",
    ];

    fn gen_zipf_doc(rng: &mut impl Rng, word_count: usize) -> String {
        let vocab_len = VOCABULARY.len();
        (0..word_count)
            .map(|_| {
                let rank = rng.random_range(0..vocab_len);
                let zipf_rank = (rank as f64).sqrt() as usize;
                VOCABULARY[zipf_rank.min(vocab_len - 1)]
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn gen_bm25_docs(n: usize) -> Vec<(u128, String)> {
        let mut rng = rand::rng();
        (0..n)
            .map(|_| {
                let word_count = rng.random_range(50..200);
                (v6_uuid(), gen_zipf_doc(&mut rng, word_count))
            })
            .collect()
    }

    fn gen_bm25_queries(n: usize) -> Vec<String> {
        let mut rng = rand::rng();
        (0..n)
            .map(|_| {
                let num_terms = rng.random_range(1..4);
                (0..num_terms)
                    .map(|_| VOCABULARY[rng.random_range(0..20)])
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect()
    }

    // ─── Ground truth (multi-threaded brute force) ───────────────────────────

    fn calc_ground_truths(
        base_vectors: &[(u128, Vec<f64>)],
        query_vectors: &[(usize, Vec<f64>)],
        k: usize,
    ) -> HashMap<usize, Vec<u128>> {
        let results = Mutex::new(HashMap::new());
        let chunk_size = (query_vectors.len() + num_cpus::get() - 1) / num_cpus::get();

        thread::scope(|s| {
            for chunk in query_vectors.chunks(chunk_size) {
                s.spawn(|| {
                    let local: HashMap<usize, Vec<u128>> = chunk
                        .iter()
                        .map(|(qid, qvec)| {
                            let mut dists: Vec<(u128, f64)> = base_vectors
                                .iter()
                                .map(|(id, data)| {
                                    let sim = cosine_similarity(qvec, data).unwrap_or(-1.0);
                                    (*id, 1.0 - sim)
                                })
                                .collect();
                            dists.sort_by(|a, b| {
                                a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            (*qid, dists.into_iter().take(k).map(|(id, _)| id).collect())
                        })
                        .collect();
                    results.lock().unwrap().extend(local);
                });
            }
        });

        results.into_inner().unwrap()
    }

    // ─── Environment helpers ─────────────────────────────────────────────────

    fn setup_temp_env(size_mb: usize) -> (Env, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap();

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(size_mb * 1024 * 1024)
                .max_dbs(20)
                .max_readers(200)
                .open(path)
                .unwrap()
        };

        (env, temp_dir)
    }

    fn map_size_for_bm25(n_docs: usize) -> usize {
        ((n_docs * 3000) / (1024 * 1024)).max(64) + 64
    }

    fn map_size_for_vectors(n_docs: usize, dim: usize) -> usize {
        let raw_bytes = n_docs * dim * 8;
        let with_overhead = (raw_bytes as f64 * 3.0) as usize;
        let mb = (with_overhead / (1024 * 1024)).max(64) + 64;
        mb
    }

    fn lmdb_disk_usage(temp_dir: &tempfile::TempDir) -> u64 {
        let data_file = temp_dir.path().join("data.mdb");
        fs::metadata(&data_file).map(|m| m.len()).unwrap_or(0)
    }

    #[cfg(target_os = "linux")]
    fn process_rss_mb() -> f64 {
        fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("VmRSS:"))
                    .and_then(|l| {
                        l.split_whitespace()
                            .nth(1)
                            .and_then(|v| v.parse::<f64>().ok())
                    })
            })
            .map(|kb| kb / 1024.0)
            .unwrap_or(0.0)
    }

    #[cfg(not(target_os = "linux"))]
    fn process_rss_mb() -> f64 {
        0.0
    }

    fn fmt_dur(d: Duration) -> String {
        let us = d.as_micros();
        if us < 1000 {
            format!("{us}us")
        } else {
            format!("{:.2}ms", us as f64 / 1000.0)
        }
    }

    // ─── Batch insert helpers ────────────────────────────────────────────────

    const BATCH_SIZE: usize = 500;

    fn insert_vectors_batched(
        env: &Env,
        index: &VectorCore,
        vecs: &[Vec<f64>],
        test_name: &str,
    ) -> Vec<(u128, Vec<f64>)> {
        let mut inserted: Vec<(u128, Vec<f64>)> = Vec::with_capacity(vecs.len());
        let mut batch_start = 0;
        let total = vecs.len();

        while batch_start < total {
            let batch_end = (batch_start + BATCH_SIZE).min(total);
            let mut txn = env.write_txn().unwrap();

            let mut arena = Bump::new();
            for vec_data in &vecs[batch_start..batch_end] {
                arena.reset();
                let data = arena.alloc_slice_copy(vec_data.as_slice());
                let label: &str = arena.alloc_str("vector");
                let hvec = index
                    .insert::<Filter>(&mut txn, label, data, None, &arena)
                    .unwrap();
                inserted.push((hvec.id, vec_data.clone()));
            }

            txn.commit().unwrap();
            eprintln!(
                "[{test_name}]   inserted {batch_end}/{total} vectors (RSS: {:.0}MB)",
                process_rss_mb()
            );
            batch_start = batch_end;
        }

        inserted
    }

    fn insert_vectors_batched_no_gt(
        env: &Env,
        index: &VectorCore,
        vecs: &[Vec<f64>],
        test_name: &str,
    ) {
        let mut batch_start = 0;
        let total = vecs.len();

        while batch_start < total {
            let batch_end = (batch_start + BATCH_SIZE).min(total);
            let mut txn = env.write_txn().unwrap();

            let mut arena = Bump::new();
            for vec_data in &vecs[batch_start..batch_end] {
                arena.reset();
                let data = arena.alloc_slice_copy(vec_data.as_slice());
                let label: &str = arena.alloc_str("vector");
                let _ = index
                    .insert::<Filter>(&mut txn, label, data, None, &arena)
                    .unwrap();
            }

            txn.commit().unwrap();
            eprintln!(
                "[{test_name}]   inserted {batch_end}/{total} vectors (RSS: {:.0}MB)",
                process_rss_mb()
            );
            batch_start = batch_end;
        }
    }

    fn insert_bm25_batched(
        env: &Env,
        bm25: &HBM25Config,
        docs: &[(u128, String)],
        test_name: &str,
    ) {
        let mut batch_start = 0;
        let total = docs.len();

        while batch_start < total {
            let batch_end = (batch_start + BATCH_SIZE).min(total);
            let mut txn = env.write_txn().unwrap();

            for (doc_id, doc) in &docs[batch_start..batch_end] {
                bm25.insert_doc(&mut txn, *doc_id, doc).unwrap();
            }

            txn.commit().unwrap();
            eprintln!(
                "[{test_name}]   inserted {batch_end}/{total} docs (RSS: {:.0}MB)",
                process_rss_mb()
            );
            batch_start = batch_end;
        }
    }

    // ─── Test 1: Vector search at vault scale ────────────────────────────────

    #[test]
    fn bench_vector_search_vault_scale() {
        let test = "vec-search";
        let vault_sizes: &[usize] = &[1_000, 5_000, 10_000, 50_000, 100_000];
        let dim = 768;
        let k = 10;
        let n_warmup = 100;
        let n_queries = 1_000;
        let n_gt_queries = 100;

        println!("\n=== Vector Search — Vault-Scale Benchmarks ===");
        println!(
            "{:<10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>10}",
            "Vault", "p50", "p90", "p99", "mean", "QPS", "Recall@10", "Disk"
        );
        println!("{}", "-".repeat(92));

        for &n in vault_sizes {
            phase(test, &format!("--- starting {n} vectors ---"));

            phase(test, &format!("generating {n} vectors ({dim}d)"));
            let vecs = gen_sim_vecs(n, dim, 0.3);
            let queries = gen_query_vecs(&vecs, n_queries + n_warmup, 0.05);

            let map_mb = map_size_for_vectors(n, dim);
            phase(test, &format!("creating LMDB env ({map_mb}MB map)"));
            let (env, temp_dir) = setup_temp_env(map_mb);
            let mut txn = env.write_txn().unwrap();
            let index =
                VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();
            txn.commit().unwrap();

            phase(test, &format!("inserting {n} vectors"));
            let t0 = Instant::now();
            let inserted = insert_vectors_batched(&env, &index, &vecs, test);
            let insert_elapsed = t0.elapsed();
            phase(test, &format!("insert done in {:.1}s", insert_elapsed.as_secs_f64()));

            phase(test, &format!("computing ground truth ({n_gt_queries} queries × {n} vecs)"));
            let gt_queries: Vec<(usize, Vec<f64>)> = queries[n_warmup..n_warmup + n_gt_queries]
                .iter()
                .enumerate()
                .map(|(i, q)| (i, q.clone()))
                .collect();
            let t0 = Instant::now();
            let ground_truths = calc_ground_truths(&inserted, &gt_queries, k);
            phase(test, &format!("ground truth done in {:.1}s", t0.elapsed().as_secs_f64()));

            drop(inserted);
            drop(vecs);
            phase(test, "freed insert data");

            phase(test, &format!("warmup ({n_warmup} queries)"));
            for i in 0..n_warmup {
                let arena = Bump::new();
                let txn = env.read_txn().unwrap();
                let q = arena.alloc_slice_copy(queries[i].as_slice());
                let label: &str = arena.alloc_str("vector");
                let _ = index.search::<Filter>(&txn, q, k, label, None, false, &arena);
            }

            phase(test, &format!("running {n_queries} timed queries"));
            let mut collector = LatencyCollector::with_capacity(n_queries);
            for i in 0..n_queries {
                let arena = Bump::new();
                let txn = env.read_txn().unwrap();
                let q = arena.alloc_slice_copy(queries[n_warmup + i].as_slice());
                let label: &str = arena.alloc_str("vector");

                let start = Instant::now();
                let _ = index
                    .search::<Filter>(&txn, q, k, label, None, false, &arena)
                    .unwrap();
                collector.record(start.elapsed());
            }

            phase(test, &format!("computing recall ({n_gt_queries} queries)"));
            let mut total_recall = 0.0;
            for (qid, query) in &gt_queries {
                let arena = Bump::new();
                let txn = env.read_txn().unwrap();
                let q = arena.alloc_slice_copy(query.as_slice());
                let label: &str = arena.alloc_str("vector");
                let results = index
                    .search::<Filter>(&txn, q, k, label, None, false, &arena)
                    .unwrap();

                let result_ids: HashSet<u128> = results.iter().map(|v| v.id).collect();
                let gt_ids: HashSet<u128> =
                    ground_truths[qid].iter().copied().collect();
                let hits = result_ids.intersection(&gt_ids).count();
                total_recall += hits as f64 / gt_ids.len() as f64;
            }
            let avg_recall = total_recall / n_gt_queries as f64;

            let disk = lmdb_disk_usage(&temp_dir);

            println!(
                "{:<10} {:>10} {:>10} {:>10} {:>10} {:>10.0} {:>11.1}% {:>9.1}MB",
                format!("{}K", n / 1000),
                fmt_dur(collector.p50()),
                fmt_dur(collector.p90()),
                fmt_dur(collector.p99()),
                fmt_dur(collector.mean()),
                collector.qps(),
                avg_recall * 100.0,
                disk as f64 / (1024.0 * 1024.0),
            );
            phase(test, &format!("--- done {n} vectors ---\n"));
        }
    }

    // ─── Test 2: BM25 search at vault scale ──────────────────────────────────

    #[test]
    fn bench_bm25_search_vault_scale() {
        let test = "bm25";
        let vault_sizes: &[usize] = &[1_000, 5_000, 10_000, 50_000, 100_000];
        let n_warmup = 100;
        let n_queries = 1_000;

        println!("\n=== BM25 Search — Vault-Scale Benchmarks ===");
        println!(
            "{:<10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
            "Vault", "p50", "p90", "p99", "mean", "QPS", "Disk"
        );
        println!("{}", "-".repeat(72));

        for &n in vault_sizes {
            phase(test, &format!("--- starting {n} docs ---"));

            phase(test, &format!("generating {n} docs"));
            let docs = gen_bm25_docs(n);
            let queries = gen_bm25_queries(n_queries + n_warmup);

            let map_mb = map_size_for_bm25(n);
            phase(test, &format!("creating LMDB env ({map_mb}MB map)"));
            let (env, temp_dir) = setup_temp_env(map_mb);
            let mut wtxn = env.write_txn().unwrap();
            let bm25 = HBM25Config::new(&env, &mut wtxn).unwrap();
            wtxn.commit().unwrap();

            phase(test, &format!("inserting {n} docs"));
            let t0 = Instant::now();
            insert_bm25_batched(&env, &bm25, &docs, test);
            phase(test, &format!("insert done in {:.1}s", t0.elapsed().as_secs_f64()));

            phase(test, &format!("warmup ({n_warmup} queries)"));
            for i in 0..n_warmup {
                let arena = Bump::new();
                let txn = env.read_txn().unwrap();
                let _ = bm25.search(&txn, &queries[i], 10, &arena);
            }

            phase(test, &format!("running {n_queries} timed queries"));
            let mut collector = LatencyCollector::with_capacity(n_queries);
            for i in 0..n_queries {
                let arena = Bump::new();
                let txn = env.read_txn().unwrap();

                let start = Instant::now();
                let _ = bm25.search(&txn, &queries[n_warmup + i], 10, &arena).unwrap();
                collector.record(start.elapsed());
            }

            let disk = lmdb_disk_usage(&temp_dir);

            println!(
                "{:<10} {:>10} {:>10} {:>10} {:>10} {:>10.0} {:>9.1}MB",
                format!("{}K", n / 1000),
                fmt_dur(collector.p50()),
                fmt_dur(collector.p90()),
                fmt_dur(collector.p99()),
                fmt_dur(collector.mean()),
                collector.qps(),
                disk as f64 / (1024.0 * 1024.0),
            );
            phase(test, &format!("--- done {n} docs ---\n"));
        }
    }

    // ─── Test 3: Hybrid RRF (vector + BM25) at vault scale ──────────────────

    #[test]
    fn bench_hybrid_rrf_vault_scale() {
        let test = "hybrid-rrf";
        let vault_sizes: &[usize] = &[1_000, 10_000, 50_000];
        let dim = 768;
        let k = 10;
        let n_warmup = 50;
        let n_queries = 1_000;

        println!("\n=== Hybrid RRF (Vector + BM25) — Vault-Scale Benchmarks ===");
        println!(
            "{:<10} {:>10} {:>10} {:>10} {:>10} {:>10}",
            "Vault", "p50", "p90", "p99", "mean", "QPS"
        );
        println!("{}", "-".repeat(62));

        for &n in vault_sizes {
            phase(test, &format!("--- starting {n} docs ---"));

            phase(test, &format!("generating {n} vectors + docs"));
            let vecs = gen_sim_vecs(n, dim, 0.3);
            let vec_queries = gen_query_vecs(&vecs, n_queries + n_warmup, 0.05);
            let bm25_docs = gen_bm25_docs(n);
            let bm25_queries = gen_bm25_queries(n_queries + n_warmup);

            let vec_map_mb = map_size_for_vectors(n, dim);
            phase(test, &format!("setting up vector index ({vec_map_mb}MB map)"));
            let (vec_env, _vec_dir) = setup_temp_env(vec_map_mb);
            {
                let mut txn = vec_env.write_txn().unwrap();
                let index = VectorCore::new(
                    &vec_env,
                    &mut txn,
                    HNSWConfig::new(None, None, None),
                )
                .unwrap();
                txn.commit().unwrap();
                insert_vectors_batched_no_gt(&vec_env, &index, &vecs, test);
            }
            drop(vecs);

            let bm25_map_mb = map_size_for_bm25(n);
            phase(test, &format!("setting up BM25 index ({bm25_map_mb}MB map)"));
            let (bm25_env, _bm25_dir) = setup_temp_env(bm25_map_mb);
            let bm25 = {
                let mut wtxn = bm25_env.write_txn().unwrap();
                let bm25 = HBM25Config::new(&bm25_env, &mut wtxn).unwrap();
                wtxn.commit().unwrap();
                insert_bm25_batched(&bm25_env, &bm25, &bm25_docs, test);
                bm25
            };

            let mut setup_txn = vec_env.write_txn().unwrap();
            let vec_index = VectorCore::new(
                &vec_env,
                &mut setup_txn,
                HNSWConfig::new(None, None, None),
            )
            .unwrap();
            setup_txn.commit().unwrap();

            phase(test, &format!("warmup ({n_warmup} queries)"));
            for i in 0..n_warmup {
                let arena = Bump::new();
                let vtxn = vec_env.read_txn().unwrap();
                let q = arena.alloc_slice_copy(vec_queries[i].as_slice());
                let label: &str = arena.alloc_str("vector");
                let _ = vec_index.search::<Filter>(&vtxn, q, k, label, None, false, &arena);

                let btxn = bm25_env.read_txn().unwrap();
                let _ = bm25.search(&btxn, &bm25_queries[i], k, &arena);
            }

            phase(test, &format!("running {n_queries} timed hybrid queries"));
            let mut collector = LatencyCollector::with_capacity(n_queries);
            for i in 0..n_queries {
                let qi = n_warmup + i;

                let start = Instant::now();

                let arena = Bump::new();
                let vtxn = vec_env.read_txn().unwrap();
                let q = arena.alloc_slice_copy(vec_queries[qi].as_slice());
                let label: &str = arena.alloc_str("vector");
                let vec_results = vec_index
                    .search::<Filter>(&vtxn, q, k, label, None, false, &arena)
                    .unwrap();

                let btxn = bm25_env.read_txn().unwrap();
                let bm25_results = bm25.search(&btxn, &bm25_queries[qi], k, &arena).unwrap();

                let rrf_k = 60.0;
                let mut scores: HashMap<u128, f64> = HashMap::new();
                for (rank, v) in vec_results.iter().enumerate() {
                    *scores.entry(v.id).or_default() += 1.0 / (rrf_k + rank as f64 + 1.0);
                }
                for (rank, (doc_id, _score)) in bm25_results.iter().enumerate() {
                    *scores.entry(*doc_id).or_default() += 1.0 / (rrf_k + rank as f64 + 1.0);
                }
                let mut fused: Vec<_> = scores.into_iter().collect();
                fused.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                fused.truncate(k);

                collector.record(start.elapsed());
            }

            println!(
                "{:<10} {:>10} {:>10} {:>10} {:>10} {:>10.0}",
                format!("{}K", n / 1000),
                fmt_dur(collector.p50()),
                fmt_dur(collector.p90()),
                fmt_dur(collector.p99()),
                fmt_dur(collector.mean()),
                collector.qps(),
            );
            phase(test, &format!("--- done {n} docs ---\n"));
        }
    }

    // ─── Test 4: PPR pipeline (stub — uses ppr() without storage) ───────────

    #[test]
    fn bench_ppr_pipeline_vault_scale() {
        use helix_db::helix_engine::graph::ppr::ppr;

        let test = "ppr-pipeline";
        let vault_sizes: &[usize] = &[1_000, 10_000, 50_000];
        let dim = 768;
        let k = 10;
        let n_queries = 1_000;

        println!("\n=== PPR Pipeline (Hybrid + PPR) — Vault-Scale Benchmarks ===");
        println!(
            "{:<10} {:>10} {:>10} {:>10} {:>10} {:>10}",
            "Vault", "p50", "p90", "p99", "mean", "QPS"
        );
        println!("{}", "-".repeat(62));

        for &n in vault_sizes {
            phase(test, &format!("--- starting {n} docs ---"));

            phase(test, &format!("generating {n} vectors + docs"));
            let vecs = gen_sim_vecs(n, dim, 0.3);
            let vec_queries = gen_query_vecs(&vecs, n_queries, 0.05);
            let bm25_docs = gen_bm25_docs(n);
            let bm25_queries = gen_bm25_queries(n_queries);

            let vec_map_mb = map_size_for_vectors(n, dim);
            phase(test, &format!("setting up vector index ({vec_map_mb}MB map)"));
            let (vec_env, _vec_dir) = setup_temp_env(vec_map_mb);
            let mut txn = vec_env.write_txn().unwrap();
            let vec_index =
                VectorCore::new(&vec_env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();
            txn.commit().unwrap();
            let inserted_vecs = insert_vectors_batched(&vec_env, &vec_index, &vecs, test);
            drop(vecs);

            let bm25_map_mb = map_size_for_bm25(n);
            phase(test, &format!("setting up BM25 index ({bm25_map_mb}MB map)"));
            let (bm25_env, _bm25_dir) = setup_temp_env(bm25_map_mb);
            let bm25 = {
                let mut wtxn = bm25_env.write_txn().unwrap();
                let bm25 = HBM25Config::new(&bm25_env, &mut wtxn).unwrap();
                wtxn.commit().unwrap();
                insert_bm25_batched(&bm25_env, &bm25, &bm25_docs, test);
                bm25
            };

            let mut universe: HashSet<u128> = bm25_docs.iter().map(|(id, _)| *id).collect();
            universe.extend(inserted_vecs.iter().map(|(id, _)| *id));
            drop(inserted_vecs);

            phase(test, &format!("running {n_queries} timed pipeline queries"));
            let mut collector = LatencyCollector::with_capacity(n_queries);
            let edge_weights: HashMap<String, f64> = HashMap::new();

            for i in 0..n_queries {
                let start = Instant::now();

                let arena = Bump::new();
                let vtxn = vec_env.read_txn().unwrap();
                let q = arena.alloc_slice_copy(vec_queries[i].as_slice());
                let label: &str = arena.alloc_str("vector");
                let vec_results = vec_index
                    .search::<Filter>(&vtxn, q, k, label, None, false, &arena)
                    .unwrap();

                let btxn = bm25_env.read_txn().unwrap();
                let bm25_results = bm25.search(&btxn, &bm25_queries[i], k, &arena).unwrap();

                let rrf_k = 60.0;
                let mut scores: HashMap<u128, f64> = HashMap::new();
                for (rank, v) in vec_results.iter().enumerate() {
                    *scores.entry(v.id).or_default() += 1.0 / (rrf_k + rank as f64 + 1.0);
                }
                for (rank, (doc_id, _)) in bm25_results.iter().enumerate() {
                    *scores.entry(*doc_id).or_default() += 1.0 / (rrf_k + rank as f64 + 1.0);
                }
                let mut fused: Vec<_> = scores.into_iter().collect();
                fused.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                fused.truncate(k * 2);

                let seeds: Vec<u128> = fused.iter().map(|(id, _)| *id).collect();
                let _ppr_results = ppr(&universe, &seeds, &edge_weights, 3, 0.85, k);

                collector.record(start.elapsed());
            }

            println!(
                "{:<10} {:>10} {:>10} {:>10} {:>10} {:>10.0}",
                format!("{}K", n / 1000),
                fmt_dur(collector.p50()),
                fmt_dur(collector.p90()),
                fmt_dur(collector.p99()),
                fmt_dur(collector.mean()),
                collector.qps(),
            );
            phase(test, &format!("--- done {n} docs ---\n"));
        }
    }

    // ─── Test 5: Concurrent vaults ───────────────────────────────────────────

    #[test]
    fn bench_concurrent_vaults() {
        let test = "concurrent";
        let n_vaults = 5;
        let docs_per_vault = 5_000;
        let dim = 768;
        let k = 10;
        let duration_secs = 15;

        println!("\n=== Concurrent Vaults — {} vaults x {}K docs ===", n_vaults, docs_per_vault / 1000);
        phase(test, &format!("will create {n_vaults} vaults with {docs_per_vault} docs each"));

        let vault_data: Vec<((Env, tempfile::TempDir), Vec<Vec<f64>>)> =
            thread::scope(|s| {
                let handles: Vec<_> = (0..n_vaults)
                    .map(|i| {
                        s.spawn(move || {
                            phase(test, &format!("populating vault {i}"));
                            let vecs = gen_sim_vecs(docs_per_vault, dim, 0.3);
                            let queries = gen_query_vecs(&vecs, 2_000, 0.05);

                            let map_mb = map_size_for_vectors(docs_per_vault, dim);
                            let (env, dir) = setup_temp_env(map_mb);
                            let mut txn = env.write_txn().unwrap();
                            let index = VectorCore::new(
                                &env,
                                &mut txn,
                                HNSWConfig::new(None, None, None),
                            )
                            .unwrap();
                            txn.commit().unwrap();
                            insert_vectors_batched_no_gt(&env, &index, &vecs, test);
                            phase(test, &format!("vault {i} ready"));
                            ((env, dir), queries)
                        })
                    })
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });

        let (vault_envs, vault_queries): (Vec<_>, Vec<_>) =
            vault_data.into_iter().unzip();

        phase(test, &format!("starting {duration_secs}s concurrent query phase"));

        let per_vault_latencies: Arc<Vec<Mutex<LatencyCollector>>> = Arc::new(
            (0..n_vaults)
                .map(|_| Mutex::new(LatencyCollector::new()))
                .collect(),
        );
        let total_queries = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let deadline = Instant::now() + Duration::from_secs(duration_secs);

        thread::scope(|s| {
            for vault_idx in 0..n_vaults {
                let env = &vault_envs[vault_idx].0;
                let queries = &vault_queries[vault_idx];
                let latencies = Arc::clone(&per_vault_latencies);
                let total = Arc::clone(&total_queries);

                s.spawn(move || {
                    let mut setup_txn = env.write_txn().unwrap();
                    let index = VectorCore::new(
                        env,
                        &mut setup_txn,
                        HNSWConfig::new(None, None, None),
                    )
                    .unwrap();
                    setup_txn.commit().unwrap();

                    let mut qi = 0;
                    while Instant::now() < deadline {
                        let arena = Bump::new();
                        let txn = env.read_txn().unwrap();
                        let q = arena.alloc_slice_copy(queries[qi % queries.len()].as_slice());
                        let label: &str = arena.alloc_str("vector");

                        let start = Instant::now();
                        let _ = index
                            .search::<Filter>(&txn, q, k, label, None, false, &arena)
                            .unwrap();
                        let elapsed = start.elapsed();

                        latencies[vault_idx].lock().unwrap().record(elapsed);
                        total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        qi += 1;
                    }
                });
            }
        });

        let total_q = total_queries.load(std::sync::atomic::Ordering::Relaxed);
        let aggregate_qps = total_q as f64 / duration_secs as f64;

        println!(
            "\n{:<10} {:>10} {:>10} {:>10} {:>10}",
            "Vault", "p50", "p90", "p99", "mean"
        );
        println!("{}", "-".repeat(52));

        for i in 0..n_vaults {
            let lat = per_vault_latencies[i].lock().unwrap();
            println!(
                "{:<10} {:>10} {:>10} {:>10} {:>10}",
                format!("vault-{i}"),
                fmt_dur(lat.p50()),
                fmt_dur(lat.p90()),
                fmt_dur(lat.p99()),
                fmt_dur(lat.mean()),
            );
        }

        println!("\nAggregate: {} total queries, {:.0} QPS across {} vaults",
            total_q, aggregate_qps, n_vaults);
        phase(test, "done");
    }

    // ─── Test 6: ef parameter comparison ─────────────────────────────────────

    #[test]
    fn bench_vector_search_ef_comparison() {
        let test = "ef-compare";
        let n = 50_000;
        let dim = 768;
        let k = 10;
        let n_queries = 1_000;
        let n_warmup = 100;
        let n_gt_queries = 100;

        phase(test, &format!("generating {n} vectors ({dim}d)"));
        let vecs = gen_sim_vecs(n, dim, 0.3);
        let queries = gen_query_vecs(&vecs, n_queries + n_warmup, 0.05);

        println!("\n=== ef Comparison at 50K docs ===");
        println!(
            "{:<10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12}",
            "ef", "p50", "p90", "p99", "mean", "QPS", "Recall@10"
        );
        println!("{}", "-".repeat(82));

        for ef_val in [128_usize, 256, 512] {
            phase(test, &format!("--- ef={ef_val} ---"));

            let map_mb = map_size_for_vectors(n, dim);
            phase(test, &format!("creating index ({map_mb}MB map)"));
            let (env, _dir) = setup_temp_env(map_mb);
            let mut txn = env.write_txn().unwrap();
            let config = HNSWConfig::new(None, None, Some(ef_val));
            let index = VectorCore::new(&env, &mut txn, config).unwrap();
            txn.commit().unwrap();

            phase(test, "inserting vectors");
            let inserted = insert_vectors_batched(&env, &index, &vecs, test);

            phase(test, "computing ground truth");
            let gt_queries: Vec<(usize, Vec<f64>)> = queries[n_warmup..n_warmup + n_gt_queries]
                .iter()
                .enumerate()
                .map(|(i, q)| (i, q.clone()))
                .collect();
            let ground_truths = calc_ground_truths(&inserted, &gt_queries, k);
            drop(inserted);

            phase(test, "warmup");
            for i in 0..n_warmup {
                let arena = Bump::new();
                let txn = env.read_txn().unwrap();
                let q = arena.alloc_slice_copy(queries[i].as_slice());
                let label: &str = arena.alloc_str("vector");
                let _ = index.search::<Filter>(&txn, q, k, label, None, false, &arena);
            }

            phase(test, &format!("running {n_queries} timed queries"));
            let mut collector = LatencyCollector::with_capacity(n_queries);
            for i in 0..n_queries {
                let arena = Bump::new();
                let txn = env.read_txn().unwrap();
                let q = arena.alloc_slice_copy(queries[n_warmup + i].as_slice());
                let label: &str = arena.alloc_str("vector");

                let start = Instant::now();
                let _ = index
                    .search::<Filter>(&txn, q, k, label, None, false, &arena)
                    .unwrap();
                collector.record(start.elapsed());
            }

            phase(test, "computing recall");
            let mut total_recall = 0.0;
            for (qid, query) in &gt_queries {
                let arena = Bump::new();
                let txn = env.read_txn().unwrap();
                let q = arena.alloc_slice_copy(query.as_slice());
                let label: &str = arena.alloc_str("vector");
                let results = index
                    .search::<Filter>(&txn, q, k, label, None, false, &arena)
                    .unwrap();

                let result_ids: HashSet<u128> = results.iter().map(|v| v.id).collect();
                let gt_ids: HashSet<u128> =
                    ground_truths[qid].iter().copied().collect();
                let hits = result_ids.intersection(&gt_ids).count();
                total_recall += hits as f64 / gt_ids.len() as f64;
            }
            let avg_recall = total_recall / n_gt_queries as f64;

            println!(
                "{:<10} {:>10} {:>10} {:>10} {:>10} {:>10.0} {:>11.1}%",
                ef_val,
                fmt_dur(collector.p50()),
                fmt_dur(collector.p90()),
                fmt_dur(collector.p99()),
                fmt_dur(collector.mean()),
                collector.qps(),
                avg_recall * 100.0,
            );
            phase(test, &format!("--- done ef={ef_val} ---\n"));
        }
    }

    // ─── Summary comparison table ────────────────────────────────────────────

    #[test]
    fn print_comparison_header() {
        println!("\n{}", "=".repeat(92));
        println!("=== HelixDB Vault-Scale Benchmark Results ===");
        println!("Machine: {} cores, Linux", num_cpus::get());
        println!("Vectors: 768-dim f64 (synthetic, similarity=0.3)");
        println!("HNSW config: m=16, ef_construct=128, ef=512");
        println!("{}", "=".repeat(92));
        println!();
        println!("Run individual benchmarks:");
        println!("  cargo test --test vault_scale_benches --features bench --release -- bench_vector_search_vault_scale --nocapture");
        println!("  cargo test --test vault_scale_benches --features bench --release -- bench_bm25_search_vault_scale --nocapture");
        println!("  cargo test --test vault_scale_benches --features bench --release -- bench_hybrid_rrf_vault_scale --nocapture");
        println!("  cargo test --test vault_scale_benches --features bench --release -- bench_ppr_pipeline_vault_scale --nocapture");
        println!("  cargo test --test vault_scale_benches --features bench --release -- bench_concurrent_vaults --nocapture");
        println!("  cargo test --test vault_scale_benches --features bench --release -- bench_vector_search_ef_comparison --nocapture");
        println!();
        println!("Turbopuffer reference (768-dim, 1M docs):");
        println!("  Vec search: p50=8ms, p90=10ms, p99=35ms");
        println!("  BM25 search: p50=343ms, p90=444ms");
        println!("  QPS: 32 (vec), recall: 90-100%");
        println!();
        println!("NOTE: HelixDB uses f64 vectors (2x memory vs turbopuffer's likely f32).");
        println!("Full pipeline = vector + BM25 + RRF + PPR — turbopuffer has no equivalent.");
    }
}
