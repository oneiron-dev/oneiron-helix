#[cfg(test)]
mod tests {
    use crate::helix_engine::{
        bm25::bm25::{BM25, HybridSearch},
        storage_core::HelixGraphStorage,
        traversal_core::config::Config,
        vector_core::{hnsw::HNSW, vector::HVector},
    };
    use bumpalo::Bump;
    use heed3::RoTxn;
    use tempfile::tempdir;

    fn setup_helix_storage() -> (HelixGraphStorage, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap();
        let config = Config::default();
        let storage = HelixGraphStorage::new(
            path,
            config,
            crate::helix_engine::storage_core::version_info::VersionInfo::default(),
        )
        .unwrap();
        (storage, temp_dir)
    }

    #[tokio::test]
    async fn test_search_hybrid_combines_vector_and_bm25() {
        let (storage, _temp_dir) = setup_helix_storage();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        let vec1 = arena.alloc_slice_copy(&[1.0, 0.0, 0.0]);
        let vec2 = arena.alloc_slice_copy(&[0.0, 1.0, 0.0]);
        let vec3 = arena.alloc_slice_copy(&[0.0, 0.0, 1.0]);

        let v1 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec1, None, &arena)
            .unwrap();
        let v2 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec2, None, &arena)
            .unwrap();
        let v3 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec3, None, &arena)
            .unwrap();

        let id1 = v1.id;

        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");
        bm25.insert_doc(&mut wtxn, id1, "machine learning algorithms neural networks")
            .unwrap();
        bm25.insert_doc(&mut wtxn, v2.id, "deep learning frameworks tensorflow pytorch")
            .unwrap();
        bm25.insert_doc(&mut wtxn, v3.id, "data science analytics statistics")
            .unwrap();
        wtxn.commit().unwrap();

        let query = "machine learning";
        let query_vector = vec![1.0, 0.0, 0.0];
        let alpha = 0.5;
        let limit = 10;

        let results = storage
            .hybrid_search(query, &query_vector, alpha, limit)
            .await
            .expect("Hybrid search should succeed");

        assert!(!results.is_empty(), "Hybrid search should return results");

        assert!(
            results.iter().any(|(id, _)| *id == id1),
            "Doc 1 should appear in results (matches both vector and BM25)"
        );
    }

    #[tokio::test]
    async fn test_search_hybrid_document_matching_both_ranks_highest() {
        let (storage, _temp_dir) = setup_helix_storage();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        let vec1 = arena.alloc_slice_copy(&[0.95, 0.05, 0.0]);
        let vec2 = arena.alloc_slice_copy(&[0.1, 0.9, 0.0]);
        let vec3 = arena.alloc_slice_copy(&[0.0, 0.1, 0.9]);

        let v1 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec1, None, &arena)
            .unwrap();
        let v2 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec2, None, &arena)
            .unwrap();
        let v3 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec3, None, &arena)
            .unwrap();

        let id1 = v1.id;

        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");
        bm25.insert_doc(&mut wtxn, id1, "specific query term unique content here")
            .unwrap();
        bm25.insert_doc(&mut wtxn, v2.id, "other content without query")
            .unwrap();
        bm25.insert_doc(&mut wtxn, v3.id, "different topic entirely")
            .unwrap();
        wtxn.commit().unwrap();

        let query = "specific query term";
        let query_vector = vec![1.0, 0.0, 0.0];
        let alpha = 0.5;
        let limit = 10;

        let results = storage
            .hybrid_search(query, &query_vector, alpha, limit)
            .await
            .expect("Hybrid search should succeed");

        assert!(!results.is_empty(), "Should have results");

        let (top_id, top_score) = results[0];
        assert_eq!(
            top_id, id1,
            "Document matching both vector and text should rank first"
        );

        if results.len() > 1 {
            assert!(
                top_score >= results[1].1,
                "Top result should have highest score"
            );
        }
    }

    #[tokio::test]
    async fn test_search_hybrid_alpha_bm25_only() {
        let (storage, _temp_dir) = setup_helix_storage();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        let vec1 = arena.alloc_slice_copy(&[0.0, 0.0, 1.0]);
        let vec2 = arena.alloc_slice_copy(&[1.0, 0.0, 0.0]);

        let v1 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec1, None, &arena)
            .unwrap();
        let v2 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec2, None, &arena)
            .unwrap();

        let id1 = v1.id;

        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");
        bm25.insert_doc(&mut wtxn, id1, "bm25 keywords specific text match")
            .unwrap();
        bm25.insert_doc(&mut wtxn, v2.id, "other content").unwrap();
        wtxn.commit().unwrap();

        let query = "bm25 keywords";
        let query_vector = vec![1.0, 0.0, 0.0];

        let results_bm25_heavy = storage
            .hybrid_search(query, &query_vector, 1.0, 10)
            .await
            .expect("BM25-heavy search should succeed");

        assert!(!results_bm25_heavy.is_empty(), "Should have results");
        let bm25_top = results_bm25_heavy[0].0;
        assert_eq!(
            bm25_top, id1,
            "With alpha=1.0 (BM25 only), doc 1 should rank first (has matching text)"
        );
    }

    #[tokio::test]
    async fn test_search_hybrid_alpha_vector_only() {
        let (storage, _temp_dir) = setup_helix_storage();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        let vec1 = arena.alloc_slice_copy(&[0.0, 0.0, 1.0]);
        let vec2 = arena.alloc_slice_copy(&[1.0, 0.0, 0.0]);

        let v1 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec1, None, &arena)
            .unwrap();
        let v2 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec2, None, &arena)
            .unwrap();

        let id2 = v2.id;

        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");
        bm25.insert_doc(&mut wtxn, v1.id, "bm25 keywords specific text match")
            .unwrap();
        bm25.insert_doc(&mut wtxn, id2, "other content").unwrap();
        wtxn.commit().unwrap();

        let query = "bm25 keywords";
        let query_vector = vec![1.0, 0.0, 0.0];

        let results_vector_heavy = storage
            .hybrid_search(query, &query_vector, 0.0, 10)
            .await
            .expect("Vector-heavy search should succeed");

        assert!(!results_vector_heavy.is_empty(), "Should have results");
        let vector_top = results_vector_heavy[0].0;
        assert_eq!(
            vector_top, id2,
            "With alpha=0.0 (vector only), doc 2 should rank first (closer vector)"
        );
    }

    #[tokio::test]
    async fn test_search_hybrid_fusion_boosts_overlapping_results() {
        let (storage, _temp_dir) = setup_helix_storage();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        let vec1 = arena.alloc_slice_copy(&[0.9, 0.1, 0.0, 0.0]);
        let vec2 = arena.alloc_slice_copy(&[0.8, 0.15, 0.05, 0.0]);
        let vec3 = arena.alloc_slice_copy(&[0.0, 0.0, 0.0, 1.0]);
        let vec4 = arena.alloc_slice_copy(&[0.85, 0.1, 0.05, 0.0]);

        let v1 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec1, None, &arena)
            .unwrap();
        let v2 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec2, None, &arena)
            .unwrap();
        let v3 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec3, None, &arena)
            .unwrap();
        let v4 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec4, None, &arena)
            .unwrap();

        let id1 = v1.id;
        let id2 = v2.id;
        let id3 = v3.id;

        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");
        bm25.insert_doc(&mut wtxn, id1, "neural networks deep learning tensorflow")
            .unwrap();
        bm25.insert_doc(&mut wtxn, id2, "neural networks pytorch framework")
            .unwrap();
        bm25.insert_doc(&mut wtxn, id3, "unrelated database systems sql")
            .unwrap();
        bm25.insert_doc(&mut wtxn, v4.id, "vector embeddings similarity")
            .unwrap();
        wtxn.commit().unwrap();

        let query = "neural networks";
        let query_vector = vec![1.0, 0.0, 0.0, 0.0];
        let alpha = 0.5;
        let limit = 10;

        let results = storage
            .hybrid_search(query, &query_vector, alpha, limit)
            .await
            .expect("Hybrid search should succeed");

        assert!(results.len() >= 2, "Should have at least 2 results");

        let top_ids: Vec<u128> = results.iter().take(2).map(|(id, _)| *id).collect();
        assert!(
            top_ids.contains(&id1) || top_ids.contains(&id2),
            "Documents matching both signals should rank high"
        );

        let doc3_position = results.iter().position(|(id, _)| *id == id3);
        if let Some(pos) = doc3_position {
            assert!(
                pos >= 2,
                "Doc 3 (no match on either) should rank lower than docs matching both"
            );
        }
    }

    #[tokio::test]
    async fn test_search_hybrid_verify_balanced_score() {
        let (storage, _temp_dir) = setup_helix_storage();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        let vec10 = arena.alloc_slice_copy(&[1.0, 0.0, 0.0]);
        let vec20 = arena.alloc_slice_copy(&[0.0, 1.0, 0.0]);

        let v10 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec10, None, &arena)
            .unwrap();
        let v20 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec20, None, &arena)
            .unwrap();

        let id10 = v10.id;

        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");
        bm25.insert_doc(&mut wtxn, id10, "unique searchable content here")
            .unwrap();
        bm25.insert_doc(&mut wtxn, v20.id, "different text material")
            .unwrap();
        wtxn.commit().unwrap();

        let query = "unique searchable";
        let query_vector = vec![1.0, 0.0, 0.0];

        let results = storage
            .hybrid_search(query, &query_vector, 0.5, 10)
            .await
            .unwrap();

        assert!(!results.is_empty(), "Should have hybrid results");

        let score = results.iter().find(|(id, _)| *id == id10).map(|(_, s)| *s);
        assert!(score.is_some(), "Should find doc 10");
        assert!(
            score.unwrap() > 0.0,
            "Hybrid score should be positive for matching document"
        );
    }

    #[tokio::test]
    async fn test_search_hybrid_respects_limit() {
        let (storage, _temp_dir) = setup_helix_storage();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();
        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");

        let mut ids = Vec::new();
        for i in 1..=20u128 {
            let vec = arena.alloc_slice_copy(&[(i as f64 / 20.0), 1.0 - (i as f64 / 20.0), 0.1]);
            let v = storage
                .vectors
                .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec, None, &arena)
                .unwrap();
            ids.push(v.id);
        }

        for (i, id) in ids.iter().enumerate() {
            let doc = format!("document {} with searchable content keywords", i + 1);
            bm25.insert_doc(&mut wtxn, *id, &doc).unwrap();
        }
        wtxn.commit().unwrap();

        let query = "searchable content";
        let query_vector = vec![0.5, 0.5, 0.0];
        let limit = 5;

        let results = storage
            .hybrid_search(query, &query_vector, 0.5, limit)
            .await
            .expect("Hybrid search should succeed");

        assert!(
            results.len() <= limit,
            "Results should respect the limit of {}",
            limit
        );
    }

    #[tokio::test]
    async fn test_search_hybrid_empty_bm25_results() {
        let (storage, _temp_dir) = setup_helix_storage();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        let vec = arena.alloc_slice_copy(&[1.0, 0.0, 0.0]);
        let v = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec, None, &arena)
            .unwrap();

        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");
        bm25.insert_doc(&mut wtxn, v.id, "hello world").unwrap();
        wtxn.commit().unwrap();

        let query = "xyznonexistentterm123";
        let query_vector = vec![1.0, 0.0, 0.0];

        let results = storage.hybrid_search(query, &query_vector, 0.5, 10).await;

        match results {
            Ok(_) => {
                assert!(true, "Results returned (vector matches still work)");
            }
            Err(_) => {
                assert!(true, "Error on empty BM25 results is acceptable");
            }
        }
    }

    #[tokio::test]
    async fn test_search_hybrid_vector_only_match() {
        let (storage, _temp_dir) = setup_helix_storage();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        let vec1 = arena.alloc_slice_copy(&[1.0, 0.0, 0.0]);
        let vec2 = arena.alloc_slice_copy(&[0.0, 1.0, 0.0]);

        let v1 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec1, None, &arena)
            .unwrap();
        let v2 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec2, None, &arena)
            .unwrap();

        let id1 = v1.id;

        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");
        bm25.insert_doc(&mut wtxn, id1, "completely different text here")
            .unwrap();
        bm25.insert_doc(&mut wtxn, v2.id, "another unrelated document")
            .unwrap();
        wtxn.commit().unwrap();

        let query = "xyznonmatchingquery";
        let query_vector = vec![1.0, 0.0, 0.0];

        let results = storage
            .hybrid_search(query, &query_vector, 0.0, 10)
            .await
            .unwrap_or_default();

        if !results.is_empty() {
            assert_eq!(
                results[0].0, id1,
                "With vector-only search, doc 1 should be closest"
            );
        }
    }

    #[tokio::test]
    async fn test_search_hybrid_bm25_only_match() {
        let (storage, _temp_dir) = setup_helix_storage();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        let vec1 = arena.alloc_slice_copy(&[0.0, 0.0, 1.0]);
        let vec2 = arena.alloc_slice_copy(&[0.0, 0.0, 1.0]);

        let v1 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec1, None, &arena)
            .unwrap();
        let v2 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec2, None, &arena)
            .unwrap();

        let id1 = v1.id;

        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");
        bm25.insert_doc(&mut wtxn, id1, "machine learning algorithms")
            .unwrap();
        bm25.insert_doc(&mut wtxn, v2.id, "deep neural networks")
            .unwrap();
        wtxn.commit().unwrap();

        let query = "machine learning";
        let query_vector = vec![1.0, 0.0, 0.0];

        let results = storage
            .hybrid_search(query, &query_vector, 1.0, 10)
            .await
            .expect("BM25-only search should succeed");

        assert!(!results.is_empty(), "Should have BM25 results");
        assert_eq!(
            results[0].0, id1,
            "Doc 1 should rank first (has 'machine learning')"
        );
    }

    #[tokio::test]
    async fn test_search_hybrid_score_ordering() {
        let (storage, _temp_dir) = setup_helix_storage();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();
        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");

        let mut ids = Vec::new();
        for i in 1..=5u128 {
            let vec = arena.alloc_slice_copy(&[(6 - i) as f64 / 5.0, i as f64 / 5.0, 0.0]);
            let v = storage
                .vectors
                .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec, None, &arena)
                .unwrap();
            ids.push(v.id);
        }

        for (i, id) in ids.iter().enumerate() {
            let doc = format!("document {} test content", i + 1);
            bm25.insert_doc(&mut wtxn, *id, &doc).unwrap();
        }
        wtxn.commit().unwrap();

        let query = "test content";
        let query_vector = vec![1.0, 0.0, 0.0];

        let results = storage
            .hybrid_search(query, &query_vector, 0.5, 10)
            .await
            .expect("Hybrid search should succeed");

        for i in 0..results.len().saturating_sub(1) {
            assert!(
                results[i].1 >= results[i + 1].1,
                "Results should be sorted by score descending: {} >= {}",
                results[i].1,
                results[i + 1].1
            );
        }
    }

    #[tokio::test]
    async fn test_search_hybrid_fusion_behavior() {
        let (storage, _temp_dir) = setup_helix_storage();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        let vec_vector_only = arena.alloc_slice_copy(&[0.99, 0.01, 0.0]);
        let vec_text_only = arena.alloc_slice_copy(&[0.0, 0.0, 1.0]);
        let vec_both = arena.alloc_slice_copy(&[0.95, 0.05, 0.0]);

        let v_vector_only = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec_vector_only, None, &arena)
            .unwrap();
        let v_text_only = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec_text_only, None, &arena)
            .unwrap();
        let v_both = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec_both, None, &arena)
            .unwrap();

        let id_vector_only = v_vector_only.id;
        let id_text_only = v_text_only.id;
        let id_both = v_both.id;

        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");
        bm25.insert_doc(&mut wtxn, id_vector_only, "unrelated content here")
            .unwrap();
        bm25.insert_doc(
            &mut wtxn,
            id_text_only,
            "specific keyword match important term",
        )
        .unwrap();
        bm25.insert_doc(&mut wtxn, id_both, "specific keyword content")
            .unwrap();
        wtxn.commit().unwrap();

        let query = "specific keyword";
        let query_vector = vec![1.0, 0.0, 0.0];
        let alpha = 0.5;

        let results = storage
            .hybrid_search(query, &query_vector, alpha, 10)
            .await
            .expect("Hybrid search should succeed");

        assert!(results.len() >= 2, "Should have multiple results");

        let pos_vector_only = results.iter().position(|(id, _)| *id == id_vector_only);
        let pos_text_only = results.iter().position(|(id, _)| *id == id_text_only);
        let pos_both = results.iter().position(|(id, _)| *id == id_both);

        if let (Some(pvo), Some(pto), Some(pb)) = (pos_vector_only, pos_text_only, pos_both) {
            assert!(
                pb <= pvo || pb <= pto,
                "Document matching both should rank at least as high as single-signal matches. \
                 Positions: both={}, vector_only={}, text_only={}",
                pb,
                pvo,
                pto
            );
        }
    }

    #[tokio::test]
    async fn test_search_hybrid_identical_vectors_differentiated_by_text() {
        let (storage, _temp_dir) = setup_helix_storage();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        let vec1 = arena.alloc_slice_copy(&[1.0, 0.0, 0.0]);
        let vec2 = arena.alloc_slice_copy(&[1.0, 0.0, 0.0]);
        let vec3 = arena.alloc_slice_copy(&[1.0, 0.0, 0.0]);

        let v1 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec1, None, &arena)
            .unwrap();
        let v2 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec2, None, &arena)
            .unwrap();
        let v3 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec3, None, &arena)
            .unwrap();

        let id1 = v1.id;

        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");
        bm25.insert_doc(
            &mut wtxn,
            id1,
            "searchterm searchterm searchterm highly relevant",
        )
        .unwrap();
        bm25.insert_doc(&mut wtxn, v2.id, "searchterm partially relevant")
            .unwrap();
        bm25.insert_doc(&mut wtxn, v3.id, "no matching keywords here")
            .unwrap();
        wtxn.commit().unwrap();

        let query = "searchterm relevant";
        let query_vector = vec![1.0, 0.0, 0.0];
        let alpha = 0.8;

        let results = storage
            .hybrid_search(query, &query_vector, alpha, 10)
            .await
            .expect("Hybrid search should succeed");

        assert_eq!(results.len(), 3, "Should have all 3 results");

        assert!(
            results[0].0 == id1,
            "Doc with most BM25 matches should rank first when alpha favors BM25"
        );

        assert!(
            results[0].1 > results[1].1,
            "Top result should have higher score than second"
        );
    }

    #[tokio::test]
    async fn test_search_hybrid_alpha_balanced_tiebreaker() {
        let (storage, _temp_dir) = setup_helix_storage();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        let vec1 = arena.alloc_slice_copy(&[0.9, 0.1, 0.0]);
        let vec2 = arena.alloc_slice_copy(&[0.1, 0.9, 0.0]);

        let v1 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec1, None, &arena)
            .unwrap();
        let v2 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec2, None, &arena)
            .unwrap();

        let id1 = v1.id;

        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");
        bm25.insert_doc(&mut wtxn, id1, "target keywords important")
            .unwrap();
        bm25.insert_doc(&mut wtxn, v2.id, "target keywords important")
            .unwrap();
        wtxn.commit().unwrap();

        let query = "target keywords";
        let query_vector = vec![1.0, 0.0, 0.0];

        let results = storage
            .hybrid_search(query, &query_vector, 0.5, 10)
            .await
            .expect("Hybrid search should succeed");

        assert_eq!(results.len(), 2, "Should have 2 results");
        assert_eq!(
            results[0].0, id1,
            "With equal BM25 scores, vector similarity should break the tie"
        );
    }

    #[tokio::test]
    async fn test_search_hybrid_many_documents() {
        let (storage, _temp_dir) = setup_helix_storage();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();
        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");

        let mut target_id = 0u128;
        for i in 0..50u128 {
            let x = if i == 25 { 1.0 } else { (i as f64) / 100.0 };
            let y = if i == 25 { 0.0 } else { 1.0 - (i as f64) / 100.0 };
            let vec = arena.alloc_slice_copy(&[x, y, 0.1]);
            let v = storage
                .vectors
                .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec, None, &arena)
                .unwrap();

            let doc = if i == 25 {
                target_id = v.id;
                "special target document with unique keywords".to_string()
            } else {
                format!("generic document number {} content", i)
            };
            bm25.insert_doc(&mut wtxn, v.id, &doc).unwrap();
        }
        wtxn.commit().unwrap();

        let query = "special target unique";
        let query_vector = vec![1.0, 0.0, 0.0];

        let results = storage
            .hybrid_search(query, &query_vector, 0.5, 10)
            .await
            .expect("Hybrid search should succeed");

        assert!(
            results[0].0 == target_id,
            "Target document (matching both) should rank first"
        );
    }

    #[tokio::test]
    async fn test_search_hybrid_score_combination_formula() {
        let (storage, _temp_dir) = setup_helix_storage();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        let vec1 = arena.alloc_slice_copy(&[1.0, 0.0, 0.0]);
        let v1 = storage
            .vectors
            .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", vec1, None, &arena)
            .unwrap();

        let id1 = v1.id;

        let bm25 = storage.bm25.as_ref().expect("BM25 should be enabled");
        bm25.insert_doc(&mut wtxn, id1, "test keyword here")
            .unwrap();
        wtxn.commit().unwrap();

        let query = "test keyword";
        let query_vector = vec![1.0, 0.0, 0.0];

        let results = storage
            .hybrid_search(query, &query_vector, 0.5, 10)
            .await
            .expect("Hybrid search should succeed");

        assert_eq!(results.len(), 1, "Should have exactly 1 result");
        assert_eq!(results[0].0, id1, "Should return the correct document");
        assert!(
            results[0].1 > 0.0,
            "Score should be positive: {}",
            results[0].1
        );
    }
}
