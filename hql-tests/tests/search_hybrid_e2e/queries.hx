QUERY CreateDocument(embedding: [F32], content: String, title: String) =>
    doc <- AddV<Document>(embedding, {
        content: content,
        title: title
    })
    RETURN doc

QUERY HybridSearch(query_vec: [F32], query_text: String, limit: I32) =>
    results <- SearchHybrid<Document>(query_vec, query_text, limit)
    RETURN results

QUERY HybridSearchWithFilter(query_vec: [F32], query_text: String, limit: I32, title_prefix: String) =>
    results <- SearchHybrid<Document>(query_vec, query_text, limit)::PREFILTER(_::{title}::EQ(title_prefix))
    RETURN results
