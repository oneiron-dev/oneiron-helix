QUERY CreateDocument(embedding: [F64], content: String, title: String) =>
    doc <- AddV<Document>(embedding, {
        content: content,
        title: title
    })
    RETURN doc

QUERY HybridSearch(query_vec: [F64], query_text: String, limit: I32) =>
    results <- SearchHybrid<Document>(query_vec, query_text, limit)
    RETURN results
