QUERY CreateTopic(name: String, category: String) =>
    topic <- AddN<Topic>({
        name: name,
        category: category
    })
    RETURN topic

QUERY LinkTopics(from_id: ID, to_id: ID) =>
    edge <- AddE<RelatesTo>({
        weight: 1.0
    })::From(from_id)::To(to_id)
    RETURN edge

QUERY RankTopics(seeds: [ID], universe: [ID], limit: I32) =>
    ranked <- PPR<Topic>(seeds: seeds, universe: universe, limit: limit)
    RETURN ranked

QUERY RankTopicsCustom(seeds: [ID], universe: [ID], depth: I32, damping: F64, limit: I32) =>
    ranked <- PPR<Topic>(seeds: seeds, universe: universe, depth: depth, damping: damping, limit: limit)
    RETURN ranked
