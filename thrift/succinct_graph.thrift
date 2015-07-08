// One per logical shard (there can be multiple shards per physical node).
service GraphQueryService {

    // Loads or constructs graph shards.
    void init(),

    list<i64> get_neighbors(1: i64 nodeId),

    list<i64> get_neighbors_atype(1: i64 nodeId, 2: i64 atype),

    list<i64> get_neighbors_attr(
        1: i64 nodeId, 2: i32 attrId, 3: string attrKey),

    set<i64> get_nodes(1: i32 attrId, 2: string attrKey),

    set<i64> get_nodes2(
        1: i32 attrId1,
        2: string attrKey1,
        3: i32 attrId2,
        4: string attrKey2),

}

// One per physical node; handles local aggregation and query routing.
service GraphQueryAggregatorService {

    // TODO: what do these do?
    i32 connect_to_local_shards(),
//    i32 connect_to_handlers(),
//    i32 disconnect_from_handlers(),
//    i32 connect_to_local_servers(),
//    i32 disconnect_from_local_servers(),

    list<i64> get_neighbors(1: i64 nodeId),

    list<i64> get_neighbors_atype(1: i64 nodeId, 2: i64 atype),

    list<i64> get_neighbors_attr(
        1: i64 nodeId, 2: i32 attrId, 3: string attrKey),

    set<i64> get_nodes(1: i32 attrId, 2: string attrKey),

    set<i64> get_nodes2(
        1: i32 attrId1,
        2: string attrKey1,
        3: i32 attrId2,
        4: string attrKey2),

}
