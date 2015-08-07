// An association. The included fields are the same as SuccinctGraph::Assoc.
struct ThriftAssoc {
    1: i64 srcId,
    2: i64 dstId,
    3: i64 atype,
    4: i64 timestamp,
    5: string attr,
}

// One per logical shard (there can be multiple shards per physical node).
service GraphQueryService {

    // Loads or constructs graph shards.
    void init(),

    list<i64> get_neighbors(1: i64 nodeId),

    list<i64> get_neighbors_atype(1: i64 nodeId, 2: i64 atype),

    set<i64> get_nodes(1: i32 attrId, 2: string attrKey),

    set<i64> get_nodes2(
        1: i32 attrId1,
        2: string attrKey1,
        3: i32 attrId2,
        4: string attrKey2),

    // Filter the nodeIds by checking whether they contain the specified
    // attribute.  Contracts:
    // (1) This shard will only check its own local node table.
    // (2) `nodeIds` contains "local keys".
    // (3) On return, the result contains "local keys" as well.
    list<i64> filter_nodes(
        1: list<i64> nodeIds,
        2: i32 attrId,
        3: string attrKey),

    list<ThriftAssoc> assoc_range(
        1: i64 src, 2: i64 atype, 3: i32 off, 4: i32 len),

    i64 assoc_count(1: i64 src, 2: i64 atype),

    list<ThriftAssoc> assoc_get(
        1: i64 src, 2: i64 atype, 3: set<i64> dstIdSet,
        4: i64 tLow, 5: i64 tHigh),

    list<string> obj_get(1: i64 nodeId),

    list<ThriftAssoc> assoc_time_range(
        1: i64 src, 2: i64 atype,
        3: i64 tLow, 4: i64 tHigh, 5: i32 limit),

}

// One per physical node; handles local aggregation and query routing.
service GraphQueryAggregatorService {

    i32 connect_to_local_shards(),

    // Initialize local shards.
    void init(),

    // Primitive queries

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

    // TAO queries

    list<ThriftAssoc> assoc_range(
        1: i64 src, 2: i64 atype, 3: i32 off, 4: i32 len),

    i64 assoc_count(1: i64 src, 2: i64 atype),

    list<ThriftAssoc> assoc_get(
        1: i64 src, 2: i64 atype, 3: set<i64> dstIdSet,
        4: i64 tLow, 5: i64 tHigh),

    list<string> obj_get(1: i64 nodeId),

    list<ThriftAssoc> assoc_time_range(
        1: i64 src, 2: i64 atype,
        3: i64 tLow, 4: i64 tHigh, 5: i32 limit),

}
