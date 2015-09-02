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
    i32 init(),

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

    void ping(),

    // Entry point to prepare a cluster.  Performs the following in parallel:
    //   (1) Have this aggregator connect to all machine-local shards, and call
    //       init() on them.
    //   (2) Have this aggregator connect to all other aggregators in the
    //       cluster, if any.
    //   (3) Have those connected aggregators connect to their own
    //       local shards as well.
    i32 init(),

    // Have this aggregator connect to machine-local shards (servers),
    // and call init() on them (i.e. loads or constructs).  End users
    // should just call init() once and not call this method.
    i32 init_local_shards(),

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
