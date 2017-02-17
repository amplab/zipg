namespace java edu.berkeley.cs.zipg
namespace py zipg

// An association. The included fields are the same as SuccinctGraph::Assoc.
struct ThriftAssoc {
  1: i64 srcId,
  2: i64 dstId,
  3: i64 atype,
  4: i64 timestamp,
  5: string attr,
}

struct ThriftEdgeUpdatePtr {
  1: i64 shardId,
  2: i64 offset,
}

// In the IDL we use list<ThriftSrcAtype>, but the implementation makes sure,
// and relies on, it actually contains no duplicates -- i.e., it is a set.
struct ThriftSrcAtype {
  1: i64 src,
  2: i64 atype,
}

struct RPQuery {
  1: list<list<i64>> path_queries,
  2: bool recurse,
}

struct Path {
  1: i64 src,
  2: i64 dst,
}

struct RPQCtx {
  1: set<Path> endpoints,
}

// One aggregator per machine; handles local aggregation and query routing.
//
// For each user-facing API myAPI(), the myAPI() call will potentially route
// to remote aggregator(s), if appropriate, whereas the myAPI_local() version
// should be used to contact machine-local shards only.
// TODO: the RPC implementation does not handle wildcard queries for now.
service GraphQueryAggregatorService {

  // Entry point to prepare a cluster.  Performs the following in parallel:
  //   (1) Have this aggregator connect to all machine-local shards, and call
  //   init() on them.
  //   (2) Have this aggregator connect both-ways to all other aggregators in
  //   the cluster, so that everyone has a socket to everyone.
  //   (3) Have those connected aggregators connect to their own
  //   local shards as well.
  i32 init(),

  // Have this aggregator connect to all other aggregators.
  i32 connect_to_aggregators(),

  // Have all aggregators shutdown connections to other aggregators.
  void disconnect_from_aggregators(),
  void shutdown(),

  // Thread-safe, since it uses a mutex.
  void record_edge_updates(
    1: i32 next_shard, // where are these updates located?
    2: i32 local_shard, // one of this aggregator's shards
    3: list<ThriftSrcAtype> updates),

  void record_node_append(
    1: i32 next_shard,
    2: i32 local_shard,
    3: i64 obj),

  // Primitive queries
  string get_attribute(1: i64 nodeId, 2: i32 attrId),

  string get_attribute_local(1: i64 shardId, 2: i64 nodeId, 3: i32 attrId),

  list<i64> get_neighbors(1: i64 nodeId),

  list<i64> get_neighbors_local(1: i32 shardId, 2: i64 nodeId),

  list<i64> get_neighbors_atype(1: i64 nodeId, 2: i64 atype),

  list<i64> get_neighbors_atype_local(
    1: i32 shardId, 2: i64 nodeId, 3: i64 atype),

  list<i64> get_neighbors_attr(
    1: i64 nodeId, 2: i32 attrId, 3: string attrKey),

  list<i64> get_neighbors_attr_local(
    1: i32 shardId, 2: i64 nodeId, 3: i32 attrId, 4: string attrKey),

  set<i64> get_nodes(1: i32 attrId, 2: string attrKey),

  set<i64> get_nodes_local(1: i32 attrId, 2: string attrKey),

  set<i64> get_nodes2(
    1: i32 attrId1,
    2: string attrKey1,
    3: i32 attrId2,
    4: string attrKey2),

  set<i64> get_nodes2_local(
    1: i32 attrId1,
    2: string attrKey1,
    3: i32 attrId2,
    4: string attrKey2),

  // The passed-in `nodeIds` are global keys that are guaranteed to only
  // belong to shards under this aggregator.  On return, the keys are global.
  list<i64> filter_nodes_local(
    1: list<i64> nodeIds,
    2: i32 attrId,
    3: string attrKey),

  list<string> get_edge_attrs(1: i64 nodeId, 2: i64 atype),
  list<string> get_edge_attrs_local(
    1: i32 shardId, 2: i64 nodeId, 3: i64 atype),

  // TAO queries
  list<ThriftAssoc> assoc_range(
    1: i64 src, 2: i64 atype, 3: i32 off, 4: i32 len),

  list<ThriftAssoc> assoc_range_local(
    1: i32 shardId, 2: i64 src, 3: i64 atype, 4: i32 off, 5: i32 len),

  i64 assoc_count(1: i64 src, 2: i64 atype),

  i64 assoc_count_local(1: i32 shardId, 2: i64 src, 3: i64 atype),

  list<ThriftAssoc> assoc_get(
    1: i64 src, 2: i64 atype, 3: set<i64> dstIdSet,
    4: i64 tLow, 5: i64 tHigh),

  list<ThriftAssoc> assoc_get_local(
    1: i32 shardId, 2: i64 src, 3: i64 atype,
    4: set<i64> dstIdSet, 5: i64 tLow, 6: i64 tHigh),

  i64 obj_add(1: list<string> attrs),

  list<string> obj_get(1: i64 nodeId),

  list<string> obj_get_local(1: i32 shardId, 2: i64 nodeId),

  list<ThriftAssoc> assoc_time_range(
    1: i64 src, 2: i64 atype,
    3: i64 tLow, 4: i64 tHigh, 5: i32 limit),

  list<ThriftAssoc> assoc_time_range_local(
    1: i32 shardId, 2: i64 src, 3: i64 atype,
    4: i64 tLow, 5: i64 tHigh, 6: i32 limit),

  i32 assoc_add(
    1: i64 src, 2: i64 atype, 3: i64 dst, 4: i64 time, 5: string attr),

  // LinkBench API
  string getNode(1: i64 id),
  string getNodeLocal(1: i64 shard_id, 2: i64 id),

  i64 addNode(1: i64 id, 2: string data),
  // i64 addNodeLocal(1: i64 shard_id, 2: i64 id, 3: string data),
  
  bool deleteNode(1: i64 id),
  bool deleteNodeLocal(1: i64 shard_id, 2: i64 id),

  bool updateNode(1: i64 id, 2: string data),

  ThriftAssoc getLink(1: i64 id1, 2: i64 link_type, 3: i64 id2),
  ThriftAssoc getLinkLocal(1: i64 shard_id, 2: i64 id1, 3: i64 link_type, 4: i64 id2),

  bool addLink(1: ThriftAssoc link),
  // bool addLinkLocal(1: i64 shard_id, 2: ThriftAssoc link),
  
  bool deleteLink(1: i64 id1, 2: i64 link_type, 3: i64 id2),
  bool deleteLinkLocal(1: i64 shard_id, 2: i64 id1, 3: i64 link_type, 4: i64 id2),

  bool updateLink(1: ThriftAssoc link),

  list<ThriftAssoc> getLinkList(1: i64 id1, 2: i64 link_type),
  list<ThriftAssoc> getLinkListLocal(1: i64 shard_id, 2: i64 id1, 3: i64 link_type),

  list<ThriftAssoc> getFilteredLinkList(1: i64 id1, 2: i64 link_type, 3: i64 min_timestamp, 4: i64 max_timestamp, 5: i64 offset, 6: i64 limit),
  list<ThriftAssoc> getFilteredLinkListLocal(1: i64 shard_id, 2: i64 id1, 3: i64 link_type, 4: i64 min_timestamp, 5: i64 max_timestamp, 6: i64 offset, 7: i64 limit),

  i64 countLinks(1: i64 id1, 2: i64 link_type),
  
  // RPQ API
  RPQCtx regular_path_query(1: string query),
  RPQCtx rpq(1: RPQuery query),
  RPQCtx path_query(1: list<i64> query),
  RPQCtx path_query_local(1: list<i64> query),
  RPQCtx advance_path_query_ctx(1: list<i64> query, 2: RPQCtx ctx),
}
