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

// One aggregator per machine; handles local aggregation and query routing.
//
// For each user-facing API myAPI(), the myAPI() call will potentially route
// to remote aggregator(s), if appropriate, whereas the myAPI_local() version
// should be used to contact machine-local shards only.
// TODO: the RPC implementation does not handle wildcard queries for now.
service GraphQueryAggregatorService {

  // Entry point to prepare a cluster.  Performs the following in parallel:
  //   (1) Have this aggregator connect to all machine-local shards, and call
  //       init() on them.
  //   (2) Have this aggregator connect both-ways to all other aggregators in
  //       the cluster, so that everyone has a socket to everyone.
  //   (3) Have those connected aggregators connect to their own
  //       local shards as well.
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

      i64 assoc_count(1: i64 src, 2: i64 atype),

      i64 assoc_count_local(1: i32 shardId, 2: i64 src, 3: i64 atype),

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
}
