#include "GraphQueryAggregatorService.h"

#include <fstream>
#include <mutex>
#include <set>
#include <unordered_map>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TSocket.h>

#include <boost/thread.hpp>

#include "ports.h"
#include "utils.h"
#include "GraphQueryService.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

// Used only in data initialization, not during serving.
boost::shared_mutex local_shards_data_mutex;
bool local_shards_data_initiated = false;

// A vector of maps: src -> (atype -> [shard id, file offset]).
// Each element (i.e. a map of the above type) corresponds to each shard
// hosted by this machine.
std::vector< std::unordered_map<int64_t,
        std::unordered_map<int64_t, std::vector<ThriftEdgeUpdatePtr>>
    > > edge_update_ptrs;
boost::shared_mutex edge_update_ptrs_mutex;

// A vector of maps: nodeId -> shard id with the latest data.
std::vector< std::unordered_map<int64_t, int64_t> > node_update_ptrs;
boost::shared_mutex node_update_ptrs_mutex;

// Running count: number of nodes in graph (across all machines).  This number
// is calculated the first time when num_nodes() is called.
int64_t curr_num_nodes = -1;
// Protects the above count, as well as node appends.
std::mutex curr_num_nodes_mutex;

class GraphQueryAggregatorServiceHandler :
    virtual public GraphQueryAggregatorServiceIf {

public:
    GraphQueryAggregatorServiceHandler(
        int total_num_shards,
        int local_num_shards,
        int local_host_id,
        const std::vector<std::string>& hostnames,
        bool multistore_enabled = false,
        int num_suffixstore_shards = 1,
        int num_logstore_shards = 1)
    : total_num_shards_(total_num_shards),
      local_num_shards_(local_num_shards),
      local_host_id_(local_host_id),
      hostnames_(hostnames),
      total_num_hosts_(hostnames.size()),
      initiated_(false),
      multistore_enabled_(multistore_enabled),
      num_suffixstore_shards_(num_suffixstore_shards),
      num_logstore_shards_(num_logstore_shards)
    {
        num_succinctstore_hosts_ = total_num_hosts_;
        num_succinctstore_shards_ = total_num_shards_; // synoynyms

        if (multistore_enabled_) {
            num_succinctstore_hosts_ = total_num_hosts_ - 2;
        }
    }

    int32_t local_data_init() {
        boost::unique_lock<boost::shared_mutex> lk(local_shards_data_mutex);
        if (local_shards_data_initiated) {
            LOG_E("Local shard processes have already loaded data!");
            return 0;
        }

        int status = init_local_shards();
        if (status) {
            LOG_E("Initialization failed at shard, status = %d\n", status);
        } else {
            local_shards_data_initiated = true;
        }

        disconnect_from_local_shards();
        return status;
    }

    // Should just be connection establishment; assumes data loading has already
    // been done.
    int32_t init() {
        if (initiated_) {
            LOG_E("Cluster already initiated\n");
            return 0;
        }
        connect_to_local_shards();
        if (connect_to_aggregators() != 0) {
            LOG_E("Connection to remote aggregators not successful!\n");
            exit(1);
        }

        for (int i = 0; i < total_num_hosts_; ++i) {
            if (i == local_host_id_) {
                continue;
            }
            aggregators_.at(i).connect_to_local_shards();
            aggregators_.at(i).connect_to_aggregators();
        }

        LOG_E("Cluster init() done\n");
        initiated_ = true;
        return 0;
    }

    int32_t init_local_shards() {
        COND_LOG_E("About to connect to local shards and init them\n");
        if (connect_to_local_shards() != 0) {
            LOG_E("Connection to local shards failed\n");
            exit(1);
        }
        for (auto& shard : local_shards_) {
            shard.send_init();
        }
        for (auto& shard : local_shards_) {
            if (shard.recv_init() != 0) {
                LOG_E("Some shard doesn't init() successfully, exiting\n");
                exit(1);
            }
        }
        COND_LOG_E("init_local_shards() done\n");
        return 0;
    }

    int32_t connect_to_aggregators() {
        aggregators_.clear();

        for (int i = 0; i < hostnames_.size(); ++i) { // FIXME: total_num_hosts_?
            if (i == local_host_id_) {
                continue;
            }

            COND_LOG_E("Connecting to remote aggregator on host %d...\n", i);
            try {
                shared_ptr<TSocket> socket(new TSocket(
                    hostnames_.at(i), QUERY_HANDLER_PORT));
                shared_ptr<TTransport> transport(
                    new TBufferedTransport(socket));
                shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
                GraphQueryAggregatorServiceClient client(protocol);

                transport->open();
                COND_LOG_E("Connected!\n");

                // Critical, otherwise this agg. client won't have sockets open
                // to its own local shard servers.
                // client.init_local_shards();
                client.connect_to_local_shards();

                aggregators_.insert(
                    std::pair<int, GraphQueryAggregatorServiceClient>(
                        i, client));
                aggregator_transports_.push_back(transport);
            } catch (std::exception& e) {
                LOG_E("Could not connect to aggregator %d: %s\n", i, e.what());
                return 1;
            }
        }
        if (hostnames_.size() != total_num_hosts_) {
            LOG_E("%zu total aggregators, but only %zu live\n",
                total_num_hosts_, hostnames_.size());
            return 1;
        }
        COND_LOG_E("Aggregators connected: cluster has %zu aggregators in total.\n",
            hostnames_.size());
        return 0;
    }

    void shutdown() {
        for (int i = 0; i < total_num_hosts_; ++i) {
            if (i == local_host_id_) {
                continue;
            }
            aggregators_.at(i).disconnect_from_local_shards();
            aggregators_.at(i).disconnect_from_aggregators();
        }
        disconnect_from_local_shards();
        disconnect_from_aggregators();
    }

    void disconnect_from_local_shards() {
        for (auto transport : shard_transports_) {
            if (transport != nullptr && transport->isOpen()) {
                transport->close();
            }
        }
        shard_transports_.clear();
        local_shards_.clear();
    }

    void disconnect_from_aggregators() {
        for (auto transport : aggregator_transports_) {
            if (transport != nullptr && transport->isOpen()) {
                transport->close();
            }
        }
        aggregator_transports_.clear();
    }

    int32_t connect_to_local_shards() {
        int num_shards_on_host = local_num_shards_;
        if (multistore_enabled_) {
            if (local_host_id_ == total_num_hosts_ - 2) {
                num_shards_on_host = num_suffixstore_shards_;
            } else if (local_host_id_ == total_num_hosts_ - 1) {
                // FIXME: we should configure if there is an empty LogStore
                num_shards_on_host = num_logstore_shards_ + 1;
            }
        }
        COND_LOG_E("num shards on this host (id %d): %d\n",
            local_host_id_, num_shards_on_host);

        for (int i = 0; i < num_shards_on_host; ++i) {
            // Desirable? Hacky way to facilitate benchmark client reconnecting
            // to a healthy cluster of aggregators & shard servers.
            if (i < local_shards_.size()) continue;

            COND_LOG_E("Connecting to local server %d...", i);
            try {
                shared_ptr<TSocket> socket(new TSocket(
                    "localhost", QUERY_SERVER_PORT + i));
                shared_ptr<TTransport> transport(
                    new TBufferedTransport(socket));
                shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
                GraphQueryServiceClient client(protocol);

                transport->open();
                COND_LOG_E("Connected!\n");
                local_shards_.push_back(client);
                shard_transports_.push_back(transport);
            } catch (std::exception& e) {
                LOG_E("Could not connect to server: %s\n", e.what());
                return 1;
            }
        }
        COND_LOG_E(
            "Currently have %zu local server connections.\n",
            local_shards_.size());
        return 0;
    }

    // The correctness relies on host 0 being called first, then host 1, etc.
    int32_t backfill_edge_updates() {
        int shard_idx = 0, src_shard_id, num_backfilled = 0;

        // dst shard id -> { (src, atype) }
        std::map<int32_t, std::vector<ThriftSrcAtype> > update_map;

        // We assume shards are ordered according to time order.
        for (auto& shard: local_shards_) {
            src_shard_id = shard_idx_to_shard_id(shard_idx);

            shard.get_edge_updates(update_map);

            for (auto it = update_map.begin(); it != update_map.end(); ++it) {
                int dst_shard_id = it->first;
                auto& src_atypes = it->second;
                int dst_host_id = dst_shard_id % num_succinctstore_hosts_;

                if (local_host_id_ == dst_host_id) {
                    record_edge_updates(src_shard_id, dst_shard_id, src_atypes);
                } else {
                    aggregators_.at(dst_host_id).record_edge_updates(
                        src_shard_id,
                        dst_shard_id, // src shard contains more recent stuff
                        src_atypes);
                }

                num_backfilled += src_atypes.size();
            }
            ++shard_idx;
        }

        return num_backfilled;
    }

    void record_edge_updates(
        const int32_t next_shard_id,
        const int32_t local_shard_id,
        const std::vector<ThriftSrcAtype> & updates)
    {
        COND_LOG_E("Recording edge updates for shard %d at host %d, "
            "from shard %d, %lld assoc lists\n",
            local_shard_id, local_host_id_, next_shard_id, updates.size());

//        COND_LOG_E("Recording %lld updates from nextShard %d\n",
//            updates.size(), next_shard_id);
//
        ThriftEdgeUpdatePtr ptr;
        boost::unique_lock<boost::shared_mutex> lk(edge_update_ptrs_mutex);
        auto& map_for_shard = edge_update_ptrs.at(
            shard_id_to_shard_idx(local_shard_id));

        for (auto& update : updates) {
            ptr.shardId = next_shard_id;
            ptr.offset = -1; // TODO: offset optimization is not implemented yet
            auto& curr_ptrs = map_for_shard[update.src][update.atype];

            // As random edges accumulate in the LogStore and as it sends
            // updates back, it could be that there are many updates from
            // the same store.  If so, record it only once.
            if (curr_ptrs.empty() || curr_ptrs.back().shardId != next_shard_id)
            {
                curr_ptrs.push_back(ptr);
            }
        }
    }

    void record_node_updates(
        const int32_t next_shard_id,
        const int32_t local_shard_id,
        const std::vector<int64_t>& updated_node_ids)
    {
        COND_LOG_E("Recording node updates for shard %d at host %d, "
            "from shard %d, updated node id %lld\n",
            local_shard_id, local_host_id_, next_shard_id, updated_node_id);

        boost::unique_lock<boost::shared_mutex> lk(node_update_ptrs_mutex);
        auto& map_for_shard = node_update_ptrs.at(
            shard_id_to_shard_idx(local_shard_id));

        for (int64_t updated_id : updated_node_ids) {
            // insert or overwrite, since we need only the latest shard
            map_for_shard[updated_id] = next_shard_id;
        }
    }

public:

    void get_attribute(
        std::string& _return,
        const int64_t nodeId,
        const int32_t attrId)
    {
        int shard_id = nodeId % total_num_shards_;
        int host_id = shard_id % total_num_hosts_;
        if (host_id == local_host_id_) {
            get_attribute_local(_return, shard_id, nodeId, attrId);
        } else {
            COND_LOG_E("nodeId %lld, host id %d, aggs size\n",
                nodeId, host_id, aggregators_.size());
            aggregators_.at(host_id).get_attribute_local(
                _return, shard_id, nodeId, attrId);
        }
    }

    void get_attribute_local(
        std::string& _return,
        const int64_t shard_id,
        const int64_t node_id,
        const int32_t attrId)
    {
        local_shards_.at(shard_id_to_shard_idx(shard_id)).get_attribute_local(
            _return, node_id, attrId);
    }

    void get_neighbors(std::vector<int64_t> & _return, const int64_t nodeId) {
        int shard_id = nodeId % total_num_shards_;
        int host_id = shard_id % total_num_hosts_;
        COND_LOG_E(
            "Received: get_neighbors(%lld), route to shard %d on host %d\n",
            nodeId, shard_id, host_id);
        if (host_id == local_host_id_) {

#ifdef DEBUG_RPC_NHBR
            auto t1 = get_timestamp();
#endif

            local_shards_.at(shard_id_to_shard_idx(shard_id))
                .get_neighbors(_return, nodeId);

#ifdef DEBUG_RPC_NHBR
            auto t2 = get_timestamp();
            if (shard_id == 0) {
                LOG_E(".%lld\n", t2 - t1);
            }
#endif

        } else {
            aggregators_.at(host_id).get_neighbors_local(
                _return, shard_id, nodeId);
        }
    }

    void get_neighbors_local(
        std::vector<int64_t> & _return,
        const int32_t shardId,
        const int64_t nodeId)
    {
        local_shards_[shardId / total_num_hosts_]
            .get_neighbors(_return, nodeId);
    }

    void get_neighbors_atype(
        std::vector<int64_t> & _return,
        const int64_t nodeId,
        const int64_t atype)
    {
        int shard_id = nodeId % total_num_shards_;
        int host_id = shard_id % total_num_hosts_;
        if (host_id == local_host_id_) {
#ifdef DEBUG_RPC_NHBR
            auto t1 = get_timestamp();
#endif
            local_shards_.at(shard_id_to_shard_idx(shard_id))
                .get_neighbors_atype(_return, nodeId, atype);

#ifdef DEBUG_RPC_NHBR
            auto t2 = get_timestamp();
            if (shard_id == 0) {
                LOG_E(".%lld\n", t2 - t1);
            }
#endif
        } else {
            aggregators_.at(host_id).get_neighbors_atype_local(
                _return, shard_id, nodeId, atype);
        }
    }

    void get_neighbors_atype_local(
        std::vector<int64_t> & _return,
        const int32_t shardId,
        const int64_t nodeId,
        const int64_t atype)
    {
        local_shards_[shardId / total_num_hosts_]
            .get_neighbors_atype(_return, nodeId, atype);
    }

    void get_edge_attrs(
        std::vector<std::string> & _return,
        const int64_t nodeId,
        const int64_t atype)
    {
        int shard_id = nodeId % total_num_shards_;
        int host_id = shard_id % total_num_hosts_;
        if (host_id == local_host_id_) {
            local_shards_.at(shard_id_to_shard_idx(shard_id))
                .get_edge_attrs(_return, nodeId, atype);
        } else {
            aggregators_.at(host_id).get_edge_attrs_local(
                _return, shard_id, nodeId, atype);
        }
    }

    void get_edge_attrs_local(
        std::vector<std::string> & _return,
        const int32_t shardId,
        const int64_t nodeId,
        const int64_t atype)
    {
        local_shards_[shardId / total_num_hosts_]
            .get_edge_attrs(_return, nodeId, atype);
    }

    void get_neighbors_attr(
        std::vector<int64_t> & _return,
        const int64_t nodeId,
        const int32_t attrId,
        const std::string& attrKey)
    {
        COND_LOG_E("Aggregator get_nhbr_node(nodeId %d, attrId %d)\n",
            nodeId, attrId);

        int shard_id = nodeId % total_num_shards_;
        int host_id = shard_id % total_num_hosts_;
        // Delegate to the shard responsible for nodeId.
        if (host_id == local_host_id_) {
            COND_LOG_E("Delegating to myself\n");

            get_neighbors_attr_local(
                _return, shard_id, nodeId, attrId, attrKey);
        } else {
            COND_LOG_E("Route to aggregator on host %d\n", host_id);

            aggregators_.at(host_id).get_neighbors_attr_local(
                _return, shard_id, nodeId, attrId, attrKey);
        }
    }

    // TODO: rid of shardId? Maybe more expensive to ship an int over network?
    void get_neighbors_attr_local(
        std::vector<int64_t> & _return,
        const int32_t shardId,
        const int64_t nodeId,
        const int32_t attrId,
        const std::string& attrKey)
    {
        COND_LOG_E("In get_nhbr_node_local(shardId %d, nodeId %d, attrId %d)\n",
            shardId, nodeId, attrId);

        std::vector<int64_t> nhbrs;
        get_neighbors_local(nhbrs, shardId, nodeId);
        COND_LOG_E("nhbrs size: %d\n", nhbrs.size());

        // hostId -> [list of responsible nhbr IDs to check]
        std::unordered_map<int, std::vector<int64_t>> splits_by_keys;
        int host_id;

        for (int64_t nhbr_id : nhbrs) {
            host_id = (nhbr_id % total_num_shards_) % total_num_hosts_;
            splits_by_keys[host_id].push_back(nhbr_id); // global
        }

        for (auto it = splits_by_keys.begin(); it != splits_by_keys.end(); ++it)
        {
            host_id = it->first;
            COND_LOG_E("send target: host %d\n", host_id);
            if (host_id == local_host_id_) {
                filter_nodes_local(_return, it->second, attrId, attrKey);
                COND_LOG_E("locally filtered result: %d\n", _return.size());
            } else {
                COND_LOG_E("host id %d\n", host_id);
                aggregators_.at(host_id).send_filter_nodes_local(
                    it->second, attrId, attrKey);
            }
        }

        COND_LOG_E("about to receive from remote hosts\n");

        std::vector<int64_t> shard_result;
        for (auto it = splits_by_keys.begin(); it != splits_by_keys.end(); ++it)
        {
            host_id = it->first;
            COND_LOG_E("recv target: host %d\n", host_id);
            // The equal case has already been computed in loop above
            if (host_id != local_host_id_) {
                aggregators_.at(host_id).recv_filter_nodes_local(shard_result);
                COND_LOG_E("remotely filtered result: %d\n",
                    shard_result.size());
                _return.insert(
                    _return.end(), shard_result.begin(), shard_result.end());
            }
        }
    }

    void filter_nodes_local(
        std::vector<int64_t>& _return,
        const std::vector<int64_t>& nodeIds,
        const int32_t attrId,
        const std::string& attrKey)
    {
        COND_LOG_E("in agg. filter_nodes_local, %d ids to filter\n",
            nodeIds.size());
        // shardId -> [list of responsible nhbr IDs to check]
        std::unordered_map<int, std::vector<int64_t>> splits_by_keys;
        int shard_id;

        for (int64_t nhbr_id : nodeIds) {
            shard_id = nhbr_id % total_num_shards_;
            splits_by_keys[shard_id].push_back(nhbr_id);
        }

        for (auto it = splits_by_keys.begin(); it != splits_by_keys.end(); ++it)
        {
            COND_LOG_E("sending to shard %d, filter_nodes\n",
                it->first / total_num_hosts_);
            // FIXME?: try to sleep a while? get_nhbr(n, attr) bug here?
            local_shards_[it->first / total_num_hosts_]
                .send_filter_nodes(it->second, attrId, attrKey);
            COND_LOG_E("sent");
        }

        _return.clear();
        std::vector<int64_t> shard_result;
        for (auto it = splits_by_keys.begin(); it != splits_by_keys.end(); ++it)
        {
            COND_LOG_E("receiving filter_nodes() result from shard %d, ",
                it->first / total_num_hosts_);
            local_shards_[it->first / total_num_hosts_]
                .recv_filter_nodes(shard_result);
            COND_LOG_E("size: %d\n", shard_result.size());
            // local back to global
            for (const int64_t local_key : shard_result) {
                // globalKey = localKey * numShards + shardId
                // localKey = (globalKey - shardId) / numShards
                _return.push_back(local_key * total_num_shards_ + it->first);
            }
        }
    }

    void get_nodes(
        std::set<int64_t> & _return,
        const int32_t attrId,
        const std::string& attrKey)
    {
        for (int i = 0; i < total_num_hosts_; ++i) {
            if (i == local_host_id_) {
                continue;
            }
            aggregators_.at(i).send_get_nodes_local(attrId, attrKey);
        }

        get_nodes_local(_return, attrId, attrKey);

        std::set<int64_t> shard_result;
        for (int i = 0; i < total_num_hosts_; ++i) {
            if (i == local_host_id_) {
                continue;
            }
            aggregators_.at(i).recv_get_nodes_local(shard_result);
            _return.insert(shard_result.begin(), shard_result.end());
        }
    }

    void get_nodes_local(
        std::set<int64_t> & _return,
        const int32_t attrId,
        const std::string& attrKey)
    {
        for (auto& shard : local_shards_) {
            shard.send_get_nodes(attrId, attrKey);
        }

        std::set<int64_t> shard_result;
        _return.clear();

        for (auto& shard : local_shards_) {
            shard.recv_get_nodes(shard_result);
            _return.insert(shard_result.begin(), shard_result.end());
        }
    }

    void get_nodes2(
        std::set<int64_t> & _return,
        const int32_t attrId1,
        const std::string& attrKey1,
        const int32_t attrId2,
        const std::string& attrKey2)
    {
        for (int i = 0; i < total_num_hosts_; ++i) {
            if (i == local_host_id_) {
                continue;
            }
            aggregators_.at(i).send_get_nodes2_local(
                attrId1, attrKey1, attrId2, attrKey2);
        }

        get_nodes2_local(_return, attrId1, attrKey1, attrId2, attrKey2);

        std::set<int64_t> shard_result;
        for (int i = 0; i < total_num_hosts_; ++i) {
            if (i == local_host_id_) {
                continue;
            }
            aggregators_.at(i).recv_get_nodes2_local(shard_result);
            _return.insert(shard_result.begin(), shard_result.end());
        }
    }

    void get_nodes2_local(
        std::set<int64_t> & _return,
        const int32_t attrId1,
        const std::string& attrKey1,
        const int32_t attrId2,
        const std::string& attrKey2)
    {
        for (auto& shard : local_shards_) {
            shard.send_get_nodes2(attrId1, attrKey1, attrId2, attrKey2);
        }

        std::set<int64_t> shard_result;
        _return.clear();

        for (auto& shard : local_shards_) {
            shard.recv_get_nodes2(shard_result);
            _return.insert(shard_result.begin(), shard_result.end());
        }
    }

    void assoc_range(
        std::vector<ThriftAssoc>& _return,
        int64_t src,
        int64_t atype,
        int32_t off,
        int32_t len)
    {
        COND_LOG_E("in aggregator assoc_range\n");

        assert(total_num_shards_ > 0 && "total_num_shards_ <= 0");

        int shard_id = src % total_num_shards_;
        int host_id = host_id_for_shard(shard_id);

        if (host_id == local_host_id_) {
            assoc_range_local(_return, shard_id, src, atype, off, len);
        } else {
            COND_LOG_E("assoc_range(src %lld, atype %lld,...) "
                "route to shard %d on host %d", src, atype, shard_id, host_id);
            aggregators_.at(host_id).assoc_range_local(
                _return, shard_id, src, atype, off, len);
        }
    }

    void assoc_range_batched(
        std::vector<std::vector<ThriftAssoc>>& _return,
        const std::vector<int64_t>& srcs,
        const std::vector<int64_t>& atypes,
        const std::vector<int32_t>& offs,
        const std::vector<int32_t>& lens)
    {
        COND_LOG_E("in aggregator assoc_range\n");

        const size_t query_len = srcs.size();
        int64_t src, atype;
        int32_t off, len;
        int shard_id, host_id;

        for (size_t i = 0; i < query_len; ++i) {
            src = srcs.at(i);
            atype = atypes.at(i);
            off = offs.at(i);
            len = lens.at(i);

            shard_id = src % total_num_shards_;
            host_id = shard_id % total_num_hosts_;
            if (host_id == local_host_id_) {
                local_shards_.at(shard_id_to_shard_idx(shard_id))
                    .send_assoc_range(src, atype, off, len);
            } else {
                aggregators_.at(host_id).send_assoc_range_local(
                    shard_id, src, atype, off, len);
            }
        }

        _return.resize(len);
        for (size_t i = 0; i < query_len; ++i) {
            src = srcs.at(i);
            shard_id = src % total_num_shards_;
            host_id = shard_id % total_num_hosts_;
            if (host_id == local_host_id_) {
                local_shards_.at(shard_id_to_shard_idx(shard_id))
                    .recv_assoc_range(_return[i]);
            } else {
                aggregators_.at(host_id).recv_assoc_range_local(
                    _return[i]);
            }
        }
    }

    // FIXME: the implementation is sequential for now...
    // FIXME: the logic is simply incorrect if off != 0.
    void assoc_range_local(
        std::vector<ThriftAssoc>& _return,
        int32_t shardId,
        int64_t src,
        int64_t atype,
        int32_t off,
        int32_t len)
    {
        int shard_idx = shard_id_to_shard_idx(shardId);
        COND_LOG_E("assoc_range_local(src %lld, atype %lld, ..., len %d) "
            "shard %d on host %d, shard idx %d",
            src, atype, len, shardId, local_host_id_, shard_idx);
        std::vector<ThriftAssoc> assocs;
        int32_t curr_len = 0;
        _return.clear();

        std::vector<ThriftEdgeUpdatePtr> ptrs;
        get_edge_update_ptrs(ptrs, shard_idx, src, atype);
        COND_LOG_E("# update ptrs: %d\n", ptrs.size());
        for (auto it = ptrs.rbegin(); it != ptrs.rend(); ++it) {
            if (curr_len >= len) {
                break;
            }

            auto& ptr = *it;
            // int64_t offset = ptr.offset; // TODO: add optimization
            int next_host_id = host_id_for_shard(ptr.shardId);
            assert(next_host_id != local_host_id_ && "next host is myself!");

            aggregators_.at(next_host_id).assoc_range_local(
                assocs,
                ptr.shardId,
                src,
                atype,
                0, // FIXME: this is hacky and potentially expensive
                len - curr_len);
            _return.insert(_return.end(), assocs.begin(), assocs.end());

            curr_len += assocs.size();
        }

        size_t from_updates = _return.size();

        if (_return.size() < len) {
            local_shards_.at(shard_idx)
                .assoc_range(assocs, src, atype, 0, len - _return.size());
            COND_LOG_E("local shard returns %d assocs\n", assocs.size());
            _return.insert(_return.end(), assocs.begin(), assocs.end());
        }

        if (!ptrs.empty()) {
            COND_LOG_E("assoc_range_local(%lld, %lld, %d, %d), %d ptrs, "
                "%d assocs from updates\n",
                src, atype, off, len, ptrs.size(), from_updates);
        }

        auto start = _return.begin();
        // Critical to have std::min here, otherwise UB -> segfault
        auto end = _return.begin() +
            std::min(_return.size(), static_cast<size_t>(len));
        COND_LOG_E("about to return, %d assocs before cutoff, ", _return.size());
        _return = std::vector<ThriftAssoc>(start, end);
        COND_LOG_E("%d assocs after\n", _return.size());
    }

    void assoc_count_batched(
        std::vector<int64_t>& _return,
        const std::vector<int64_t>& srcs,
        const std::vector<int64_t>& atypes)
    {
        const size_t query_len = srcs.size();
        int64_t src, atype;
        int shard_id, host_id;

        for (size_t i = 0; i < query_len; ++i) {
            src = srcs.at(i);
            atype = atypes.at(i);

            shard_id = src % total_num_shards_;
            host_id = shard_id % total_num_hosts_;
            if (host_id == local_host_id_) {
                local_shards_.at(shard_id_to_shard_idx(shard_id))
                    .send_assoc_count(src, atype);
            } else {
                aggregators_.at(host_id).send_assoc_count_local(
                    shard_id, src, atype);
            }
        }

        _return.resize(query_len);
        for (size_t i = 0; i < query_len; ++i) {
            src = srcs.at(i);

            shard_id = src % total_num_shards_;
            host_id = shard_id % total_num_hosts_;
            if (host_id == local_host_id_) {
                _return[i] = local_shards_.at(shard_id_to_shard_idx(shard_id))
                    .recv_assoc_count();
            } else {
                _return[i] = aggregators_.at(host_id).recv_assoc_count_local();
            }
        }
    }

    int64_t assoc_count(int64_t src, int64_t atype) {
        assert(total_num_shards_ > 0 && "total_num_shards_ <= 0");

        // %'ing with total_num_shards_ means always going to a primary
        int primary_shard_id = src % total_num_shards_;
        int host_id = host_id_for_shard(primary_shard_id);

        if (host_id == local_host_id_) {
            return assoc_count_local(primary_shard_id, src, atype);
        } else {
            COND_LOG_E("assoc_count(src %lld, atype %lld) "
                "route to shard %d on host %d, shard idx",
                src, atype, primary_shard_id, host_id);
            return aggregators_.at(host_id).assoc_count_local(
                primary_shard_id, src, atype);
        }
    }

    // This can be called on any Succinct, Suffix, and Log Store machine.
    // Therefore, shardId can be >= num_succinctstore_shards_.
    int64_t assoc_count_local(
        int32_t shardId, int64_t src, int64_t atype)
    {
        int shard_idx = shard_id_to_shard_idx(shardId);
        COND_LOG_E("assoc_count_local(src %lld, atype %lld) "
            "shard %d on host %d, shard idx %d",
            src, atype, shardId, local_host_id_, shard_idx);

        std::vector<ThriftEdgeUpdatePtr> ptrs;
        get_edge_update_ptrs(ptrs, shard_idx, src, atype);
        COND_LOG_E("# update ptrs: %d\n", ptrs.size());
        // Follow all pointers.  Suffix and Log Stores should not have them.
        for (auto& ptr : ptrs) {
            // int64_t offset = ptr.offset; // TODO: add optimization
            int next_host_id = host_id_for_shard(ptr.shardId);
            assert(next_host_id != local_host_id_ && "next host is myself!");
            aggregators_.at(next_host_id).send_assoc_count_local(
                ptr.shardId, src, atype);
        }

        // Execute locally
        int64_t cnt = local_shards_.at(shard_idx).assoc_count(src, atype);

        for (auto& ptr : ptrs) {
            int next_host_id = host_id_for_shard(ptr.shardId);
            cnt += aggregators_.at(next_host_id).recv_assoc_count_local();
        }

        return cnt;
    }

    void assoc_get(
        std::vector<ThriftAssoc>& _return,
        const int64_t src,
        const int64_t atype,
        const std::set<int64_t>& dstIdSet,
        const int64_t tLow,
        const int64_t tHigh)
    {
        COND_LOG_E("in agg. assoc_get(src %lld, atype %lld)\n", src, atype);
        assert(total_num_shards_ > 0 && "total_num_shards_ <= 0");

        int shard_id = src % total_num_shards_;
        int host_id = host_id_for_shard(shard_id);

        if (host_id == local_host_id_) {
            COND_LOG_E("sending to shard %d on host %d\n", shard_id, host_id);
            assoc_get_local(
                _return, shard_id, src, atype, dstIdSet, tLow, tHigh);
            COND_LOG_E("done\n");
        } else {
            aggregators_.at(host_id).assoc_get_local(
                _return, shard_id, src, atype, dstIdSet, tLow, tHigh);
        }
    }

    void assoc_get_batched(
        std::vector<std::vector<ThriftAssoc>>& _return,
        const std::vector<int64_t>& srcs,
        const std::vector<int64_t>& atypes,
        const std::vector<std::set<int64_t>>& dstIdSets,
        const std::vector<int64_t>& tLows,
        const std::vector<int64_t>& tHighs)
    {
        COND_LOG_E("in agg. assoc_get()\n");

        const size_t query_len = srcs.size();
        int64_t src, atype, tLow, tHigh;
        int shard_id, host_id;

        for (size_t i = 0; i < query_len; ++i) {
            src = srcs.at(i);
            atype = atypes.at(i);
            const auto& dstIdSet = dstIdSets.at(i);
            tLow = tLows.at(i);
            tHigh = tHighs.at(i);

            shard_id = src % total_num_shards_;
            host_id = shard_id % total_num_hosts_;
            if (host_id == local_host_id_) {
                COND_LOG_E("sending to shard %d\n", shard_id);
                local_shards_.at(shard_id_to_shard_idx(shard_id))
                    .send_assoc_get(src, atype, dstIdSet, tLow, tHigh);
                COND_LOG_E("done\n");
            } else {
                aggregators_.at(host_id).send_assoc_get_local(
                    shard_id, src, atype, dstIdSet, tLow, tHigh);
            }
        }

        _return.resize(query_len);
        for (size_t i = 0; i < query_len; ++i) {
            src = srcs.at(i);

            shard_id = src % total_num_shards_;
            host_id = shard_id % total_num_hosts_;

            if (host_id == local_host_id_) {
                COND_LOG_E("sending to shard %d\n", shard_id);
                local_shards_.at(shard_id_to_shard_idx(shard_id))
                    .recv_assoc_get(_return[i]);
                COND_LOG_E("done\n");
            } else {
                aggregators_.at(host_id).recv_assoc_get_local(
                    _return[i]);
            }
        }
    }

    // FIXME: it currently goes to all shards in parallel, due to lack of times.
    void assoc_get_local(
        std::vector<ThriftAssoc>& _return,
        const int32_t shardId,
        const int64_t src,
        const int64_t atype,
        const std::set<int64_t>& dstIdSet,
        const int64_t tLow,
        const int64_t tHigh)
    {
        int shard_idx = shard_id_to_shard_idx(shardId);

        COND_LOG_E("assoc_get_local(src %lld, atype %lld) "
            "; shardId %d on host %d, shard idx %d\n",
            src, atype, shardId, local_host_id_, shard_idx);

        std::vector<ThriftEdgeUpdatePtr> ptrs;
        get_edge_update_ptrs(ptrs, shard_idx, src, atype);

        COND_LOG_E("# update ptrs: %d\n", ptrs.size());

        for (auto it = ptrs.rbegin(); it != ptrs.rend(); ++it) {
            // int64_t offset = ptr.offset; // TODO: add optimization
            int next_host_id = host_id_for_shard(it->shardId);
            assert(next_host_id != local_host_id_ && "next host is myself!");

            aggregators_.at(next_host_id).send_assoc_get_local(
                it->shardId, src, atype, dstIdSet, tLow, tHigh);
        }

        local_shards_.at(shard_idx)
            .send_assoc_get(src, atype, dstIdSet, tLow, tHigh);

        std::vector<ThriftAssoc> assocs;
        _return.clear();

        for (auto it = ptrs.rbegin(); it != ptrs.rend(); ++it) {
            // int64_t offset = ptr.offset; // TODO: add optimization
            int next_host_id = host_id_for_shard(it->shardId);
            aggregators_.at(next_host_id).recv_assoc_get_local(assocs);
            _return.insert(_return.end(), assocs.begin(), assocs.end());
        }

        local_shards_.at(shard_idx).recv_assoc_get(assocs);
        _return.insert(_return.end(), assocs.begin(), assocs.end());

        COND_LOG_E("assoc_get_local done, returning %d assocs!\n",
            _return.size());
    }

    void obj_get(std::vector<std::string>& _return, const int64_t nodeId) {
        assert(total_num_shards_ > 0 && "total_num_shards_ <= 0");

        int shard_id = nodeId % total_num_shards_;
        int host_id = host_id_for_shard(shard_id);

        if (host_id == local_host_id_) {
            int latest_shard_id = get_node_update_ptr(
                shard_id_to_shard_idx(shard_id), nodeId);

            if (latest_shard_id != -1) {
                // found
                int next_host_id = host_id_for_shard(latest_shard_id);
                assert(next_host_id != local_host_id_ &&
                    "next host is myself!");
                aggregators_.at(next_host_id)
                    .obj_get_local(_return, latest_shard_id, nodeId);
            } else {
                // either non-existent, or managed by myself
                obj_get_local(_return, shard_id, nodeId);
            }

        } else {
            aggregators_.at(host_id).obj_get(_return, nodeId);
        }
    }

    void obj_get_local(
        std::vector<std::string>& _return,
        const int32_t shardId,
        const int64_t nodeId)
    {
        local_shards_.at(shard_id_to_shard_idx(shardId))
            .obj_get(_return, nodeId);
    }

    void obj_get_batched(
        std::vector<std::vector<std::string>>& _return,
        const std::vector<int64_t>& nodeIds)
    {
        const size_t query_len = nodeIds.size();
        int64_t nodeId;
        int shard_id, host_id;

        for (size_t i = 0; i < query_len; ++i) {
            nodeId = nodeIds.at(i);

            shard_id = nodeId % total_num_shards_;
            host_id = shard_id % total_num_hosts_;
            if (host_id == local_host_id_) {
                local_shards_.at(shard_id_to_shard_idx(shard_id)).send_obj_get(
                    nodeId);
            } else {
                aggregators_.at(host_id).send_obj_get_local(
                    shard_id, nodeId);
            }
        }

        _return.resize(query_len);
        for (size_t i = 0; i < query_len; ++i) {
            nodeId = nodeIds.at(i);

            shard_id = nodeId % total_num_shards_;
            host_id = shard_id % total_num_hosts_;
            if (host_id == local_host_id_) {
                local_shards_.at(shard_id_to_shard_idx(shard_id)).recv_obj_get(
                    _return[i]);
            } else {
                aggregators_.at(host_id).recv_obj_get_local(_return[i]);
            }
        }
    }

    void assoc_time_range(
        std::vector<ThriftAssoc>& _return,
        const int64_t src,
        const int64_t atype,
        const int64_t tLow,
        const int64_t tHigh,
        const int32_t limit)
    {
        assert(total_num_shards_ > 0 && "total_num_shards_ <= 0");

        int shard_id = src % total_num_shards_;
        int host_id = host_id_for_shard(shard_id);

        if (host_id == local_host_id_) {
            assoc_time_range_local(
                _return, shard_id, src, atype, tLow, tHigh, limit);
        } else {
            aggregators_.at(host_id).assoc_time_range_local(
                _return, shard_id, src, atype, tLow, tHigh, limit);
        }
    }

    void assoc_time_range_batched(
        std::vector<std::vector<ThriftAssoc>>& _return,
        const std::vector<int64_t>& srcs,
        const std::vector<int64_t>& atypes,
        const std::vector<int64_t>& tLows,
        const std::vector<int64_t>& tHighs,
        const std::vector<int32_t>& limits)
    {
        const size_t query_len = srcs.size();
        int64_t src, atype, tLow, tHigh;
        int32_t limit;
        int shard_id, host_id;

        for (size_t i = 0; i < query_len; ++i) {
            src = srcs.at(i);
            atype = atypes.at(i);
            tLow = tLows.at(i);
            tHigh = tHighs.at(i);
            limit = limits.at(i);

            shard_id = src % total_num_shards_;
            host_id = shard_id % total_num_hosts_;
            if (host_id == local_host_id_) {
                local_shards_.at(shard_id_to_shard_idx(shard_id))
                    .send_assoc_time_range(src, atype, tLow, tHigh, limit);
            } else {
                aggregators_.at(host_id).send_assoc_time_range_local(
                    shard_id, src, atype, tLow, tHigh, limit);
            }
        }

        _return.resize(query_len);
        for (size_t i = 0; i < query_len; ++i) {
            src = srcs.at(i);

            shard_id = src % total_num_shards_;
            host_id = shard_id % total_num_hosts_;
            if (host_id == local_host_id_) {
                local_shards_.at(shard_id_to_shard_idx(shard_id))
                    .recv_assoc_time_range(_return[i]);
            } else {
                aggregators_.at(host_id).recv_assoc_time_range_local(
                    _return[i]);
            }
        }
    }

    void assoc_time_range_local(
        std::vector<ThriftAssoc>& _return,
        const int32_t shardId,
        const int64_t src,
        const int64_t atype,
        const int64_t tLow,
        const int64_t tHigh,
        const int32_t limit)
    {
        int shard_idx = shard_id_to_shard_idx(shardId);

        COND_LOG_E("assoc_time_range_local(src %lld, atype %lld,...) "
            "; shardId %d on host %d, shard idx %d\n",
            src, atype, shardId, local_host_id_, shard_idx);

        std::vector<ThriftEdgeUpdatePtr> ptrs;
        get_edge_update_ptrs(ptrs, shard_idx, src, atype);

        COND_LOG_E("# update ptrs: %d\n", ptrs.size());

        for (auto it = ptrs.rbegin(); it != ptrs.rend(); ++it) {
            // int64_t offset = ptr.offset; // TODO: add optimization
            int next_host_id = host_id_for_shard(it->shardId);
            assert(next_host_id != local_host_id_ && "next host is myself!");

            aggregators_.at(next_host_id).send_assoc_time_range_local(
                it->shardId, src, atype, tLow, tHigh, limit);
        }

        local_shards_.at(shard_idx)
            .send_assoc_time_range(src, atype, tLow, tHigh, limit);

        std::vector<ThriftAssoc> assocs;
        _return.clear();

        for (auto it = ptrs.rbegin(); it != ptrs.rend(); ++it) {
            // int64_t offset = ptr.offset; // TODO: add optimization
            int next_host_id = host_id_for_shard(it->shardId);
            aggregators_.at(next_host_id).recv_assoc_time_range_local(assocs);

            if (_return.size() + assocs.size() <= limit) {
                _return.insert(_return.end(), assocs.begin(), assocs.end());
            } else {
                _return.insert(_return.end(), assocs.begin(),
                    assocs.begin() + std::min(
                        assocs.size(), limit - _return.size()));
            }
        }

        local_shards_.at(shard_idx).recv_assoc_time_range(assocs);

        if (_return.size() + assocs.size() <= limit) {
            _return.insert(_return.end(), assocs.begin(), assocs.end());
        } else {
            _return.insert(_return.end(), assocs.begin(),
                assocs.begin() + std::min(
                    assocs.size(), limit - _return.size()));
        }

        COND_LOG_E("assoc_time_range done, returning %d assocs (limit %d)!\n",
            _return.size(), limit);
    }

    int64_t num_nodes() {
        if (curr_num_nodes != -1) {
            return curr_num_nodes;
        }

        curr_num_nodes = 0;
        for (int i = 0; i < total_num_hosts_; ++i) {
            if (i == local_host_id_) {
                curr_num_nodes += num_nodes_local();
            } else {
                curr_num_nodes += aggregators_.at(i).num_nodes_local();
            }
        }
        return curr_num_nodes;
    }

    int64_t num_nodes_local() {
        int64_t num = 0;
        for (auto& shard : local_shards_) {
            num += shard.num_nodes_local();
        }
        return num;
    }

    int assoc_add(
        const int64_t src,
        const int64_t atype,
        const int64_t dst,
        const int64_t time,
        const std::string& attr)
    {
        assert(multistore_enabled_ &&
            "multistore not enabled but assoc_add called");
        COND_LOG_E("Received assoc_add(%lld,%d,%lld,...)\n", src, atype, dst);

        // NOTE: this hard-codes the knowledge that:
        // (1) the last machine is LogStore machine, and
        // (2) its last shard is the append-only store
        if (local_host_id_ == total_num_hosts_ - 1) {
            int ret = local_shards_.back()
                .assoc_add(src, atype, dst, time, attr);

            if (!ret) {
                int primary_shard_id = src % num_succinctstore_shards_;
                int primary_host_id = host_id_for_shard(primary_shard_id);
                assert(local_host_id_ != primary_host_id);

                ThriftSrcAtype src_atype;
                src_atype.src = src;
                src_atype.atype = atype;

                COND_LOG_E("Updating host %d, shard %d about (%lld,%d)\n",
                    primary_host_id, primary_shard_id, src, atype);

                aggregators_.at(primary_host_id).record_edge_updates(
                    num_succinctstore_shards_ +
                        num_suffixstore_shards_ +
                        num_logstore_shards_ - 1,
                    primary_shard_id,
                    { src_atype });
            }

            return ret;
        } else {
            return aggregators_.at(total_num_hosts_ - 1)
                .assoc_add(src, atype, dst, time, attr);
        }
    }

    // Uses a mutex to serialize calls of this API; this only means that at any
    // time, there could be at most one obj_add() being processed.
    int64_t obj_add(const std::vector<std::string>& attributes) {
        assert(multistore_enabled_ &&
            "multistore not enabled but obj_add called");
        COND_LOG_E("Received obj_add(...)\n");

        // NOTE: this hard-codes the knowledge that:
        // (1) the last machine is LogStore machine, and
        // (2) its last shard is the append-only store
        if (local_host_id_ == total_num_hosts_ - 1) {
            // Used only here, so just use a standard mutex/lock_guard
            std::lock_guard<std::mutex> lk(curr_num_nodes_mutex);
            int64_t next_node_id = num_nodes();

            int ret = local_shards_.back().obj_add(attributes, next_node_id);

            if (ret) {
                return -1;
            } else {
                int primary_shard_id = next_node_id % num_succinctstore_shards_;
                int primary_host_id = host_id_for_shard(primary_shard_id);
                assert(local_host_id_ != primary_host_id);

                COND_LOG_E("Updating host %d, shard %d about node %lld\n",
                    primary_host_id, primary_shard_id, src);

                aggregators_.at(primary_host_id).record_node_updates(
                    num_succinctstore_shards_ +
                        num_suffixstore_shards_ +
                        num_logstore_shards_ - 1,
                    primary_shard_id,
                    { next_node_id });

                ++curr_num_nodes;
                return next_node_id;
            }
        } else {
            return aggregators_.at(total_num_hosts_ - 1)
                .obj_add(attributes);
        }
    }

private:

    // globalKey = localKey * numShards + shardId
    // localKey = (globalKey - shardId) / numShards
    // DEPRECATED: see handling-updates.md.  Now all KV impls of all stores
    // contain actual, "global keys".
//    inline int64_t global_to_local_node_id(
//        int64_t global_node_id, int shard_id)
//    {
//        assert(total_num_shards_ > 0 && "total_num_shards_ <= 0");
//        return (global_node_id - shard_id) / total_num_shards_;
//    }

    // Host 0 to n - 3: the SuccinctStores, hash-partitioned
    // Host n - 2: the SuffixStores
    // Host n - 1: the LogStores
    inline int host_id_for_shard(int shard_id) {
        if (!multistore_enabled_ || shard_id < num_succinctstore_shards_) {
            assert(num_succinctstore_hosts_ > 0 && "num_succinctstore_hosts_ <= 0");
            return shard_id % num_succinctstore_hosts_;
        }
        if (shard_id - num_succinctstore_shards_ < num_suffixstore_shards_) {
            return num_succinctstore_hosts_; // host n - 2
        } else {
            return num_succinctstore_hosts_ + 1; // host n - 1
        }
    }

    // Limitation: this assumes 1 SuffixStore machine and 1 LogStore machine.
    inline int shard_id_to_shard_idx(int shard_id) {
        if (!multistore_enabled_) {
            assert(total_num_hosts_ > 0 && "total_num_hosts_ <= 0");
            return shard_id / total_num_hosts_;
        }
        int diff = shard_id - num_succinctstore_shards_
            - num_suffixstore_shards_;
        if (diff >= 0) {
            return diff; // log store
        }
        diff = shard_id - num_succinctstore_shards_;
        return diff >= 0
            ? diff // suffix store
            : shard_id / num_succinctstore_hosts_; // succinct st., round-robin
    }

    // Limitation: this assumes 1 SuffixStore machine and 1 LogStore machine.
    inline int shard_idx_to_shard_id(int shard_idx) {
        if (local_host_id_ < num_succinctstore_hosts_) {
            return shard_idx * num_succinctstore_hosts_ + local_host_id_;
        } else if (local_host_id_ == num_succinctstore_hosts_) {
            // case: suffix store machine
            return shard_idx + num_succinctstore_shards_;
        } else {
            // case: log store machine
            assert(local_host_id_ == num_succinctstore_hosts_ + 1);
            return shard_idx +
                num_succinctstore_shards_ + num_suffixstore_shards_;
        }
    }

    // Returns the latest shard ID holding data for `node_id`, or -1 if the
    // node is either (1) non-existent, or (2) stored in the specified shard
    // already.
    inline int get_node_update_ptr(int shard_idx, int64_t node_id) {
        if (!multistore_enabled_) {
            return -1;
        }
        boost::shared_lock<boost::shared_mutex> lk(node_update_ptrs_mutex);
        auto& shard_map = node_update_ptrs.at(shard_idx);
        auto it = shard_map.find(node_id);
        return it == shard_map.end() ? -1 : it->second;
    }

    inline void get_edge_update_ptrs(
        std::vector<ThriftEdgeUpdatePtr>& ptrs,
        int shard_idx,
        int64_t src,
        int64_t atype)
    {
        if (!multistore_enabled_) {
            ptrs.clear();
            return;
        }
        boost::shared_lock<boost::shared_mutex> lk(edge_update_ptrs_mutex);
        ptrs = edge_update_ptrs.at(shard_idx)[src][atype];
    }

    const int total_num_shards_; // total # of logical shards
    const int local_num_shards_;

    // id of this physical node
    const int local_host_id_;
    // all aggregators in the cluster, hostnames used for opening sockets
    std::vector<std::string> hostnames_;
    const int total_num_hosts_;
    bool initiated_, multistore_enabled_;

    int num_succinctstore_hosts_;

    int num_succinctstore_shards_;
    int num_suffixstore_shards_;
    int num_logstore_shards_;

    std::vector<GraphQueryServiceClient> local_shards_;

    // Maps host id to aggregator handle.  Does not contain self.
    std::unordered_map<int, GraphQueryAggregatorServiceClient> aggregators_;

    std::vector<shared_ptr<TTransport>> aggregator_transports_;
    std::vector<shared_ptr<TTransport>> shard_transports_;
};

// Dummy factory that just delegates fields.
class ProcessorFactory : public TProcessorFactory {
public:
    ProcessorFactory(
        int total_num_shards,
        int local_num_shards,
        int local_host_id,
        const std::vector<std::string>& hostnames,
        bool multistore_enabled,
        int num_suffixstore_shards,
        int num_logstore_shards)
    : total_num_shards_(total_num_shards),
      local_num_shards_(local_num_shards),
      local_host_id_(local_host_id),
      hostnames_(hostnames),
      multistore_enabled_(multistore_enabled),
      num_suffixstore_shards_(num_suffixstore_shards),
      num_logstore_shards_(num_logstore_shards)
    { }

    boost::shared_ptr<TProcessor> getProcessor(const TConnectionInfo&) {
        boost::shared_ptr<GraphQueryAggregatorServiceHandler> handler(
            new GraphQueryAggregatorServiceHandler(
                total_num_shards_,
                local_num_shards_,
                local_host_id_,
                hostnames_,
                multistore_enabled_,
                num_suffixstore_shards_,
                num_logstore_shards_));
        boost::shared_ptr<TProcessor> handlerProcessor(
            new GraphQueryAggregatorServiceProcessor(handler));
        return handlerProcessor;
    }
private:
    int total_num_shards_;
    int local_num_shards_;
    int local_host_id_;
    const std::vector<std::string>& hostnames_;
    bool multistore_enabled_;
    int num_suffixstore_shards_, num_logstore_shards_;
};

void print_usage(char *exec) {
    LOG_E(
        "Usage: %s [-t total_num_shards] [-s local_num_shards] "
        "[-h hostsfile] [-i local_host_id]\n",
        exec);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        print_usage(argv[0]);
        return -1;
    }

    int c, total_num_shards, local_num_shards, local_host_id;
    bool multistore_enabled;
    int num_suffixstore_shards, num_logstore_shards;
    std::string hostsfile;
    while ((c = getopt(argc, argv, "t:s:i:h:f:l:m:")) != -1) {
        switch(c) {
        case 't':
            total_num_shards = atoi(optarg);
            break;
        case 's':
            local_num_shards = atoi(optarg);
            break;
        case 'i':
            local_host_id = atoi(optarg);
            break;
        case 'h':
            hostsfile = optarg;
            break;
        case 'f':
            num_suffixstore_shards = atoi(optarg);
            break;
        case 'l':
            num_logstore_shards = atoi(optarg);
            break;
        case 'm':
            multistore_enabled = (std::string(optarg) == "T");
            break;
        default:
            total_num_shards = local_num_shards = 1;
            local_host_id = 0;
            hostsfile = "./conf/hosts";
        }
    }

    std::ifstream hosts(hostsfile);
    std::string host;
    std::vector<std::string> hostnames;
    while (std::getline(hosts, host)) {
        hostnames.push_back(host);
    }

    // Init the update ptrs
    {
        boost::lock_guard<boost::shared_mutex> lk(edge_update_ptrs_mutex);
        boost::lock_guard<boost::shared_mutex> lk2(node_update_ptrs_mutex);
        if (local_host_id == hostnames.size() - 1) {
            // LogStore
            // +1 because of the last, empty shard
            edge_update_ptrs.resize(num_logstore_shards + 1);
        } else if (local_host_id == hostnames.size() - 2) {
            // Suf.
            edge_update_ptrs.resize(num_suffixstore_shards);
        } else {
            // Succ.
            edge_update_ptrs.resize(total_num_shards);
        }
        node_update_ptrs.resize(edge_update_ptrs.size());
    }

    int port = QUERY_HANDLER_PORT;
    try {
        shared_ptr<ProcessorFactory> processor_factory(
            new ProcessorFactory(
                total_num_shards,
                local_num_shards,
                local_host_id,
                hostnames,
                multistore_enabled,
                num_suffixstore_shards,
                num_logstore_shards));
        shared_ptr<TServerTransport> server_transport(new TServerSocket(port));
        shared_ptr<TTransportFactory> transport_factory(
            new TBufferedTransportFactory());
        shared_ptr<TProtocolFactory> protocol_factory(
            new TBinaryProtocolFactory());

        // Note: 1st arg being a processor factory is essential in supporting
        // multiple clients (e.g. in throughput benchmarks).
        TThreadedServer server(
            processor_factory,
            server_transport,
            transport_factory,
            protocol_factory);

        server.serve();
    } catch (std::exception& e) {
        LOG_E("Exception at GraphQueryAggregator:main(): %s\n", e.what());
    }
    return 0;
}
