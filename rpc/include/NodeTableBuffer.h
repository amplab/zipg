#ifndef NODE_TABLE_BUFFER_H_
#define NODE_TABLE_BUFFER_H_

#include <unordered_map>

class NodeTableBuffer {
public:
	NodeTableBuffer() {}

	void obj_get(std::vector<std::string>& results, int64_t obj_id) {
	    auto it = node_map_.find(obj_id);
		results.insert(results.end(), it.second.begin(), it.second.end());
	}

	void obj_add(int64_t obj_id, std::vector<std::string>& properties) {
		node_map_[obj_id] = properties;
	}

	bool contains(int64_t obj_id) {
		return node_map_.find(obj_id) != node_map_.end();
	}

private:
	std::unordered_map<int64_t, std::vector<std::string>> node_map_;
};

#endif /* NODE_BUFFER_H_ */
