/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#ifndef GraphQueryService_H
#define GraphQueryService_H

#include <thrift/TDispatchProcessor.h>
#include "thrift/succinct_graph_types.h"



class GraphQueryServiceIf {
 public:
  virtual ~GraphQueryServiceIf() {}
  virtual void get_neighbors(std::vector<int64_t> & _return, const int64_t nodeId) = 0;
  virtual void get_neighbors_atype(std::vector<int64_t> & _return, const int64_t nodeId, const int64_t atype) = 0;
  virtual void get_neighbors_attr(std::vector<int64_t> & _return, const int64_t nodeId, const int32_t attrId, const std::string& attrKey) = 0;
  virtual void get_nodes(std::vector<int64_t> & _return, const int32_t attrId, const std::string& attrKey) = 0;
  virtual void get_nodes2(std::vector<int64_t> & _return, const int32_t attrId1, const std::string& attrKey1, const int32_t attrId2, const std::string& attrKey2) = 0;
};

class GraphQueryServiceIfFactory {
 public:
  typedef GraphQueryServiceIf Handler;

  virtual ~GraphQueryServiceIfFactory() {}

  virtual GraphQueryServiceIf* getHandler(const ::apache::thrift::TConnectionInfo& connInfo) = 0;
  virtual void releaseHandler(GraphQueryServiceIf* /* handler */) = 0;
};

class GraphQueryServiceIfSingletonFactory : virtual public GraphQueryServiceIfFactory {
 public:
  GraphQueryServiceIfSingletonFactory(const boost::shared_ptr<GraphQueryServiceIf>& iface) : iface_(iface) {}
  virtual ~GraphQueryServiceIfSingletonFactory() {}

  virtual GraphQueryServiceIf* getHandler(const ::apache::thrift::TConnectionInfo&) {
    return iface_.get();
  }
  virtual void releaseHandler(GraphQueryServiceIf* /* handler */) {}

 protected:
  boost::shared_ptr<GraphQueryServiceIf> iface_;
};

class GraphQueryServiceNull : virtual public GraphQueryServiceIf {
 public:
  virtual ~GraphQueryServiceNull() {}
  void get_neighbors(std::vector<int64_t> & /* _return */, const int64_t /* nodeId */) {
    return;
  }
  void get_neighbors_atype(std::vector<int64_t> & /* _return */, const int64_t /* nodeId */, const int64_t /* atype */) {
    return;
  }
  void get_neighbors_attr(std::vector<int64_t> & /* _return */, const int64_t /* nodeId */, const int32_t /* attrId */, const std::string& /* attrKey */) {
    return;
  }
  void get_nodes(std::vector<int64_t> & /* _return */, const int32_t /* attrId */, const std::string& /* attrKey */) {
    return;
  }
  void get_nodes2(std::vector<int64_t> & /* _return */, const int32_t /* attrId1 */, const std::string& /* attrKey1 */, const int32_t /* attrId2 */, const std::string& /* attrKey2 */) {
    return;
  }
};

typedef struct _GraphQueryService_get_neighbors_args__isset {
  _GraphQueryService_get_neighbors_args__isset() : nodeId(false) {}
  bool nodeId;
} _GraphQueryService_get_neighbors_args__isset;

class GraphQueryService_get_neighbors_args {
 public:

  GraphQueryService_get_neighbors_args() : nodeId(0) {
  }

  virtual ~GraphQueryService_get_neighbors_args() throw() {}

  int64_t nodeId;

  _GraphQueryService_get_neighbors_args__isset __isset;

  void __set_nodeId(const int64_t val) {
    nodeId = val;
  }

  bool operator == (const GraphQueryService_get_neighbors_args & rhs) const
  {
    if (!(nodeId == rhs.nodeId))
      return false;
    return true;
  }
  bool operator != (const GraphQueryService_get_neighbors_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const GraphQueryService_get_neighbors_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};


class GraphQueryService_get_neighbors_pargs {
 public:


  virtual ~GraphQueryService_get_neighbors_pargs() throw() {}

  const int64_t* nodeId;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _GraphQueryService_get_neighbors_result__isset {
  _GraphQueryService_get_neighbors_result__isset() : success(false) {}
  bool success;
} _GraphQueryService_get_neighbors_result__isset;

class GraphQueryService_get_neighbors_result {
 public:

  GraphQueryService_get_neighbors_result() {
  }

  virtual ~GraphQueryService_get_neighbors_result() throw() {}

  std::vector<int64_t>  success;

  _GraphQueryService_get_neighbors_result__isset __isset;

  void __set_success(const std::vector<int64_t> & val) {
    success = val;
  }

  bool operator == (const GraphQueryService_get_neighbors_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const GraphQueryService_get_neighbors_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const GraphQueryService_get_neighbors_result & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _GraphQueryService_get_neighbors_presult__isset {
  _GraphQueryService_get_neighbors_presult__isset() : success(false) {}
  bool success;
} _GraphQueryService_get_neighbors_presult__isset;

class GraphQueryService_get_neighbors_presult {
 public:


  virtual ~GraphQueryService_get_neighbors_presult() throw() {}

  std::vector<int64_t> * success;

  _GraphQueryService_get_neighbors_presult__isset __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

typedef struct _GraphQueryService_get_neighbors_atype_args__isset {
  _GraphQueryService_get_neighbors_atype_args__isset() : nodeId(false), atype(false) {}
  bool nodeId;
  bool atype;
} _GraphQueryService_get_neighbors_atype_args__isset;

class GraphQueryService_get_neighbors_atype_args {
 public:

  GraphQueryService_get_neighbors_atype_args() : nodeId(0), atype(0) {
  }

  virtual ~GraphQueryService_get_neighbors_atype_args() throw() {}

  int64_t nodeId;
  int64_t atype;

  _GraphQueryService_get_neighbors_atype_args__isset __isset;

  void __set_nodeId(const int64_t val) {
    nodeId = val;
  }

  void __set_atype(const int64_t val) {
    atype = val;
  }

  bool operator == (const GraphQueryService_get_neighbors_atype_args & rhs) const
  {
    if (!(nodeId == rhs.nodeId))
      return false;
    if (!(atype == rhs.atype))
      return false;
    return true;
  }
  bool operator != (const GraphQueryService_get_neighbors_atype_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const GraphQueryService_get_neighbors_atype_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};


class GraphQueryService_get_neighbors_atype_pargs {
 public:


  virtual ~GraphQueryService_get_neighbors_atype_pargs() throw() {}

  const int64_t* nodeId;
  const int64_t* atype;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _GraphQueryService_get_neighbors_atype_result__isset {
  _GraphQueryService_get_neighbors_atype_result__isset() : success(false) {}
  bool success;
} _GraphQueryService_get_neighbors_atype_result__isset;

class GraphQueryService_get_neighbors_atype_result {
 public:

  GraphQueryService_get_neighbors_atype_result() {
  }

  virtual ~GraphQueryService_get_neighbors_atype_result() throw() {}

  std::vector<int64_t>  success;

  _GraphQueryService_get_neighbors_atype_result__isset __isset;

  void __set_success(const std::vector<int64_t> & val) {
    success = val;
  }

  bool operator == (const GraphQueryService_get_neighbors_atype_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const GraphQueryService_get_neighbors_atype_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const GraphQueryService_get_neighbors_atype_result & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _GraphQueryService_get_neighbors_atype_presult__isset {
  _GraphQueryService_get_neighbors_atype_presult__isset() : success(false) {}
  bool success;
} _GraphQueryService_get_neighbors_atype_presult__isset;

class GraphQueryService_get_neighbors_atype_presult {
 public:


  virtual ~GraphQueryService_get_neighbors_atype_presult() throw() {}

  std::vector<int64_t> * success;

  _GraphQueryService_get_neighbors_atype_presult__isset __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

typedef struct _GraphQueryService_get_neighbors_attr_args__isset {
  _GraphQueryService_get_neighbors_attr_args__isset() : nodeId(false), attrId(false), attrKey(false) {}
  bool nodeId;
  bool attrId;
  bool attrKey;
} _GraphQueryService_get_neighbors_attr_args__isset;

class GraphQueryService_get_neighbors_attr_args {
 public:

  GraphQueryService_get_neighbors_attr_args() : nodeId(0), attrId(0), attrKey() {
  }

  virtual ~GraphQueryService_get_neighbors_attr_args() throw() {}

  int64_t nodeId;
  int32_t attrId;
  std::string attrKey;

  _GraphQueryService_get_neighbors_attr_args__isset __isset;

  void __set_nodeId(const int64_t val) {
    nodeId = val;
  }

  void __set_attrId(const int32_t val) {
    attrId = val;
  }

  void __set_attrKey(const std::string& val) {
    attrKey = val;
  }

  bool operator == (const GraphQueryService_get_neighbors_attr_args & rhs) const
  {
    if (!(nodeId == rhs.nodeId))
      return false;
    if (!(attrId == rhs.attrId))
      return false;
    if (!(attrKey == rhs.attrKey))
      return false;
    return true;
  }
  bool operator != (const GraphQueryService_get_neighbors_attr_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const GraphQueryService_get_neighbors_attr_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};


class GraphQueryService_get_neighbors_attr_pargs {
 public:


  virtual ~GraphQueryService_get_neighbors_attr_pargs() throw() {}

  const int64_t* nodeId;
  const int32_t* attrId;
  const std::string* attrKey;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _GraphQueryService_get_neighbors_attr_result__isset {
  _GraphQueryService_get_neighbors_attr_result__isset() : success(false) {}
  bool success;
} _GraphQueryService_get_neighbors_attr_result__isset;

class GraphQueryService_get_neighbors_attr_result {
 public:

  GraphQueryService_get_neighbors_attr_result() {
  }

  virtual ~GraphQueryService_get_neighbors_attr_result() throw() {}

  std::vector<int64_t>  success;

  _GraphQueryService_get_neighbors_attr_result__isset __isset;

  void __set_success(const std::vector<int64_t> & val) {
    success = val;
  }

  bool operator == (const GraphQueryService_get_neighbors_attr_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const GraphQueryService_get_neighbors_attr_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const GraphQueryService_get_neighbors_attr_result & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _GraphQueryService_get_neighbors_attr_presult__isset {
  _GraphQueryService_get_neighbors_attr_presult__isset() : success(false) {}
  bool success;
} _GraphQueryService_get_neighbors_attr_presult__isset;

class GraphQueryService_get_neighbors_attr_presult {
 public:


  virtual ~GraphQueryService_get_neighbors_attr_presult() throw() {}

  std::vector<int64_t> * success;

  _GraphQueryService_get_neighbors_attr_presult__isset __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

typedef struct _GraphQueryService_get_nodes_args__isset {
  _GraphQueryService_get_nodes_args__isset() : attrId(false), attrKey(false) {}
  bool attrId;
  bool attrKey;
} _GraphQueryService_get_nodes_args__isset;

class GraphQueryService_get_nodes_args {
 public:

  GraphQueryService_get_nodes_args() : attrId(0), attrKey() {
  }

  virtual ~GraphQueryService_get_nodes_args() throw() {}

  int32_t attrId;
  std::string attrKey;

  _GraphQueryService_get_nodes_args__isset __isset;

  void __set_attrId(const int32_t val) {
    attrId = val;
  }

  void __set_attrKey(const std::string& val) {
    attrKey = val;
  }

  bool operator == (const GraphQueryService_get_nodes_args & rhs) const
  {
    if (!(attrId == rhs.attrId))
      return false;
    if (!(attrKey == rhs.attrKey))
      return false;
    return true;
  }
  bool operator != (const GraphQueryService_get_nodes_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const GraphQueryService_get_nodes_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};


class GraphQueryService_get_nodes_pargs {
 public:


  virtual ~GraphQueryService_get_nodes_pargs() throw() {}

  const int32_t* attrId;
  const std::string* attrKey;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _GraphQueryService_get_nodes_result__isset {
  _GraphQueryService_get_nodes_result__isset() : success(false) {}
  bool success;
} _GraphQueryService_get_nodes_result__isset;

class GraphQueryService_get_nodes_result {
 public:

  GraphQueryService_get_nodes_result() {
  }

  virtual ~GraphQueryService_get_nodes_result() throw() {}

  std::vector<int64_t>  success;

  _GraphQueryService_get_nodes_result__isset __isset;

  void __set_success(const std::vector<int64_t> & val) {
    success = val;
  }

  bool operator == (const GraphQueryService_get_nodes_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const GraphQueryService_get_nodes_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const GraphQueryService_get_nodes_result & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _GraphQueryService_get_nodes_presult__isset {
  _GraphQueryService_get_nodes_presult__isset() : success(false) {}
  bool success;
} _GraphQueryService_get_nodes_presult__isset;

class GraphQueryService_get_nodes_presult {
 public:


  virtual ~GraphQueryService_get_nodes_presult() throw() {}

  std::vector<int64_t> * success;

  _GraphQueryService_get_nodes_presult__isset __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

typedef struct _GraphQueryService_get_nodes2_args__isset {
  _GraphQueryService_get_nodes2_args__isset() : attrId1(false), attrKey1(false), attrId2(false), attrKey2(false) {}
  bool attrId1;
  bool attrKey1;
  bool attrId2;
  bool attrKey2;
} _GraphQueryService_get_nodes2_args__isset;

class GraphQueryService_get_nodes2_args {
 public:

  GraphQueryService_get_nodes2_args() : attrId1(0), attrKey1(), attrId2(0), attrKey2() {
  }

  virtual ~GraphQueryService_get_nodes2_args() throw() {}

  int32_t attrId1;
  std::string attrKey1;
  int32_t attrId2;
  std::string attrKey2;

  _GraphQueryService_get_nodes2_args__isset __isset;

  void __set_attrId1(const int32_t val) {
    attrId1 = val;
  }

  void __set_attrKey1(const std::string& val) {
    attrKey1 = val;
  }

  void __set_attrId2(const int32_t val) {
    attrId2 = val;
  }

  void __set_attrKey2(const std::string& val) {
    attrKey2 = val;
  }

  bool operator == (const GraphQueryService_get_nodes2_args & rhs) const
  {
    if (!(attrId1 == rhs.attrId1))
      return false;
    if (!(attrKey1 == rhs.attrKey1))
      return false;
    if (!(attrId2 == rhs.attrId2))
      return false;
    if (!(attrKey2 == rhs.attrKey2))
      return false;
    return true;
  }
  bool operator != (const GraphQueryService_get_nodes2_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const GraphQueryService_get_nodes2_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};


class GraphQueryService_get_nodes2_pargs {
 public:


  virtual ~GraphQueryService_get_nodes2_pargs() throw() {}

  const int32_t* attrId1;
  const std::string* attrKey1;
  const int32_t* attrId2;
  const std::string* attrKey2;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _GraphQueryService_get_nodes2_result__isset {
  _GraphQueryService_get_nodes2_result__isset() : success(false) {}
  bool success;
} _GraphQueryService_get_nodes2_result__isset;

class GraphQueryService_get_nodes2_result {
 public:

  GraphQueryService_get_nodes2_result() {
  }

  virtual ~GraphQueryService_get_nodes2_result() throw() {}

  std::vector<int64_t>  success;

  _GraphQueryService_get_nodes2_result__isset __isset;

  void __set_success(const std::vector<int64_t> & val) {
    success = val;
  }

  bool operator == (const GraphQueryService_get_nodes2_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const GraphQueryService_get_nodes2_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const GraphQueryService_get_nodes2_result & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _GraphQueryService_get_nodes2_presult__isset {
  _GraphQueryService_get_nodes2_presult__isset() : success(false) {}
  bool success;
} _GraphQueryService_get_nodes2_presult__isset;

class GraphQueryService_get_nodes2_presult {
 public:


  virtual ~GraphQueryService_get_nodes2_presult() throw() {}

  std::vector<int64_t> * success;

  _GraphQueryService_get_nodes2_presult__isset __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

class GraphQueryServiceClient : virtual public GraphQueryServiceIf {
 public:
  GraphQueryServiceClient(boost::shared_ptr< ::apache::thrift::protocol::TProtocol> prot) :
    piprot_(prot),
    poprot_(prot) {
    iprot_ = prot.get();
    oprot_ = prot.get();
  }
  GraphQueryServiceClient(boost::shared_ptr< ::apache::thrift::protocol::TProtocol> iprot, boost::shared_ptr< ::apache::thrift::protocol::TProtocol> oprot) :
    piprot_(iprot),
    poprot_(oprot) {
    iprot_ = iprot.get();
    oprot_ = oprot.get();
  }
  boost::shared_ptr< ::apache::thrift::protocol::TProtocol> getInputProtocol() {
    return piprot_;
  }
  boost::shared_ptr< ::apache::thrift::protocol::TProtocol> getOutputProtocol() {
    return poprot_;
  }
  void get_neighbors(std::vector<int64_t> & _return, const int64_t nodeId);
  void send_get_neighbors(const int64_t nodeId);
  void recv_get_neighbors(std::vector<int64_t> & _return);
  void get_neighbors_atype(std::vector<int64_t> & _return, const int64_t nodeId, const int64_t atype);
  void send_get_neighbors_atype(const int64_t nodeId, const int64_t atype);
  void recv_get_neighbors_atype(std::vector<int64_t> & _return);
  void get_neighbors_attr(std::vector<int64_t> & _return, const int64_t nodeId, const int32_t attrId, const std::string& attrKey);
  void send_get_neighbors_attr(const int64_t nodeId, const int32_t attrId, const std::string& attrKey);
  void recv_get_neighbors_attr(std::vector<int64_t> & _return);
  void get_nodes(std::vector<int64_t> & _return, const int32_t attrId, const std::string& attrKey);
  void send_get_nodes(const int32_t attrId, const std::string& attrKey);
  void recv_get_nodes(std::vector<int64_t> & _return);
  void get_nodes2(std::vector<int64_t> & _return, const int32_t attrId1, const std::string& attrKey1, const int32_t attrId2, const std::string& attrKey2);
  void send_get_nodes2(const int32_t attrId1, const std::string& attrKey1, const int32_t attrId2, const std::string& attrKey2);
  void recv_get_nodes2(std::vector<int64_t> & _return);
 protected:
  boost::shared_ptr< ::apache::thrift::protocol::TProtocol> piprot_;
  boost::shared_ptr< ::apache::thrift::protocol::TProtocol> poprot_;
  ::apache::thrift::protocol::TProtocol* iprot_;
  ::apache::thrift::protocol::TProtocol* oprot_;
};

class GraphQueryServiceProcessor : public ::apache::thrift::TDispatchProcessor {
 protected:
  boost::shared_ptr<GraphQueryServiceIf> iface_;
  virtual bool dispatchCall(::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, const std::string& fname, int32_t seqid, void* callContext);
 private:
  typedef  void (GraphQueryServiceProcessor::*ProcessFunction)(int32_t, ::apache::thrift::protocol::TProtocol*, ::apache::thrift::protocol::TProtocol*, void*);
  typedef std::map<std::string, ProcessFunction> ProcessMap;
  ProcessMap processMap_;
  void process_get_neighbors(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, void* callContext);
  void process_get_neighbors_atype(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, void* callContext);
  void process_get_neighbors_attr(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, void* callContext);
  void process_get_nodes(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, void* callContext);
  void process_get_nodes2(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, void* callContext);
 public:
  GraphQueryServiceProcessor(boost::shared_ptr<GraphQueryServiceIf> iface) :
    iface_(iface) {
    processMap_["get_neighbors"] = &GraphQueryServiceProcessor::process_get_neighbors;
    processMap_["get_neighbors_atype"] = &GraphQueryServiceProcessor::process_get_neighbors_atype;
    processMap_["get_neighbors_attr"] = &GraphQueryServiceProcessor::process_get_neighbors_attr;
    processMap_["get_nodes"] = &GraphQueryServiceProcessor::process_get_nodes;
    processMap_["get_nodes2"] = &GraphQueryServiceProcessor::process_get_nodes2;
  }

  virtual ~GraphQueryServiceProcessor() {}
};

class GraphQueryServiceProcessorFactory : public ::apache::thrift::TProcessorFactory {
 public:
  GraphQueryServiceProcessorFactory(const ::boost::shared_ptr< GraphQueryServiceIfFactory >& handlerFactory) :
      handlerFactory_(handlerFactory) {}

  ::boost::shared_ptr< ::apache::thrift::TProcessor > getProcessor(const ::apache::thrift::TConnectionInfo& connInfo);

 protected:
  ::boost::shared_ptr< GraphQueryServiceIfFactory > handlerFactory_;
};

class GraphQueryServiceMultiface : virtual public GraphQueryServiceIf {
 public:
  GraphQueryServiceMultiface(std::vector<boost::shared_ptr<GraphQueryServiceIf> >& ifaces) : ifaces_(ifaces) {
  }
  virtual ~GraphQueryServiceMultiface() {}
 protected:
  std::vector<boost::shared_ptr<GraphQueryServiceIf> > ifaces_;
  GraphQueryServiceMultiface() {}
  void add(boost::shared_ptr<GraphQueryServiceIf> iface) {
    ifaces_.push_back(iface);
  }
 public:
  void get_neighbors(std::vector<int64_t> & _return, const int64_t nodeId) {
    size_t sz = ifaces_.size();
    size_t i = 0;
    for (; i < (sz - 1); ++i) {
      ifaces_[i]->get_neighbors(_return, nodeId);
    }
    ifaces_[i]->get_neighbors(_return, nodeId);
    return;
  }

  void get_neighbors_atype(std::vector<int64_t> & _return, const int64_t nodeId, const int64_t atype) {
    size_t sz = ifaces_.size();
    size_t i = 0;
    for (; i < (sz - 1); ++i) {
      ifaces_[i]->get_neighbors_atype(_return, nodeId, atype);
    }
    ifaces_[i]->get_neighbors_atype(_return, nodeId, atype);
    return;
  }

  void get_neighbors_attr(std::vector<int64_t> & _return, const int64_t nodeId, const int32_t attrId, const std::string& attrKey) {
    size_t sz = ifaces_.size();
    size_t i = 0;
    for (; i < (sz - 1); ++i) {
      ifaces_[i]->get_neighbors_attr(_return, nodeId, attrId, attrKey);
    }
    ifaces_[i]->get_neighbors_attr(_return, nodeId, attrId, attrKey);
    return;
  }

  void get_nodes(std::vector<int64_t> & _return, const int32_t attrId, const std::string& attrKey) {
    size_t sz = ifaces_.size();
    size_t i = 0;
    for (; i < (sz - 1); ++i) {
      ifaces_[i]->get_nodes(_return, attrId, attrKey);
    }
    ifaces_[i]->get_nodes(_return, attrId, attrKey);
    return;
  }

  void get_nodes2(std::vector<int64_t> & _return, const int32_t attrId1, const std::string& attrKey1, const int32_t attrId2, const std::string& attrKey2) {
    size_t sz = ifaces_.size();
    size_t i = 0;
    for (; i < (sz - 1); ++i) {
      ifaces_[i]->get_nodes2(_return, attrId1, attrKey1, attrId2, attrKey2);
    }
    ifaces_[i]->get_nodes2(_return, attrId1, attrKey1, attrId2, attrKey2);
    return;
  }

};



#endif
