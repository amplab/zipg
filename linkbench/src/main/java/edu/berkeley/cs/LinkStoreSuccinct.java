package edu.berkeley.cs;

import com.facebook.LinkBench.GraphStore;
import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.Node;
import com.facebook.LinkBench.Phase;
import edu.berkeley.cs.zipg.GraphQueryAggregatorService;
import edu.berkeley.cs.zipg.ThriftAssoc;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class LinkStoreSuccinct extends GraphStore {

  private final Logger LOG = Logger.getLogger("com.facebook.linkbench");
  private GraphQueryAggregatorService.Client client;
  private TTransport transport;
  private AtomicLong idGenerator = new AtomicLong(1L);

  // Helper methods
  private Link thriftAssocToLink(ThriftAssoc a) {
    return new Link(a.srcId, a.atype, a.dstId, (byte) 0, a.attr.getBytes(), 0, a.timestamp);
  }

  private ThriftAssoc linkToThriftAssoc(Link a) {
    return new ThriftAssoc(a.id1, a.id2, a.link_type, a.time, new String(a.data));
  }

  /**
   * initialize the store object
   */
  @Override public void initialize(Properties p, Phase currentPhase, int threadId)
    throws Exception {
    String hostname = p.getProperty("hostname", "localhost");
    int port = Integer.parseInt(p.getProperty("port", "11001"));

    LOG.info("Attempting to connect to thrift server @ " + hostname + ":" + port);
    transport = new TSocket(hostname, port);
    client = new GraphQueryAggregatorService.Client(new TBinaryProtocol(transport));
    transport.open();
    LOG.info("Initializing connection.");
    client.init();
    LOG.info("Connection successful.");
    if (currentPhase == Phase.REQUEST) {
      long startId = Long.parseLong(p.getProperty("nodeidoffset")) + 1;
      LOG.info("Request phase: setting startId to " + startId);
      idGenerator.set(startId);
    }
  }

  /**
   * Reset node storage to a clean state in shard:
   * deletes all stored nodes
   * resets id allocation, with new IDs to be allocated starting from startID
   *
   * @param dbid
   * @param startID
   */
  @Override public void resetNodeStore(String dbid, long startID) throws Exception {
    // Not supported
  }

  /**
   * Adds a new node object to the database.
   * <p/>
   * This allocates a new id for the object and returns i.
   * <p/>
   * The benchmark assumes that, after resetStore() is called,
   * node IDs are allocated in sequence, i.e. startID, startID + 1, ...
   * Add node should return the next ID in the sequence.
   *
   * @param dbid the db shard to put that object in
   * @param node a node with all data aside from id filled in.  The id
   *             field is *not* updated to the new value by this function
   * @return the id allocated for the node
   */
  @Override public long addNode(String dbid, Node node) throws Exception {
    long id = idGenerator.getAndAdd(1L);
    return client.addNode(id, new String(node.data));
  }

  /**
   * Get a node of the specified type
   *
   * @param dbid the db shard the id is mapped to
   * @param type the type of the object
   * @param id   the id of the object
   * @return null if not found, a Node with all fields filled in otherwise
   */
  @Override public Node getNode(String dbid, int type, long id) throws Exception {
    String data = client.getNode(id);
    return new Node(id, type, 0, 0, data.getBytes());
  }

  /**
   * Update all parameters of the node specified.
   *
   * @param dbid
   * @param node
   * @return true if the update was successful, false if not present
   */
  @Override public boolean updateNode(String dbid, Node node) throws Exception {
    return client.updateNode(node.id, new String(node.data));
  }

  /**
   * Delete the object specified by the arguments
   *
   * @param dbid
   * @param type
   * @param id
   * @return true if the node was deleted, false if not present
   */
  @Override public boolean deleteNode(String dbid, int type, long id) throws Exception {
    return client.deleteNode(id);
  }

  /**
   * Do any cleanup.  After this is called, store won't be reused
   */
  @Override public void close() {
    transport.close();
  }

  @Override public void clearErrors(int threadID) {
    // Do nothing
  }

  /**
   * Add provided link to the store.  If already exists, update with new data
   *
   * @param dbid
   * @param a
   * @param noinverse
   * @return true if new link added, false if updated. Implementation is
   * optional, for informational purposes only.
   * @throws Exception
   */
  @Override public boolean addLink(String dbid, Link a, boolean noinverse) throws Exception {
    return client.addLink(linkToThriftAssoc(a));
  }

  /**
   * Delete link identified by parameters from store
   *
   * @param dbid
   * @param id1
   * @param link_type
   * @param id2
   * @param noinverse
   * @param expunge   if true, delete permanently.  If false, hide instead
   * @return true if row existed. Implementation is optional, for informational
   * purposes only.
   * @throws Exception
   */
  @Override public boolean deleteLink(String dbid, long id1, long link_type, long id2,
    boolean noinverse, boolean expunge) throws Exception {
    return client.deleteLink(id1, link_type, id2);
  }

  /**
   * Update a link in the database, or add if not found
   *
   * @param dbid
   * @param a
   * @param noinverse
   * @return true if link found, false if new link created.  Implementation is
   * optional, for informational purposes only.
   * @throws Exception
   */
  @Override public boolean updateLink(String dbid, Link a, boolean noinverse) throws Exception {
    return client.updateLink(linkToThriftAssoc(a));
  }

  /**
   * lookup using id1, type, id2
   * Returns hidden links.
   *
   * @param dbid
   * @param id1
   * @param link_type
   * @param id2
   * @return
   * @throws Exception
   */
  @Override public Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
    return thriftAssocToLink(client.getLink(id1, link_type, id2));
  }

  /**
   * lookup using just id1, type
   * Does not return hidden links
   *
   * @param dbid
   * @param id1
   * @param link_type
   * @return list of links in descending order of time, or null
   * if no matching links
   * @throws Exception
   */
  @Override public Link[] getLinkList(String dbid, long id1, long link_type) throws Exception {
    List<ThriftAssoc> assocList = client.getLinkList(id1, link_type);
    Link[] linkList = new Link[assocList.size()];
    int i = 0;
    for (ThriftAssoc assoc : assocList) {
      linkList[i++] = thriftAssocToLink(assoc);
    }
    return linkList;
  }

  /**
   * lookup using just id1, type
   * Does not return hidden links
   *
   * @param dbid
   * @param id1
   * @param link_type
   * @param minTimestamp
   * @param maxTimestamp
   * @param offset
   * @param limit
   * @return list of links in descending order of time, or null
   * if no matching links
   * @throws Exception
   */
  @Override public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp,
    long maxTimestamp, int offset, int limit) throws Exception {
    List<ThriftAssoc> assocList =
      client.getFilteredLinkList(id1, link_type, minTimestamp, maxTimestamp, offset, limit);
    Link[] linkList = new Link[assocList.size()];
    int i = 0;
    for (ThriftAssoc assoc : assocList) {
      linkList[i++] = thriftAssocToLink(assoc);
    }
    return linkList;
  }

  @Override public long countLinks(String dbid, long id1, long link_type) throws Exception {
    return client.countLinks(id1, link_type);
  }
}
