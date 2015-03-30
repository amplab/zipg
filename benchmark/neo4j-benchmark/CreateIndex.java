import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;


public class CreateIndex {
//	private static final String DB_DIR = "target/";
	public static void main(String[] args) {
		String dataset = args[0];
//		String db_path = DB_DIR + dataset;
//		String db_path = "/Users/evanye/succinct-graph/benchmark/neo4j_1000_1";
		createIndex(dataset);
	}
	private static void createIndex(String db_path) {
		GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(db_path);
		registerShutdownHook(graphDb);
		IndexDefinition indexDefinition;
		try ( Transaction tx = graphDb.beginTx() )
		{
		    Schema schema = graphDb.schema();
		    indexDefinition = schema.indexFor( DynamicLabel.label( "Node" ) )
		            .on( "name" )
		            .create();
		    tx.success();
		}
		try ( Transaction tx = graphDb.beginTx() )
		{
		    Schema schema = graphDb.schema();
		    schema.awaitIndexOnline( indexDefinition, 5, TimeUnit.MINUTES );
		}
	}
	
	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				graphDb.shutdown();
			}
		});
	}
}
