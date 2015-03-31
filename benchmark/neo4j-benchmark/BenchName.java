import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

public class BenchName {
	private static final String DB_DIR = "/home/ec2-user/target/";
	private static final String QUERY_DIR = "/home/ec2-user/queries/";
	private static final String OUTPUT_FILE = "neo4j_name_benchmark.txt";
	private static final long WARMUP_TIME = (long) (60 * 1E9); // 60 seconds
	private static final long MEASURE_TIME = (long) (120 * 1E9); // 120 seconds
	private static final long COOLDOWN_TIME = (long) (30 * 1E9); // 30 seconds

	public static void main(String[] args) {
		String dataset = args[0];
		String db_path = DB_DIR + dataset;
		String warmup_query_path = QUERY_DIR + "warmup_" + dataset + ".txt";
		String query_path = QUERY_DIR + "query_" + dataset + ".txt";
		nameThroughput(db_path, warmup_query_path, query_path);
	}

	private static void nameThroughput(String DB_PATH,
			String warmup_query_path, String query_path) {
		String[] warmupQueries = getQueries(warmup_query_path);
		String[] queries = getQueries(query_path);

		// START SNIPPET: startDb
		GraphDatabaseService graphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabase(DB_PATH);
		registerShutdownHook(graphDb);
		IndexDefinition indexDefinition;
		Label label = DynamicLabel.label("Node");
		try (Transaction tx = graphDb.beginTx()) {
			// warmup
			int i = 0;
			System.out.println("Warming up queries");
			long warmupStart = System.nanoTime();
			while (System.nanoTime() - warmupStart < WARMUP_TIME) {
				List<Node> nodes = getNodes(graphDb, label, warmupQueries[i
						% warmupQueries.length]);
				i++;
			}

			// measure
			i = 0;
			System.out.println("Measure queries");
			long edges = 0;
			double totalSeconds = 0;
			long start = System.nanoTime();
			while (System.nanoTime() - start < MEASURE_TIME) {
				long queryStart = System.nanoTime();
				List<Node> nodes = getNodes(graphDb, label, queries[i
						% queries.length]);
				long queryEnd = System.nanoTime();
				totalSeconds += (queryEnd - queryStart) / ((double) 1E9);
				i++;
			}
			double thput = ((double) i) / totalSeconds;
			
			System.out.println("Get Name throughput: " + thput);
			try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(OUTPUT_FILE, true)))) {
			    out.println(DB_PATH);
			    out.println(thput + "\n");
			} catch (IOException e) {
			    e.printStackTrace();
			}

			// cooldown
			i = 0;
			long cooldownStart = System.nanoTime();
			while (System.nanoTime() - cooldownStart < COOLDOWN_TIME) {
				List<Node> nodes = getNodes(graphDb, label, warmupQueries[i
						% warmupQueries.length]);
				i++;
			}
			tx.success();
		}
		System.out.println("Shutting down database ...");
		graphDb.shutdown();
	}

	private static List<Node> getNodes(GraphDatabaseService graphDb,
			Label label, String name) {
		try (ResourceIterator<Node> nodes = graphDb.findNodes(label, "name",
				name)) {
			ArrayList<Node> userNodes = new ArrayList<>();
			while (nodes.hasNext()) {
				userNodes.add(nodes.next());
			}
			return userNodes;
		}
	}

	private static String[] getQueries(String file) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			List<String> lines = new ArrayList<String>();
			String line = br.readLine();
			while (line != null) {
				lines.add(line);
				line = br.readLine();
			}
			String[] queries = new String[lines.size()];
			for (int i = 0; i < lines.size(); i++) {
				queries[i] = lines.get(i);
			}
			return queries;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
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
