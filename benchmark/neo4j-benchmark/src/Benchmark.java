import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Benchmark {
	private static final String DB_DIR = "target/";
	private static final long WARMUP_TIME = (long) (60 * 1E9); // 60 seconds
	private static final long MEASURE_TIME = (long) (120 * 1E9); // 120 seconds
	private static final long COOLDOWN_TIME = (long) (10 * 1E9); // 10 seconds

	public static void main(String[] args) {
		String dataset = args[0];
		String db_path = DB_DIR + dataset;
		String warmup_query_path = "queries/warmup" + dataset + ".txt";
		String query_path = "queries/query" + dataset + ".txt";
		benchmark(db_path, warmup_query_path, query_path);
	}

	private static void benchmark(String db_path, String warmup_query_path,
			String query_path) {
		int[] warmupQueries = getQueries(warmup_query_path);
		int[] queries = getQueries(query_path);
		GraphDatabaseService graphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabase(db_path);
		registerShutdownHook(graphDb);
		try (Transaction tx = graphDb.beginTx()) {
			List<Node> tempNodes = new LinkedList<>();
			for (Node n : graphDb.getAllNodes()) {
				tempNodes.add(n);
			}
			int N = tempNodes.size();

			Node[] nodes = new Node[N];
			for (int i = 0; i < N; i++) {
				nodes[i] = tempNodes.remove(0);
			}

			// warmup
			int i = 0;
			long warmupStart = System.nanoTime();
			while (System.nanoTime() - warmupStart < WARMUP_TIME) {
				List<Node> neighbors = getNeighbors(nodes[warmupQueries[i % warmupQueries.length]]);
				i++;
			}

			// measure
			i = 0;
			long edges = 0;
			double totalSeconds = 0;
			long start = System.nanoTime();
			while (System.nanoTime() - start < MEASURE_TIME) {
				long queryStart = System.nanoTime();
				List<Node> neighbors = getNeighbors(nodes[queries[i % queries.length]]);
				long queryEnd = System.nanoTime();
				totalSeconds += (queryEnd - queryStart) / ((double) 1E9);
				edges += neighbors.size();
				i++;
			}
			double getNeighborThput = ((double) i) / totalSeconds;
			double edgesThput = ((double) edges) / totalSeconds;

			// cooldown
			i = 0;
			long cooldownStart = System.nanoTime();
			while (System.nanoTime() - cooldownStart < COOLDOWN_TIME) {
				List<Node> neighbors = getNeighbors(nodes[warmupQueries[i % warmupQueries.length]]);
				i++;
			}

			System.out.println("neighbor throughput: " + getNeighborThput);
			System.out.println("edge throughput: " + edgesThput);

			tx.success();
		}
	}

	private static List<Node> getNeighbors(Node n) {
		List<Node> neighbors = new LinkedList<>();
		for (Relationship r : n.getRelationships(Direction.OUTGOING)) {
			neighbors.add(r.getOtherNode(n));
		}
		return neighbors;
	}

	private static int[] getQueries(String file) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			List<String> lines = new ArrayList<String>();
			String line = br.readLine();
			while (line != null) {
				lines.add(line);
				line = br.readLine();
			}
			int[] queries = new int[lines.size()];
			for (int i = 0; i < lines.size(); i++) {
				queries[i] = Integer.parseInt(lines.get(i));
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
