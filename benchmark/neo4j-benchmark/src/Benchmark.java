import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Benchmark {
	private static final String DB_PATH = "target/100000";
	private static final long WARMUP_TIME = (long) (60 * 1E9); // 60 seconds
	private static final long MEASURE_TIME = (long) (120 * 1E9); // 120 seconds
	private static final long COOLDOWN_TIME = (long) (10 * 1E9); // 10 seconds
	
	public static void main(String[] args) {
		benchmark();
	}
	
	private static void benchmark() {
		GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		registerShutdownHook(graphDb);
		try (Transaction tx = graphDb.beginTx()) {
			List<Node> tempNodes = new LinkedList<>();
			for (Node n: graphDb.getAllNodes()) {
				tempNodes.add(n);
			}
			int N = tempNodes.size();
			
			Node[] nodes = new Node[N];
			for (int i = 0; i < N; i++) {
				nodes[i] = tempNodes.remove(0);
			}
			
			Random rand = new Random();
			// TODO: use the same random_indices size as succinct benchmark
			// TODO: use different query files for warmup and measure
			// TODO: remove cooldown benchmarking
			// TODO: larger graphs
			int[] random_indices = new int[100000000];
			for (int i = 0; i < random_indices.length; i++) {
				random_indices[i] = rand.nextInt(N);
			}
			
			//warmup
			int queries = 0;
			long warmupStart = System.nanoTime();
			while (System.nanoTime() - warmupStart < WARMUP_TIME) {
				List<Node> neighbors = getNeighbors(nodes[random_indices[queries]]);
				queries++;
			}
			
			//measure
			queries = 0;
			long edges = 0;
			double totalSeconds = 0;
			long start = System.nanoTime();
			while (System.nanoTime() - start < MEASURE_TIME) {
				long queryStart = System.nanoTime();
				List<Node> neighbors = getNeighbors(nodes[random_indices[queries]]);
				long queryEnd = System.nanoTime();
				totalSeconds += (queryEnd - queryStart) / (1000.0 * 1000.0 * 1000.0);
				edges += neighbors.size();
				queries++;
			}
			double getNeighborThput = ((double) queries) / totalSeconds;
			double edgesThput = ((double) edges) / totalSeconds;
			
			//cooldown
			queries = 0;
			long cooldownStart = System.nanoTime();
			while (System.nanoTime() - cooldownStart < COOLDOWN_TIME) {
				List<Node> neighbors = getNeighbors(nodes[random_indices[queries]]);
				queries++;
			}
			
			System.out.println("neighbor throughput: " + getNeighborThput);
			System.out.println("edge throughput: " + edgesThput);
			
			tx.success();
		}
	}
	
	private static List<Node> getNeighbors(Node n) {
		List<Node> neighbors = new LinkedList<>();
		for (Relationship r: n.getRelationships(Direction.OUTGOING)) {
			neighbors.add(r.getOtherNode(n));
		}
		return neighbors;
	}
	
	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				graphDb.shutdown();
			}
		});
	}
}
