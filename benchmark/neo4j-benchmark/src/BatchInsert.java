import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class BatchInsert {
	private final static String dataDirectory = "data/";
	private final static String targetDirectory = "target/";
	
	public static void main(String[] args) {
		File folder = new File(dataDirectory);
		for (File f: folder.listFiles()) {
			if (f.getName().toLowerCase().endsWith(".txt") &&
				!(new File(targetDirectory + fileName(f)).exists())) {
				try {
					System.out.println("processing: " + f.getName());
					createGraph(f);
					System.out.println("created graph for: " + f.getName());
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
		}
	}
	
	private static void createGraph(File f) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(f));
		BatchInserter inserter = BatchInserters.inserter(targetDirectory + fileName(f));
		RelationshipType edgeType = DynamicRelationshipType.withName("EDGE");
		boolean[] usedNodes = new boolean[31000000];
		String line = br.readLine();
		long edgesProcessed = 0;
		while (line != null) {
			String[] edge = line.split(" ");
			int fromNode = Integer.parseInt(edge[0]);
			int toNode = Integer.parseInt(edge[1]);
			if (!usedNodes[fromNode]) {
				usedNodes[fromNode] = true;
				inserter.createNode(fromNode, null);
			}
			if (!usedNodes[toNode]) {
				usedNodes[toNode] = true;
				inserter.createNode(toNode, null);
			}
			inserter.createRelationship(fromNode, toNode, edgeType, null);
			edgesProcessed++;
			if (edgesProcessed % 10000000 == 0) {
				System.out.println("processed " + edgesProcessed + " lines");
			}
			line = br.readLine();
		}
		inserter.shutdown();
	}
	
	private static String fileName(File f) {
		return f.getName().substring(0, f.getName().lastIndexOf('.'));
	}
}
