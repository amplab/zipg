/**
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

public class BenchName {
	// private static final String DB_PATH =
	// "target/neo4j-store-with-new-indexing";
	private static final String DB_DIR = "/Users/evanye/succinct-graph/benchmark/";

	public static void main(String[] args) {
		// String dataset = args[0];
		String dataset = "1000_10";
		String db_path = DB_DIR + "neo4j_" + dataset;
		String warmup_query_path = DB_DIR + "warmup_" + dataset + ".txt";
		String query_path = DB_DIR + "query_" + dataset + ".txt";
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
			Schema schema = graphDb.schema();
			if (!schema.getIndexes(label).iterator().hasNext()) {
				indexDefinition = schema.indexFor(label).on("name").create();

				schema.awaitIndexOnline(indexDefinition, 1000, TimeUnit.SECONDS);
			}
			tx.success();
		}

		{
			// START SNIPPET: findUsers

			String nameToFind = warmupQueries[0];
			System.out.println(nameToFind);
			try (Transaction tx = graphDb.beginTx()) {
				try (ResourceIterator<Node> nodes = graphDb.findNodes(label,
						"name", nameToFind)) {
					ArrayList<Node> userNodes = new ArrayList<>();
					while (nodes.hasNext()) {
						userNodes.add(nodes.next());
					}
					for (Node node : userNodes) {
						System.out.println("The id of user " + nameToFind
								+ " is " + node.getId());
					}
				}
			}
			// END SNIPPET: findUsers

		}
		System.out.println("Shutting down database ...");
		// START SNIPPET: shutdownDb
		graphDb.shutdown();
		// END SNIPPET: shutdownDb
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
