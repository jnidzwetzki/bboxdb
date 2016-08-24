import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import de.fernunihagen.dna.scalephant.network.client.OperationFuture;
import de.fernunihagen.dna.scalephant.network.client.Scalephant;
import de.fernunihagen.dna.scalephant.network.client.ScalephantCluster;
import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;


public class ScalephantClientExample {

	/**
	 * Connect to the Scalephant Server at localhost and insert some tuples
	 * 
	 * @param args
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
		// A 2 dimensional table (member of distribution group 'mygroup3') with the name 'testdata'
		final String distributionGroup = "2_mygroup3"; 
		final String mytable = distributionGroup + "_testdata";
		
		// The name of the cluster
		final String clustername = "mycluster";
		
		// The zookeeper connect points
		final List<String> connectPoints = Arrays.asList("localhost:2181");
		
		// Connect to the server
		final Scalephant scalephantClient = new ScalephantCluster(connectPoints, clustername);
		scalephantClient.connect();
		
		// Check the connection state
		if (! scalephantClient.isConnected() ) {
			System.out.println("Error while connecting to the scalephant");
			System.exit(-1);
		}
		
		// Clean the old content of the distribution group
		final OperationFuture deleteGroupResult = scalephantClient.deleteDistributionGroup(distributionGroup);
		deleteGroupResult.waitForAll();
		if(deleteGroupResult.isFailed()) {
			System.err.println("Unable to delete distribution group: " + distributionGroup);
			System.exit(-1);
		}
		
		// Create a new distribution group
		final OperationFuture createGroupResult = scalephantClient.createDistributionGroup(distributionGroup, (short) 3);
		createGroupResult.waitForAll();
		if(createGroupResult.isFailed()) {
			System.err.println("Unable to create distribution group: " + distributionGroup);
			System.exit(-1);
		}
		
		// Insert two new tuples
		final Tuple tuple1 = new Tuple("key1", new BoundingBox(0f, 5f, 0f, 1f), "mydata1".getBytes());
		final OperationFuture insertResult1 = scalephantClient.insertTuple(mytable, tuple1);
		
		final Tuple tuple2 = new Tuple("key2", new BoundingBox(-1f, 2f, -1f, 2f), "mydata2".getBytes());
		final OperationFuture insertResult2 = scalephantClient.insertTuple(mytable, tuple2);
		
		// Wait for the insert operations to complete
		insertResult1.waitForAll();
		insertResult2.waitForAll();
		
		if(insertResult1.isFailed() || insertResult2.isFailed()) {
			System.err.println("Unable to insert tuples");
			System.exit(-1);
		}
		
		// Query by key
		final OperationFuture resultFuture1 = scalephantClient.queryKey(mytable, "key");
		
		// We got a future object, the search is performed asynchronous
		// Wait for the result
		for(int requestId = 0; requestId < resultFuture1.getNumberOfResultObjets(); requestId++) {
			final Object queryResult = resultFuture1.get(requestId);
			if(queryResult instanceof Tuple) {
				System.out.println(queryResult);
			} else { 
				System.out.println("Query failed");
			} 
		}
		
		// Query by bounding box
		final OperationFuture resultFuture2 = scalephantClient.queryBoundingBox(mytable, new BoundingBox(-0.5f, 1f, -0.5f, 1f));
		
		// Again, we got a future object, the search is performed asynchronous
		for(int requestId = 0; requestId < resultFuture2.getNumberOfResultObjets(); requestId++) {
			final Object queryResult = resultFuture2.get(requestId);
			if(queryResult instanceof List) {
				System.out.println("Result list: " + queryResult);
			} else { 
				System.out.println("Query failed");
			} 
		}

		scalephantClient.disconnect();
	}
	
}
