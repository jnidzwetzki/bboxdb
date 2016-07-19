import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import de.fernunihagen.dna.jkn.scalephant.network.client.ClientOperationFuture;
import de.fernunihagen.dna.jkn.scalephant.network.client.Scalephant;
import de.fernunihagen.dna.jkn.scalephant.network.client.ScalephantCluster;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;


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
		final ClientOperationFuture deleteGroupResult = scalephantClient.deleteDistributionGroup(distributionGroup);
		deleteGroupResult.get();
		if(deleteGroupResult.isFailed()) {
			System.err.println("Unable to delete distribution group: " + distributionGroup);
			System.exit(-1);
		}
		
		// Create a new distribution group
		final ClientOperationFuture createGroupResult = scalephantClient.createDistributionGroup(distributionGroup, (short) 3);
		createGroupResult.get();
		if(createGroupResult.isFailed()) {
			System.err.println("Unable to create distribution group: " + distributionGroup);
			System.exit(-1);
		}
		
		// Insert two new tuples
		final Tuple tuple1 = new Tuple("key1", new BoundingBox(0f, 5f, 0f, 1f), "mydata1".getBytes());
		final ClientOperationFuture insertResult1 = scalephantClient.insertTuple(mytable, tuple1);
		
		final Tuple tuple2 = new Tuple("key2", new BoundingBox(-1f, 2f, -1f, 2f), "mydata2".getBytes());
		final ClientOperationFuture insertResult2 = scalephantClient.insertTuple(mytable, tuple2);
		
		// Wait for the insert operations to complete
		insertResult1.get();
		insertResult2.get();
		
		if(insertResult1.isFailed() || insertResult2.isFailed()) {
			System.err.println("Unable to insert tuples");
			System.exit(-1);
		}
		
		// Query by key
		final ClientOperationFuture resultFuture1 = scalephantClient.queryKey(mytable, "key");
		
		// We got a future object, the search is performed asynchronous
		// Wait for the result
		final Tuple resultTuple = (Tuple) resultFuture1.get();
		System.out.println(resultTuple);
		
		// Query by bounding box
		final ClientOperationFuture resultFuture2 = scalephantClient.queryBoundingBox(mytable, new BoundingBox(-0.5f, 1f, -0.5f, 1f));
		
		// Again, we got a future object, the search is performed asynchronous
		final List<Tuple> resultList = (List<Tuple>) resultFuture2.get();
		System.out.println("Result list: " + resultList);
		
		scalephantClient.disconnect();
	}
	
}
