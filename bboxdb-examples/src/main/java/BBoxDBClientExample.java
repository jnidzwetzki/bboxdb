/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
 *  
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License. 
 *    
 *******************************************************************************/
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreConfigurationBuilder;
import org.bboxdb.storage.entity.Tuple;


public class BBoxDBClientExample {

	/**
	 * Connect to the BBoxDB Server at localhost and insert some tuples
	 * 
	 * @param args
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	public static void main(String[] args) throws InterruptedException, ExecutionException, BBoxDBException {
		
		// A 2 dimensional table (member of distribution group 'mygroup3') with the name 'testdata'
		final int dimensions = 2;
		final String distributionGroup = "mygroup3"; 
		final String mytable = distributionGroup + "_testdata";
		
		// The name of the cluster
		final String clustername = "mycluster";
		
		// The zookeeper connect points
		final List<String> connectPoints = Arrays.asList("localhost:2181");
		
		// Connect to the server
		final BBoxDB bboxdbClient = new BBoxDBCluster(connectPoints, clustername);
		bboxdbClient.connect();
		
		// Check the connection state
		if (! bboxdbClient.isConnected()) {
			System.out.println("Error while connecting to the BBoxDB cluster");
			System.exit(-1);
		}
		
		// Clean the old content of the distribution group
		final EmptyResultFuture deleteGroupResult = bboxdbClient.deleteDistributionGroup(distributionGroup);
		deleteGroupResult.waitForAll();
		if(deleteGroupResult.isFailed()) {
			System.err.println("Unable to delete distribution group: " + distributionGroup);
			System.err.println(deleteGroupResult.getAllMessages());
			System.exit(-1);
		}
		
		// Create a new distribution group
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder.create(dimensions)
				.withReplicationFactor((short) 3)
				.build();
		
		final EmptyResultFuture createGroupResult = bboxdbClient.createDistributionGroup(distributionGroup, 
				configuration);
		
		createGroupResult.waitForAll();
		if(createGroupResult.isFailed()) {
			System.err.println("Unable to create distribution group: " + distributionGroup);
			System.err.println(createGroupResult.getAllMessages());
			System.exit(-1);
		}
		
		// Create the table
		final TupleStoreConfiguration tableConfig = TupleStoreConfigurationBuilder.create()
				.allowDuplicates(false)
				.build();
		
		final EmptyResultFuture createTableResult = bboxdbClient.createTable(mytable, tableConfig);
		
		createTableResult.waitForAll();
		if(createTableResult.isFailed()) {
			System.err.println("Unable to create table group: " + mytable);
			System.err.println(createTableResult.getAllMessages());
			System.exit(-1);
		}
		
		// Insert two new tuples
		final Tuple tuple1 = new Tuple("key1", new BoundingBox(0d, 5d, 0d, 1d), "mydata1".getBytes());
		final EmptyResultFuture insertResult1 = bboxdbClient.insertTuple(mytable, tuple1);
		
		final Tuple tuple2 = new Tuple("key2", new BoundingBox(-1d, 2d, -1d, 2d), "mydata2".getBytes());
		final EmptyResultFuture insertResult2 = bboxdbClient.insertTuple(mytable, tuple2);
		
		// Wait for the insert operations to complete
		insertResult1.waitForAll();
		insertResult2.waitForAll();
		
		if(insertResult1.isFailed()) {
			System.err.println("Unable to insert tuple: " + insertResult1.getAllMessages());
			System.exit(-1);
		}
		
		if(insertResult2.isFailed()) {
			System.err.println("Unable to insert tuple: " + insertResult2.getAllMessages());
			System.exit(-1);
		}
		
		// Query by key
		final TupleListFuture resultFuture1 = bboxdbClient.queryKey(mytable, "key");
		
		// We got a future object, the search is performed asynchronous
		// Wait for the result
		resultFuture1.waitForAll();
		
		if(resultFuture1.isFailed()) {
			System.err.println("Future is failed: " + resultFuture1.getAllMessages());
			System.exit(-1);
		}
		
		// Output all tuples
		for(final Tuple tuple : resultFuture1) {
			System.out.println(tuple);
		}
		
		// Query by bounding box
		final TupleListFuture resultFuture2 = bboxdbClient.queryBoundingBox(mytable, new BoundingBox(-0.5d, 1d, -0.5d, 1d));
		
		// Again, we got a future object, the search is performed asynchronous
		resultFuture2.waitForAll();
		
		if(resultFuture2.isFailed()) {
			System.err.println("Future is failed: " + resultFuture2.getAllMessages());
			System.exit(-1);
		}
		
		// Output all tuples
		for(final Tuple tuple : resultFuture2) {
			System.out.println("Tuple: " + tuple);
		}

		bboxdbClient.disconnect();
	}
	
}
