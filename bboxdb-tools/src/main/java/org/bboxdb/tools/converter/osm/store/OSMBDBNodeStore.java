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
package org.bboxdb.tools.converter.osm.store;

import java.io.File;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.tools.converter.osm.util.SerializableNode;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class OSMBDBNodeStore implements OSMNodeStore {

	/**
	 * The environments connection
	 */
	protected final List<Environment> environments = new ArrayList<>();

	/**
	 * The databases
	 */
	protected final List<Database> databases = new ArrayList<>();

	/**
	 * The pending node writes 
	 * 
	 * We have to use a list implementation here, instead of the 
	 * Java 7 BlockingQueue implementation, because we want to let 
	 * the elements placed in the queue until the BDB has stored them. 
	 * 
	 * The getNodeForId method reads this queue and query the BDB. 
	 * Removing elements before they are stored into the BDB could 
	 * cause missing elements.
	 */
	protected List<List<SerializableNode>> pendingWriteQueues = new ArrayList<>();
	
	/**
	 * The pending reads
	 */
	protected List<List<Long>> pendingReadQueue = new ArrayList<>();
	
	/**
	 * Max elements per pending write queue
	 */
	protected final static int MAX_ELEMENTS_PER_QUEUE = 200;

	/**
	 * Use transactions
	 */
	public final static boolean USE_TRANSACTIONS = false;
	
	/**
	 * The number of instances
	 */
	protected int instances;
	
	/**
	 * The thread pool
	 */
	protected ExecutorService threadPool = Executors.newCachedThreadPool();

	public OSMBDBNodeStore(final List<String> baseDir, final long inputLength) {

		if(baseDir.size() == 1) {
			this.instances = 4;
		} else {
			this.instances = baseDir.size();
		}
		
		// Prepare DB_Instances
		for (int i = 0; i < this.instances; i++) {

			final String workfolder = baseDir.get(i % baseDir.size());
			final String folderName = workfolder + "/osm_" + i;
			final File folder = new File(folderName);
			
			if(folder.exists()) {
				System.err.println("Folder already exists, exiting: " + folderName);
			}
			
			folder.mkdirs();

			pendingWriteQueues.add(new LinkedList<SerializableNode>());
			
			final EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setTransactional(USE_TRANSACTIONS);
			envConfig.setAllowCreate(true);
		    envConfig.setSharedCache(true);
		   // envConfig.setCachePercent(80);
		   /* 
		    envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
		    envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
		    envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_IN_COMPRESSOR, "false");
		*/	
			initNewBDBEnvironment(folder, envConfig);

			final BDBWriterThread bdbWriter = new BDBWriterThread(pendingWriteQueues.get(i), 
					environments.get(i), databases.get(i));
			
			threadPool.submit(bdbWriter);
		}

	}

	/**
	 * Init a new BDB environment in the given folder
	 * @param folder
	 */
	@SuppressWarnings("unused")
	protected void initNewBDBEnvironment(final File folder, final EnvironmentConfig envConfig) {

		final Environment dbEnv = new Environment(folder, envConfig);

		Transaction txn = null;
		if(USE_TRANSACTIONS) {
			txn = dbEnv.beginTransaction(null, null);
		}
		
		final DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(USE_TRANSACTIONS);
		dbConfig.setAllowCreate(true);
		dbConfig.setSortedDuplicates(true);
		dbConfig.setDeferredWrite(true);
		//dbConfig.setKeyPrefixing(true);
		//dbConfig.setNodeMaxEntries(128);
	
		final Database database = dbEnv.openDatabase(txn, "osm", dbConfig);

		if(txn != null) {
			txn.commit();
		}
		
		environments.add(dbEnv);
		databases.add(database);
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.performance.osm.store.OSMNodeStore#close()
	 */
	@Override
	public void close() {
		databases.stream().forEach(p -> p.close());
		databases.clear();

		environments.stream().forEach(p -> p.close());
		environments.clear();
		
		threadPool.shutdownNow();
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.performance.osm.store.OSMNodeStore#storeNode(org.openstreetmap.osmosis.core.domain.v0_6.Node)
	 */
	@Override
	public void storeNode(final Node node) throws Exception {

		final int connectionNumber = getConnectionPositionForNode(node.getId());
		final List<SerializableNode> queue = pendingWriteQueues.get(connectionNumber);

		// Submit write request to queue (write will be handled async in a BDBWriter thread)
		synchronized (queue) {
			while(queue.size() > MAX_ELEMENTS_PER_QUEUE) {
				queue.wait();
			}
			
			final SerializableNode serializableNode = new SerializableNode(node);
			queue.add(serializableNode);
			queue.notifyAll();
		}
	}

	/**
	 * Get the key db entry for a given node id
	 * @param node
	 * @return
	 */
	public static DatabaseEntry buildDatabaseKeyEntry(final long nodeId) {
		final ByteBuffer keyByteBuffer = DataEncoderHelper.longToByteBuffer(nodeId);
		return new DatabaseEntry(keyByteBuffer.array());
	}

	/**
	 * Get the database for nodes
	 * 
	 * @param node
	 * @return
	 */
	protected int getConnectionPositionForNode(final long nodeid) {
		return (int) (nodeid % instances);
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.performance.osm.store.OSMNodeStore#getNodeForId(long)
	 */
	@Override
	public SerializableNode getNodeForId(final long nodeId) throws SQLException {
		final int connectionNumber = getConnectionPositionForNode(nodeId);
		
		final Database database = databases.get(connectionNumber);
		final List<SerializableNode> queue = pendingWriteQueues.get(connectionNumber);
						
		synchronized (queue) {
			final SerializableNode node = queue
					.stream()
					.filter(n -> n.getId() == nodeId)
					.findFirst()
					.orElse(null);
			
			if(node != null) {
				return node;
			}
		}
		
		final DatabaseEntry key = buildDatabaseKeyEntry(nodeId);
	    final DatabaseEntry value = new DatabaseEntry();
	    
	    final OperationStatus result = database.get(null, key, value, LockMode.DEFAULT);
	    
	    if (result != OperationStatus.SUCCESS) {
	        throw new RuntimeException("Data insertion got status " + result);
	    }
	
		return SerializableNode.fromByteArray(value.getData());
	}
	
	@Override
	public int getInstances() {
		return instances;
	}
}
