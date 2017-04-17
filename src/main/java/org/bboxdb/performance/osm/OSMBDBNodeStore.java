/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.performance.osm;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.bboxdb.performance.osm.util.SerializableNode;
import org.bboxdb.util.DataEncoderHelper;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class OSMBDBNodeStore {

	/**
	 * The environments connection
	 */
	protected final List<Environment> environments = new ArrayList<>();

	/**
	 * The databases
	 */
	protected final List<Database> databases = new ArrayList<>();
	
	/**
	 * Use transactions
	 */
	protected final boolean USE_TRANSACTIONS = false;

	/**
	 * The number of instances
	 */
	protected int instances;

	public OSMBDBNodeStore(final List<String> baseDir, final int instances) {

		//this.instances = instances;
		this.instances = 1;
		
		// Prepare DB_Instances
		for (int i = 0; i < this.instances; i++) {

			final String workfolder = baseDir.get(i % baseDir.size());

			final String folderName = workfolder + "/osm_" + i;
			final File folder = new File(folderName);
			
			if(folder.exists()) {
				System.err.println("Folder already exists, exiting: " + folderName);
			}
			
			folder.mkdirs();

			final EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setTransactional(USE_TRANSACTIONS);
			envConfig.setAllowCreate(true);
			final Environment dbEnv = new Environment(folder, envConfig);

			final Transaction txn = dbEnv.beginTransaction(null, null);
			final DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setTransactional(USE_TRANSACTIONS);
			dbConfig.setAllowCreate(true);
			dbConfig.setSortedDuplicates(true);
			final Database database = dbEnv.openDatabase(txn, "osm", dbConfig);
			txn.commit();

			environments.add(dbEnv);
			databases.add(database);
		}

	}

	/**
	 * Close all resources
	 */
	public void close() {
		databases.stream().forEach(p -> p.close());
		databases.clear();

		environments.stream().forEach(p -> p.close());
		environments.clear();
	}

	/**
	 * Store a new node
	 * 
	 * @param node
	 * @throws SQLException
	 * @throws IOException
	 */
	public void storeNode(final Node node) throws SQLException, IOException {

		final int connectionNumber = getDatabaseForNode(node.getId());
		
		final Environment environment = environments.get(connectionNumber);
		final Database database = databases.get(connectionNumber);
				
		final SerializableNode serializableNode = new SerializableNode(node);
		final byte[] nodeBytes = serializableNode.toByteArray();

		final Transaction txn = environment.beginTransaction(null, null);
		
		final DatabaseEntry key = getKey(node.getId());
		
		final DatabaseEntry value = new DatabaseEntry(nodeBytes);
        final OperationStatus status = database.put(txn, key, value);

        if (status != OperationStatus.SUCCESS) {
            throw new RuntimeException("Data insertion got status " + status);
        }
        
        txn.commit();
	}

	/**
	 * Get the key db entry for a given node id
	 * @param node
	 * @return
	 */
	protected DatabaseEntry getKey(final long nodeId) {
		final ByteBuffer keyByteBuffer = DataEncoderHelper.longToByteBuffer(nodeId);
		final DatabaseEntry key = new DatabaseEntry(keyByteBuffer.array());
		return key;
	}

	/**
	 * Get the database for nodes
	 * 
	 * @param node
	 * @return
	 */
	protected int getDatabaseForNode(final long nodeid) {
		return (int) (nodeid % instances);
	}

	/**
	 * Get the id for the node
	 * 
	 * @param nodeId
	 * @return
	 * @throws SQLException
	 */
	public SerializableNode getNodeForId(final long nodeId) throws SQLException {
		final int connectionNumber = getDatabaseForNode(nodeId);
		
		final Database database = databases.get(connectionNumber);
						
		final DatabaseEntry key = getKey(nodeId);
	    final DatabaseEntry value = new DatabaseEntry();
	    
	    final OperationStatus result = database.get(null, key, value, LockMode.DEFAULT);
	    
	    if (result != OperationStatus.SUCCESS) {
	        throw new RuntimeException("Data insertion got status " + result);
	    }
	
		final SerializableNode node = SerializableNode.fromByteArray(value.getData());
		return node;
	}
}