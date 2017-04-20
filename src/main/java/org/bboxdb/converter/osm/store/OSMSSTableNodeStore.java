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
package org.bboxdb.converter.osm.store;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.bboxdb.BBoxDBConfigurationManager;
import org.bboxdb.converter.osm.util.SerializableNode;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.StorageRegistry;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.SSTableManager;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;

public class OSMSSTableNodeStore implements OSMNodeStore {
	
    
    /**
     * The number of instances
     */
    protected int instances;
    
    /**
     * The sstable manager
     */
	private SSTableManager storageManager;

	public OSMSSTableNodeStore(final List<String> baseDir, final long inputLength) {
		
		try {
			BBoxDBConfigurationManager.getConfiguration().setStorageCheckpointInterval(0);
			storageManager = StorageRegistry.getInstance().getSSTableManager(new SSTableName("2_group1_test"));
		} catch (StorageManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Close all resources
	 */
	public void close() {
		storageManager.shutdown();
	}

	/**
	 * Store a new node
	 * @param node
	 * @throws SQLException 
	 * @throws IOException 
	 * @throws StorageManagerException 
	 */
	public void storeNode(final Node node) throws StorageManagerException {
		
		final SerializableNode serializableNode = new SerializableNode(node);
		final byte[] nodeBytes = serializableNode.toByteArray();
		
		final Tuple tuple = new Tuple(Long.toString(node.getId()), BoundingBox.EMPTY_BOX, nodeBytes);
		storageManager.put(tuple);
	}

	/**
	 * Get the database for nodes
	 * @param node
	 * @return
	 */
	protected int getDatabaseForNode(final long nodeid) {
		return (int) (nodeid % instances);
	}
	
	/**
	 * Get the id for the node
	 * @param nodeId
	 * @return
	 * @throws SQLException 
	 * @throws StorageManagerException 
	 */
	public SerializableNode getNodeForId(final long nodeId) throws StorageManagerException {
		final Tuple tuple = storageManager.get(Long.toString(nodeId));
		
		final byte[] nodeBytes = tuple.getDataBytes();
		
		final SerializableNode node = SerializableNode.fromByteArray(nodeBytes);
		return node;
	}

	@Override
	public int getInstances() {
		return instances;
	}
}
