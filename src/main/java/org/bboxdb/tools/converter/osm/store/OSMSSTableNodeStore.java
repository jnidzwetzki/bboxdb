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
package org.bboxdb.tools.converter.osm.store;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.facade.StorageRegistry;
import org.bboxdb.storage.sstable.SSTableManager;
import org.bboxdb.tools.converter.osm.util.SerializableNode;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OSMSSTableNodeStore implements OSMNodeStore {
    
    /**
     * The number of instances
     */
    protected int instances;
    
    /**
     * The sstable manager
     */
	private SSTableManager storageManager;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(OSMSSTableNodeStore.class);


	public OSMSSTableNodeStore(final List<String> storageDirectories, final long inputLength) {
		
		try {
			final SSTableName tableName = new SSTableName("2_group1_test");
			BBoxDBConfigurationManager.getConfiguration().setStorageDirectories(storageDirectories);
			
			StorageRegistry.getInstance().deleteTable(tableName);
			storageManager = StorageRegistry.getInstance().getSSTableManager(tableName);
		} catch (StorageManagerException e) {
			logger.error("Got an exception while getting sstable manager: ", e);
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
		
		if(tuple == null) {
			throw new StorageManagerException("Unable to locate tuple for: " + nodeId);
		}
		
		final byte[] nodeBytes = tuple.getDataBytes();
		return SerializableNode.fromByteArray(nodeBytes);
	}

	@Override
	public int getInstances() {
		return instances;
	}
}
