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
package org.bboxdb.network.server;

import java.io.IOException;

import org.bboxdb.distribution.zookeeper.TupleStoreAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.server.connection.ClientConnectionHandler;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryHelper {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(QueryHelper.class);

	
	/**
	 * Get or create the tuple store manager
	 * @param storageRegistry
	 * @param tupleStoreName
	 * @return
	 * @throws ZookeeperException
	 * @throws StorageManagerException
	 */
	public static TupleStoreManager getTupleStoreManager(TupleStoreManagerRegistry storageRegistry, 
			final TupleStoreName tupleStoreName) throws ZookeeperException, StorageManagerException {
		
		if(storageRegistry.isStorageManagerKnown(tupleStoreName)) {
			return storageRegistry.getTupleStoreManager(tupleStoreName);
		}
			
		final TupleStoreAdapter tupleStoreAdapter = ZookeeperClientFactory
				.getZookeeperClient().getTupleStoreAdapter();
		
		if(! tupleStoreAdapter.isTableKnown(tupleStoreName)) {
			throw new StorageManagerException("Table: " + tupleStoreName.getFullname() + " is unkown");
		}
		
		final TupleStoreConfiguration config = tupleStoreAdapter.readTuplestoreConfiguration(tupleStoreName);
		
		return storageRegistry.createTableIfNotExist(tupleStoreName, config);
	}

	/**
	 * Handle a non existing table
	 * @param requestTable
	 * @param clientConnectionHandler
	 * @return
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	public static boolean handleNonExstingTable(final TupleStoreName requestTable, 
			final short packageSequence,	final ClientConnectionHandler clientConnectionHandler) 
					throws IOException, PackageEncodeException {
		
		
		// Table is locally known, prevent expensive zookeeper call
		final TupleStoreManagerRegistry storageRegistry = clientConnectionHandler.getStorageRegistry();
		
		if(storageRegistry.isStorageManagerKnown(requestTable)) {
			return true;
		}
		
		final TupleStoreAdapter tupleStoreAdapter = ZookeeperClientFactory
				.getZookeeperClient().getTupleStoreAdapter();

		try {
			final boolean tableKnown = tupleStoreAdapter.isTableKnown(requestTable);
			
			if(! tableKnown) {
				clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_TABLE_NOT_EXIST));	
				return false;
			}
		} catch (ZookeeperException e) {
			logger.error("Got an exception while query for table", e);
			return false;
		}
		
		return true;
	}
}
