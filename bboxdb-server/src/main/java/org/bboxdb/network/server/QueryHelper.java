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

import org.bboxdb.distribution.zookeeper.TupleStoreAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;

public class QueryHelper {
	
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
		
		if(! storageRegistry.isStorageManagerKnown(tupleStoreName)) {
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
			final TupleStoreAdapter tupleStoreAdapter = new TupleStoreAdapter(zookeeperClient);
			
			final TupleStoreConfiguration config = tupleStoreAdapter.readTuplestoreConfiguration(tupleStoreName);
			
			return storageRegistry.createTableIfNotExist(tupleStoreName, config);
		} else {
			return storageRegistry.getTupleStoreManager(tupleStoreName);
		}
	}
}
