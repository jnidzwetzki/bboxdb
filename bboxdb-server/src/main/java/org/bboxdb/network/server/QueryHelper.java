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
