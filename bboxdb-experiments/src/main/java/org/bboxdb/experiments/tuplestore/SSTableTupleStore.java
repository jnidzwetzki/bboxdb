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
package org.bboxdb.experiments.tuplestore;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.bboxdb.commons.ServiceState;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableTupleStore implements TupleStore {

	/**
	 * The storage manager
	 */
	private TupleStoreManager storageManager;
	
	/**
	 * The database dir
	 */
	private File dir;
	
	/**
	 * The storage registry
	 */
	protected TupleStoreManagerRegistry storageRegistry;

	/**
	 * The sstable name
	 */
	protected final static TupleStoreName SSTABLE_NAME = new TupleStoreName("2_group1_test");

	/**
	 * The service state
	 */
	protected final ServiceState serviceState = new ServiceState();
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableTupleStore.class);


	public SSTableTupleStore(final File dir) {
		this.dir = dir;
	}

	@Override
	public void writeTuple(final Tuple tuple) throws Exception {
		storageManager.put(tuple);		
	}

	@Override
	public Tuple readTuple(final String key) throws Exception {
		final List<Tuple> tuples = storageManager.get(key);
		
		if(tuples.isEmpty()) {
			throw new RuntimeException("Unable to locate tuple for key: " + key);
		}
		
		return tuples.get(0);
	}

	@Override
	public void close() throws Exception {
		
		logger.info("Close for sstable {} called", SSTABLE_NAME.getFullname());

		if(! serviceState.isInRunningState()) {
			logger.error("Service state is not running, ignoring close");
			return;
		}
		
		serviceState.dispatchToStopping();
		
		if(storageRegistry != null) {
			storageRegistry.shutdown();
			storageRegistry = null;
		}
		
		serviceState.dispatchToTerminated();
	}

	@Override
	public void open() throws Exception {
		
		logger.info("Open for sstable {} called", SSTABLE_NAME.getFullname());
		
		if(serviceState.isInRunningState()) {
			logger.error("Service is already in running state, ignoring call");
			return;
		}
		
		serviceState.dipatchToStarting();
		
		BBoxDBConfigurationManager.getConfiguration().setStorageDirectories(Arrays.asList(dir.getAbsolutePath()));		

		final File dataDir = new File(dir.getAbsoluteFile() + "/data");
		dataDir.mkdirs();
		
		storageRegistry = new TupleStoreManagerRegistry();
		storageRegistry.init();
		
		storageManager = storageRegistry.getTupleStoreManager(SSTABLE_NAME);
		
		serviceState.dispatchToRunning();
	}
	
	/**
	 * Get the storage registry
	 * @return
	 */
	public TupleStoreManagerRegistry getStorageRegistry() {
		return storageRegistry;
	}
}
