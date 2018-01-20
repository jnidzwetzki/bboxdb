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
package org.bboxdb.storage.tuplestore.manager;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.bboxdb.commons.ServiceState;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.sstable.SSTableHelper;
import org.bboxdb.storage.tuplestore.DiskStorage;
import org.bboxdb.storage.tuplestore.TupleStoreLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class TupleStoreManagerRegistry implements BBoxDBService {

	/**
	 * The used storage configuration
	 */
	protected final BBoxDBConfiguration configuration;
	
	/**
	 * The list of the storage directories
	 */
	protected final Map<String, DiskStorage> storages;
	
	/**
	 * A map that contains the storage directory for the sstable
	 */
	protected final Map<TupleStoreName, String> tupleStoreLocations;
	
	/**
	 * A map with all created storage instances
	 */
	protected final Map<TupleStoreName, TupleStoreManager> managerInstances;
	
	/**
	 * The flush callbacks
	 */
	protected final List<BiConsumer<TupleStoreName, Long>> flushCallbacks;

	/**
	 * The service state
	 */
	protected final ServiceState serviceState;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TupleStoreManagerRegistry.class);

	public TupleStoreManagerRegistry() {
		this.configuration = BBoxDBConfigurationManager.getConfiguration();
		this.managerInstances = Collections.synchronizedMap(new HashMap<>());
		this.tupleStoreLocations = Collections.synchronizedMap(new HashMap<>());
		this.storages = new HashMap<>();
		this.flushCallbacks = new ArrayList<>();
		this.serviceState = new ServiceState();
	}
	
	/**
	 * Init the service
	 * @throws BBoxDBException 
	 */
	@Override
	public synchronized void init() throws InterruptedException, BBoxDBException {
		
		if(! serviceState.isInNewState()) {
			throw new BBoxDBException("Unable to init service is in state: " + serviceState.getState());
		}
		
		serviceState.dipatchToStarting();
		
		final List<String> storageDirs = configuration.getStorageDirectories();
		
		if(storageDirs.isEmpty()) {
			throw new IllegalArgumentException("Unable to init storage registry without any data directory");
		}
		
		// Populate the sstable location map
		for(final String directory : storageDirs) {
			try {
				tupleStoreLocations.putAll(TupleStoreLocator.scanDirectoryForExistingTables(directory));
				final int flushThreadsPerStorage = configuration.getMemtableFlushThreadsPerStorage();
				final DiskStorage storage = new DiskStorage(this, new File(directory), flushThreadsPerStorage);
				storage.init();
				storages.put(directory, storage);
			} catch (StorageManagerException e) {
				final String dataDirString = SSTableHelper.getDataDir(directory);
				logger.error("Directory {} does not exists, exiting...", dataDirString);
				System.exit(-1);
			}
		}
		
		serviceState.dispatchToRunning();
	}
	
	/**
	 * Get the storage manager for a given table. If the storage manager does not 
	 * exist, it will be created
	 * 
	 * @return
	 */
	public synchronized TupleStoreManager getTupleStoreManager(final TupleStoreName table) 
			throws StorageManagerException {
		
		if(! table.isValid()) {
			throw new StorageManagerException("Invalid tablename: " + table);
		}

		// Instance is known
		if(managerInstances.containsKey(table)) {
			return managerInstances.get(table);
		}

		// Find a new storage directory for the sstable manager
		if(! tupleStoreLocations.containsKey(table)) {
			throw new StorageManagerException("Unknown location for table " 
					+ table.getFullname() + " does the table exist?");
		}
		
		final String location = tupleStoreLocations.get(table);
		final DiskStorage storage = storages.get(location);
		final TupleStoreManager sstableManager = new TupleStoreManager(storage, table, configuration);

		sstableManager.init();
		managerInstances.put(table, sstableManager);
		
		return sstableManager;
	}
	
	/**
	 * Shut down the storage manager for a given relation
	 * @param table
	 * @return
	 */
	public synchronized boolean shutdownSStable(final TupleStoreName table) {
		
		if(! managerInstances.containsKey(table)) {
			return false;
		}
		
		logger.info("Shutting down SSTable manager for: {}", table);
		final TupleStoreManager sstableManager = managerInstances.remove(table);
		sstableManager.shutdown();	
		
		try {
			sstableManager.awaitShutdown();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		}
		
		logger.info("Shuting down SSTable manager DONE for: {}", table);
		
		return true;
	}
	
	/**
	 * Shutdown the storage registry
	 * @throws BBoxDBException 
	 */
	public synchronized void shutdown() {
		
		if(! serviceState.isInRunningState()) {
			logger.warn("Igonring shutdown, service is in state: {}", serviceState.getState());
			return;
		}
		
		serviceState.dispatchToStopping();
		
		logger.info("Shutting down SSTable manager instances");
		managerInstances.values().forEach(s -> s.shutdown());
		
		logger.info("Shutting down storages");
		storages.values().forEach(s -> s.shutdown());

		managerInstances.clear();
		tupleStoreLocations.clear();
		storages.clear();
		
		serviceState.dispatchToTerminated();
	}
	
	/**
	 * Get the lowest utilized data storage location
	 * @return
	 */
	public String getLowestUtilizedDataLocation() {

		final Multiset<String> usage = HashMultiset.create();
		
		// Put every location into the table (even unused ones)
		storages.keySet().forEach(s -> usage.add(s));
		
		// Add SSTables per storage usage 
		tupleStoreLocations.values().forEach(v -> usage.add(v));
		
		// Return the lowest usage
		return usage.entrySet().stream()
			.reduce((a,b) -> a.getCount() < b.getCount() ? a : b)
			.get()
			.getElement();
	}
	
	/**
	 * Delete the given table
	 * @param table
	 * @throws StorageManagerException 
	 */
	public synchronized void deleteTable(final TupleStoreName table) throws StorageManagerException {
		
		if(! table.isValid()) {
			throw new StorageManagerException("Invalid tablename: " + table);
		}
		
		if(managerInstances.containsKey(table)) {
			shutdownSStable(table);
		}
		
		if(! tupleStoreLocations.containsKey(table)) {
			logger.error("Table {} not known during deletion", table.getFullname());
			return;
		}
		
		final String storageDirectory = tupleStoreLocations.get(table);
		TupleStoreManager.deletePersistentTableData(storageDirectory, table);
		
		tupleStoreLocations.remove(table);	
	}
	
	/**
	 * Create a new table
	 * @param tupleStoreName
	 * @param configuration
	 * @return 
	 * @throws StorageManagerException 
	 */
	public synchronized TupleStoreManager createTable(final TupleStoreName tupleStoreName, 
			final TupleStoreConfiguration tupleStoreConfiguration) throws StorageManagerException {
		
		// Find a new storage directory for the sstable manager
		if(tupleStoreLocations.containsKey(tupleStoreName)) {
			throw new StorageManagerException("Table already exist");
		}
		
		final String location = getLowestUtilizedDataLocation();
		tupleStoreLocations.put(tupleStoreName, location);
		
		final DiskStorage storage = storages.get(location);
		
		final TupleStoreManager tupleStoreManager = new TupleStoreManager(storage, 
				tupleStoreName, configuration);
		
		tupleStoreManager.create(tupleStoreConfiguration);
		
		tupleStoreManager.init();
		managerInstances.put(tupleStoreName, tupleStoreManager);
		
		return tupleStoreManager;
	}
	
	/**
	 * Delete all tables that are part of the distribution group
	 * @param distributionGroupName
	 * @throws StorageManagerException 
	 */
	public synchronized void deleteAllTablesInDistributionGroup(
			final DistributionGroupName distributionGroupName) throws StorageManagerException {
		
		final String distributionGroupString = distributionGroupName.getFullname();
		
		// Memtabes
		logger.info("Shuting down active memtables for distribution group: " + distributionGroupString);
		
		// Create a copy of the key set to allow deletions (performed by shutdown) during iteration
		final Set<TupleStoreName> copyOfInstances = new HashSet<TupleStoreName>(managerInstances.keySet());
		for(final TupleStoreName ssTableName : copyOfInstances) {
			if(ssTableName.getDistributionGroup().equals(distributionGroupString)) {
				shutdownSStable(ssTableName);
			}
		}
		
		// Storage on disk
		final List<TupleStoreName> allTables = getAllTables();
		for(final TupleStoreName ssTableName : allTables) {
			if(ssTableName.getDistributionGroup().equals(distributionGroupString)) {
				deleteTable(ssTableName);
			}
		}
		
		// Delete the group dir
		for(final String directory : storages.keySet()) {
			logger.info("Deleting all local stored data for distribution group {} in path {} ",
					distributionGroupString, directory);
			
			deleteMedatadaOfDistributionGroup(distributionGroupString, directory);
	
			final String groupDirName = SSTableHelper.getDistributionGroupDir(directory, distributionGroupString);
			final File groupDir = new File(groupDirName);
			final String[] childs = groupDir.list();
			
			if(childs != null && childs.length > 0) {
				final List<String> childList = Arrays.asList(childs);
				throw new StorageManagerException("Unable to delete non empty dir: " 
						+ groupDirName + " / " + childList);
			}
			
			if(groupDir.exists()) {
				logger.debug("Deleting {}", groupDir);
				groupDir.delete();
			}
		}
	}

	/**
	 * Delete medatada file
	 * @param distributionGroupString
	 * @param directory
	 */
	protected static void deleteMedatadaOfDistributionGroup(final String distributionGroupString,
			final String directory) {
		
		final String medatadaFileName = SSTableHelper.getDistributionGroupMedatadaFile(directory, 
				distributionGroupString);
		
		final File medatadaFile = new File(medatadaFileName);
		
		if(medatadaFile.exists()) {
			logger.debug("Remove medatada file {}", medatadaFile);
			medatadaFile.delete();
		}
	}
	
	/**
	 * Is a storage manager for the relation active?
	 * @param table
	 * @return
	 */
	public synchronized boolean isStorageManagerActive(final TupleStoreName table) {
		return managerInstances.containsKey(table);
	}
	
	/**
	 * Is the given tupe store known?
	 * @param table
	 * @return
	 */
	public synchronized boolean isStorageManagerKnown(final TupleStoreName table) {
		return tupleStoreLocations.containsKey(table);
	}
	
	/**
	 * Returns a list with all known tables
	 * 
	 * @return
	 */
	public List<TupleStoreName> getAllTables() {
		return new ArrayList<>(tupleStoreLocations.keySet());
	}
	
	/**
	 * Get all tables for the given distribution group and region id
	 * @param distributionGroupName 
	 * @param regionId
	 * @return
	 */
	public List<TupleStoreName> getAllTablesForDistributionGroupAndRegionId
		(final DistributionGroupName distributionGroupName, final long regionId) {
		
		final List<TupleStoreName> groupTables = getAllTablesForDistributionGroup(distributionGroupName);
		
		return groupTables
			.stream()
			.filter(s -> s.getRegionId() == regionId)
			.collect(Collectors.toList());
	}
	
	/**
	 * Get the size of all sstables in the distribution group and region id
	 * @param distributionGroupName
	 * @param regionId
	 * @return
	 * @throws StorageManagerException
	 */
	public long getSizeOfDistributionGroupAndRegionId
		(final DistributionGroupName distributionGroupName, final long regionId) 
				throws StorageManagerException {
		
		final List<TupleStoreName> tables 
			= getAllTablesForDistributionGroupAndRegionId(distributionGroupName, regionId);
		
		long totalSize = 0;
		
		for(TupleStoreName ssTableName : tables) {
			totalSize = totalSize + getTupleStoreManager(ssTableName).getSize();
		}
		
		return totalSize;
	}
	
	/**
	 * Get the amount of tuples  in the distribution group and region id
	 * @param distributionGroupName
	 * @param regionId
	 * @return
	 * @throws StorageManagerException
	 */
	public long getTuplesInDistributionGroupAndRegionId
		(final DistributionGroupName distributionGroupName, final long regionId) 
				throws StorageManagerException {
		
		final List<TupleStoreName> tables 
			= getAllTablesForDistributionGroupAndRegionId(distributionGroupName, regionId);
		
		long tuples = 0;
		
		for(TupleStoreName ssTableName : tables) {
			tuples = tuples + getTupleStoreManager(ssTableName).getNumberOfTuples();
		}
		
		return tuples;
	}
	
	/**
	 * Get all tables for a given distribution group
	 * @return
	 */
	public synchronized List<TupleStoreName> getAllTablesForDistributionGroup
		(final DistributionGroupName distributionGroupName) {
		
		return tupleStoreLocations.keySet()
			.stream()
			.filter(s -> s.getDistributionGroupObject().equals(distributionGroupName))
			.collect(Collectors.toList());
	}

	/**
	 * Register a new SSTable flush callback
	 * @param callback
	 */
	public void registerSSTableFlushCallback(final BiConsumer<TupleStoreName, Long> callback) {
		flushCallbacks.add(callback);
	}
	
	/**
	 * Get a list with all SSTable flush callbacks
	 * @return 
	 * @return
	 */
	public List<BiConsumer<TupleStoreName, Long>> getSSTableFlushCallbacks() {
		return Collections.unmodifiableList(flushCallbacks);
	}
	
	/**
	 * Get all sstables for the given location
	 * @param basedir
	 * @return 
	 */
	public synchronized List<TupleStoreName> getTupleStoresForLocation(final String basedir) {
		return tupleStoreLocations.entrySet().stream()
				.filter(e -> e.getValue().equals(basedir))
				.map(e -> e.getKey())
				.collect(Collectors.toList());
	}

	/**
	 * Get all storages
	 * @return 
	 */
	public synchronized Collection<DiskStorage> getAllStorages() {
		return storages.values();
	}
	
	/**
	 * Get the BBoxDB configuration
	 * @return
	 */
	public BBoxDBConfiguration getConfiguration() {
		return configuration;
	}

	@Override
	public String getServicename() {
		return "The storage registry";
	}
}
