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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.bboxdb.commons.ServiceState;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.misc.BBoxDBService;
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
	private final BBoxDBConfiguration configuration;
	
	/**
	 * The list of the storage directories
	 */
	private final Map<String, DiskStorage> storages;
	
	/**
	 * A map that contains the storage directory for the sstable
	 */
	private final Map<TupleStoreName, String> tupleStoreLocations;
	
	/**
	 * A map with all created storage instances
	 */
	private final Map<TupleStoreName, TupleStoreManager> managerInstances;
	
	/**
	 * The flush callbacks
	 */
	private final List<BiConsumer<TupleStoreName, Long>> flushCallbacks;

	/**
	 * The service state
	 */
	private final ServiceState serviceState;
	
	/**
	 * The zookeeper observer
	 */
	private final TupleStoreZookeeperObserver zookeeperObserver;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TupleStoreManagerRegistry.class);

	public TupleStoreManagerRegistry() {
		this.configuration = BBoxDBConfigurationManager.getConfiguration();
		this.managerInstances = new ConcurrentHashMap<>();
		this.tupleStoreLocations = new ConcurrentHashMap<>();
		this.storages = new ConcurrentHashMap<>();
		this.flushCallbacks = new CopyOnWriteArrayList<>();
		this.serviceState = new ServiceState();
		this.zookeeperObserver = new TupleStoreZookeeperObserver(this);
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
	public synchronized TupleStoreManager getTupleStoreManager(final TupleStoreName tupleStoreName) 
			throws StorageManagerException {
		
		if(! tupleStoreName.isValid()) {
			throw new StorageManagerException("Invalid tablename: " + tupleStoreName);
		}
		
		zookeeperObserver.registerTable(tupleStoreName);

		// Instance is known
		if(managerInstances.containsKey(tupleStoreName)) {
			return managerInstances.get(tupleStoreName);
		}

		// Find a new storage directory for the sstable manager
		if(! tupleStoreLocations.containsKey(tupleStoreName)) {
			throw new StorageManagerException("Unknown location for table " 
					+ tupleStoreName.getFullname() + " does the table exist?");
		}
		
		final String location = tupleStoreLocations.get(tupleStoreName);
		final DiskStorage storage = storages.get(location);
		final TupleStoreManager sstableManager = new TupleStoreManager(storage, tupleStoreName, configuration);

		sstableManager.init();
		managerInstances.put(tupleStoreName, sstableManager);
		
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
	public void shutdown() {
		
		if(! serviceState.isInRunningState()) {
			logger.warn("Igonring shutdown, service is in state: {}", serviceState.getState());
			return;
		}
		
		serviceState.dispatchToStopping();
		
		logger.info("Shutting down SSTable manager instances");
		managerInstances.values().forEach(s -> s.shutdown());
		
		logger.info("Shutting down storages");
		storages.values().forEach(s -> s.shutdown());

		synchronized (this) {
			managerInstances.clear();
			tupleStoreLocations.clear();
			storages.clear();
		}

		serviceState.dispatchToTerminated();
	}
	
	/**
	 * Get the lowest utilized data storage location
	 * @return
	 */
	public String getLocationLowestUtilizedDataLocation() {

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
	public void deleteTable(final TupleStoreName table, final boolean synchronous) 
			throws StorageManagerException {
		
		if(! table.isValid()) {
			throw new StorageManagerException("Invalid tablename: " + table);
		}
		
		String storageDirectory = null;
		
		synchronized (this) {
			if(managerInstances.containsKey(table)) {
				shutdownSStable(table);
			}
			
			if(! tupleStoreLocations.containsKey(table)) {
				logger.error("Table {} not known during deletion", table.getFullname());
				return;
			}
			
			storageDirectory = tupleStoreLocations.get(table);
			tupleStoreLocations.remove(table);	
		}
			
		final DiskStorage storage = storages.get(storageDirectory);
		
		logger.info("Deleting table {} synchronous {}", table.getFullname(), synchronous);
		
		if(synchronous) {
			TupleStoreManager.deletePersistentTableData(storageDirectory, table);
		} else {
			storage.getPendingTableDeletions().add(table);
		}
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
		
		final String location = getLocationLowestUtilizedDataLocation();
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
	 * Create the given table if not already exists (thread safe / guarded by synchronized)
	 * @param tupleStoreName
	 * @param tupleStoreConfiguration
	 * @return
	 * @throws StorageManagerException
	 */
	public synchronized TupleStoreManager createTableIfNotExist(final TupleStoreName tupleStoreName, 
			final TupleStoreConfiguration tupleStoreConfiguration) throws StorageManagerException {
		
		zookeeperObserver.registerTable(tupleStoreName);
		
		if(tupleStoreLocations.containsKey(tupleStoreName)) {
			return getTupleStoreManager(tupleStoreName);
		}
		
		return createTable(tupleStoreName, tupleStoreConfiguration);
	}
	
	/**
	 * Delete all tables that are part of the distribution group
	 * @param distributionGroupName
	 * @throws StorageManagerException 
	 */
	public synchronized void deleteAllTablesInDistributionGroup(
			final String distributionGroupName) throws StorageManagerException {
				
		// Memtables
		logger.info("Shuting down active memtables for distribution group: {}", distributionGroupName);
		
		final Predicate<TupleStoreName> deleteTablePredicate = (t) 
				-> (t.getDistributionGroup().equals(distributionGroupName));
		
		shutdownAndDeleteTablesForPredicate(deleteTablePredicate, true);
		
		// Delete the group dir
		for(final String directory : storages.keySet()) {
			logger.info("Deleting all local stored data for distribution group {} in path {} ",
					distributionGroupName, directory);
			
			executePendingDeletes(directory);
			
			deleteMedatadaOfDistributionGroup(distributionGroupName, directory);
	
			final String groupDirName = SSTableHelper.getDistributionGroupDir(directory, distributionGroupName);
			final File groupDir = new File(groupDirName);
			final String[] children = groupDir.list();
			
			if(children != null && children.length > 0) {
				final List<String> childList = Arrays.asList(children);
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
	 * Execute the pending deletes immediately
	 * 
	 * @param directory
	 */
	private void executePendingDeletes(final String directory) {
		final Collection<TupleStoreName> pendingDeletes = new ArrayList<>();
		storages.get(directory).getPendingTableDeletions().drainTo(pendingDeletes);
		logger.info("Executing pending deleted immediately {}", pendingDeletes);
		
		pendingDeletes.forEach(t -> TupleStoreManager.deletePersistentTableData(directory, t));
	}
	
	/**
	 * Delete all data of a distribution region
	 * @throws StorageManagerException 
	 */
	public synchronized void deleteDataOfDistributionRegion(final String distributionGroup, 
			final long region, final boolean synchronous) throws StorageManagerException {
		
		logger.info("Deleting all data for: {} / {}", distributionGroup, region);
		
		final Predicate<TupleStoreName> namePredicate = (t) -> 
			(t.getDistributionGroup().equals(distributionGroup));
			
		final Predicate<TupleStoreName> regionPredicate = (t) ->
			(t.getRegionId().getAsLong() == region);
		
		final Predicate<TupleStoreName> deleteTablePredicate = (t) 
				-> namePredicate.test(t) && regionPredicate.test(t);
		
		shutdownAndDeleteTablesForPredicate(deleteTablePredicate, synchronous);
	}

	/**
	 * Shutdown and delete data for the given predicate
	 * @param deleteTablePredicate
	 * @throws StorageManagerException
	 */
	private void shutdownAndDeleteTablesForPredicate(final Predicate<TupleStoreName> deleteTablePredicate, 
			final boolean synchronous) throws StorageManagerException {
		
		// Create a copy of the key set to allow deletions (performed by shutdown) during iteration
		final Set<TupleStoreName> copyOfInstances = new HashSet<>(managerInstances.keySet());
		for(final TupleStoreName tupleStoreName : copyOfInstances) {
			if(deleteTablePredicate.test(tupleStoreName)) {
				shutdownSStable(tupleStoreName);
			}
		}
		
		// Storage on disk
		final List<TupleStoreName> allTables = getAllTables();
		for(final TupleStoreName tupleStoreName : allTables) {
			if(deleteTablePredicate.test(tupleStoreName)) {
				deleteTable(tupleStoreName, synchronous);
			}
		}
	}

	/**
	 * Delete medatada file
	 * @param distributionGroupString
	 * @param directory
	 */
	private static void deleteMedatadaOfDistributionGroup(final String distributionGroupString,
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
	public boolean isStorageManagerActive(final TupleStoreName table) {
		return managerInstances.containsKey(table);
	}
	
	/**
	 * Is the given tuple store known?
	 * @param table
	 * @return
	 */
	public boolean isStorageManagerKnown(final TupleStoreName table) {
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
	 * Get all tables for a given distribution group
	 * @return
	 */
	public List<TupleStoreName> getAllTablesForDistributionGroup(final String distributionGroupName) {
		
		return tupleStoreLocations.keySet()
			.stream()
			.filter(s -> s.getDistributionGroup().equals(distributionGroupName))
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
	public List<TupleStoreName> getTupleStoresForLocation(final String basedir) {
		return tupleStoreLocations.entrySet().stream()
				.filter(e -> e.getValue().equals(basedir))
				.map(e -> e.getKey())
				.collect(Collectors.toList());
	}

	/**
	 * Get all storages
	 * @return 
	 */
	public List<DiskStorage> getAllStorages() {
		return new ArrayList<>(storages.values());
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
