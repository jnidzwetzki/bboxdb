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
package org.bboxdb.storage.registry;

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
import java.util.stream.Collectors;

import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.SSTableFlushCallback;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.sstable.SSTableHelper;
import org.bboxdb.storage.sstable.SSTableManager;
import org.bboxdb.util.ServiceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class StorageRegistry implements BBoxDBService {

	/**
	 * The used storage configuration
	 */
	protected final BBoxDBConfiguration configuration;
	
	/**
	 * The list of the storage directories
	 */
	protected final Map<String, Storage> storages;
	
	/**
	 * A map that contains the storage directory for the sstable
	 */
	protected final Map<SSTableName, String> sstableLocations;
	
	/**
	 * A map with all created storage instances
	 */
	protected final Map<SSTableName, SSTableManager> managerInstances;
	
	/**
	 * The flush callbacks
	 */
	protected final List<SSTableFlushCallback> flushCallbacks;

	/**
	 * The service state
	 */
	protected final ServiceState serviceState;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(StorageRegistry.class);

	public StorageRegistry() {
		this.configuration = BBoxDBConfigurationManager.getConfiguration();
		this.managerInstances = Collections.synchronizedMap(new HashMap<>());
		this.sstableLocations = Collections.synchronizedMap(new HashMap<>());
		this.storages = new HashMap<>();
		this.flushCallbacks = new ArrayList<>();
		this.serviceState = new ServiceState();
	}
	
	/**
	 * Init the service
	 * @throws BBoxDBException 
	 */
	@Override
	public void init() throws InterruptedException, BBoxDBException {
		
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
				scanDirectory(directory);
				final Storage storage = new Storage(this, new File(directory), configuration.getMemtableFlushThreadsPerStorage());
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
	public synchronized SSTableManager getSSTableManager(final SSTableName table) throws StorageManagerException {
		
		if(! table.isValid()) {
			throw new StorageManagerException("Invalid tablename: " + table);
		}

		// Instance is known
		if(managerInstances.containsKey(table)) {
			return managerInstances.get(table);
		}

		// Find a new storage directory for the sstable manager
		if(! sstableLocations.containsKey(table)) {
			final String location = getLowestUtilizedDataLocation();
			sstableLocations.put(table, location);
		}
		
		final String location = sstableLocations.get(table);
		final Storage storage = storages.get(location);
		final SSTableManager sstableManager = new SSTableManager(storage, table, configuration);

		sstableManager.init();
		managerInstances.put(table, sstableManager);
		
		return sstableManager;
	}
	
	/**
	 * Shut down the storage manager for a given relation
	 * @param table
	 * @return
	 */
	public synchronized boolean shutdownSStable(final SSTableName table) {
		
		if(! managerInstances.containsKey(table)) {
			return false;
		}
		
		logger.info("Shutting down SSTable manager for: {}", table);
		final SSTableManager sstableManager = managerInstances.remove(table);
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

		managerInstances.clear();
		sstableLocations.clear();
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
		sstableLocations.values().forEach(v -> usage.add(v));
		
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
	public synchronized void deleteTable(final SSTableName table) throws StorageManagerException {
		
		if(! table.isValid()) {
			throw new StorageManagerException("Invalid tablename: " + table);
		}
		
		if(managerInstances.containsKey(table)) {
			shutdownSStable(table);
		}
		
		if(! sstableLocations.containsKey(table)) {
			logger.error("Table {} not known during deletion", table.getFullname());
			return;
		}
		
		final String storageDirectory = sstableLocations.get(table);
		SSTableManager.deletePersistentTableData(storageDirectory, table);
		
		sstableLocations.remove(table);	
	}
	
	/**
	 * Delete all tables that are part of the distribution group
	 * @param distributionGroupName
	 * @throws StorageManagerException 
	 */
	public synchronized void deleteAllTablesInDistributionGroup(final DistributionGroupName distributionGroupName) throws StorageManagerException {
		
		final String distributionGroupString = distributionGroupName.getFullname();
		
		// Memtabes
		logger.info("Shuting down active memtables for distribution group: " + distributionGroupString);
		
		// Create a copy of the key set to allow deletions (performed by shutdown) during iteration
		final Set<SSTableName> copyOfInstances = new HashSet<SSTableName>(managerInstances.keySet());
		for(final SSTableName ssTableName : copyOfInstances) {
			if(ssTableName.getDistributionGroup().equals(distributionGroupString)) {
				shutdownSStable(ssTableName);
			}
		}
		
		// Storage on disk
		final List<SSTableName> allTables = getAllTables();
		for(final SSTableName ssTableName : allTables) {
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
		
		final String medatadaFileName = SSTableHelper.getDistributionGroupMedatadaFile(directory, distributionGroupString);
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
	public synchronized boolean isStorageManagerActive(final SSTableName table) {
		return managerInstances.containsKey(table);
	}
	
	/**
	 * Returns a list with all known tables
	 * 
	 * @return
	 */
	public List<SSTableName> getAllTables() {
		final List<SSTableName> tables = new ArrayList<>(sstableLocations.size());
		tables.addAll(sstableLocations.keySet());
		return tables;
	}
	
	/**
	 * Scan the given directory for existing sstables and add them
	 * to the sstable location map
	 * @param storageDirectory
	 * @throws StorageManagerException 
	 */
	protected void scanDirectory(final String storageDirectory) throws StorageManagerException {
	
		final String dataDirString = SSTableHelper.getDataDir(storageDirectory);
		final File dataDir = new File(dataDirString);
		
		if(! dataDir.exists()) {
			throw new StorageManagerException("Root dir does not exist: " + dataDir);
		}

		// Distribution groups
		for (final File fileEntry : dataDir.listFiles()) {
			
	        if (fileEntry.isDirectory()) {
	        	final String distributionGroup = fileEntry.getName();
	        	final DistributionGroupName distributionGroupName = new DistributionGroupName(distributionGroup);
	        	
	        	assert(distributionGroupName.isValid()) : "Invalid name: " + distributionGroup;
	        	
	        	// Tables
	    		for (final File tableEntry : fileEntry.listFiles()) {
			        if (tableEntry.isDirectory()) {
			        	final String tablename = tableEntry.getName();
			        	final String fullname = distributionGroupName.getFullname() + "_" + tablename;
			        	final SSTableName sstableName = new SSTableName(fullname);
						sstableLocations.put(sstableName, storageDirectory);
			        }
	    		}
	        } 
	    }
	}
	
	/**
	 * Get all tables for the given distribution group and region id
	 * @param distributionGroupName 
	 * @param regionId
	 * @return
	 */
	public List<SSTableName> getAllTablesForDistributionGroupAndRegionId
		(final DistributionGroupName distributionGroupName, final int regionId) {
		
		final List<SSTableName> groupTables = getAllTablesForDistributionGroup(distributionGroupName);
		
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
		(final DistributionGroupName distributionGroupName, final int regionId) 
				throws StorageManagerException {
		
		final List<SSTableName> tables 
			= getAllTablesForDistributionGroupAndRegionId(distributionGroupName, regionId);
		
		long totalSize = 0;
		
		for(SSTableName ssTableName : tables) {
			totalSize = totalSize + getSSTableManager(ssTableName).getSize();
		}
		
		return totalSize;
	}
	
	/**
	 * Get all tables for a given distribution group
	 * @return
	 */
	public synchronized List<SSTableName> getAllTablesForDistributionGroup
		(final DistributionGroupName distributionGroupName) {
		
		return sstableLocations.keySet()
			.stream()
			.filter(s -> s.getDistributionGroupObject().equals(distributionGroupName))
			.collect(Collectors.toList());
	}

	/**
	 * Register a new SSTable flush callback
	 * @param callback
	 */
	public void registerSSTableFlushCallback(final SSTableFlushCallback callback) {
		flushCallbacks.add(callback);
	}
	
	/**
	 * Get a list with all SSTable flush callbacks
	 * @return
	 */
	public List<SSTableFlushCallback> getSSTableFlushCallbacks() {
		return Collections.unmodifiableList(flushCallbacks);
	}
	
	/**
	 * Get all sstables for the given location
	 * @param basedir
	 * @return 
	 */
	public synchronized List<SSTableName> getSSTablesForLocation(final String basedir) {
		return sstableLocations.entrySet().stream()
				.filter(e -> e.getValue().equals(basedir))
				.map(e -> e.getKey())
				.collect(Collectors.toList());
	}

	/**
	 * Get all storages
	 * @return 
	 */
	public Collection<Storage> getAllStorages() {
		return storages.values();
	}
	
	/**
	 * Get the bboxdb configuration
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
