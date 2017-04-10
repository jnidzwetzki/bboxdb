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
package org.bboxdb.storage;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.bboxdb.BBoxDBConfiguration;
import org.bboxdb.BBoxDBConfigurationManager;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.sstable.SSTableHelper;
import org.bboxdb.storage.sstable.SSTableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageRegistry {

	/**
	 * A map with all created storage instances
	 */
	protected final Map<SSTableName, SSTableManager> managerInstances;
	
	/**
	 * The used storage configuration
	 */
	protected final BBoxDBConfiguration configuration;
	
	/**
	 * The list of the storage directories
	 */
	protected final List<String> storageDirectories;
	
	/**
	 * A map that contains the storage directory for the sstable
	 */
	protected final Map<SSTableName, File> sstableLocations;
	
	/**
	 * The singleton instance
	 */
	protected static StorageRegistry instance;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(StorageRegistry.class);

	private StorageRegistry() {
		configuration = BBoxDBConfigurationManager.getConfiguration();
		managerInstances = new HashMap<>();
		sstableLocations = new HashMap<>();
		storageDirectories = configuration.getStorageDirectories();
		
		if(storageDirectories.isEmpty()) {
			throw new IllegalArgumentException("Unable to build a storage registry without any data directory");
		}
		
		// Populate the sstable location map
		for(final String directory : storageDirectories) {
			try {
				scanDirectory(directory);
			} catch (StorageManagerException e) {
				logger.error("Directory {} does not exists, exiting...", directory);
				System.exit(-1);
			}
		}
	}

	@Override
	protected Object clone() throws CloneNotSupportedException {
		throw new IllegalStateException("Unable to clone a singleton");
	}
	
	/**
	 * Get the singleton instance
	 * @return
	 */
	public static synchronized StorageRegistry getInstance() {
		if(instance == null) {
			instance = new StorageRegistry();
		}
		
		return instance;
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
		
		
		SSTableManager sstableManager = null;
		
		// We have already data
		if(sstableLocations.containsKey(table)) {
			final File location = sstableLocations.get(table);
			sstableManager = new SSTableManager(location.getAbsolutePath(), table, configuration);
		} else {
			// Find a new storate directory for the sstable manager
			final String location = getLowerstUtilizedDataLocation();
			sstableManager = new SSTableManager(location, table, configuration);
		}
		
		sstableManager.init();
		
		managerInstances.put(table, sstableManager);
		
		return sstableManager;
	}
	
	/**
	 * Shut down the storage manager for a given relation
	 * @param table
	 * @return
	 */
	public synchronized boolean shutdown(final SSTableName table) {
		
		if(! managerInstances.containsKey(table)) {
			return false;
		}
		
		logger.info("Shuting down storage interface for: {}", table);
		final SSTableManager sstableManager = managerInstances.remove(table);
		sstableManager.shutdown();	
		sstableManager.waitForShutdownToComplete();
		
		return true;
	}
	
	/**
	 * Get the lowest utilized data storage location
	 * @return
	 */
	public String getLowerstUtilizedDataLocation() {
		
		// Group by usage
		final Map<File, Long> usage = sstableLocations
			.values()
			.stream()
			.collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
		
		// Find the lowest usage
		final long lowestUsage = usage.values()
			.stream()
			.mapToLong(e -> e)
			.min()
			.orElseThrow(() -> new IllegalArgumentException("Unable to found lowest usage: " + sstableLocations));
			
		// Return the location
		final File location = usage.entrySet()
			.stream()
			.filter(e -> e.getValue() == lowestUsage)
			.findFirst()
			.map(e -> e.getKey())
			.orElseThrow(() -> new IllegalArgumentException("Unable to found lowest location" + sstableLocations));
		
		return location.getAbsolutePath();
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
			shutdown(table);
		}

		assert (sstableLocations.containsKey(table)) : "Table not known" + table;
		
		final String absolutePath = sstableLocations.get(table).getAbsolutePath();
		SSTableManager.deletePersistentTableData(absolutePath, table);
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
				shutdown(ssTableName);
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
		for(final String directory : storageDirectories) {
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
	
		final String dataDir = SSTableHelper.getDataDir(storageDirectory);
		final File folder = new File(dataDir);

		// Distribution groups
		for (final File fileEntry : folder.listFiles()) {
						
			if(! fileEntry.exists()) {
				throw new StorageManagerException("Root dir does not exist: " + fileEntry);
			}
			
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
						sstableLocations.put(sstableName, tableEntry);
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
	public List<SSTableName> getAllTablesForDistributionGroup
		(final DistributionGroupName distributionGroupName) {
		
		return sstableLocations.keySet()
			.stream()
			.filter(s -> s.getDistributionGroupObject().equals(distributionGroupName))
			.collect(Collectors.toList());
	}
	
	/**
	 * Shutdown all instances
	 * 
	 */
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		
		if(managerInstances != null) {
			for(final SSTableName table : managerInstances.keySet()) {
				final SSTableManager sstableManager = managerInstances.get(table);
				sstableManager.shutdown();
			}
		}
		
		storageDirectories.clear();
		sstableLocations.clear();
	}
	
	/**
	 * Get the storage directory for the sstable
	 * @param ssTableName
	 * @return
	 */
	public String getStorageDirForSSTable(final SSTableName ssTableName) {
		return sstableLocations.get(ssTableName).getAbsolutePath();
	}
}
