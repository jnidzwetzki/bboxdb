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
	protected static final Map<SSTableName, SSTableManager> instances;
	
	/**
	 * The used storage configuration
	 */
	protected static BBoxDBConfiguration configuration;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(StorageRegistry.class);

	
	static {
		configuration = BBoxDBConfigurationManager.getConfiguration();
		instances = new HashMap<SSTableName, SSTableManager>();
	}
	
	/**
	 * Get the storage manager for a given table. If the storage manager does not 
	 * exist, it will be created
	 * 
	 * @return
	 */
	public static synchronized SSTableManager getSSTableManager(final SSTableName table) throws StorageManagerException {
		
		if(! table.isValid()) {
			throw new StorageManagerException("Invalid tablename: " + table);
		}

		if(instances.containsKey(table)) {
			return instances.get(table);
		}
		
		final SSTableManager sstableManager = new SSTableManager(table, configuration);
		sstableManager.init();
		
		instances.put(table, sstableManager);
		
		return sstableManager;
	}
	
	/**
	 * Shut down the storage manager for a given relation
	 * @param table
	 * @return
	 */
	public static synchronized boolean shutdown(final SSTableName table) {
		
		if(! instances.containsKey(table)) {
			return false;
		}
		
		logger.info("Shuting down storage interface for: " + table);
		final SSTableManager sstableManager = instances.remove(table);
		sstableManager.shutdown();	
		sstableManager.waitForShutdownToComplete();
		
		return true;
	}
	
	/**
	 * Delete the given table
	 * @param table
	 * @throws StorageManagerException 
	 */
	public static synchronized void deleteTable(final SSTableName table) throws StorageManagerException {
		
		if(! table.isValid()) {
			throw new StorageManagerException("Invalid tablename: " + table);
		}
		
		if(instances.containsKey(table)) {
			shutdown(table);
		}

		SSTableManager.deletePersistentTableData(configuration.getDataDirectory(), table);
	}
	
	/**
	 * Delete all tables that are part of the distribution group
	 * @param distributionGroupName
	 * @throws StorageManagerException 
	 */
	public static synchronized void deleteAllTablesInDistributionGroup(final DistributionGroupName distributionGroupName) throws StorageManagerException {
		
		final String distributionGroupString = distributionGroupName.getFullname();
		
		// Memtabes
		logger.info("Shuting down active memtables for distribution group: " + distributionGroupString);
		
		// Create a copy of the key set to allow deletions (performed by shutdown) during iteration
		final Set<SSTableName> copyOfInstances = new HashSet<SSTableName>(instances.keySet());
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
		logger.info("Deleting all local stored data for distribution group: " + distributionGroupString);
		final String directory = configuration.getDataDirectory();
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
	public static synchronized boolean isStorageManagerActive(final SSTableName table) {
		return instances.containsKey(table);
	}
	
	/**
	 * Returns a list with all known tables
	 * 
	 * @return
	 */
	public static List<SSTableName> getAllTables() {
		final List<SSTableName> allTables = new ArrayList<SSTableName>();
		
		final File folder = new File(configuration.getDataDirectory());
		
		// Distribution groups
		for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	        	final String distributionGroup = fileEntry.getName();
	        	final DistributionGroupName distributionGroupName = new DistributionGroupName(distributionGroup);
	        	assert(distributionGroupName.isValid());
	        	
	        	// Tables
	    		for (final File tableEntry : fileEntry.listFiles()) {
			        if (tableEntry.isDirectory()) {
			        	final String tablename = tableEntry.getName();
			        	final String fullname = distributionGroupName.getFullname() + "_" + tablename;
			        	allTables.add(new SSTableName(fullname));
			        }
	    		}
	        } 
	    }
		
		return allTables;
	}
	
	/**
	 * Get all tables for the given distribution group and region id
	 * @param distributionGroupName 
	 * @param regionId
	 * @return
	 */
	public static List<SSTableName> getAllTablesForDistributionGroupAndRegionId
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
	public static long getSizeOfDistributionGroupAndRegionId
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
	public static List<SSTableName> getAllTablesForDistributionGroup
		(final DistributionGroupName distributionGroupName) {
		
		final List<SSTableName> allTables = getAllTables();
		
		return allTables
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
		
		if(instances != null) {
			for(final SSTableName table : instances.keySet()) {
				final SSTableManager sstableManager = instances.get(table);
				sstableManager.shutdown();
			}
		}
	}
}
