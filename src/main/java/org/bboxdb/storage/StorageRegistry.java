/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bboxdb.BBoxDBConfiguration;
import org.bboxdb.BBoxDBConfigurationManager;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.storage.entity.SSTableName;
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
			final SSTableManager sstableManager = getSSTableManager(table);
			sstableManager.shutdown();
			sstableManager.waitForShutdownToComplete();

			instances.remove(table);

		}

		SSTableManager.deletePersistentTableData(configuration.getDataDirectory(), table.getFullname());
	}
	
	/**
	 * Delete all tables that are part of the distribution group
	 * @param distributionGroupName
	 * @throws StorageManagerException 
	 */
	public static synchronized void deleteAllTablesInDistributionGroup(final DistributionGroupName distributionGroupName) throws StorageManagerException {
		
		final String fullname = distributionGroupName.getFullname();
		
		// Memtabes
		logger.info("Shuting down active memtables for distribution group: " + fullname);
		
		// Create a copy of the key set to allow deletions (performed by shutdown) during iteration
		final Set<SSTableName> copyOfInstances = new HashSet<SSTableName>(instances.keySet());
		for(final SSTableName ssTableName : copyOfInstances) {
			if(ssTableName.getDistributionGroup().equals(fullname)) {
				shutdown(ssTableName);
			}
		}
		
		// Storage on disk
		logger.info("Deleting all local stored data for distribution group: " + fullname);
		final List<SSTableName> allTables = getAllTables();
		for(final SSTableName ssTableName : allTables) {
			if(ssTableName.getDistributionGroup().equals(fullname)) {
				deleteTable(ssTableName);
			}
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
		
		for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	        	final String tableName = fileEntry.getName();
	        	allTables.add(new SSTableName(tableName));
	        } 
	    }
		
		return allTables;
	}
	
	/**
	 * Get all tables for the given nameprefix
	 * @param nameprefix
	 * @return
	 */
	public static List<SSTableName> getAllTablesForNameprefix(final int nameprefix) {
		final List<SSTableName> allTables = getAllTables();
		final List<SSTableName> resultTables = new ArrayList<SSTableName>();

		for(final SSTableName ssTableName : allTables) {
			if(ssTableName.getNameprefix() == nameprefix) {
				resultTables.add(ssTableName);
			}
		}
		
		return resultTables;
	}
	
	/**
	 * Get all tables for a given distribution group
	 * @return
	 */
	public static List<SSTableName> getAllTablesForDistributionGroup(final DistributionGroupName distributionGroupName) {
		final List<SSTableName> allTables = getAllTables();
		final List<SSTableName> resultTables = new ArrayList<SSTableName>();
		
		for(final SSTableName ssTableName : allTables) {
			if(ssTableName.getDistributionGroup().equals(distributionGroupName.getFullname())) {
				resultTables.add(ssTableName);
			}
		}
		
		return resultTables;
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
