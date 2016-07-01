package de.fernunihagen.dna.jkn.scalephant.storage;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.jkn.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.jkn.scalephant.distribution.DistributionGroupName;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.SSTableName;

public class StorageInterface {

	/**
	 * A map with all created storage instances
	 */
	protected static final Map<SSTableName, StorageManager> instances;
	
	/**
	 * The used storage configuration
	 */
	protected static ScalephantConfiguration configuration;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(StorageInterface.class);

	
	static {
		configuration = ScalephantConfigurationManager.getConfiguration();
		instances = new HashMap<SSTableName, StorageManager>();
	}
	
	/**
	 * Get the storage manager for a given table. If the storage manager does not 
	 * exist, it will be created
	 * 
	 * @return
	 */
	public static synchronized StorageManager getStorageManager(final SSTableName table) throws StorageManagerException {
		
		if(! table.isValid()) {
			throw new StorageManagerException("Invalid tablename: " + table);
		}

		if(instances.containsKey(table)) {
			return instances.get(table);
		}
		
		final StorageManager storageManager = new StorageManager(table, configuration);
		storageManager.init();
		
		instances.put(table, storageManager);
		
		return storageManager;
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
		
		final StorageManager storageManager = instances.remove(table);
		storageManager.shutdown();		
		
		return true;
	}
	
	/**
	 * Delete the given table
	 * @param table
	 * @throws StorageManagerException 
	 */
	public static void deleteTable(final SSTableName table) throws StorageManagerException {
		
		if(! table.isValid()) {
			throw new StorageManagerException("Invalid tablename: " + table);
		}
		
		final StorageManager storageManager = getStorageManager(table);
		instances.remove(table);

		storageManager.deleteExistingTables();
	}
	
	/**
	 * Delete all tables that are part of the distribution group
	 * @param distributionGroupName
	 * @throws StorageManagerException 
	 */
	public static void deleteAllTablesInDistributionGroup(final DistributionGroupName distributionGroupName) throws StorageManagerException {
		
		final String fullname = distributionGroupName.getFullname();
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
	 * Shutdown all instances
	 * 
	 */
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		
		if(instances != null) {
			for(final SSTableName table : instances.keySet()) {
				final StorageManager storageManager = instances.get(table);
				storageManager.shutdown();
			}
		}
	}
}
