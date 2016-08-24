package de.fernunihagen.dna.scalephant.storage;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.scalephant.distribution.DistributionGroupName;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableManager;

public class StorageInterface {

	/**
	 * A map with all created storage instances
	 */
	protected static final Map<SSTableName, SSTableManager> instances;
	
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
		
		final SSTableManager sstableManager = getSSTableManager(table);
		instances.remove(table);

		sstableManager.deleteExistingTables();
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
