package de.fernunihagen.dna.jkn.scalephant.storage;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.fernunihagen.dna.jkn.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.jkn.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.jkn.scalephant.util.Tablename;

public class StorageInterface {

	/**
	 * A map with all created storage instances
	 */
	protected static final Map<String, StorageManager> instances;
	
	/**
	 * The used storage configuration
	 */
	protected static ScalephantConfiguration configuration;
	
	static {
		configuration = ScalephantConfigurationManager.getConfiguration();
		instances = new HashMap<String, StorageManager>();
	}
	
	/**
	 * Get the storage manager for a given table. If the storage manager does not 
	 * exist, it will be created
	 * 
	 * @return
	 */
	public static synchronized StorageManager getStorageManager(final String table) throws StorageManagerException {
		
		final Tablename tablename = new Tablename(table);
		if(! tablename.isValid()) {
			throw new StorageManagerException("Invalid tablename: " + tablename);
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
	public static synchronized boolean shutdown(final String table) {
		
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
	public static void deleteTable(final String table) throws StorageManagerException {
		
		final Tablename tablename = new Tablename(table);
		if(! tablename.isValid()) {
			throw new StorageManagerException("Invalid tablename: " + tablename);
		}
		
		final StorageManager storageManager = getStorageManager(table);
		instances.remove(table);

		storageManager.deleteExistingTables();
	}
	
	/**
	 * Is a storage manager for the relation active?
	 * @param table
	 * @return
	 */
	public static synchronized boolean isStorageManagerActive(final String table) {
		return instances.containsKey(table);
	}
	
	/**
	 * Returns a list with all known tables
	 * 
	 * @return
	 */
	public static List<String> getAllTables() {
		final List<String> allTables = new ArrayList<String>();
		
		final File folder = new File(configuration.getDataDirectory());
		
		for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	           allTables.add(fileEntry.getName());
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
			for(final String table : instances.keySet()) {
				final StorageManager storageManager = instances.get(table);
				storageManager.shutdown();
			}
		}
	}
}
