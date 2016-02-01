package de.fernunihagen.dna.jkn.scalephant.storage;

import java.util.HashMap;
import java.util.Map;

public class StorageInterface {

	/**
	 * A map with all created storage instances
	 */
	protected static final Map<String, StorageManager> instances;
	
	/**
	 * The used storage configuration
	 */
	protected static StorageConfiguration storageConfiguration;
	
	static {
		storageConfiguration = new StorageConfiguration();
		instances = new HashMap<String, StorageManager>();
	}
	
	/**
	 * Get the storage manager for a given table. If the storage manager does not 
	 * exist, it will be created
	 * 
	 * @return
	 */
	public static synchronized StorageManager getStorageManager(final String table) {

		if(instances.containsKey(table)) {
			return instances.get(table);
		}
		
		final StorageManager storageManager = new StorageManager(table, storageConfiguration);
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
	 * Is a storage manager for the relation active?
	 * @param table
	 * @return
	 */
	public static synchronized boolean isStorageManagerActive(final String table) {
		return instances.containsKey(table);
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

	/**
	 * Get the current storage configuration
	 * @return
	 */
	public static StorageConfiguration getStorageConfiguration() {
		return storageConfiguration;
	}

	/**
	 * Set a new storage configuration
	 * @param storageConfiguration
	 */
	public static void setStorageConfiguration(
			StorageConfiguration storageConfiguration) {
		StorageInterface.storageConfiguration = storageConfiguration;
	}
}
