package de.fernunihagen.dna.jkn.scalephant.storage;

import java.util.HashMap;
import java.util.Map;

public class StorageInterface {

	private static Map<String, StorageManager> instances;
	
	/**
	 * Get the storage manager for a given table. If the storage manager does not 
	 * exist, it will be created
	 * 
	 * @return
	 */
	public static synchronized StorageManager getStorageManager(final String table) {
		
		if(instances == null) {
			instances = new HashMap<String, StorageManager>();
		}
		
		if(instances.containsKey(table)) {
			return instances.get(table);
		}
		
		final StorageConfiguration storageConfiguration = new StorageConfiguration();
		final StorageManager storageManager = new StorageManager(table, storageConfiguration);
		storageManager.init();
		
		instances.put(table, storageManager);
		
		return storageManager;
	}
	
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
