package de.fernunihagen.dna.jkn.scalephant.storage;

public class StorageInterface {

	private static StorageManager storageManager;
	
	/**
	 * Initializes the storage manager if necessary and retuns the 
	 * instance.
	 * 
	 * @return
	 */
	public static synchronized StorageManager getStorageManager() {
		
		if(storageManager == null) {
			final StorageConfiguration storageConfiguration = new StorageConfiguration();
			storageManager = new StorageManager(storageConfiguration);
			storageManager.init();
		}
		
		return storageManager;
	}
	
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		
		if(storageManager != null) {
			storageManager.shutdown();
		}
		
	}
	
}
