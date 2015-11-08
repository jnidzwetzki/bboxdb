package de.fernunihagen.dna.jkn.scalephant.storage;

public class StorageConfiguration {
	
	private final static int MEMORY_THRESHOLD = 128;
	
	/**
	 * The size of the in memory tuple buffer
	 * 
	 * @return Memory size in MB
	 */
	public int getMemoryThreshold() {
		return MEMORY_THRESHOLD;
	}
	
}
