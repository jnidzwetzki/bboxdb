package de.fernunihagen.dna.jkn.scalephant.storage;

public class StorageConfiguration {
	
	// 128 MB
	private final static int MEMTABLE_THRESHOLD = 128 * 1024;
	
	// 10000 Entries
	private final static int MEMTABLE_ENTRIES = 10000;
	
	/**
	 * The size of the in memory tuple buffer
	 * 
	 * @return Memory size in KB
	 */
	public int getMemtableSize() {
		return MEMTABLE_THRESHOLD;
	}
	
	/**
	 * The maximal amount of entries in the memtable
	 * 
	 * @return maximal amount
	 */
	public int getMemtableEntries() {
		return MEMTABLE_ENTRIES;
	}
	
}
