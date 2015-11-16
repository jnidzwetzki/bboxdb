package de.fernunihagen.dna.jkn.scalephant.storage;

public class StorageConfiguration {
	
	// 128 MB
	private final static int MEMTABLE_THRESHOLD = 128 * 1024;
	
	// 10000 Entries
	private final static int MEMTABLE_ENTRIES = 10000;
	
	// Data dir
	private final static String DATA_DIR = "/tmp/scalephant/data";
	
	// Commit log dir
	private final static String COMMITLOG_DIR = "/tmp/scalephant/commitlog";
	
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
	 * @return maximal amount of enties
	 */
	public int getMemtableEntries() {
		return MEMTABLE_ENTRIES;
	}
	
	/**
	 * Get the directory for storing the SSTables
	 * 
	 * @return name of the data directory
	 */
	public static String getDataDir() {
		return DATA_DIR;
	}
	
	/**
	 * Get the name of the commit log
	 * 
	 * @return name of the commit log
	 */
	public static String getCommitlogDir() {
		return COMMITLOG_DIR;
	}
	
}
