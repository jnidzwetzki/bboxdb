package de.fernunihagen.dna.jkn.scalephant.storage;

public class StorageConfiguration {
	
	// Max 128 MB size per memtable
	private final static int MEMTABLE_THRESHOLD = 128 * 1024;
	
	// Max 10000 entries per memtable
	private final static int MEMTABLE_ENTRIES = 10000;
	
	// The directory to store data
	private final static String DATA_DIR = "/tmp/scalephant/data";
	
	// Commit log dir
	private final static String COMMITLOG_DIR = "/tmp/scalephant/commitlog";
	
	// Start compact thread (can be disabled for tests)
	private static boolean runCompactThread = true;
	
	// Start flush thread (can be disabled for tests - all data stays in memory)
	private static boolean runMemtableFlushThread;
	
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
	public String getDataDir() {
		return DATA_DIR;
	}
	
	/**
	 * Get the name of the commit log
	 * 
	 * @return name of the commit log
	 */
	public String getCommitlogDir() {
		return COMMITLOG_DIR;
	}

	/**
	 * Should the compact thread run?
	 * @return
	 */
	public boolean isRunCompactThread() {
		return runCompactThread;
	}

	/**
	 * Set compact thread run, must be set before the StorageManager is started
	 * @param runCompactThread
	 */
	public void setRunCompactThread(boolean runCompactThread) {
		StorageConfiguration.runCompactThread = runCompactThread;
	}

	/**
	 * Should the memtable flush thread start?
	 * @return
	 */
	public boolean isRunMemtableFlushThread() {
		return runMemtableFlushThread;
	}

	/**
	 * Set memtable flush thread start, must be set before the StorageManager is started
	 * @param runMemtableFlushThread
	 */
	public void setRunMemtableFlushThread(boolean runMemtableFlushThread) {
		StorageConfiguration.runMemtableFlushThread = runMemtableFlushThread;
	}	
}
