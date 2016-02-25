package de.fernunihagen.dna.jkn.scalephant;

public class ScalephantConfiguration {
	
	/**
	 * The root directory of the application
	 */
	protected String rootDirectory = "/tmp/scalephant_test";
	
	/**
	 *  The directory to store data
	 */
	protected String dataDirectory = "/tmp/scalephant_test/data";
	
	/**
	 * The commit log dir
	 */
	protected String commitlogDir = "/tmp/scalephant_test/commitlog";
	
	/**
	 *  Number of entries per memtable
	 */
	protected int memtableEntriesMax = 10000;
	
	/**
	 * Size of the memtable in KB
	 */
	protected int memtableSizeMax = 128 * 1024;
	
	/**
	 * Start compact thread (can be disabled for tests)
	 */
	protected boolean storageRunCompactThread = true;
	
	/**
	 * Start flush thread (can be disabled for tests - all data stays in memory)
	 */
	protected boolean storageRunMemtableFlushThread = true;
	
	
	public String getRootDirectory() {
		return rootDirectory;
	}
	
	public void setRootDirectory(final String rootDirectory) {
		this.rootDirectory = rootDirectory;
	}
	
	public String getDataDirectory() {
		return dataDirectory;
	}
	
	public void setDataDirectory(final String dataDirectory) {
		this.dataDirectory = dataDirectory;
	}

	public String getCommitlogDir() {
		return commitlogDir;
	}

	public void setCommitlogDir(final String commitlogDir) {
		this.commitlogDir = commitlogDir;
	}

	public int getMemtableEntriesMax() {
		return memtableEntriesMax;
	}

	public void setMemtableEntriesMax(final int memtableEntriesMax) {
		this.memtableEntriesMax = memtableEntriesMax;
	}

	public int getMemtableSizeMax() {
		return memtableSizeMax;
	}

	public void setMemtableSizeMax(final int memtableSizeMax) {
		this.memtableSizeMax = memtableSizeMax;
	}

	public boolean isStorageRunCompactThread() {
		return storageRunCompactThread;
	}

	public void setStorageRunCompactThread(final boolean storageRunCompactThread) {
		this.storageRunCompactThread = storageRunCompactThread;
	}

	public boolean isStorageRunMemtableFlushThread() {
		return storageRunMemtableFlushThread;
	}

	public void setStorageRunMemtableFlushThread(
			final boolean storageRunMemtableFlushThread) {
		this.storageRunMemtableFlushThread = storageRunMemtableFlushThread;
	}

}
