package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

public class SSTableCheckpointThread implements Runnable {

	/**
	 * The maximal number of seconds for data to stay in memory
	 */
	protected final int maxUncheckpointedSeconds;
	
	public SSTableCheckpointThread(final int maxUncheckpointedSeconds) {
		this.maxUncheckpointedSeconds = maxUncheckpointedSeconds;
	}

	@Override
	public void run() {
	
	}

}
