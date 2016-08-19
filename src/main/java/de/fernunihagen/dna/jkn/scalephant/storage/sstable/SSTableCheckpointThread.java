package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import de.fernunihagen.dna.jkn.scalephant.util.Stoppable;

public class SSTableCheckpointThread implements Runnable, Stoppable {

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

	@Override
	public void stop() {
		
	}

}
