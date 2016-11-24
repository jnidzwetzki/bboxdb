package de.fernunihagen.dna.scalephant.storage;

public interface TupleStorage {
	
	/**
	 * Get the timestamp of the oldest tuple
	 * @return
	 */
	public long getOldestTupleTimestamp();
	
	/**
	 * Get the timestamp of the newest tuple
	 * @return
	 */
	public long getNewestTupleTimestamp();
	
}
