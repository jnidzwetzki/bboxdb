package de.fernunihagen.dna.scalephant.storage;

import de.fernunihagen.dna.scalephant.storage.entity.Tuple;

public interface ReadOnlyTupleStorage {

	/**
	 * Search for tuple and return the most recent version
	 * @param key
	 * @return
	 * @throws StorageManagerException
	 */
	public Tuple get(final String key) throws StorageManagerException;
	
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
