package de.fernunihagen.dna.scalephant.storage;

import java.util.Iterator;

import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.queryprocessor.Predicate;

public interface ReadOnlyTupleStorage {

	/**
	 * Search for tuple and return the most recent version
	 * @param key
	 * @return
	 * @throws StorageManagerException
	 */
	public Tuple get(final String key) throws StorageManagerException;
	
	/**
	 * Get all tuples that match the given predicate
	 * @param predicate
	 * @return
	 */
	public Iterator<Tuple> getMatchingTuples(final Predicate predicate);
	
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
