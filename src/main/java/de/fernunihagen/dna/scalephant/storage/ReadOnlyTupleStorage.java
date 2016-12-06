package de.fernunihagen.dna.scalephant.storage;

import java.util.Iterator;

import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.queryprocessor.predicate.Predicate;

public interface ReadOnlyTupleStorage extends Iterable<Tuple> {
	
	/**
	 * Get the name of the tuple store
	 * @return
	 */
	public String getName();

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
	 * Get the number of tuples in the storage
	 * @return
	 */
	public int getNumberOfTuples();
	
	/**
	 * Get the n-th tuple
	 * @param position
	 * @return
	 * @throws StorageManagerException 
	 */
	public Tuple getTupleAtPosition(final int position) throws StorageManagerException;
	
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
	
	/**
	 * Delete the object and persistent data as soon as usage == 0
	 */
	public abstract void deleteOnClose();

	/** 
	 * Increment the usage counter
	 * @return
	 */
	public abstract boolean acquire();

	/**
	 * Decrement the usage counter
	 */
	public abstract void release();
	
}
