package de.fernunihagen.dna.jkn.scalephant.storage;

import java.util.Collection;

import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;

public interface Storage {

	/**
	 * Store a tuple
	 * @param tuple
	 * @throws StorageManagerException
	 */
	public void put(final Tuple tuple) throws StorageManagerException;

	/**
	 * Search for tuple and return the most recent version
	 * @param key
	 * @return
	 * @throws StorageManagerException
	 */
	public Tuple get(final String key) throws StorageManagerException;
	
	/**
	 * Search retuns all tuples that are inside the query box
	 * @param boundingBox
	 * @return
	 * @throws StorageManagerException
	 */
	public Collection<Tuple> getTuplesInside(final BoundingBox boundingBox) throws StorageManagerException;
	
	/**
	 * Delete a tuple
	 * @param key
	 * @throws StorageManagerException
	 */
	public void delete(final String key) throws StorageManagerException;
	
	/**
	 * Truncate the stored data
	 * @throws StorageManagerException
	 */
	public void clear() throws StorageManagerException;

}