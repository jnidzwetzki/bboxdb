package de.fernunihagen.dna.jkn.scalephant.storage;

public interface Storage {

	public void put(final Tuple tuple) throws StorageManagerException;

	public Tuple get(final String key) throws StorageManagerException;
	
	public void delete(final String key) throws StorageManagerException;
	
	public void clear() throws StorageManagerException;

}