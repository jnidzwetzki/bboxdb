package de.fernunihagen.dna.jkn.scalephant.storage;

public interface Storage {

	public abstract void put(final Tuple tuple) throws StorageManagerException;

	public abstract Tuple get(final String key) throws StorageManagerException;
	
	public abstract void clear() throws StorageManagerException;

}