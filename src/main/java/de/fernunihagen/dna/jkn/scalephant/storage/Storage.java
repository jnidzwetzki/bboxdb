package de.fernunihagen.dna.jkn.scalephant.storage;

public interface Storage {

	public abstract void put(final Tuple tuple) throws StorageManagerException;

	public abstract Tuple get(int key) throws StorageManagerException;
	
	public abstract void clear() throws StorageManagerException;

}