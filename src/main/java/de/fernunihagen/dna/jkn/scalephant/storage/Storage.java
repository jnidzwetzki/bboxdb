package de.fernunihagen.dna.jkn.scalephant.storage;

import java.util.Collection;

import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;

public interface Storage {

	public void put(final Tuple tuple) throws StorageManagerException;

	public Tuple get(final String key) throws StorageManagerException;
	
	public Collection<Tuple> getTuplesInside(final BoundingBox boundingBox) throws StorageManagerException;
	
	public void delete(final String key) throws StorageManagerException;
	
	public void clear() throws StorageManagerException;

}