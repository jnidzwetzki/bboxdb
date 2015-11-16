package de.fernunihagen.dna.jkn.scalephant.storage;

public interface Storage {

	public abstract void put(final Tuple tuple);

	public abstract Tuple get(int key);
	
	public abstract void clear();

}