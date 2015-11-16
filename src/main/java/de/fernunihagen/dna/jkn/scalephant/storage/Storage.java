package de.fernunihagen.dna.jkn.scalephant.storage;

public interface Storage {

	public abstract void put(int key, final Tuple value);

	public abstract Tuple get(int key);

}