package de.fernunihagen.dna.scalephant.storage;

public interface Acquirable {

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