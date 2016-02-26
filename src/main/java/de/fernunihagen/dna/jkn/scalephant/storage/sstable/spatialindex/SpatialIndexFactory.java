package de.fernunihagen.dna.jkn.scalephant.storage.sstable.spatialindex;

public interface SpatialIndexFactory {

	/**
	 * Get a spatial index reader
	 */
	public void getReader();
	
	/**
	 * Get a spatial index writer
	 */
	public void getWriter();
	
}
