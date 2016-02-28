package de.fernunihagen.dna.jkn.scalephant.storage.sstable.spatialindex;

public interface SpatialIndexFactory {

	/**
	 * Get a spatial index reader
	 */
	public SpatialIndexReader getReader();
	
	/**
	 * Get a spatial index writer
	 */
	public SpatialIndexWriter getWriter();
	
}
