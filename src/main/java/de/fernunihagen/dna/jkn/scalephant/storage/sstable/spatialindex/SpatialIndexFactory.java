package de.fernunihagen.dna.jkn.scalephant.storage.sstable.spatialindex;

public interface SpatialIndexFactory {

	/**
	 * Get a spatial indexer
	 */
	public SpatialIndexer getIndexer();
	
}
