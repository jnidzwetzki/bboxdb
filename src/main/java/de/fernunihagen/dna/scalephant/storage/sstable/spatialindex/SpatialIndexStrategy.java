package de.fernunihagen.dna.scalephant.storage.sstable.spatialindex;

import java.util.List;

import de.fernunihagen.dna.scalephant.storage.entity.Tuple;


public interface SpatialIndexStrategy {
	
	/**
	 * Construct the index from a list of tuples
	 * 
	 * @param tuples
	 */
	public void constructFromTuples(final List<Tuple> tuples);

}
