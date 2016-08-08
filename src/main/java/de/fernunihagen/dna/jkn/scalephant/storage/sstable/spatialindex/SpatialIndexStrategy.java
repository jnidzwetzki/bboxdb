package de.fernunihagen.dna.jkn.scalephant.storage.sstable.spatialindex;

import java.util.List;

import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;


public interface SpatialIndexStrategy {
	
	/**
	 * Construct the index from a list of tuples
	 * 
	 * @param tuples
	 */
	public void constructFromTuples(final List<Tuple> tuples);

}
