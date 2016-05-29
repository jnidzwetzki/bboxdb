package de.fernunihagen.dna.jkn.scalephant.distribution.sstable;

public interface SStableSplitFactory {

	/**
	 * Get a sstable distributor
	 */
	public SSTableSplitter getDistributor();
	
}
