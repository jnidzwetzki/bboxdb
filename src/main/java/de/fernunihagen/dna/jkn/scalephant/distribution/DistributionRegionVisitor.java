package de.fernunihagen.dna.jkn.scalephant.distribution;

public interface DistributionRegionVisitor {

	/**
	 * Visit the next distribution region
	 * 
	 * @return true or false - visit next region or not
	 * @param distributionRegion
	 */
	public boolean visitRegion(final DistributionRegion distributionRegion);
	
}
