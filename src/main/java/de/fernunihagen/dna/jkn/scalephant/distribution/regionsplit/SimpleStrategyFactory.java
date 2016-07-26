package de.fernunihagen.dna.jkn.scalephant.distribution.regionsplit;

public class SimpleStrategyFactory implements RegionSplitFactory {

	@Override
	public RegionSplitter getRegionSplitter() {
		return new SimpleSplitStrategy();
	}

}
