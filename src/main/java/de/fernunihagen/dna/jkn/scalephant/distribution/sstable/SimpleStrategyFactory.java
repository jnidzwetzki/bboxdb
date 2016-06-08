package de.fernunihagen.dna.jkn.scalephant.distribution.sstable;

public class SimpleStrategyFactory implements SStableSplitFactory {

	@Override
	public SSTableSplitter getSSTableSplitter() {
		return new SimpleSplitStrategy();
	}

}
