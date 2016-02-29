package de.fernunihagen.dna.jkn.scalephant.storage.sstable.spatialindex;

public class BoxSearchSpatialIndexFactory implements SpatialIndexFactory {

	@Override
	public SpatialIndexer getIndexer() {
		return new BoxSearchIndex();
	}


}
