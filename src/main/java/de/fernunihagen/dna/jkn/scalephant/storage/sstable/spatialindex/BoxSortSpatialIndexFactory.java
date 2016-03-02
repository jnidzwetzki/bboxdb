package de.fernunihagen.dna.jkn.scalephant.storage.sstable.spatialindex;

public class BoxSortSpatialIndexFactory implements SpatialIndexFactory {

	@Override
	public SpatialIndexer getIndexer() {
		return new BoxSortIndex();
	}


}
