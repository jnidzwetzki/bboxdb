package org.bboxdb.storage.rtree;

import org.bboxdb.storage.sstable.spatialindex.rtree.AbstractRTreeReader;
import org.bboxdb.storage.sstable.spatialindex.rtree.partialreader.RTreeMMFReader;

public class TestRTreeMMFDeserializer extends TestRTreeMemoryDeserializer {

	@Override
	protected AbstractRTreeReader getRTreeReader() {
		return new RTreeMMFReader();
	}

}
