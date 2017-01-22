/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
 *  
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License. 
 *    
 *******************************************************************************/
package org.bboxdb.storage.sstable.spatialindex.rtree;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;

public class RTreeLeafNode extends AbstractRTreeNode {

	/**
	 * The Childs
	 */
	protected final List<SpatialIndexEntry> childs;

	public RTreeLeafNode(final int maxNodeSize) {
		super(maxNodeSize);
		this.childs = new ArrayList<>();
	}
	
	/**
	 * Add an entry to the leaf node
	 * @param spatialIndexEntry
	 */
	@Override
	public void insertIndexEntry(final SpatialIndexEntry entry) {
		childs.add(entry);
		
		// Enlarge bounding box
		this.boundingBox = BoundingBox.getCoveringBox(boundingBox, entry.getBoundingBox());
	}

	@Override
	public List<SpatialIndexEntry> getEntriesForRegion(final BoundingBox boundingBox) {
		return childs
			.stream()
			.filter(c -> c.getBoundingBox().overlaps(boundingBox))
			.collect(Collectors.toList());
	}
}