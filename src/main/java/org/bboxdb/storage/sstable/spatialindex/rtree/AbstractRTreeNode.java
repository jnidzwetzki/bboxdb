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

import java.util.List;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;

public abstract class AbstractRTreeNode {
	
	/**
	 * The maximal size of a node
	 */
	protected final int MAX_NODE_SIZE;

	/**
	 * The bounding box of the node
	 */
	protected BoundingBox boundingBox;
	
	public AbstractRTreeNode(final int nodeSize) {
		this.MAX_NODE_SIZE = nodeSize;
	}

	/**
	 * Return the bounding box of the node
	 * @return
	 */
	public BoundingBox getBoundingBox() {
		return boundingBox;
	}
	
	/**
	 * Insert a new index entry
	 * @param entry
	 */
	public abstract void insertIndexEntry(final SpatialIndexEntry entry);
	
	/**
	 * Get the entries for the given region
	 * @param boundingBox
	 * @return 
	 */
	public abstract List<SpatialIndexEntry> getEntriesForRegion(final BoundingBox boundingBox);
	
}