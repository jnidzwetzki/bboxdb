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

public class RTreeDirectoryNode extends AbstractRTreeNode {
	
	/**
	 * The directory node childs
	 */
	protected final List<RTreeDirectoryNode> directoryNodeChilds;
	
	/**
	 * The leaf node childs
	 */
	protected final List<SpatialIndexEntry> indexEntries;
	
	/**
	 * The id of the node
	 */
	protected final int nodeId;
	
	public RTreeDirectoryNode(final int nodeId) {
		this.directoryNodeChilds = new ArrayList<>();
		this.indexEntries = new ArrayList<>();
		this.nodeId = nodeId;
	}
	
	/**
	 * Add a directory node as child
	 * @param rTreeDirectoryNode
	 */
	public void addDirectoryNodeChild(final RTreeDirectoryNode rTreeDirectoryNode) {
		
		// We can carry directory nodes or index entries
		assert (indexEntries.isEmpty());
		
		directoryNodeChilds.add(rTreeDirectoryNode);
	}
	
	/**
	 * Remove child directory node
	 * @param rTreeDirectoryNode
	 * @return
	 */
	public boolean removeDirectoryNodeChild(final RTreeDirectoryNode rTreeDirectoryNode) {
		return directoryNodeChilds.remove(rTreeDirectoryNode);
	}
	
	/**
	 * Remove child leaf node 
	 * @param rTreeLeafNode
	 * @return
	 */
	public boolean removeIndexEntry(final SpatialIndexEntry entry) {
		
		// We can carry directory nodes or index entries
		assert (directoryNodeChilds.isEmpty());
		
		return indexEntries.remove(entry);
	}

	
	/**
	 * Add an entry to the leaf node
	 * @param spatialIndexEntry
	 * @return 
	 */
	@Override
	public RTreeDirectoryNode insertIndexEntry(final SpatialIndexEntry entry) {
		indexEntries.add(entry);
		
		// Enlarge bounding box
		this.boundingBox = BoundingBox.getCoveringBox(boundingBox, entry.getBoundingBox());
		
		// Return the leaf node that has stored the data
		return this;
	}

	@Override
	public List<SpatialIndexEntry> getEntriesForRegion(final BoundingBox boundingBox) {
		return indexEntries
			.stream()
			.filter(c -> c.getBoundingBox().overlaps(boundingBox))
			.collect(Collectors.toList());
	}
	
	/**
	 * Get the size of the node
	 * @return
	 */
	public int getSize() {
		return indexEntries.size() + directoryNodeChilds.size();
	}
	
	/**
	 * Is this a leaf node
	 */
	public boolean isLeafNode() {
		return indexEntries.isEmpty();
	}
}