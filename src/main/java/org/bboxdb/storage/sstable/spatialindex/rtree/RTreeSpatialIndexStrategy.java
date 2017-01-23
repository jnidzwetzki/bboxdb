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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexStrategy;

public class RTreeSpatialIndexStrategy implements SpatialIndexStrategy {

	/**
	 * The node factory
	 */
	protected final RTreeNodeFactory nodeFactory;
	
	/**
	 * The root node of the tree
	 */
	protected AbstractRTreeNode rootNode;
	
	/**
	 * The max size of a child node
	 */
	protected int MAX_SIZE = 32;

	public RTreeSpatialIndexStrategy() {
		this.nodeFactory = new RTreeNodeFactory();
		this.rootNode = nodeFactory.buildDirectoryNode();
	}

	@Override
	public void readFromStream(final InputStream inputStream) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeToStream(final OutputStream outputStream) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<SpatialIndexEntry> getEntriesForRegion(final BoundingBox boundingBox) {
		return rootNode.getEntriesForRegion(boundingBox);
	}

	@Override
	public void bulkInsert(final List<SpatialIndexEntry> elements) {
		for(final SpatialIndexEntry entry : elements) {
			insert(entry);
		}
	}

	@Override
	public void insert(final SpatialIndexEntry entry) {
		final RTreeDirectoryNode insertedNode = rootNode.insertIndexEntry(entry);
		
		if(insertedNode.getSize() > MAX_SIZE) {
			splitNode(insertedNode);
		}
	}

	/**
	 * Split the given node
	 * @param insertedNode
	 */
	protected void splitNode(RTreeDirectoryNode insertedNode) {
		
	}

	
}
