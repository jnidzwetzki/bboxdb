/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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
package org.bboxdb.storage.sstable.spatialindex.rtree.mmf;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;
import org.bboxdb.storage.sstable.spatialindex.rtree.RTreeBuilder;
import org.bboxdb.util.DataEncoderHelper;

public class DirectoryNode {
	
	/**
	 * The node id
	 */
	protected int nodeId;
	
	/**
	 * The bounding box
	 */
	protected BoundingBox boundingBox;
	
	/**
	 * The leaf node children
	 */
	protected final List<SpatialIndexEntry> indexEntries;
	
	/**
	 * The child nodes
	 */
	protected final List<Integer> childNodes;
	
	public DirectoryNode() {
		this.indexEntries = new ArrayList<>();
		this.childNodes = new ArrayList<>();
	}
	
	/**
	 * Read the node from byte buffer
	 * @param memory
	 * @param maxNodeSize
	 * @throws IOException 
	 */
	public void initFromByteBuffer(final MappedByteBuffer memory, final int maxNodeSize) throws IOException {
		nodeId = memory.getInt();				
		
		// Bounding box data
		final int boundingBoxLength = memory.getInt();
		final byte[] boundingBoxBytes = new byte[boundingBoxLength];
		memory.get(boundingBoxBytes, 0, boundingBoxBytes.length);

		boundingBox = BoundingBox.fromByteArray(boundingBoxBytes);
		
		final byte[] followingByte = new byte[RTreeBuilder.MAGIC_VALUE_SIZE];

		// Read index entries
		for(int i = 0; i < maxNodeSize; i++) {
			memory.get(followingByte, 0, followingByte.length);
			
			if(Arrays.equals(followingByte, RTreeBuilder.MAGIC_CHILD_NODE_FOLLOWING)) {
				final SpatialIndexEntry spatialIndexEntry = SpatialIndexEntry.readFromByteBuffer(memory);
				indexEntries.add(spatialIndexEntry);
			} else if(! Arrays.equals(followingByte, RTreeBuilder.MAGIC_CHILD_NODE_NOT_EXISTING)) {
				throw new IllegalArgumentException("Unknown node type following: " + followingByte);
			}				
		}
		
		// Read pointer positions
		for(int i = 0; i < maxNodeSize; i++) {
			memory.get(followingByte, 0, followingByte.length);
			if(! Arrays.equals(followingByte, RTreeBuilder.MAGIC_CHILD_NODE_NOT_EXISTING)) {
				final int childPointer = DataEncoderHelper.readIntFromByte(followingByte);
				assert (childPointer > 0) : "Child pointer needs to be > 0 " + childPointer;
				childNodes.add(childPointer);
			}
		}
	}
	
	public BoundingBox getBoundingBox() {
		return boundingBox;
	}
	
	public int getNodeId() {
		return nodeId;
	}
	
	public List<Integer> getChildNodes() {
		return childNodes;
	}
	
	public List<SpatialIndexEntry> getIndexEntries() {
		return indexEntries;
	}

}
