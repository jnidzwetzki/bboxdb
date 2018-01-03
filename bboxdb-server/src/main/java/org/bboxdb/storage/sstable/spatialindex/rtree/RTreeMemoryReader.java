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
package org.bboxdb.storage.sstable.spatialindex.rtree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.LinkedTransferQueue;

import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;
import org.bboxdb.util.DataEncoderHelper;

public class RTreeMemoryReader extends AbstractRTreeReader {

	/**
	 * The root node of the tree
	 */
	protected RTreeDirectoryNode rootNode;
	
	/**
	 * The child to read queue
	 * 
	 * Parent node -> Child Pointer
	 */
	protected Queue<Entry<RTreeDirectoryNode, Integer>> childToReadQueue = new LinkedTransferQueue<>();
	
	
	public RTreeMemoryReader() {
		
	}
	
	public RTreeMemoryReader(final RTreeBuilder indexBuilder) {
		this.rootNode = indexBuilder.rootNode;
		this.maxNodeSize = indexBuilder.maxNodeSize;
	}

	@Override
	public void readFromFile(final RandomAccessFile randomAccessFile) 
			throws StorageManagerException, InterruptedException {
		
		assert (rootNode == null);
		
		try {
			// Validate the magic bytes
			validateStream(randomAccessFile);
			maxNodeSize = DataEncoderHelper.readIntFromDataInput(randomAccessFile);
			readDirectoryNode(randomAccessFile, null);
						
			while(! childToReadQueue.isEmpty()) {
				final Entry<RTreeDirectoryNode, Integer> element = childToReadQueue.remove();
				readDirectoryNode(randomAccessFile, element.getKey());
			}
			
		} catch (IOException e) {
			throw new StorageManagerException(e);
		}
	}

	/**
	 * Read the directory node
	 * @param randomAccessFile
	 * @param parent
	 * @return
	 * @throws IOException
	 */
	protected RTreeDirectoryNode readDirectoryNode(final RandomAccessFile randomAccessFile, 
			final RTreeDirectoryNode parent) throws IOException {
		
		// Node data
		final int nodeId = DataEncoderHelper.readIntFromDataInput(randomAccessFile);
		final RTreeDirectoryNode node = new RTreeDirectoryNode(nodeId);
		node.setParentNode(parent);

		if(parent != null) {
			parent.directoryNodeChilds.add(node);
		}
		
		// Make this to the new root node
		if(rootNode == null) {
			rootNode = node;
		}

		// Bounding box data
		final int boundingBoxLength = DataEncoderHelper.readIntFromDataInput(randomAccessFile);
		final byte[] boundingBoxBytes = new byte[boundingBoxLength];
		randomAccessFile.readFully(boundingBoxBytes, 0, boundingBoxBytes.length);
		final BoundingBox boundingBox = BoundingBox.fromByteArray(boundingBoxBytes);
		node.setBoundingBox(boundingBox);
		
		// Read entry entries
		readEntryNodes(randomAccessFile);
		
		// Read directory nodes
		readDirectoryNodes(randomAccessFile, node);
		
		return node;
	}

	/**
	 * Read the directory nodes
	 * @param randomAccessFile
	 * @param node
	 * @throws IOException
	 */
	protected void readDirectoryNodes(final RandomAccessFile randomAccessFile, final RTreeDirectoryNode node)
			throws IOException {

		final byte[] followingByte = new byte[RTreeBuilder.MAGIC_VALUE_SIZE];

		for(int i = 0; i < maxNodeSize; i++) {
			randomAccessFile.readFully(followingByte, 0, followingByte.length);
			
			if(! Arrays.equals(followingByte, RTreeBuilder.MAGIC_CHILD_NODE_NOT_EXISTING)) {
				final int childPointer = DataEncoderHelper.readIntFromByte(followingByte);
				
				// Add the pointer for later decoding
				childToReadQueue.add(
						new AbstractMap.SimpleImmutableEntry<RTreeDirectoryNode, Integer>(node, childPointer)
				);
			} 
		}
	}

	/**
	 * Read the entry nodes
	 * @param inputStream
	 * @throws IOException
	 */
	protected void readEntryNodes(final RandomAccessFile randomAccessFile) throws IOException {
		
		final byte[] followingByte = new byte[RTreeBuilder.MAGIC_VALUE_SIZE];

		for(int i = 0; i < maxNodeSize; i++) {
			randomAccessFile.readFully(followingByte, 0, followingByte.length);
			
			if(Arrays.equals(followingByte, RTreeBuilder.MAGIC_CHILD_NODE_FOLLOWING)) {
				final SpatialIndexEntry spatialIndexEntry = SpatialIndexEntry.readFromFile(randomAccessFile);
				rootNode.indexEntries.add(spatialIndexEntry);
			} else if(! Arrays.equals(followingByte, RTreeBuilder.MAGIC_CHILD_NODE_NOT_EXISTING)) {
				throw new IllegalArgumentException("Unknown node type following: " + followingByte);
			}				
		}
	}

	@Override
	public List<SpatialIndexEntry> getEntriesForRegion(final BoundingBox boundingBox) {
		return rootNode.getEntriesForRegion(boundingBox);
	}
	

	@Override
	public void close() {
		maxNodeSize = -1;
		rootNode = null;
		childToReadQueue.clear();
	}
}
