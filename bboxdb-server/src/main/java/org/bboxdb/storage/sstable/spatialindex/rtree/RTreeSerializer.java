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
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.sstable.SSTableConst;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;

public class RTreeSerializer {

	/**
	 * The maximal node size
	 */
	protected int maxNodeSize;
	
	/**
	 * The root node
	 */
	protected RTreeDirectoryNode rootNode;

	/**
	 * The node start position
	 */
	protected final Map<RTreeDirectoryNode, Integer> nodeStartPosition = new HashMap<>();

	/**
	 * The node start child nodes position
	 */
	protected final Map<RTreeDirectoryNode, Integer> nodeFixedEndPosition = new HashMap<>();

	/**
	 * The nodes queue
	 */
	protected final Deque<RTreeDirectoryNode> nodesQueue = new ArrayDeque<>();


	public RTreeSerializer(final RTreeDirectoryNode rootNode, final int maxNodeSize) {
		this.rootNode = rootNode;
		this.maxNodeSize = maxNodeSize;
	}

	/**
	 * Serialize the tree to the file
	 * @param randomAccessFile
	 * @throws StorageManagerException
	 */
	public void writeToStream(final RandomAccessFile randomAccessFile) throws StorageManagerException {
		
		nodesQueue.clear();
		
		try {
			// Write the magic bytes
			randomAccessFile.write(SSTableConst.MAGIC_BYTES_SPATIAL_RTREE_INDEX);

			// Write the tree configuration
			final ByteBuffer nodeSizeBytes = DataEncoderHelper.intToByteBuffer(maxNodeSize);
			randomAccessFile.write(nodeSizeBytes.array());

			nodesQueue.push(rootNode);

			while(! nodesQueue.isEmpty()) {
				final RTreeDirectoryNode node = nodesQueue.pop();
				handleNewNode(randomAccessFile, node);			    
			}
			
			updateIndexNodePointer(randomAccessFile);
			
		} catch (IOException e) {
			throw new StorageManagerException(e);
		}
	}
	
	/**
	 * Update the index node pointer
	 * @param randomAccessFile 
	 * @throws IOException 
	 */
	protected void updateIndexNodePointer(final RandomAccessFile randomAccessFile) throws IOException {
		for(final RTreeDirectoryNode node : nodeFixedEndPosition.keySet()) {
			// Seek to the first pointer
			randomAccessFile.seek(nodeFixedEndPosition.get(node));
			
			for(final RTreeDirectoryNode child : node.getDirectoryNodeChilds()) {
				final Integer childNodePosition = nodeStartPosition.get(child);
				final ByteBuffer childNodePointer = DataEncoderHelper.intToByteBuffer(childNodePosition);
				
				// Override node pointer placeholder
				randomAccessFile.write(childNodePointer.array());
			}
		}
	}

	/**
	 * Handle the nodes
	 * @param randomAccessFile
	 * @param nodesQueue
	 * @param node
	 * @throws IOException
	 */
	protected void handleNewNode(final RandomAccessFile randomAccessFile, final RTreeDirectoryNode node) 
			throws IOException {

		// Node data
		nodeStartPosition.put(node, (int) randomAccessFile.getFilePointer());
		final ByteBuffer nodeIdBytes = DataEncoderHelper.intToByteBuffer(node.getNodeId());
		randomAccessFile.write(nodeIdBytes.array());
		
		// Bounding box data
		final byte[] boundingBoxBytes = node.getBoundingBox().toByteArray();
		final ByteBuffer boundingBoxLength = DataEncoderHelper.intToByteBuffer(boundingBoxBytes.length);
		randomAccessFile.write(boundingBoxLength.array());
		randomAccessFile.write(boundingBoxBytes);

		// Write entry nodes
		writeEntryNodes(randomAccessFile, node);
		nodeFixedEndPosition.put(node, (int) randomAccessFile.getFilePointer());

		// Write directory nodes
		addDirectoryNodesToQueue(randomAccessFile, node);		
	}

	/**
	 * Write the entry nodes to the output stream
	 * @param randomAccessFile
	 * @param node
	 * @throws IOException
	 */
	protected void writeEntryNodes(final RandomAccessFile randomAccessFile, final RTreeDirectoryNode node) 
			throws IOException {
		
		final List<SpatialIndexEntry> indexEntries = node.getIndexEntries();
		for(int i = 0; i < maxNodeSize; i++) {
			if(i < indexEntries.size()) {
				randomAccessFile.write(RTreeBuilder.MAGIC_CHILD_NODE_FOLLOWING);
				indexEntries.get(i).writeToFile(randomAccessFile);
			} else {
				randomAccessFile.write(RTreeBuilder.MAGIC_CHILD_NODE_NOT_EXISTING);
			}
		}
	}

	/**
	 * Add the directory nodes to the queue
	 * @param nodesQueue
	 * @param node
	 * @throws IOException 
	 */
	protected void addDirectoryNodesToQueue(final RandomAccessFile randomAccessFile, 
			final RTreeDirectoryNode node) throws IOException {
		
		final List<RTreeDirectoryNode> directoryNodeChilds = node.getDirectoryNodeChilds();
		for(int i = 0; i < maxNodeSize; i++) {
			if(i < directoryNodeChilds.size()) {
				nodesQueue.addFirst(directoryNodeChilds.get(i));
			}
			
			// Existing pointer will be written in a second step
			randomAccessFile.write(RTreeBuilder.MAGIC_CHILD_NODE_NOT_EXISTING);
		}
	}
	
}
