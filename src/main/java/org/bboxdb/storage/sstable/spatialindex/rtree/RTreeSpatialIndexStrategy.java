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
import java.util.ArrayList;
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
	protected RTreeDirectoryNode rootNode;
	
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
		final RTreeSpatialIndexEntry treeEntry = nodeFactory.buildRTreeIndex(entry);
		final RTreeDirectoryNode insertedNode = rootNode.insertEntryIntoIndex(treeEntry);
		adjustTree(insertedNode);		
	}

	/**
	 * Anjust the tree, beginning from the argument to the tree root
	 * @param insertedNode
	 */
	protected void adjustTree(final RTreeDirectoryNode insertedNode) {
		
		if(insertedNode == null) {
			return;
		}
		
		RTreeDirectoryNode nodeToCheck = insertedNode;
		
		// Adjust beginning from the bottom
		do {
			
			if(nodeToCheck.getSize() > MAX_SIZE) {
				nodeToCheck = splitNode(insertedNode);
			}
			
			nodeToCheck = nodeToCheck.getParentNode();
		} while(nodeToCheck != null);	
	}

	/**
	 * Split the given node
	 * @param nodeToSplit
	 * @return 
	 */
	protected RTreeDirectoryNode splitNode(final RTreeDirectoryNode nodeToSplit) {
		final RTreeDirectoryNode newNode1 = nodeFactory.buildDirectoryNode();
		final RTreeDirectoryNode newNode2 = nodeFactory.buildDirectoryNode();
		RTreeDirectoryNode newParent = null;
		
		// Root node is full
		if(nodeToSplit.getParentNode() == null) {
			rootNode = nodeFactory.buildDirectoryNode();
			newParent = rootNode;
		} else {
			newParent = nodeFactory.buildDirectoryNode();
			nodeToSplit.getParentNode().addDirectoryNodeChild(newParent);
			nodeToSplit.getParentNode().removeDirectoryNodeChild(nodeToSplit);
		}
		
		// Insert new directory node
		newParent.addDirectoryNodeChild(newNode1);
		newParent.addDirectoryNodeChild(newNode2);
		newNode1.setParentNode(newParent);
		newNode2.setParentNode(newParent);
		
		// Find seeds and distribute data
		if(nodeToSplit.isLeafNode()) {
			distributeLeafData(nodeToSplit, newNode1, newNode2);
		} else {
			distributeIndexData(nodeToSplit, newNode1, newNode2);
		}

		// Recalculate the bounding boxes
		newNode1.updateBoundingBox();
		newNode2.updateBoundingBox();
		newParent.updateBoundingBox();
		
		return newNode1;
	}

	/**
	 * Distribute the leaf data
	 * @param nodeToSplit
	 * @param newNode1
	 * @param newNode2
	 */
	protected void distributeIndexData(final RTreeDirectoryNode nodeToSplit, final RTreeDirectoryNode newNode1,
			final RTreeDirectoryNode newNode2) {
		
		final List<RTreeDirectoryNode> dataToDistribute = nodeToSplit.getDirectoryNodeChilds();
		final List<RTreeDirectoryNode> seeds = new ArrayList<>();
		
		final QuadraticSeedPicker<RTreeDirectoryNode> seedPicker = new QuadraticSeedPicker<>();
		seedPicker.quadraticPickSeeds(dataToDistribute, seeds);
		
		newNode1.addDirectoryNodeChild(seeds.get(0));
		newNode2.addDirectoryNodeChild(seeds.get(1));
		
		for(int i = 0; i < dataToDistribute.size(); i++) {
			newNode1.updateBoundingBox();
			newNode2.updateBoundingBox();

			final int remainingObjects = dataToDistribute.size() - i;
			final RTreeDirectoryNode entry = dataToDistribute.get(i);
			
			if(newNode1.getDirectoryNodeChilds().size() + remainingObjects <= MAX_SIZE / 2) {
				newNode1.addDirectoryNodeChild(entry);
				continue;
			}
			
			if(newNode2.getDirectoryNodeChilds().size() + remainingObjects <= MAX_SIZE / 2) {
				newNode2.addDirectoryNodeChild(entry);
				continue;
			}
			
			final double node1Enlargement = newNode1.getBoundingBox().calculateEnlargement(entry.getBoundingBox());
			final double node2Enlargement = newNode2.getBoundingBox().calculateEnlargement(entry.getBoundingBox());
		
			if(node1Enlargement == node2Enlargement) {
				if(newNode1.getDirectoryNodeChilds().size() < newNode2.getDirectoryNodeChilds().size()) {
					newNode1.addDirectoryNodeChild(entry);
					continue;
				} else {
					newNode2.addDirectoryNodeChild(entry);
					continue;
				}
			}
			
			if(node1Enlargement < node2Enlargement) {
				newNode1.addDirectoryNodeChild(entry);
				continue;
			} else {
				newNode2.addDirectoryNodeChild(entry);
				continue;
			}
		}
	}

	/**
	 * Distribute the index data
	 * @param nodeToSplit
	 * @param newNode1
	 * @param newNode2
	 */
	protected void distributeLeafData(final RTreeDirectoryNode nodeToSplit, final RTreeDirectoryNode newNode1,
			final RTreeDirectoryNode newNode2) {
		
		final List<RTreeSpatialIndexEntry> dataToDistribute = nodeToSplit.getIndexEntries();
		final List<RTreeSpatialIndexEntry> seeds = new ArrayList<>();
		
		final QuadraticSeedPicker<RTreeSpatialIndexEntry> seedPicker = new QuadraticSeedPicker<>();
		seedPicker.quadraticPickSeeds(dataToDistribute, seeds);

		newNode1.insertEntryIntoIndex(seeds.get(0));
		newNode2.insertEntryIntoIndex(seeds.get(1));
		
		for(int i = 0; i < dataToDistribute.size(); i++) {
			newNode1.updateBoundingBox();
			newNode2.updateBoundingBox();

			final int remainingObjects = dataToDistribute.size() - i;
			final RTreeSpatialIndexEntry entry = dataToDistribute.get(i);
			
			if(newNode1.getIndexEntries().size() + remainingObjects <= MAX_SIZE / 2) {
				newNode1.insertEntryIntoIndex(entry);
				continue;
			}
			
			if(newNode2.getIndexEntries().size() + remainingObjects <= MAX_SIZE / 2) {
				newNode2.insertEntryIntoIndex(entry);
				continue;
			}
			
			final double node1Enlargement = newNode1.getBoundingBox().calculateEnlargement(entry.getBoundingBox());
			final double node2Enlargement = newNode2.getBoundingBox().calculateEnlargement(entry.getBoundingBox());
		
			if(node1Enlargement == node2Enlargement) {
				if(newNode1.getIndexEntries().size() < newNode2.getIndexEntries().size()) {
					newNode1.insertEntryIntoIndex(entry);
					continue;
				} else {
					newNode2.insertEntryIntoIndex(entry);
					continue;
				}
			}
			
			if(node1Enlargement < node2Enlargement) {
				newNode1.insertEntryIntoIndex(entry);
				continue;
			} else {
				newNode2.insertEntryIntoIndex(entry);
				continue;
			}
		}
	}
}
