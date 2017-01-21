package org.bboxdb.storage.sstable.spatialindex.rtree;

import java.util.concurrent.atomic.AtomicInteger;

public class RTreeNodeFactory {

	/**
	 * The node id generator
	 */
	protected final AtomicInteger nodeIdInteger;
	
	/**
	 * The maximal size of a node
	 */
	protected final int MAX_NODE_SIZE;
		
	
	public RTreeNodeFactory(final int maxNodeSize) {
		this.nodeIdInteger = new AtomicInteger(0);
		this.MAX_NODE_SIZE = maxNodeSize;
	}

	public RTreeNodeFactory() {
		this(32);
	}
	
	/**
	 * Build a new leaf node
	 * @return 
	 */
	public RTreeLeafNode buildLeafNode() {
		return new RTreeLeafNode(MAX_NODE_SIZE);
	}
	
	/**
	 * Build a new directory node
	 * @return 
	 */
	public RTreeDirectoryNode buildDirectoryNode() {
		return new RTreeDirectoryNode(MAX_NODE_SIZE);
	}

}
