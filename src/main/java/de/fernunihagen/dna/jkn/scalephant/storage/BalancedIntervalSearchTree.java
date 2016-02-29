package de.fernunihagen.dna.jkn.scalephant.storage;

import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;

public class BalancedIntervalSearchTree {

	/**
	 * The dimension of the tree
	 */
	protected final int dimension;

	/**
	 * The root element of the tree
	 */
	protected BalancedIntervalSearchTreeNode root = null;
	
	public BalancedIntervalSearchTree(final int dimension) {
		super();
		this.dimension = dimension;
	}
	
	/**
	 * Insert a new 
	 * @param boundingBox
	 * @return
	 */
	public boolean insert(final BoundingBox boundingBox) {
		
		if(boundingBox.getDimension() != dimension) {
			return false;
		}
		
		final BalancedIntervalSearchTreeNode newNode = new BalancedIntervalSearchTreeNode(boundingBox);
		
		if(root == null) {
			root = newNode;
		}
		
		return true;
	}
	
}

class BalancedIntervalSearchTreeNode {
	protected final BoundingBox boundingBox;
	
	public BalancedIntervalSearchTreeNode(final BoundingBox boundingBox) {
		this.boundingBox = boundingBox;
	}
}