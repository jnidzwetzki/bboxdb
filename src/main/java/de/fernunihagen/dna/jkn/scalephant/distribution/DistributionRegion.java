package de.fernunihagen.dna.jkn.scalephant.distribution;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;

public class DistributionRegion implements Watcher {

	/**
	 * The name of the distribution group
	 */
	protected final String name;
	
	/**
	 * The split position
	 */
	protected long split = Long.MIN_VALUE;
	
	/**
	 * The left child
	 */
	protected DistributionRegion leftChild = null;
	
	/**
	 * The right child
	 */
	protected DistributionRegion rightChild = null;
	
	/**
	 * The parent of this node
	 */
	protected DistributionRegion parent = null;
	
	/**
	 * The level of the node
	 * @param name
	 */
	protected final int level;
	
	/**
	 * Private constructor, the factory method and the set split methods should
	 * be used to create a tree
	 * @param name
	 * @param level
	 */
	private DistributionRegion(final String name, final int level) {
		this.name = name;
		this.level = level;
	}
	
	/**
	 * Factory method for a new root region
	 * @param name
	 * @return
	 */
	public static DistributionRegion createRootRegion(final String name) {
		return new DistributionRegion(name, 0);
	}

	/**
	 * Process structure updates (e.g. changes in the distribution group)
	 */
	@Override
	public void process(final WatchedEvent event) {
		
	}

	/**
	 * Get the left child
	 * @return
	 */
	public DistributionRegion getLeftChild() {
		return leftChild;
	}

	/**
	 * Get the right child
	 * @return
	 */
	public DistributionRegion getRightChild() {
		return rightChild;
	}

	/**
	 * Get the parent of the node
	 * @return
	 */
	public DistributionRegion getParent() {
		return parent;
	}

	/**
	 * Set the parent of the node
	 * @param parent
	 */
	public void setParent(final DistributionRegion parent) {
		this.parent = parent;
	}
	
	/**
	 * Set the split coordinate
	 * @param split
	 */
	public void setSplit(final long split) {
		this.split = split;
		
		if(leftChild != null || rightChild != null) {
			throw new IllegalArgumentException("Split called, but left or right node are empty");
		}
		
		leftChild = new DistributionRegion(getName(), level + 1);
		rightChild = new DistributionRegion(getName(), level + 1);
		
		leftChild.setParent(this);
		rightChild.setParent(this);
	}
	
	/**
	 * Get the split coordinate
	 * @return
	 */
	public long getSplit() {
		return split;
	}
	
	/**
	 * Get the name
	 * @return
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * Calculate the bounding box for this node
	 * @return
	 */
	public BoundingBox getBoindingBox() {
		final BoundingBox boundingBox = BoundingBox.EMPTY_BOX;
		final DistributionGroupHelper distributionGroupHelper = new DistributionGroupHelper(getName());
		distributionGroupHelper.getDimension();
		
		return boundingBox;
	}

	@Override
	public String toString() {
		return "DistributionNode [name=" + name + ", split=" + split
				+ ", leftChild=" + leftChild + ", rightChild=" + rightChild
				+ ", parent=" + parent + "]";
	}
	
	/**
	 * Get the root element of the region
	 */
	public DistributionRegion getRootRegion() {
		DistributionRegion currentElement = this;
		
		// Follow the links to the root element
		while(currentElement.getParent() != null) {
			currentElement = currentElement.getParent();
		}
		
		return currentElement;
	}
	
	/**
	 * Is this a leaf region node?
	 * @return
	 */
	protected boolean isLeafRegion() {
		if(leftChild == null || rightChild == null) {
			return true;
		}
		
		return false;
	}
	
}
