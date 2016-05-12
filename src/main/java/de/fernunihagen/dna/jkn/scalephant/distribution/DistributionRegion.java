package de.fernunihagen.dna.jkn.scalephant.distribution;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class DistributionRegion implements Watcher {

	/**
	 * The name of the distribution group
	 */
	protected final DistributionGroupName distributionGroupName;
	
	/**
	 * A pointer that points to the total number of levels
	 */
	protected final TotalLevel totalLevel;
	
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
	 * @param distributionGroupName
	 */
	protected final int level;
	
	/**
	 * Private constructor, the factory method and the set split methods should
	 * be used to create a tree
	 * @param name
	 * @param level
	 */
	private DistributionRegion(final DistributionGroupName name, final int level, final TotalLevel totalLevel) {
		this.distributionGroupName = name;
		this.level = level;
		this.totalLevel = totalLevel;
		
		totalLevel.registerNewLevel(level + 1);
	}
	
	/**
	 * Factory method for a new root region
	 * @param name
	 * @return
	 */
	public static DistributionRegion createRootRegion(final String name) {
		final DistributionGroupName distributionGroupName = new DistributionGroupName(name);
		
		if(! distributionGroupName.isValid()) {
			throw new IllegalArgumentException("Invalid region name: " + name);
		}
		
		return new DistributionRegion(distributionGroupName, 0, new TotalLevel());
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
		
		leftChild = new DistributionRegion(distributionGroupName, level + 1, totalLevel);
		rightChild = new DistributionRegion(distributionGroupName, level + 1, totalLevel);
		
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
		return distributionGroupName.getFullname();
	}
	
	/**
	 * Get the level of the region
	 * @return
	 */
	public int getLevel() {
		return level;
	}
	
	/**
	 * Get the number of levels in this tree
	 * @return
	 */
	public int getTotalLevel() {
		return totalLevel.getMaxLevel();
	}
	
	/**
	 * Get the dimension of the distribution region
	 * @return
	 */
	public int getDimension() {
		return distributionGroupName.getDimension();
	}
	
	/**
	 * Returns the dimension of the split
	 * @return
	 */
	public int getSplitDimension() {
		return level % getDimension();
	}

	@Override
	public String toString() {
		return "DistributionNode [name=" + distributionGroupName + ", split=" + split
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
	public boolean isLeafRegion() {
		if(leftChild == null || rightChild == null) {
			return true;
		}
		
		return false;
	}
	
	/**
	 * Is this the left child of the parent
	 * @return
	 */
	public boolean isLeftChild() {
		if(getParent() == null) {
			return false;
		}
		
		return (getParent().getLeftChild() == this);
	}
	
	/**
	 * Is this the right child of the parent
	 * @return
	 */
	public boolean isRightChild() {
		if(getParent() == null) {
			return false;
		}
		
		return (getParent().getRightChild() == this);
	}
	
}

/**
 * A class that represents the max number of levels
 *
 */
class TotalLevel {
	protected int maxLevel = 0;
	
	public void registerNewLevel(int level) {
		maxLevel = Math.max(maxLevel, level);
	}
	
	public int getMaxLevel() {
		return maxLevel;
	}
}
