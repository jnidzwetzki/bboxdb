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
	protected long split;
	
	/**
	 * The left child
	 */
	protected DistributionRegion leftChild;
	
	/**
	 * The right child
	 */
	protected DistributionRegion rightChild;
	
	/**
	 * The parent of this node
	 */
	protected DistributionRegion parent = null;
	
	public DistributionRegion(final String name) {
		this.name = name;
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
	 * Set the left child
	 * @return
	 */
	public void setLeftChild(final DistributionRegion leftChild) {
		this.leftChild = leftChild;
		leftChild.setParent(this);
	}

	/**
	 * Get the right child
	 * @return
	 */
	public DistributionRegion getRightChild() {
		return rightChild;
	}
	
	/**
	 * Set the right child
	 * @return
	 */
	public void setRightChild(final DistributionRegion rightChild) {
		this.rightChild = rightChild;
		rightChild.setParent(this);
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
	}
	
	/**
	 * Get the split coordinate
	 * @return
	 */
	public long getSplit() {
		return split;
	}
	
	/**
	 * Calculate the bounding box for this node
	 * @return
	 */
	public BoundingBox getBoindingBox() {
		final BoundingBox boundingBox = BoundingBox.EMPTY_BOX;
		
		return boundingBox;
	}

	@Override
	public String toString() {
		return "DistributionNode [name=" + name + ", split=" + split
				+ ", leftChild=" + leftChild + ", rightChild=" + rightChild
				+ ", parent=" + parent + "]";
	}
	
}
