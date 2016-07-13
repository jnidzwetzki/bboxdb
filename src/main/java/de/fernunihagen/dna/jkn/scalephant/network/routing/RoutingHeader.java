package de.fernunihagen.dna.jkn.scalephant.network.routing;

import java.util.List;

import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;

public class RoutingHeader {
	
	/**
	 * Is this a routed or a direct package?
	 */
	protected boolean routedPackage;
	
	/**
	 * The hop of the package
	 */
	protected short hop;
	
	/**
	 * The hops for this package
	 */
	protected List<DistributedInstance> hops;

	/**
	 * The flag for direct packages
	 */
	public static byte DIRECT_PACKAGE = 0x00;
	
	/**
	 * The flag for routed packages
	 */
	public static byte ROUTED_PACKAGE = 0x01;
	
	
	public RoutingHeader() {
	
	}
	
	@Override
	public String toString() {
		return "RoutingHeader [routedPackage=" + routedPackage + ", hop=" + hop + ", hops=" + hops + "]";
	}

	/**
	 * Is this a routed or a direct package
	 * @return
	 */
	public boolean isRoutedPackage() {
		return routedPackage;
	}

	/**
	 * Set routed flag
	 * @param routedPackage
	 */
	public void setRoutedPackage(final boolean routedPackage) {
		this.routedPackage = routedPackage;
	}

	/**
	 * Get the current hop
	 * @return
	 */
	public short getHop() {
		return hop;
	}

	/**
	 * Set the current hop
	 * @param hop
	 */
	public void setHop(short hop) {
		this.hop = hop;
	}

	/**
	 * Get the list with hops
	 * @return
	 */
	public List<DistributedInstance> getHops() {
		return hops;
	}

	/**
	 * Set the list with hops
	 * @param hops
	 */
	public void setHops(final List<DistributedInstance> hops) {
		this.hops = hops;
	}
	
}
