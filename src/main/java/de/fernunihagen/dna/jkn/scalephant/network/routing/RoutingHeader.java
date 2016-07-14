package de.fernunihagen.dna.jkn.scalephant.network.routing;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	public final static byte DIRECT_PACKAGE = 0x00;
	
	/**
	 * The flag for routed packages
	 */
	public final static byte ROUTED_PACKAGE = 0x01;

	/**
	 * The separator char for the host / hop list
	 */
	public final static String SEPARATOR_CHAR = ",";
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(RoutingHeader.class);

	
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
	public void setHop(final short hop) {
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
	
	/**
	 * Set the list with hops (as string list)
	 * @param hops
	 */
	public void setHops(final String hopList) {
		final String[] parts = hopList.split(SEPARATOR_CHAR);
		hops.clear();
		
		for(final String hop : parts) {
			try {
				final DistributedInstance distributedInstance = new DistributedInstance(hop);
				hops.add(distributedInstance);
			} catch(IllegalArgumentException e) {
				logger.warn("Unable to parse as distributed instance: " + hop);
			}
		}
	}
	
	/**
	 * Get the hop list as string list
	 * @return
	 */
	public String getHopList() {
		final StringBuilder sb = new StringBuilder();
		for(final DistributedInstance distributedInstance : hops) {
			if(sb.length() != 0) {
				sb.append(SEPARATOR_CHAR);
			}
			sb.append(distributedInstance.getStringValue());
		}
		return sb.toString();
	}
	
}
