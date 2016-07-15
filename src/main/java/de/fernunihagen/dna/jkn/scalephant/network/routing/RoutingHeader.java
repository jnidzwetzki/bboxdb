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
	protected List<DistributedInstance> routingList;

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
	
	public RoutingHeader(final boolean routedPackage) {
		this.routedPackage = routedPackage;
	}

	public RoutingHeader(final boolean routedPackage, final short hop, final List<DistributedInstance> routingList) {
		this.routedPackage = routedPackage;
		this.hop = hop;
		this.routingList = routingList;
	}

	@Override
	public String toString() {
		return "RoutingHeader [routedPackage=" + routedPackage + ", hop=" + hop + ", routingList=" + routingList + "]";
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
	public List<DistributedInstance> getRoutingList() {
		return routingList;
	}

	/**
	 * Set the list with hops
	 * @param routingList
	 */
	public void setRoutingList(final List<DistributedInstance> routingList) {
		this.routingList = routingList;
	}
	
	/**
	 * Set the list with hops (as string list)
	 * @param stringRoutingList
	 */
	public void setRoutingList(final String stringRoutingList) {
		
		routingList.clear();
		
		// Routing list is empty
		if(stringRoutingList == null || stringRoutingList.length() == 0) {
			return;
		}
		
		final String[] parts = stringRoutingList.split(SEPARATOR_CHAR);
		
		
		for(final String hop : parts) {
			try {
				final DistributedInstance distributedInstance = new DistributedInstance(hop);
				routingList.add(distributedInstance);
			} catch(IllegalArgumentException e) {
				logger.warn("Unable to parse as distributed instance: " + hop);
			}
		}
	}
	
	/**
	 * Get the hop list as string list
	 * @return
	 */
	public String getRoutingListAsString() {
		final StringBuilder sb = new StringBuilder();
		for(final DistributedInstance distributedInstance : routingList) {
			if(sb.length() != 0) {
				sb.append(SEPARATOR_CHAR);
			}
			sb.append(distributedInstance.getStringValue());
		}
		return sb.toString();
	}
	
}
