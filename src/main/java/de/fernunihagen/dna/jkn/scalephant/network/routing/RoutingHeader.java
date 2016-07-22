package de.fernunihagen.dna.jkn.scalephant.network.routing;

import java.util.ArrayList;
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
	protected final List<DistributedInstance> routingList = new ArrayList<DistributedInstance>();

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
		this.routingList.addAll(routingList);
	}
	
	public RoutingHeader(final boolean routedPackage, final short hop, final String routingList) {
		this.routedPackage = routedPackage;
		this.hop = hop;
		setRoutingList(routingList);
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
		
		if(hop >= routingList.size()) {
			throw new IllegalArgumentException("Try to set hop : " + hop + " of an routing list of: " + routingList);
		}
		
		this.hop = hop;
	}
	
	/**
	 * Set the hop to the next position
	 * @return
	 */
	public boolean dispatchToNextHop() {
		if(hop < routingList.size() - 1) {
			hop++;
			return true;
		}
		
		return false;
	}
	
	/**
	 * Reached the package the last hop
	 * @return
	 */
	public boolean reachedFinalInstance() {
		return hop == routingList.size() - 1;
	}
	
	/**
	 * Get the next receiver of the package
	 * @return
	 */
	public DistributedInstance getHopInstance() {
		return routingList.get(hop);
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
		this.routingList.clear();
		this.routingList.addAll(routingList);
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + hop;
		result = prime * result + (routedPackage ? 1231 : 1237);
		result = prime * result + ((routingList == null) ? 0 : routingList.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RoutingHeader other = (RoutingHeader) obj;
		if (hop != other.hop)
			return false;
		if (routedPackage != other.routedPackage)
			return false;
		if (routingList == null) {
			if (other.routingList != null)
				return false;
		} else if (!routingList.equals(other.routingList))
			return false;
		return true;
	}
	
}
