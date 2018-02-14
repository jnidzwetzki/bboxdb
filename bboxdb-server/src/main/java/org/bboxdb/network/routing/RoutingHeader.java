/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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
package org.bboxdb.network.routing;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	protected final List<RoutingHop> routingList = new ArrayList<>();

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
	public final static String SEPARATOR_CHAR_HOST = ";";
	
	/**
	 * The separator char for the region
	 */
	public final static String SEPARATOR_CHAR_REGION = ",";
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(RoutingHeader.class);

	public RoutingHeader(final boolean routedPackage) {
		this.routedPackage = routedPackage;
	}

	public RoutingHeader(final short hop, final List<RoutingHop> routingList) {
		this.routedPackage = true;
		this.hop = hop;
		this.routingList.addAll(routingList);
	}
	
	public RoutingHeader(final short hop, final String routingList) {
		this.routedPackage = true;
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
		
		if(routingList.isEmpty()) {
			return true;
		}
		
		return hop == routingList.size() - 1;
	}
	
	/**
	 * Get the next receiver of the package
	 * @return
	 */
	public RoutingHop getRoutingHop() {
		
		assert (hop < routingList.size()) : "Unable to return hop " + hop 
			+ ", total hops " + routingList.size();
		
		return routingList.get(hop);
	}

	/**
	 * Get the list with hops
	 * @return
	 */
	public List<RoutingHop> getRoutingList() {
		return routingList;
	}

	/**
	 * Set the list with hops
	 * @param routingList
	 */
	public void setRoutingList(final List<RoutingHop> routingList) {
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
		
		final String[] hostParts = stringRoutingList.split(SEPARATOR_CHAR_HOST);
		
		for(final String hostPart : hostParts) {
			try {
				final String[] regionParts = hostPart.split(SEPARATOR_CHAR_REGION);
				
				assert (regionParts.length > 1) : "Unable to split into regions: " + hostPart;
				
				final BBoxDBInstance distributedInstance = new BBoxDBInstance(regionParts[0]);
				final List<Long> distributionRegions = new ArrayList<>();
				
				for(int i = 1; i < regionParts.length; i++) {
					final long distributionRegion = Long.parseLong(regionParts[i]);
					distributionRegions.add(distributionRegion);
				}
				
				final RoutingHop routingHop = new RoutingHop(distributedInstance, distributionRegions);
				routingList.add(routingHop);
				
			} catch(IllegalArgumentException e) {
				logger.warn("Unable to parse as distributed instance: " + hostPart);
			} 
		}
	}
	
	/**
	 * Get the hop list as string list
	 * @return
	 */
	public String getRoutingListAsString() {
		
		final StringBuilder sb = new StringBuilder();

		for(final RoutingHop routingHop : routingList) {
			final String regionString = routingHop
					.getDistributionRegions()
					.stream()
					.map(i -> Long.toString(i))
					.collect(Collectors.joining(SEPARATOR_CHAR_REGION));

			if(sb.length() != 0) {
				sb.append(SEPARATOR_CHAR_HOST);
			}
			
			sb.append(routingHop.getDistributedInstance().getStringValue());
			sb.append(SEPARATOR_CHAR_REGION);
			sb.append(regionString);
		}

		return sb.toString();
	}

	/**
	 * Get the hop count
	 * @return
	 */
	public int getHopCount() {
		return routingList.size();
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
