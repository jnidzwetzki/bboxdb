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
package org.bboxdb.distribution.placement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoundRobinResourcePlacementStrategy extends ResourcePlacementStrategy {

	/**
	 * The last assigned instance
	 */
	protected BBoxDBInstance lastInstance;
	
	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(RoundRobinResourcePlacementStrategy.class);
	
	public RoundRobinResourcePlacementStrategy() {
		lastInstance = null;
	}
	
	@Override
	public BBoxDBInstance getInstancesForNewRessource(final List<BBoxDBInstance> systems, final Collection<BBoxDBInstance> blacklist) throws ResourceAllocationException {
		
		if(systems.isEmpty()) {
			throw new ResourceAllocationException("Unable to choose a system, list of systems is empty");
		}
		
		final List<BBoxDBInstance> availableSystems = new ArrayList<BBoxDBInstance>(systems);
		availableSystems.removeAll(blacklist);

		removeAllNonReadySystems(availableSystems);
		
		if(availableSystems.isEmpty()) {
			throw new ResourceAllocationException("Unable to choose a system, all systems are blacklisted");
		}
		
		// First resource allocation
		if(lastInstance == null) {
			lastInstance = availableSystems.get(0);
			return availableSystems.get(0);
		}

		// Last allocated system was removed or is not ready, start on position 0
		if(! availableSystems.contains(lastInstance)) {
			lastInstance = availableSystems.get(0);
			return availableSystems.get(0);
		}
		
		// Find the position of the last assignment
		int lastPosition = 0; 
		for(; lastPosition < availableSystems.size(); lastPosition++) {
			if(availableSystems.get(lastPosition).equals(lastInstance)) {
				break;
			}
		}
		
		final int nextPosition = (lastPosition + 1) % availableSystems.size();
		lastInstance = availableSystems.get(nextPosition);
		return lastInstance;
	}
}
