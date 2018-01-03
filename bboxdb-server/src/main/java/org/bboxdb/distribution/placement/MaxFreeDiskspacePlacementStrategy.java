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

public class MaxFreeDiskspacePlacementStrategy extends ResourcePlacementStrategy {

	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(MaxFreeDiskspacePlacementStrategy.class);
	
	public MaxFreeDiskspacePlacementStrategy() {
	}
	
	@Override
	public BBoxDBInstance getInstancesForNewRessource(final List<BBoxDBInstance> systems, 
			final Collection<BBoxDBInstance> blacklist) throws ResourceAllocationException {
		
		if(systems.isEmpty()) {
			throw new ResourceAllocationException("Unable to choose a system, list of systems is empty");
		}
		
		final List<BBoxDBInstance> availableInstances = new ArrayList<>(systems);
		availableInstances.removeAll(blacklist);
		removeAllNonReadySystems(availableInstances);
		
		if(availableInstances.isEmpty()) {
			throw new ResourceAllocationException("Unable to choose a system, all systems are blacklisted. Blacklisted: " + blacklist + " / All: " + systems);
		}
		
		// Get system with max free space
		return availableInstances
				.stream()
				.reduce((a, b) -> (a.getFreeSpace() > b.getFreeSpace() ? a : b))
				.get();
	}
}
