/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.distribution.allocator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.bboxdb.distribution.membership.BBoxDBInstance;

public class RandomResourceAllocator extends AbstractResourceAllocator {

	@Override
	public BBoxDBInstance getInstancesForNewRessource(final List<BBoxDBInstance> systems, 
			final Collection<BBoxDBInstance> blacklist) throws ResourceAllocationException {
		
		if(systems.isEmpty()) {
			throw new ResourceAllocationException("Unable to choose a system, list of systems is empty");
		}
		
		final List<BBoxDBInstance> availableSystems = new ArrayList<>(systems);
		availableSystems.removeAll(blacklist);
		removeAllNonReadySystems(availableSystems);
		
		if(availableSystems.isEmpty()) {
			throw new ResourceAllocationException("Unable to choose a system, all systems are blacklisted. Blacklisted: " + blacklist + " / All: " + systems);
		}
		
		final int element = ThreadLocalRandom.current().nextInt(availableSystems.size());
		return availableSystems.get(element);
	}
}
