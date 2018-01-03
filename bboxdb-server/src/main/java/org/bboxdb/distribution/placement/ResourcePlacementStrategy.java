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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.BBoxDBInstanceState;

public abstract class ResourcePlacementStrategy {

	/**
	 * Get a set with distributed instances. These instances will be responsible for 
	 * a new resource. The systems from the blacklist are excluded.
	 * 
	 * @return
	 * @throws ResourceAllocationException 
	 */
	public abstract BBoxDBInstance getInstancesForNewRessource(final List<BBoxDBInstance> systems, 
			final Collection<BBoxDBInstance> blacklist) throws ResourceAllocationException;

	/**
	 * Get a set with distributed instances. These instances will be responsible for 
	 * a new resource.
	 * 
	 * @return
	 * @throws ResourceAllocationException 
	 */
	public BBoxDBInstance getInstancesForNewRessource(final List<BBoxDBInstance> systems) 
			throws ResourceAllocationException {
		
		final HashSet<BBoxDBInstance> emptyBlacklist = new HashSet<BBoxDBInstance>();
		return getInstancesForNewRessource(systems, emptyBlacklist);
	}
	
	/**
	 * Remove all non ready (state != READWRITE) systems 
	 * @param systems
	 */
	public static void removeAllNonReadySystems(final List<BBoxDBInstance> systems) {
		systems.removeIf(s -> s.getState() != BBoxDBInstanceState.READY);
	}

}
