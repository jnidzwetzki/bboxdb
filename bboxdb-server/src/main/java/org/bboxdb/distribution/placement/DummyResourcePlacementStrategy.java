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

import com.google.common.annotations.VisibleForTesting;

public class DummyResourcePlacementStrategy extends ResourcePlacementStrategy {

	/**
	 * The system
	 */
	public final static BBoxDBInstance DUMMY_INSTANCE = new BBoxDBInstance("10.10.10.10:50050");
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DummyResourcePlacementStrategy.class);
	
	

	@Override
	@VisibleForTesting
	public BBoxDBInstance getInstancesForNewRessource(final List<BBoxDBInstance> systems, 
			final Collection<BBoxDBInstance> blacklist) throws ResourceAllocationException {
		
		if(systems == null) {
			return DUMMY_INSTANCE;
		}
		
		final ArrayList<BBoxDBInstance> availableSystems = new ArrayList<>(systems);
		availableSystems.removeAll(blacklist);
		
		if(! availableSystems.isEmpty()) {
			return availableSystems.get(0);
		}
		
		logger.debug("Executing new dummy allocation to {}", DUMMY_INSTANCE.getStringValue());
		
		return DUMMY_INSTANCE;
	}
}
