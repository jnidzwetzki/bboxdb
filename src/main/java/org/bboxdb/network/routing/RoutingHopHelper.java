/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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

import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.misc.Const;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;

public class RoutingHopHelper {

	/**
	 * Get a non empty routing list
	 * @param tuple
	 * @param distributionRegion
	 * @return
	 * @throws InterruptedException
	 */
	public static List<RoutingHop> getRoutingHopsForWrite(final Tuple tuple,
			final DistributionRegion distributionRegion) throws InterruptedException {
		
		final List<RoutingHop> hops = new ArrayList<>();
		
		final BoundingBox boundingBox = tuple.getBoundingBox();

		for(int execution = 0; execution < Const.OPERATION_RETRY; execution++) {
			hops.addAll(distributionRegion.getRoutingHopsForWrite(boundingBox));
			
			if(! hops.isEmpty()) {
				break;
			}
			
			Thread.sleep(20 * execution);	
		}
		
		return hops;
	}

}
