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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

import org.bboxdb.commons.Retryer;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.misc.Const;
import org.bboxdb.storage.entity.BoundingBox;

public class RoutingHopHelper {

	/**
	 * Get a non empty routing list
	 * @param tuple
	 * @param distributionRegion
	 * @return
	 * @throws InterruptedException
	 */
	public static List<RoutingHop> getRoutingHopsForWrite(final BoundingBox boundingBox,
			final DistributionRegion distributionRegion) throws InterruptedException {
		
		final Callable<List<RoutingHop>> getHops = new Callable<List<RoutingHop>>() {

			@Override
			public List<RoutingHop> call() throws Exception {
				final Collection<RoutingHop> hopCollection 
					= distributionRegion.getRoutingHopsForWrite(boundingBox);
				
				if(hopCollection.isEmpty()) {
					throw new Exception("Hop collection is empty");
				}

				return new ArrayList<RoutingHop>(hopCollection);
			}
		};
		
		final Retryer<List<RoutingHop>> retryer = new Retryer<>(Const.OPERATION_RETRY, 20, getHops);
		retryer.execute();
		return retryer.getResult();
	}

}
