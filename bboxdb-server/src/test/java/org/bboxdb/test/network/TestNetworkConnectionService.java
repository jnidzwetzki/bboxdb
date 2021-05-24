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
package org.bboxdb.test.network;

import java.util.Set;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.network.server.connection.NetworkConnectionService;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestNetworkConnectionService {
	
	@Test(timeout = 60_000)
	public void testSystemsToContact1() {
		final TupleStoreManagerRegistry tupleStoreManagerRegistry = Mockito.mock(TupleStoreManagerRegistry.class);
		final NetworkConnectionService networkConnectionService = new NetworkConnectionService(tupleStoreManagerRegistry);
		
		final DistributionRegion rootRegion = new DistributionRegion("abc", Hyperrectangle.FULL_SPACE);
		final BBoxDBInstance parentInstance = new BBoxDBInstance("localhost:34343");
		rootRegion.addSystem(parentInstance);
		
		final Set<BBoxDBInstance> regions = networkConnectionService.getSystemsToConnectForStateSync(rootRegion);
		
		Assert.assertTrue(regions.isEmpty());
	}
	
	@Test(timeout = 60_000)
	public void testSystemsToContact2() {
		final TupleStoreManagerRegistry tupleStoreManagerRegistry = Mockito.mock(TupleStoreManagerRegistry.class);
		final NetworkConnectionService networkConnectionService = new NetworkConnectionService(tupleStoreManagerRegistry);
		
		final DistributionRegion rootRegion = new DistributionRegion("abc", Hyperrectangle.FULL_SPACE);
		final BBoxDBInstance parentInstance = new BBoxDBInstance("localhost:34343");
		rootRegion.addSystem(parentInstance);
		
		final DistributionRegion childRegion = new DistributionRegion("abc", rootRegion, Hyperrectangle.FULL_SPACE, 4);
		rootRegion.addChildren(0, childRegion);
		
		final Set<BBoxDBInstance> regions = networkConnectionService.getSystemsToConnectForStateSync(childRegion);
		
		Assert.assertEquals(1, regions.size());
		Assert.assertTrue(regions.contains(parentInstance));
	}
	
	@Test(timeout = 60_000)
	public void testSystemsToContact3() {
		final TupleStoreManagerRegistry tupleStoreManagerRegistry = Mockito.mock(TupleStoreManagerRegistry.class);
		final NetworkConnectionService networkConnectionService = new NetworkConnectionService(tupleStoreManagerRegistry);
		
		final DistributionRegion rootRegion = new DistributionRegion("abc", Hyperrectangle.FULL_SPACE);
		final BBoxDBInstance parentInstance = new BBoxDBInstance("localhost:34343");
		rootRegion.addSystem(parentInstance);
		
		final DistributionRegion childRegion = new DistributionRegion("abc", rootRegion, Hyperrectangle.FULL_SPACE, 4);
		rootRegion.addChildren(0, childRegion);
		
		final DistributionRegion childRegion2 = new DistributionRegion("abc", childRegion, Hyperrectangle.FULL_SPACE, 6);
		childRegion.addChildren(0, childRegion2);
		final BBoxDBInstance childInstance = new BBoxDBInstance("localhost:1111");
		childRegion2.addSystem(childInstance);
		
		final Set<BBoxDBInstance> regions = networkConnectionService.getSystemsToConnectForStateSync(childRegion);
		
		Assert.assertEquals(1, regions.size());
		Assert.assertTrue(regions.contains(childInstance));
	}
	
}
