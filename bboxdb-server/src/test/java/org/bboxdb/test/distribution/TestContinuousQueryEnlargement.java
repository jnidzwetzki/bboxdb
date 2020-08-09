/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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
package org.bboxdb.test.distribution;

import org.bboxdb.distribution.zookeeper.ContinuousQueryRegisterer;
import org.bboxdb.distribution.zookeeper.QueryEnlagement;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.storage.util.EnvironmentHelper;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestContinuousQueryEnlargement {

	/**
	 * The name of the cluster for this test
	 */
	private static final String CLUSTER_NAME = "testcluster";
	
	/**
	 * The delta for the asserts
	 */
	private static final double DELTA = 0.00001;

	@BeforeClass
	public static void init() throws Exception {
		EnvironmentHelper.resetTestEnvironment();
	}
	
	@Test(timeout=60_000)
	public void testQueryRegister0() {
		final ContinuousQueryRegisterer registerer = new ContinuousQueryRegisterer(CLUSTER_NAME, "abc1");
		final QueryEnlagement enlargement = registerer.getMaxEnlagementFactorForTable();
		Assert.assertEquals(0, enlargement.getMaxAbsoluteEnlargement(), DELTA);
		Assert.assertEquals(0, enlargement.getMaxEnlargementFactor(), DELTA);
	}
	
	@Test(timeout=60_000)
	public void testQueryRegister1() throws ZookeeperException {
		final ContinuousQueryRegisterer registerer = new ContinuousQueryRegisterer(CLUSTER_NAME, "abc2");
		registerer.updateQueryOnTable(10, 20);
		
		final QueryEnlagement enlargement = registerer.getMaxEnlagementFactorForTable();
		Assert.assertEquals(10, enlargement.getMaxAbsoluteEnlargement(), DELTA);
		Assert.assertEquals(20, enlargement.getMaxEnlargementFactor(), DELTA);
	}
	
	
	@Test(timeout=60_000)
	public void testQueryRegister2() throws ZookeeperException {
		final ContinuousQueryRegisterer registerer = new ContinuousQueryRegisterer(CLUSTER_NAME, "abc3");
		registerer.updateQueryOnTable(30, 40);
		
		final QueryEnlagement enlargement = registerer.getMaxEnlagementFactorForTable();
		Assert.assertEquals(30, enlargement.getMaxAbsoluteEnlargement(), DELTA);
		Assert.assertEquals(40, enlargement.getMaxEnlargementFactor(), DELTA);
	}
	
	@Test(timeout=60_000)
	public void testQueryUnRegister0() throws ZookeeperException, InterruptedException {
		final ContinuousQueryRegisterer registerer = new ContinuousQueryRegisterer(CLUSTER_NAME, "abc4");
		registerer.updateQueryOnTable(30, 40);

		final QueryEnlagement enlargement = registerer.getMaxEnlagementFactorForTable();
		Assert.assertEquals(30, enlargement.getMaxAbsoluteEnlargement(), DELTA);
		Assert.assertEquals(40, enlargement.getMaxEnlargementFactor(), DELTA);
		
		registerer.unregisterOldQuery();
		Thread.sleep(3000);
				
		Assert.assertEquals(0, enlargement.getMaxAbsoluteEnlargement(), DELTA);
		Assert.assertEquals(0, enlargement.getMaxEnlargementFactor(), DELTA);
	}
	
}
