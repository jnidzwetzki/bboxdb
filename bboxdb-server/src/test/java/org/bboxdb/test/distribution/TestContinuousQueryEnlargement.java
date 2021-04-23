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
package org.bboxdb.test.distribution;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.zookeeper.ContinuousQueryEnlargementRegisterer;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.QueryEnlargement;
import org.bboxdb.distribution.zookeeper.TupleStoreAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.query.ContinuousQueryPlan;
import org.bboxdb.network.query.QueryPlanBuilder;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.util.EnvironmentHelper;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestContinuousQueryEnlargement {

	/**
	 * The name of the cluster for this test
	 */
	private static final String DISTRIBUTION_GROUP = "testgroupenlagement";
	private static final TupleStoreName TUPLE_STORE_1 = new TupleStoreName(DISTRIBUTION_GROUP, "abc1", 0);
	private static final TupleStoreName TUPLE_STORE_2 = new TupleStoreName(DISTRIBUTION_GROUP, "abc2", 0);
	private static final TupleStoreName TUPLE_STORE_3 = new TupleStoreName(DISTRIBUTION_GROUP, "abc3", 0);
	private static final TupleStoreName TUPLE_STORE_4 = new TupleStoreName(DISTRIBUTION_GROUP, "abc4", 0);
	private static final TupleStoreName TUPLE_STORE_5 = new TupleStoreName(DISTRIBUTION_GROUP, "abc5", 0);

	/**
	 * The delta for the asserts
	 */
	private static final double DELTA = 0.00001;

	@BeforeClass
	public static void init() throws Exception {
		EnvironmentHelper.resetTestEnvironment();
		
		final DistributionGroupAdapter distributionGroupAdapter 
			= ZookeeperClientFactory.getZookeeperClient().getDistributionGroupAdapter();
	
		final DistributionGroupConfiguration configuration = new DistributionGroupConfiguration(2);
		configuration.setReplicationFactor((short) 0);
		
		distributionGroupAdapter.createDistributionGroup(DISTRIBUTION_GROUP, 
				configuration);
		
		
		final TupleStoreAdapter tupleStoreAdapter = ZookeeperClientFactory
				.getZookeeperClient().getTupleStoreAdapter();
		
		tupleStoreAdapter.writeTuplestoreConfiguration(TUPLE_STORE_1, new TupleStoreConfiguration());
		tupleStoreAdapter.writeTuplestoreConfiguration(TUPLE_STORE_2, new TupleStoreConfiguration());
		tupleStoreAdapter.writeTuplestoreConfiguration(TUPLE_STORE_3, new TupleStoreConfiguration());
		tupleStoreAdapter.writeTuplestoreConfiguration(TUPLE_STORE_4, new TupleStoreConfiguration());
		tupleStoreAdapter.writeTuplestoreConfiguration(TUPLE_STORE_5, new TupleStoreConfiguration());

	}
	
	@Test(timeout=60_000)
	public void testQueryRegister0() throws StorageManagerException, ZookeeperException {
		final ContinuousQueryEnlargementRegisterer registerer = ContinuousQueryEnlargementRegisterer.getInstanceFor(TUPLE_STORE_1);
		final QueryEnlargement enlargement = registerer.getEnlagementForTable();
		Assert.assertEquals(0, enlargement.getMaxAbsoluteEnlargement(), DELTA);
		Assert.assertEquals(1, enlargement.getMaxEnlargementFactor(), DELTA);
		Assert.assertEquals(0, enlargement.getMaxEnlargementLat(), DELTA);
		Assert.assertEquals(0, enlargement.getMaxEnlargementLon(), DELTA);
	}
	
	@Test(timeout=60_000)
	public void testQueryRegister1() throws ZookeeperException, StorageManagerException, BBoxDBException {
		final ContinuousQueryEnlargementRegisterer registerer = ContinuousQueryEnlargementRegisterer.getInstanceFor(TUPLE_STORE_2);
		
		final ContinuousQueryPlan queryPlan = QueryPlanBuilder.createQueryOnTable(TUPLE_STORE_2.getFullnameWithoutPrefix())
			.compareWithStaticSpace(new Hyperrectangle(1.0, 2.0))
			.enlargeStreamTupleBoundBoxByValue(10)
			.enlargeStreamTupleBoundBoxByFactor(20)
			.enlargeStreamTupleBoundBoxByWGS84Meter(30, 40)
			.build();
			
		registerer.registerQueryEnlargement(queryPlan);
		
		final QueryEnlargement enlargement = registerer.getEnlagementForTable();
		Assert.assertEquals(10, enlargement.getMaxAbsoluteEnlargement(), DELTA);
		Assert.assertEquals(20, enlargement.getMaxEnlargementFactor(), DELTA);
		Assert.assertEquals(30, enlargement.getMaxEnlargementLat(), DELTA);
		Assert.assertEquals(40, enlargement.getMaxEnlargementLon(), DELTA);
	}
	
	
	@Test(timeout=60_000)
	public void testQueryRegister2() throws ZookeeperException, StorageManagerException, BBoxDBException, InterruptedException {
		final ContinuousQueryEnlargementRegisterer registerer = ContinuousQueryEnlargementRegisterer.getInstanceFor(TUPLE_STORE_3);
		
		final ContinuousQueryPlan queryPlan = QueryPlanBuilder.createQueryOnTable(TUPLE_STORE_3.getFullnameWithoutPrefix())
				.compareWithStaticSpace(new Hyperrectangle(1.0, 2.0))
				.enlargeStreamTupleBoundBoxByValue(30)
				.enlargeStreamTupleBoundBoxByFactor(40)
				.enlargeStreamTupleBoundBoxByWGS84Meter(50, 60)
				.build();
				
		registerer.registerQueryEnlargement(queryPlan);
		Thread.sleep(3000);
		
		final QueryEnlargement enlargement = registerer.getEnlagementForTable();
		Assert.assertEquals(30, enlargement.getMaxAbsoluteEnlargement(), DELTA);
		Assert.assertEquals(40, enlargement.getMaxEnlargementFactor(), DELTA);
		Assert.assertEquals(50, enlargement.getMaxEnlargementLat(), DELTA);
		Assert.assertEquals(60, enlargement.getMaxEnlargementLon(), DELTA);
	}
	
	@Test(timeout=60_000)
	public void testQueryRegister3() throws ZookeeperException, StorageManagerException, BBoxDBException {
		final ContinuousQueryEnlargementRegisterer registerer = ContinuousQueryEnlargementRegisterer.getInstanceFor(TUPLE_STORE_3);
		
		final ContinuousQueryPlan queryPlan1 = QueryPlanBuilder.createQueryOnTable(TUPLE_STORE_5.getFullnameWithoutPrefix())
				.compareWithStaticSpace(new Hyperrectangle(1.0, 2.0))
				.enlargeStreamTupleBoundBoxByValue(30)
				.enlargeStreamTupleBoundBoxByFactor(40)
				.enlargeStreamTupleBoundBoxByWGS84Meter(50, 60)
				.build();
		
		final ContinuousQueryPlan queryPlan2 = QueryPlanBuilder.createQueryOnTable(TUPLE_STORE_5.getFullnameWithoutPrefix())
				.compareWithStaticSpace(new Hyperrectangle(1.0, 2.0))
				.enlargeStreamTupleBoundBoxByValue(20)
				.enlargeStreamTupleBoundBoxByFactor(60)
				.enlargeStreamTupleBoundBoxByWGS84Meter(20, 10)
				.build();
				
		registerer.registerQueryEnlargement(queryPlan1);
		registerer.registerQueryEnlargement(queryPlan2);
		
		final QueryEnlargement enlargement = registerer.getEnlagementForTable();
		Assert.assertEquals(30, enlargement.getMaxAbsoluteEnlargement(), DELTA);
		Assert.assertEquals(60, enlargement.getMaxEnlargementFactor(), DELTA);
		Assert.assertEquals(50, enlargement.getMaxEnlargementLat(), DELTA);
		Assert.assertEquals(60, enlargement.getMaxEnlargementLon(), DELTA);
	}
	
	@Test(timeout=60_000)
	public void testQueryUnRegister0() throws ZookeeperException, InterruptedException, StorageManagerException, BBoxDBException {
		final ContinuousQueryEnlargementRegisterer registerer = ContinuousQueryEnlargementRegisterer.getInstanceFor(TUPLE_STORE_4);
		
		final ContinuousQueryPlan queryPlan = QueryPlanBuilder.createQueryOnTable(TUPLE_STORE_4.getFullnameWithoutPrefix())
				.compareWithStaticSpace(new Hyperrectangle(1.0, 2.0))
				.enlargeStreamTupleBoundBoxByValue(30)
				.enlargeStreamTupleBoundBoxByFactor(40)
				.enlargeStreamTupleBoundBoxByWGS84Meter(50, 60)
				.build();
				
		registerer.registerQueryEnlargement(queryPlan);

		final QueryEnlargement enlargement = registerer.getEnlagementForTable();
		Assert.assertEquals(30, enlargement.getMaxAbsoluteEnlargement(), DELTA);
		Assert.assertEquals(40, enlargement.getMaxEnlargementFactor(), DELTA);
		Assert.assertEquals(50, enlargement.getMaxEnlargementLat(), DELTA);
		Assert.assertEquals(60, enlargement.getMaxEnlargementLon(), DELTA);
		
		registerer.unregisterAllQueries();
		Thread.sleep(3000);
				
		Assert.assertEquals(0, enlargement.getMaxAbsoluteEnlargement(), DELTA);
		Assert.assertEquals(1, enlargement.getMaxEnlargementFactor(), DELTA);
		Assert.assertEquals(0, enlargement.getMaxEnlargementLat(), DELTA);
		Assert.assertEquals(0, enlargement.getMaxEnlargementLon(), DELTA);
	}
	
	
}
