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
package org.bboxdb.distribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.partitioner.regionsplit.tuplesink.AbstractTupleSink;
import org.bboxdb.distribution.partitioner.regionsplit.tuplesink.LocalTupleSink;
import org.bboxdb.distribution.partitioner.regionsplit.tuplesink.NetworkTupleSink;
import org.bboxdb.distribution.partitioner.regionsplit.tuplesink.TupleRedistributor;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.TupleStoreAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class TestTupleSink {

	/**
	 * The tablename
	 */
	private static final TupleStoreName TABLENAME = new TupleStoreName("region_mytable");

	/**
	 * The distribution region
	 */
	private static final String TEST_GROUP = "region";
	
	/**
	 * The distribution group adapter
	 */
	private static DistributionGroupAdapter distributionGroupZookeeperAdapter;
	
	/**
	 * The tuple store adapter
	 */
	private static TupleStoreAdapter tupleStoreAdapter;

	
	@BeforeClass
	public static void before() throws ZookeeperException, BBoxDBException {
		distributionGroupZookeeperAdapter = ZookeeperClientFactory.getZookeeperClient().getDistributionGroupAdapter();
		tupleStoreAdapter = ZookeeperClientFactory.getZookeeperClient().getTupleStoreAdapter();
		
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
				.create(2)
				.withPlacementStrategy("org.bboxdb.distribution.placement.DummyResourcePlacementStrategy", "")
				.build();
		
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration); 
	}
	
	/**
	 * Redistribute a tuple without any registered regions
	 * @throws Exception
	 */
	@Test(expected=StorageManagerException.class)
	public void testTupleWithoutRegions() throws Exception {
		final TupleRedistributor tupleRedistributor = createTupleRedistributor();
		
		final Tuple tuple1 = new Tuple("abc", Hyperrectangle.FULL_SPACE, "".getBytes());
		
		tupleRedistributor.redistributeTuple(tuple1);
	}
	
	/**
	 * Register region two times
	 * @throws BBoxDBException 
	 * @throws InterruptedException 
	 * @throws Exception
	 */
	@Test(expected=StorageManagerException.class)
	public void testRegisterRegionDuplicate() throws StorageManagerException, InterruptedException, BBoxDBException {
		
		final DistributionRegion distributionRegion = new DistributionRegion(
				TEST_GROUP, Hyperrectangle.createFullCoveringDimensionBoundingBox(3));

		final TupleRedistributor tupleRedistributor = createTupleRedistributor();
		
		tupleRedistributor.registerRegion(distributionRegion, new ArrayList<>());
		tupleRedistributor.registerRegion(distributionRegion, new ArrayList<>());
	}

	/**
	 * Get the tuple redistributor
	 * @return
	 * @throws BBoxDBException 
	 * @throws InterruptedException 
	 */
	protected TupleRedistributor createTupleRedistributor() throws InterruptedException, BBoxDBException {
		final TupleStoreName tupleStoreName = new TupleStoreName(TABLENAME.getFullname());
		final TupleStoreManagerRegistry tupleStoreManagerRegistry = new TupleStoreManagerRegistry();
		tupleStoreManagerRegistry.init();
		return new TupleRedistributor(tupleStoreManagerRegistry, tupleStoreName);
	}
	
	/**
	 * Test the tuple redistribution
	 * @throws Exception 
	 */
	@Test(timeout=60000)
	public void testTupleRedistribution1() throws Exception {
		
		final DistributionRegion distributionRegion1 = new DistributionRegion(
				TEST_GROUP, DistributionRegion.ROOT_NODE_ROOT_POINTER, 
				new Hyperrectangle(0.0, 1.0, 0.0, 1.0, 0.0, 1.0), 1);

		final TupleRedistributor tupleRedistributor = createTupleRedistributor();
		
		final AbstractTupleSink tupleSink1 = Mockito.mock(AbstractTupleSink.class);
		tupleRedistributor.registerRegion(distributionRegion1, Arrays.asList(tupleSink1));
		
		final Tuple tuple1 = new Tuple("abc", new Hyperrectangle(0.0, 1.0, 0.0, 1.0, 0.0, 1.0), "".getBytes());
	
		tupleRedistributor.redistributeTuple(tuple1);
		(Mockito.verify(tupleSink1, Mockito.times(1))).sinkTuple(Mockito.any(Tuple.class));
		
		tupleRedistributor.redistributeTuple(tuple1);
		(Mockito.verify(tupleSink1, Mockito.times(2))).sinkTuple(Mockito.any(Tuple.class));

		System.out.println(tupleRedistributor.getStatistics());
	}
	

	/**
	 * Test the tuple redistribution
	 * @throws Exception 
	 */
	@Test(timeout=60000)
	public void testTupleRedistribution2() throws Exception {		
		final DistributionRegion distributionRegion1 = new DistributionRegion(
				TEST_GROUP, DistributionRegion.ROOT_NODE_ROOT_POINTER, 
				new Hyperrectangle(0.0, 1.0, 0.0, 1.0, 0.0, 1.0), 1);

		final DistributionRegion distributionRegion2 = new DistributionRegion(
				TEST_GROUP, DistributionRegion.ROOT_NODE_ROOT_POINTER, 
				new Hyperrectangle(5.0, 6.0, 5.0, 6.0, 5.0, 6.0), 1);

		final TupleRedistributor tupleRedistributor = createTupleRedistributor();
		
		final AbstractTupleSink tupleSink1 = Mockito.mock(AbstractTupleSink.class);
		tupleRedistributor.registerRegion(distributionRegion1, Arrays.asList(tupleSink1));
		
		final AbstractTupleSink tupleSink2 = Mockito.mock(AbstractTupleSink.class);
		tupleRedistributor.registerRegion(distributionRegion2, Arrays.asList(tupleSink2));
		
		final Tuple tuple1 = new Tuple("abc", new Hyperrectangle(0.0, 1.0, 0.0, 1.0, 0.0, 1.0), "".getBytes());
	
		tupleRedistributor.redistributeTuple(tuple1);
		(Mockito.verify(tupleSink1, Mockito.times(1))).sinkTuple(Mockito.any(Tuple.class));
		(Mockito.verify(tupleSink2, Mockito.never())).sinkTuple(Mockito.any(Tuple.class));

		final Tuple tuple2 = new Tuple("abc", new Hyperrectangle(5.0, 6.0, 5.0, 6.0, 5.0, 6.0), "".getBytes());
		tupleRedistributor.redistributeTuple(tuple2);
		(Mockito.verify(tupleSink1, Mockito.times(1))).sinkTuple(Mockito.any(Tuple.class));
		(Mockito.verify(tupleSink2, Mockito.times(1))).sinkTuple(Mockito.any(Tuple.class));

		final Tuple tuple3 = new Tuple("abc", new Hyperrectangle(0.0, 6.0, 0.0, 6.0, 0.0, 6.0), "".getBytes());
		tupleRedistributor.redistributeTuple(tuple3);
		(Mockito.verify(tupleSink1, Mockito.atLeast(2))).sinkTuple(Mockito.any(Tuple.class));
		(Mockito.verify(tupleSink2, Mockito.atLeast(2))).sinkTuple(Mockito.any(Tuple.class));
		
		final Tuple tuple4 = new DeletedTuple("abc");
		tupleRedistributor.redistributeTuple(tuple4);
		(Mockito.verify(tupleSink1, Mockito.atLeast(3))).sinkTuple(Mockito.any(Tuple.class));
		(Mockito.verify(tupleSink2, Mockito.atLeast(3))).sinkTuple(Mockito.any(Tuple.class));


		System.out.println(tupleRedistributor.getStatistics());
	}
	
	/**
	 * Test the tuple sinks
	 * @throws StorageManagerException 
	 * @throws BBoxDBException 
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 */
	@Test(timeout=60000)
	public void testTupleSink() throws StorageManagerException, BBoxDBException, ZookeeperException, InterruptedException {
		final DistributionRegion distributionRegion = SpacePartitionerCache
				.getInstance()
				.getSpacePartitionerForGroupName(TEST_GROUP)
				.getRootNode();
		
		tupleStoreAdapter.deleteTable(TABLENAME);
		tupleStoreAdapter.writeTuplestoreConfiguration(TABLENAME, new TupleStoreConfiguration());
		
		final List<BBoxDBInstance> systems = Arrays.asList(
				new BBoxDBInstance("10.0.0.1:10000"), 
				new BBoxDBInstance("10.0.0.2:10000"), 
				ZookeeperClientFactory.getLocalInstanceName());
		
		distributionRegion.setSystems(systems);
		
		final TupleRedistributor tupleRedistributor = createTupleRedistributor();
		tupleRedistributor.registerRegion(distributionRegion);
		
		final Map<DistributionRegion, List<AbstractTupleSink>> map = tupleRedistributor.getRegionMap();
		Assert.assertEquals(1, map.size());
		
		final long networkSinks = map.values()
				.stream()
				.flatMap(e -> e.stream())
				.filter(s -> s instanceof NetworkTupleSink)
				.count();
		
		Assert.assertEquals(2, networkSinks);
		
		final long localSinks = map.values()
				.stream()
				.flatMap(e -> e.stream())
				.filter(s -> s instanceof LocalTupleSink)
				.count();
		
		Assert.assertEquals(1, localSinks);
	}

}
