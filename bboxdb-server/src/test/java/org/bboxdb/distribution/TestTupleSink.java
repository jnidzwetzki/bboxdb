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

import org.bboxdb.distribution.partitioner.regionsplit.tuplesink.AbstractTupleSink;
import org.bboxdb.distribution.partitioner.regionsplit.tuplesink.TupleRedistributor;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class TestTupleSink {

	/**
	 * The distribution region
	 */
	private static final String DREGION = "region";


	@BeforeClass
	public static void before() {
		DistributionGroupConfigurationCache.getInstance().clear();
		DistributionGroupConfigurationCache.getInstance().addNewConfiguration(DREGION, new DistributionGroupConfiguration(2));
	}
	
	/**
	 * Redistribute a tuple without any registered regions
	 * @throws Exception
	 */
	@Test(expected=StorageManagerException.class)
	public void testTupleWithoutRegions() throws Exception {
		final TupleRedistributor tupleRedistributor = createTupleRedistributor();
		
		final Tuple tuple1 = new Tuple("abc", BoundingBox.EMPTY_BOX, "".getBytes());
		
		tupleRedistributor.redistributeTuple(tuple1);
	}
	
	/**
	 * Register region two times
	 * @throws Exception
	 */
	@Test(expected=StorageManagerException.class)
	public void testRegisterRegionDuplicate() throws StorageManagerException {
		final DistributionGroupName distributionGroupName = new DistributionGroupName(DREGION);
		
		final DistributionRegion distributionRegion = new DistributionRegion(
				distributionGroupName, DistributionRegion.ROOT_NODE_ROOT_POINTER);

		final TupleRedistributor tupleRedistributor = createTupleRedistributor();
		
		tupleRedistributor.registerRegion(distributionRegion, new ArrayList<>());
		tupleRedistributor.registerRegion(distributionRegion, new ArrayList<>());
	}

	/**
	 * Get the tuple redistributor
	 * @return
	 */
	protected TupleRedistributor createTupleRedistributor() {
		final TupleStoreName tupleStoreName = new TupleStoreName("region_abc");

		return new TupleRedistributor(new TupleStoreManagerRegistry(), tupleStoreName);
	}
	
	/**
	 * Test the tuple redistribution
	 * @throws Exception 
	 */
	@Test
	public void testTupleRedistribution1() throws Exception {
		final DistributionGroupName distributionGroupName = new DistributionGroupName(DREGION);
		
		final DistributionRegion distributionRegion1 = new DistributionRegion(
				distributionGroupName, DistributionRegion.ROOT_NODE_ROOT_POINTER);
		distributionRegion1.setConveringBox(new BoundingBox(0.0, 1.0, 0.0, 1.0, 0.0, 1.0));

		final TupleRedistributor tupleRedistributor = createTupleRedistributor();
		
		final AbstractTupleSink tupleSink1 = Mockito.mock(AbstractTupleSink.class);
		tupleRedistributor.registerRegion(distributionRegion1, Arrays.asList(tupleSink1));
		
		final Tuple tuple1 = new Tuple("abc", new BoundingBox(0.0, 1.0, 0.0, 1.0, 0.0, 1.0), "".getBytes());
	
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
	@Test
	public void testTupleRedistribution2() throws Exception {
		final DistributionGroupName distributionGroupName = new DistributionGroupName(DREGION);
		
		final DistributionRegion distributionRegion1 = new DistributionRegion(
				distributionGroupName, DistributionRegion.ROOT_NODE_ROOT_POINTER);
		distributionRegion1.setConveringBox(new BoundingBox(0.0, 1.0, 0.0, 1.0, 0.0, 1.0));

		final DistributionRegion distributionRegion2 = new DistributionRegion(
				distributionGroupName, DistributionRegion.ROOT_NODE_ROOT_POINTER);
		distributionRegion2.setConveringBox(new BoundingBox(5.0, 6.0, 5.0, 6.0, 5.0, 6.0));

		final TupleRedistributor tupleRedistributor = createTupleRedistributor();
		
		final AbstractTupleSink tupleSink1 = Mockito.mock(AbstractTupleSink.class);
		tupleRedistributor.registerRegion(distributionRegion1, Arrays.asList(tupleSink1));
		
		final AbstractTupleSink tupleSink2 = Mockito.mock(AbstractTupleSink.class);
		tupleRedistributor.registerRegion(distributionRegion2, Arrays.asList(tupleSink2));
		
		final Tuple tuple1 = new Tuple("abc", new BoundingBox(0.0, 1.0, 0.0, 1.0, 0.0, 1.0), "".getBytes());
	
		tupleRedistributor.redistributeTuple(tuple1);
		(Mockito.verify(tupleSink1, Mockito.times(1))).sinkTuple(Mockito.any(Tuple.class));
		(Mockito.verify(tupleSink2, Mockito.never())).sinkTuple(Mockito.any(Tuple.class));

		final Tuple tuple2 = new Tuple("abc", new BoundingBox(5.0, 6.0, 5.0, 6.0, 5.0, 6.0), "".getBytes());
		tupleRedistributor.redistributeTuple(tuple2);
		(Mockito.verify(tupleSink1, Mockito.times(1))).sinkTuple(Mockito.any(Tuple.class));
		(Mockito.verify(tupleSink2, Mockito.times(1))).sinkTuple(Mockito.any(Tuple.class));

		final Tuple tuple3 = new Tuple("abc", new BoundingBox(0.0, 6.0, 0.0, 6.0, 0.0, 6.0), "".getBytes());
		tupleRedistributor.redistributeTuple(tuple3);
		(Mockito.verify(tupleSink1, Mockito.atLeast(2))).sinkTuple(Mockito.any(Tuple.class));
		(Mockito.verify(tupleSink2, Mockito.atLeast(2))).sinkTuple(Mockito.any(Tuple.class));

		System.out.println(tupleRedistributor.getStatistics());
	}

}
