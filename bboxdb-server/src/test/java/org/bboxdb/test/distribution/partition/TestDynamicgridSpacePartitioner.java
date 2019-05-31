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
package org.bboxdb.test.distribution.partition;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.DynamicgridSpacePartitioner;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionHelper;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.bboxdb.storage.util.EnvironmentHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDynamicgridSpacePartitioner {

	/**
	 * The name of the test region
	 */
	private static final String TEST_GROUP = "abc";

	/**
	 * The distribution group adapter
	 */
	private static DistributionGroupAdapter distributionGroupZookeeperAdapter;

	@BeforeClass
	public static void beforeClass() throws ZookeeperException {
		EnvironmentHelper.resetTestEnvironment();

		distributionGroupZookeeperAdapter
			= ZookeeperClientFactory.getZookeeperClient().getDistributionGroupAdapter();
	}

	@Before
	public void before() throws ZookeeperException, BBoxDBException {

		final String config = "[[0.0,5.0]:[0.0,6.0]];0.5";

		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
				.create(2)
				.withSpacePartitioner("org.bboxdb.distribution.partitioner.DynamicgridSpacePartitioner", config)
				.withPlacementStrategy("org.bboxdb.distribution.placement.DummyResourcePlacementStrategy", "")
				.build();

		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration);
	}

	@Test(timeout=60000)
	public void testRootElement() throws ZookeeperException, ZookeeperNotFoundException, BBoxDBException {
		final DynamicgridSpacePartitioner spacePartitioner = getSpacePartitioner();

		final DistributionRegion rootElement = spacePartitioner.getRootNode();
		Assert.assertEquals(rootElement.getState(), DistributionRegionState.SPLIT);

		final Hyperrectangle box = rootElement.getConveringBox();
		Assert.assertEquals(new Hyperrectangle(0.0, 5.0, 0.0, 6.0), box);
	}

	@Test(timeout=60000)
	public void createGridCells() throws ZookeeperException, ZookeeperNotFoundException, BBoxDBException {
		final DynamicgridSpacePartitioner spacePartitioner = getSpacePartitioner();
		final DistributionRegion rootElement = spacePartitioner.getRootNode();

		final long regions = rootElement
				.getThisAndChildRegions()
				.stream()
				.map(r -> r.getState())
				.filter(DistributionRegionHelper.PREDICATE_REGIONS_FOR_WRITE)
				.count();

		Assert.assertEquals(12, regions);
	}

	/**
	 * Get the merge candidates
	 * @throws ZookeeperNotFoundException
	 * @throws ZookeeperException
	 * @throws BBoxDBException
	 */
	@Test(timeout=60000)
	public void testGetMergeCandidates() throws ZookeeperException,
		ZookeeperNotFoundException, BBoxDBException {

		final DynamicgridSpacePartitioner spacePartitioner = getSpacePartitioner();
		final DistributionRegion rootElement = spacePartitioner.getRootNode();

		// Unsplitted parent region
		final DistributionRegion regionToMerge1 = rootElement.getChildNumber(0);
		final List<List<DistributionRegion>> candidates1 = spacePartitioner.getMergeCandidates(regionToMerge1);
		Assert.assertEquals(1, candidates1.size());

		// Unmergeable child region
		final DistributionRegion regionToMerge2 = rootElement.getChildNumber(0).getChildNumber(0);
		final List<List<DistributionRegion>> candidates2 = spacePartitioner.getMergeCandidates(regionToMerge2);
		Assert.assertEquals(0, candidates2.size());

		// Split root element
		final DistributionRegion regionToMerge3 = rootElement;
		final List<List<DistributionRegion>> candidates3 = spacePartitioner.getMergeCandidates(regionToMerge3);
		Assert.assertEquals(0, candidates3.size());
	}

	/**
	 * Is the region splitable
	 * @throws ZookeeperNotFoundException
	 * @throws ZookeeperException
	 * @throws BBoxDBException
	 */
	@Test(timeout=60000)
	public void testIsSplitable() throws ZookeeperException, ZookeeperNotFoundException, BBoxDBException {

		final DynamicgridSpacePartitioner spacePartitioner = getSpacePartitioner();
		final DistributionRegion rootElement = spacePartitioner.getRootNode();

		final DistributionRegion regionToSplit = rootElement.getChildNumber(0).getChildNumber(0);

		Assert.assertTrue(spacePartitioner.isSplitable(regionToSplit));
	}

	/**
	 * Split the given region
	 * @throws BBoxDBException
	 * @throws ZookeeperNotFoundException
	 * @throws ZookeeperException
	 * @throws InterruptedException
	 */
	@Test(timeout=60_000)
	public void testSplitAndMergeRegion() throws BBoxDBException, ZookeeperException, ZookeeperNotFoundException, InterruptedException {
		final DynamicgridSpacePartitioner spacePartitioner = getSpacePartitioner();
		final DistributionRegion rootElement = spacePartitioner.getRootNode();
		
		// Wait until is read complete
		spacePartitioner.waitUntilNodeStateIs(rootElement, DistributionRegionState.ACTIVE);

		final DistributionRegion regionToSplit = rootElement.getChildNumber(0).getChildNumber(0);

		final int oldChildren = regionToSplit.getParent().getThisAndChildRegions().size();

		final List<Hyperrectangle> samples = Arrays.asList(new Hyperrectangle(1d, 2d, 1d, 2d));
		final List<DistributionRegion> newRegions = spacePartitioner.splitRegion(regionToSplit, samples);
		Assert.assertEquals(2, newRegions.size());

		System.out.println("---> Test-Debug (1): " + regionToSplit.getParent().getThisAndChildRegions());
		final int newChilden1 = regionToSplit.getParent().getThisAndChildRegions().size();
		Assert.assertEquals(oldChildren + 2, newChilden1);

		spacePartitioner.splitComplete(regionToSplit, newRegions);

		for(final DistributionRegion region : newRegions) {
			spacePartitioner.waitUntilNodeStateIs(region, DistributionRegionState.ACTIVE);
		}

		System.out.println("---> Test-Debug (2): " + regionToSplit.getParent().getThisAndChildRegions());
		final int newChilden2 = regionToSplit.getParent().getThisAndChildRegions().size();
		Assert.assertEquals(oldChildren + 1, newChilden2);

		// Merge failed
		final DistributionRegion mergeRegion1 = spacePartitioner.getDestinationForMerge(newRegions);
		Assert.assertNotNull(mergeRegion1);
		spacePartitioner.mergeFailed(newRegions, mergeRegion1);

		// Merge successfully
		System.out.println("---> Test-Debug: Waiting for get");
		final DistributionRegion mergeRegion2 = spacePartitioner.getDestinationForMerge(newRegions);
		Assert.assertNotNull(mergeRegion2);
		spacePartitioner.mergeComplete(newRegions, mergeRegion2);
	}

	/**
	 * Get the space partitioner
	 * @return
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	private DynamicgridSpacePartitioner getSpacePartitioner() throws ZookeeperException, ZookeeperNotFoundException {
		final DynamicgridSpacePartitioner spacepartitionier = (DynamicgridSpacePartitioner)
				distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP,
						new HashSet<>(), new DistributionRegionIdMapper(TEST_GROUP));

		return spacepartitionier;
	}
}
