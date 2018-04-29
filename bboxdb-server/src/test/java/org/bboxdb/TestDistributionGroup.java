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
package org.bboxdb;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TestDistributionGroup {

	/**
	 * Create an illegal distribution group
	 */
	@Test(expected=RuntimeException.class)
	public void createInvalidDistributionGroup1() {
		@SuppressWarnings("unused")
		final DistributionRegion distributionRegion = new DistributionRegion(
				"foo__", Hyperrectangle.createFullCoveringDimensionBoundingBox(1));
	}
	
	/**
	 * Create an illegal distribution group
	 */
	@Test(expected=RuntimeException.class)
	public void createInvalidDistributionGroup2() {
		@SuppressWarnings("unused")
		final DistributionRegion distributionRegion = new DistributionRegion(
				"12_foo_bar", Hyperrectangle.createFullCoveringDimensionBoundingBox(1));
	}
	
	/**
	 * Test a distribution region with only one level
	 */
	@Test(timeout=60000)
	public void testLeafNode() {
		final DistributionRegion distributionRegion = createDistributionGroup(2);
		Assert.assertTrue(distributionRegion.isLeafRegion());
		Assert.assertEquals(0, distributionRegion.getLevel());
		
		Assert.assertEquals(1, distributionRegion.getTotalLevel());
	}

	/**
	 * @param dimensions 
	 * @return
	 */ 
	private DistributionRegion createDistributionGroup(final int dimensions) {
		final String name = "foo";

		DistributionGroupConfigurationCache.getInstance().clear();
		final DistributionGroupConfiguration config = new DistributionGroupConfiguration();
		config.setDimensions(dimensions);
		DistributionGroupConfigurationCache.getInstance().addNewConfiguration(name, config);
		
		final DistributionRegion level0 = new DistributionRegion(name, 
				Hyperrectangle.createFullCoveringDimensionBoundingBox(dimensions));
		return level0;
	}
	
	/**
	 * Test the distribution group config
	 */
	@Test(timeout=60000)
	public void testDistributionGroupConfiguration1() {
		final DistributionGroupConfiguration config = DistributionGroupConfigurationBuilder
				.create(45)
				.withMaximumRegionSize(342)
				.withMinimumRegionSize(53454)
				.withPlacementStrategy("place1", "place2")
				.withSpacePartitioner("abc", "456")
				.withReplicationFactor((short) 99)
				.build();
		
		Assert.assertEquals(45, config.getDimensions());
		Assert.assertEquals(342, config.getMaximumRegionSize());
		Assert.assertEquals(53454, config.getMinimumRegionSize());
		Assert.assertEquals("place1", config.getPlacementStrategy());
		Assert.assertEquals("place2", config.getPlacementStrategyConfig());
		Assert.assertEquals("abc", config.getSpacePartitioner());
		Assert.assertEquals("456", config.getSpacePartitionerConfig());
		Assert.assertEquals((short) 99, config.getReplicationFactor());
	}
	
	/**
	 * Test the distribution group config
	 */
	@Test(timeout=60000)
	public void testDistributionGroupConfiguration2() {
		final DistributionGroupConfiguration config1 = DistributionGroupConfigurationBuilder
				.create(45)
				.build();
		
		final DistributionGroupConfiguration config2 = DistributionGroupConfigurationBuilder
				.create(45)
				.build();
		
		Assert.assertTrue(config1.toString().length() > 10);
		Assert.assertEquals(config1, config2);
		Assert.assertEquals(config1.hashCode(), config2.hashCode());
	}
	
}
