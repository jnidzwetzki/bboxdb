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

import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class TestDistributionGroup {

	protected static ZookeeperClient zookeeperClient;

	/**
	 * Create an illegal distribution group
	 */
	@Test(expected=RuntimeException.class)
	public void createInvalidDistributionGroup1() {
		@SuppressWarnings("unused")
		final DistributionRegion distributionRegion = new DistributionRegion(new DistributionGroupName("foo__"), 1);
	}
	
	/**
	 * Create an illegal distribution group
	 */
	@Test(expected=RuntimeException.class)
	public void createInvalidDistributionGroup2() {
		@SuppressWarnings("unused")
		final DistributionRegion distributionRegion = new DistributionRegion(new DistributionGroupName("12_foo_bar"), 1);
	}
	
	/**
	 * Test a distribution region with only one level
	 */
	@Test
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
		
		final DistributionRegion level0 = new DistributionRegion(new DistributionGroupName(name), dimensions);
		return level0;
	}
	
}
