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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRegionIdMapper {
	
	/**
	 * The name of the distribution region
	 */
	private final static String DISTRIBUTION_REGION_NAME = "abc";
	
	/**
	 * The tablename as string
	 */
	private static final String DEFAULT_TABLE_NAME = DISTRIBUTION_REGION_NAME + "_regiontable";
	
	/**
	 * The default table name as SSTableName
	 */
	private final static TupleStoreName DEFAULT_SSTABLE_NAME = new TupleStoreName(DEFAULT_TABLE_NAME);

	@BeforeClass
	public static void before() {
		DistributionGroupConfigurationCache.getInstance().clear();
		DistributionGroupConfigurationCache.getInstance().addNewConfiguration(
				DISTRIBUTION_REGION_NAME, new DistributionGroupConfiguration(2));
	}
	
	/**
	 * Test the mapping with no entries
	 */
	@Test
	public void testZeroMapping() {
		final DistributionRegionIdMapper regionIdMapper = new DistributionRegionIdMapper();
		
		Assert.assertEquals(0, regionIdMapper.getLocalTablesForRegion(
				new BoundingBox(2.5d, 2.5d, 1.5d, 1.5d), DEFAULT_SSTABLE_NAME).size());
	}
	
	/**
	 * Test the mapping with one entry
	 */
	@Test
	public void testOneMapping() {
		final DistributionRegionIdMapper regionIdMapper = new DistributionRegionIdMapper();
		regionIdMapper.addMapping(1, new BoundingBox(1d, 2d, 1d, 2d), DISTRIBUTION_REGION_NAME);
				
		Assert.assertEquals(0, regionIdMapper.getLocalTablesForRegion(
				new BoundingBox(2.5d, 2.5d, 1.5d, 1.5d), DEFAULT_SSTABLE_NAME).size());
		
		Assert.assertEquals(1, regionIdMapper.getLocalTablesForRegion(
				new BoundingBox(1.5d, 1.5d, 1.5d, 1.5d), DEFAULT_SSTABLE_NAME).size());
		
		regionIdMapper.clear();
		
		Assert.assertEquals(0, regionIdMapper.getLocalTablesForRegion(
				new BoundingBox(1.5d, 1.5d, 1.5d, 1.5d), DEFAULT_SSTABLE_NAME).size());
	}
	
	
	/**
	 * Test the mapping with two entries
	 */
	@Test
	public void testTwoMapping() {
		final DistributionRegionIdMapper regionIdMapper = new DistributionRegionIdMapper();
		regionIdMapper.addMapping(1, new BoundingBox(1d, 2d, 1d, 2d), DISTRIBUTION_REGION_NAME);
		regionIdMapper.addMapping(2, new BoundingBox(10d, 20d, 10d, 20d), DISTRIBUTION_REGION_NAME);

		Assert.assertEquals(0, regionIdMapper.getLocalTablesForRegion(
				new BoundingBox(2.5d, 2.5d, 1.5d, 1.5d), DEFAULT_SSTABLE_NAME).size());
		
		Assert.assertEquals(1, regionIdMapper.getLocalTablesForRegion(
				new BoundingBox(1.5d, 1.5d, 1.5d, 1.5d), DEFAULT_SSTABLE_NAME).size());
		
		Assert.assertEquals(2, regionIdMapper.getLocalTablesForRegion(
				new BoundingBox(1.5d, 55d, 1.5d, 55d), DEFAULT_SSTABLE_NAME).size());
		
		regionIdMapper.clear();
		
		Assert.assertEquals(0, regionIdMapper.getLocalTablesForRegion(
				new BoundingBox(1.5d, 1.5d, 1.5d, 1.5d), DEFAULT_SSTABLE_NAME).size());
	}
	
	/**
	 * Test the mapping with three entries
	 */
	@Test
	public void testThreeMapping() {
		final DistributionRegionIdMapper regionIdMapper = new DistributionRegionIdMapper();
		regionIdMapper.addMapping(1, new BoundingBox(1d, 2d, 1d, 2d), DISTRIBUTION_REGION_NAME);
		regionIdMapper.addMapping(2, new BoundingBox(10d, 20d, 10d, 20d), DISTRIBUTION_REGION_NAME);
		regionIdMapper.addMapping(3, new BoundingBox(15d, 18d, 15d, 18d), DISTRIBUTION_REGION_NAME);

		Assert.assertEquals(3, regionIdMapper.getLocalTablesForRegion(
				new BoundingBox(1.5d, 55d, 1.5d, 55d), DEFAULT_SSTABLE_NAME).size());
	}
	
	/**
	 * Test the tablename result
	 */
	@Test
	public void testGetTableNames1() {
		final DistributionRegionIdMapper regionIdMapper = new DistributionRegionIdMapper();
		regionIdMapper.addMapping(1, new BoundingBox(1d, 2d, 1d, 2d), DISTRIBUTION_REGION_NAME);
		regionIdMapper.addMapping(2, new BoundingBox(10d, 20d, 10d, 20d), DISTRIBUTION_REGION_NAME);
		regionIdMapper.addMapping(3, new BoundingBox(15d, 18d, 15d, 18d), DISTRIBUTION_REGION_NAME);

		final Collection<TupleStoreName> mappingResult = regionIdMapper.getLocalTablesForRegion(
				new BoundingBox(1.5d, 55d, 1.5d, 55d), DEFAULT_SSTABLE_NAME);
		
		Assert.assertTrue(mappingResult.contains(new TupleStoreName(DEFAULT_TABLE_NAME + "_1")));
		Assert.assertTrue(mappingResult.contains(new TupleStoreName(DEFAULT_TABLE_NAME + "_2")));
		Assert.assertTrue(mappingResult.contains(new TupleStoreName(DEFAULT_TABLE_NAME + "_3")));
		Assert.assertFalse(mappingResult.contains(new TupleStoreName(DEFAULT_TABLE_NAME + "_4")));
	}
	
	/**
	 * Test the tablename result
	 */
	@Test
	public void testGetTableNames2() {
		final DistributionRegionIdMapper regionIdMapper = new DistributionRegionIdMapper();
		regionIdMapper.addMapping(1, new BoundingBox(1d, 2d, 1d, 2d), DISTRIBUTION_REGION_NAME);
		regionIdMapper.addMapping(2, new BoundingBox(10d, 20d, 10d, 20d), DISTRIBUTION_REGION_NAME);
		regionIdMapper.addMapping(3, new BoundingBox(15d, 18d, 15d, 18d), DISTRIBUTION_REGION_NAME);

		final Collection<TupleStoreName> mappingResult = regionIdMapper.getLocalTablesForRegion(
				new BoundingBox(1.5d, 1.5d, 1.5d, 1.5d), DEFAULT_SSTABLE_NAME);
		
		Assert.assertTrue(mappingResult.contains(new TupleStoreName(DEFAULT_TABLE_NAME + "_1")));
		Assert.assertFalse(mappingResult.contains(new TupleStoreName(DEFAULT_TABLE_NAME + "_2")));
		Assert.assertFalse(mappingResult.contains(new TupleStoreName(DEFAULT_TABLE_NAME + "_3")));
		Assert.assertFalse(mappingResult.contains(new TupleStoreName(DEFAULT_TABLE_NAME + "_4")));		
	}
	
	/**
	 * Get all known mappings
	 */
	@Test
	public void testGetAll() {
		final DistributionRegionIdMapper regionIdMapper = new DistributionRegionIdMapper();
		regionIdMapper.addMapping(1, new BoundingBox(1d, 2d, 1d, 2d), DISTRIBUTION_REGION_NAME);
		regionIdMapper.addMapping(2, new BoundingBox(10d, 20d, 10d, 20d), DISTRIBUTION_REGION_NAME);
		regionIdMapper.addMapping(3, new BoundingBox(15d, 18d, 15d, 18d), DISTRIBUTION_REGION_NAME);

		final List<TupleStoreName> mappingResult = regionIdMapper.getAllLocalTables(DEFAULT_SSTABLE_NAME);
		Assert.assertEquals(3, mappingResult.size());
	}
	
	/**
	 * Wait until mapping appears
	 * @throws InterruptedException 
	 * @throws TimeoutException 
	 */
	@Test(timeout=10000)
	public void testMappingAppears1() throws TimeoutException, InterruptedException {
		final DistributionRegionIdMapper regionIdMapper = new DistributionRegionIdMapper();
		regionIdMapper.addMapping(3, new BoundingBox(15d, 18d, 15d, 18d), DISTRIBUTION_REGION_NAME);
		
		regionIdMapper.waitUntilMappingAppears(3);
	}
	
	/**
	 * Wait until mapping appears
	 * @throws InterruptedException 
	 * @throws TimeoutException 
	 */
	@Test(timeout=10000, expected=TimeoutException.class)
	public void testMappingAppears2() throws TimeoutException, InterruptedException {
		final DistributionRegionIdMapper regionIdMapper = new DistributionRegionIdMapper();
		regionIdMapper.waitUntilMappingAppears(3, 5, TimeUnit.SECONDS);
	}
	
	/**
	 * Wait until mapping appears
	 * @throws InterruptedException 
	 * @throws TimeoutException 
	 */
	@Test(timeout=15000)
	public void testMappingAppears3() throws TimeoutException, InterruptedException {
		final DistributionRegionIdMapper regionIdMapper = new DistributionRegionIdMapper();
		
		final Runnable runable = new Runnable() {
			
			@Override
			public void run() {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				regionIdMapper.addMapping(3, new BoundingBox(15d, 18d, 15d, 18d), DISTRIBUTION_REGION_NAME);
			}
		};
		
		final Thread thread = new Thread(runable);
		thread.start();
		
		regionIdMapper.waitUntilMappingAppears(3, 5, TimeUnit.SECONDS);
	}
	
	
	/**
	 * Wait until mapping disappears
	 * @throws InterruptedException 
	 * @throws TimeoutException 
	 */
	@Test(timeout=10000)
	public void testMappingDisappears1() throws TimeoutException, InterruptedException {
		final DistributionRegionIdMapper regionIdMapper = new DistributionRegionIdMapper();
		regionIdMapper.waitUntilMappingDisappears(3);
	}
	
	/**
	 * Wait until mapping disappears
	 * @throws InterruptedException 
	 * @throws TimeoutException 
	 */
	@Test(timeout=15000, expected=TimeoutException.class)
	public void testMappingDisappears2() throws TimeoutException, InterruptedException {
		final DistributionRegionIdMapper regionIdMapper = new DistributionRegionIdMapper();
		regionIdMapper.addMapping(3, new BoundingBox(15d, 18d, 15d, 18d), DISTRIBUTION_REGION_NAME);

		regionIdMapper.waitUntilMappingDisappears(3, 5, TimeUnit.SECONDS);
	}
	
	/**
	 * Wait until mapping disappears
	 * @throws InterruptedException 
	 * @throws TimeoutException 
	 */
	@Test(timeout=10000)
	public void testMappingDisappears3() throws TimeoutException, InterruptedException {
		final DistributionRegionIdMapper regionIdMapper = new DistributionRegionIdMapper();
		regionIdMapper.addMapping(3, new BoundingBox(15d, 18d, 15d, 18d), DISTRIBUTION_REGION_NAME);
		
		final Runnable runable = new Runnable() {
			
			@Override
			public void run() {
				try {
					Thread.sleep(2000);
					regionIdMapper.removeMapping(3, DISTRIBUTION_REGION_NAME);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		};
		
		final Thread thread = new Thread(runable);
		thread.start();
		
		regionIdMapper.waitUntilMappingDisappears(3, 5, TimeUnit.SECONDS);
	}
}
