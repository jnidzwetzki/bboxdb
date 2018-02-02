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

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.TupleStoreName;
import org.junit.Assert;
import org.junit.Test;

public class TestRegionIdMapper {
	
	/**
	 * The tablename as string
	 */
	protected static final String DEFAULT_TABLE_NAME = "region_table";
	
	/**
	 * The default table name as SSTableName
	 */
	protected final static TupleStoreName DEFAULT_SSTABLE_NAME = new TupleStoreName(DEFAULT_TABLE_NAME);

	/**
	 * Test the mapping with no entries
	 */
	@Test
	public void testZeroMapping() {
		final RegionIdMapper regionIdMapper = new RegionIdMapper();
		
		Assert.assertEquals(0, regionIdMapper.getLocalTablesForRegion(
				new BoundingBox(2.5d, 2.5d, 1.5d, 1.5d), DEFAULT_SSTABLE_NAME).size());
	}
	
	/**
	 * Test the mapping with one entry
	 */
	@Test
	public void testOneMapping() {
		final RegionIdMapper regionIdMapper = new RegionIdMapper();
		final DistributionRegion region = new DistributionRegion(DEFAULT_SSTABLE_NAME.getDistributionGroupObject(), null);
		region.setRegionId(1);
		region.setConveringBox(new BoundingBox(1d, 2d, 1d, 2d));
		regionIdMapper.addMapping(region);
				
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
		final RegionIdMapper regionIdMapper = new RegionIdMapper();
		
		final DistributionRegion region1 = new DistributionRegion(DEFAULT_SSTABLE_NAME.getDistributionGroupObject(), null);
		region1.setRegionId(1);
		region1.setConveringBox(new BoundingBox(1d, 2d, 1d, 2d));
		regionIdMapper.addMapping(region1);
		
		final DistributionRegion region2 = new DistributionRegion(DEFAULT_SSTABLE_NAME.getDistributionGroupObject(), null);
		region2.setRegionId(2);
		region2.setConveringBox(new BoundingBox(10d, 20d, 10d, 20d));
		regionIdMapper.addMapping(region2);

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
		final RegionIdMapper regionIdMapper = new RegionIdMapper();
		
		final DistributionRegion region1 = new DistributionRegion(DEFAULT_SSTABLE_NAME.getDistributionGroupObject(), null);
		region1.setRegionId(1);
		region1.setConveringBox(new BoundingBox(1d, 2d, 1d, 2d));
		regionIdMapper.addMapping(region1);
		
		final DistributionRegion region2 = new DistributionRegion(DEFAULT_SSTABLE_NAME.getDistributionGroupObject(), null);
		region2.setRegionId(2);
		region2.setConveringBox(new BoundingBox(10d, 20d, 10d, 20d));
		regionIdMapper.addMapping(region2);
		
		final DistributionRegion region3 = new DistributionRegion(DEFAULT_SSTABLE_NAME.getDistributionGroupObject(), null);
		region3.setRegionId(3);
		region3.setConveringBox(new BoundingBox(15d, 18d, 15d, 18d));
		regionIdMapper.addMapping(region3);

		Assert.assertEquals(3, regionIdMapper.getLocalTablesForRegion(
				new BoundingBox(1.5d, 55d, 1.5d, 55d), DEFAULT_SSTABLE_NAME).size());
	}
	
	/**
	 * Test the tablename result
	 */
	@Test
	public void testGetTableNames1() {
		final RegionIdMapper regionIdMapper = new RegionIdMapper();
		
		final DistributionRegion region1 = new DistributionRegion(DEFAULT_SSTABLE_NAME.getDistributionGroupObject(), null);
		region1.setRegionId(1);
		region1.setConveringBox(new BoundingBox(1d, 2d, 1d, 2d));
		regionIdMapper.addMapping(region1);
		
		final DistributionRegion region2 = new DistributionRegion(DEFAULT_SSTABLE_NAME.getDistributionGroupObject(), null);
		region2.setRegionId(2);
		region2.setConveringBox(new BoundingBox(10d, 20d, 10d, 20d));
		regionIdMapper.addMapping(region2);
		
		final DistributionRegion region3 = new DistributionRegion(DEFAULT_SSTABLE_NAME.getDistributionGroupObject(), null);
		region3.setRegionId(3);
		region3.setConveringBox(new BoundingBox(15d, 18d, 15d, 18d));
		regionIdMapper.addMapping(region3);

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
		final RegionIdMapper regionIdMapper = new RegionIdMapper();
		
		final DistributionRegion region1 = new DistributionRegion(DEFAULT_SSTABLE_NAME.getDistributionGroupObject(), null);
		region1.setRegionId(1);
		region1.setConveringBox(new BoundingBox(1d, 2d, 1d, 2d));
		regionIdMapper.addMapping(region1);
		
		final DistributionRegion region2 = new DistributionRegion(DEFAULT_SSTABLE_NAME.getDistributionGroupObject(), null);
		region2.setRegionId(2);
		region2.setConveringBox(new BoundingBox(10d, 20d, 10d, 20d));
		regionIdMapper.addMapping(region2);
		
		final DistributionRegion region3 = new DistributionRegion(DEFAULT_SSTABLE_NAME.getDistributionGroupObject(), null);
		region3.setRegionId(3);
		region3.setConveringBox(new BoundingBox(15d, 18d, 15d, 18d));
		regionIdMapper.addMapping(region3);

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
		final RegionIdMapper regionIdMapper = new RegionIdMapper();
		
		final DistributionRegion region1 = new DistributionRegion(DEFAULT_SSTABLE_NAME.getDistributionGroupObject(), null);
		region1.setRegionId(1);
		region1.setConveringBox(new BoundingBox(1d, 2d, 1d, 2d));
		regionIdMapper.addMapping(region1);
		
		final DistributionRegion region2 = new DistributionRegion(DEFAULT_SSTABLE_NAME.getDistributionGroupObject(), null);
		region2.setRegionId(2);
		region2.setConveringBox(new BoundingBox(10d, 20d, 10d, 20d));
		regionIdMapper.addMapping(region2);
		
		final DistributionRegion region3 = new DistributionRegion(DEFAULT_SSTABLE_NAME.getDistributionGroupObject(), null);
		region3.setRegionId(3);
		region3.setConveringBox(new BoundingBox(15d, 18d, 15d, 18d));
		regionIdMapper.addMapping(region3);

		final List<TupleStoreName> mappingResult = regionIdMapper.getAllLocalTables(DEFAULT_SSTABLE_NAME);
		Assert.assertEquals(3, mappingResult.size());
	}
}
