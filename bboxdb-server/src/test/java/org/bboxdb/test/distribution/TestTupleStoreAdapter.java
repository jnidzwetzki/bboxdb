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

import java.util.concurrent.TimeUnit;

import org.bboxdb.distribution.zookeeper.TupleStoreAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreConfigurationBuilder;
import org.bboxdb.storage.entity.TupleStoreName;
import org.junit.Assert;
import org.junit.Test;

public class TestTupleStoreAdapter {
	
	protected final TupleStoreAdapter tupleStoreAdapter;
	
	public TestTupleStoreAdapter() {
		tupleStoreAdapter = ZookeeperClientFactory.getZookeeperClient().getTupleStoreAdapter();
	}

	/**
	 * Test the tuplestore creation and deletion
	 * @throws ZookeeperException
	 */
	@Test(timeout=60000)
	public void deleteAndCreateTable() throws ZookeeperException {
		final TupleStoreName tupleStoreName1 = new TupleStoreName("dg_table1");
		final TupleStoreName tupleStoreName2 = new TupleStoreName("dg_table2");

		tupleStoreAdapter.deleteTable(tupleStoreName1);
		tupleStoreAdapter.deleteTable(tupleStoreName2);

		Assert.assertFalse(tupleStoreAdapter.isTableKnown(tupleStoreName1));
		Assert.assertFalse(tupleStoreAdapter.isTableKnown(tupleStoreName2));

		final TupleStoreConfiguration tupleStoreConfiguration = TupleStoreConfigurationBuilder
				.create()
				.build();
		
		tupleStoreAdapter.writeTuplestoreConfiguration(tupleStoreName1, tupleStoreConfiguration);
		Assert.assertTrue(tupleStoreAdapter.isTableKnown(tupleStoreName1));
		Assert.assertFalse(tupleStoreAdapter.isTableKnown(tupleStoreName2));

		tupleStoreAdapter.writeTuplestoreConfiguration(tupleStoreName2, tupleStoreConfiguration);
		Assert.assertTrue(tupleStoreAdapter.isTableKnown(tupleStoreName1));
		Assert.assertTrue(tupleStoreAdapter.isTableKnown(tupleStoreName2));
		
		tupleStoreAdapter.deleteTable(tupleStoreName1);
		tupleStoreAdapter.deleteTable(tupleStoreName2);
		
		Assert.assertFalse(tupleStoreAdapter.isTableKnown(tupleStoreName1));
		Assert.assertFalse(tupleStoreAdapter.isTableKnown(tupleStoreName2));
	}
	
	/**
	 * Test configuration 1
	 * @throws ZookeeperException
	 */
	@Test(timeout=60000)
	public void testTupleStoreConfig1() throws ZookeeperException {
		final TupleStoreConfiguration tupleStoreConfiguration = TupleStoreConfigurationBuilder
				.create()
				.build();
		
		final TupleStoreName tupleStoreName = new TupleStoreName("dg_table1");
		tupleStoreAdapter.deleteTable(tupleStoreName);
		Assert.assertFalse(tupleStoreAdapter.isTableKnown(tupleStoreName));
		
		tupleStoreAdapter.writeTuplestoreConfiguration(tupleStoreName, tupleStoreConfiguration);
		final TupleStoreConfiguration readConfig = tupleStoreAdapter.readTuplestoreConfiguration(tupleStoreName);
		
		Assert.assertEquals(tupleStoreConfiguration, readConfig);
		Assert.assertTrue(tupleStoreAdapter.isTableKnown(tupleStoreName));
	}
	
	/**
	 * Test configuration 2
	 * @throws ZookeeperException
	 */
	@Test(timeout=60000)
	public void testTupleStoreConfig2() throws ZookeeperException {
		final TupleStoreConfiguration tupleStoreConfiguration = TupleStoreConfigurationBuilder.create()
				.allowDuplicates(true)
				.withTTL(10000, TimeUnit.MILLISECONDS)
				.withVersions(100)
				.build();
		
		final TupleStoreName tupleStoreName = new TupleStoreName("dg_table1");
		tupleStoreAdapter.deleteTable(tupleStoreName);
		Assert.assertFalse(tupleStoreAdapter.isTableKnown(tupleStoreName));
		
		tupleStoreAdapter.writeTuplestoreConfiguration(tupleStoreName, tupleStoreConfiguration);
		final TupleStoreConfiguration readConfig = tupleStoreAdapter.readTuplestoreConfiguration(tupleStoreName);
		
		Assert.assertEquals(tupleStoreConfiguration, readConfig);
		Assert.assertTrue(tupleStoreAdapter.isTableKnown(tupleStoreName));
	}
	
	/**
	 * Test configuration 3
	 * @throws ZookeeperException
	 */
	@Test(timeout=60000)
	public void testTupleStoreConfig3() throws ZookeeperException {
		final TupleStoreConfiguration tupleStoreConfiguration = TupleStoreConfigurationBuilder.create()
				.withSpatialIndexReader("reader")
				.withSpatialIndexWriter("writer")
				.build();
		
		final TupleStoreName tupleStoreName = new TupleStoreName("dg_table1");
		tupleStoreAdapter.deleteTable(tupleStoreName);
		Assert.assertFalse(tupleStoreAdapter.isTableKnown(tupleStoreName));
		
		tupleStoreAdapter.writeTuplestoreConfiguration(tupleStoreName, tupleStoreConfiguration);
		final TupleStoreConfiguration readConfig = tupleStoreAdapter.readTuplestoreConfiguration(tupleStoreName);
		
		Assert.assertEquals(tupleStoreConfiguration, readConfig);
		Assert.assertTrue(tupleStoreAdapter.isTableKnown(tupleStoreName));
	}

	@Test(timeout=60000)
	public void testDeleteDistributionGroup() throws ZookeeperException {
		final TupleStoreConfiguration tupleStoreConfiguration = TupleStoreConfigurationBuilder
				.create()
				.build();
		
		final TupleStoreName tupleStoreName1 = new TupleStoreName("dg_table1");
		final TupleStoreName tupleStoreName2 = new TupleStoreName("dg_table2");
		tupleStoreAdapter.deleteTable(tupleStoreName1);
		tupleStoreAdapter.deleteTable(tupleStoreName2);
		
		Assert.assertFalse(tupleStoreAdapter.isTableKnown(tupleStoreName1));
		Assert.assertFalse(tupleStoreAdapter.isTableKnown(tupleStoreName2));

		tupleStoreAdapter.writeTuplestoreConfiguration(tupleStoreName1, tupleStoreConfiguration);
		tupleStoreAdapter.writeTuplestoreConfiguration(tupleStoreName2, tupleStoreConfiguration);

		Assert.assertTrue(tupleStoreAdapter.isTableKnown(tupleStoreName1));
		Assert.assertTrue(tupleStoreAdapter.isTableKnown(tupleStoreName2));
	}
}
