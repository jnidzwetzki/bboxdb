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

import java.io.File;
import java.io.IOException;

import org.bboxdb.distribution.DistributionGroupMetadataHelper;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.storage.entity.DistributionGroupMetadata;
import org.bboxdb.storage.sstable.SSTableHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDistributionGroupMedadata {

	/**
	 * The distribution group name for the tests
	 */
	protected final static String DGROUP_NAME = "testgroup";
	
	/**
	 * The storage directory
	 */
	private static final String STORAGE_DIRECTORY = BBoxDBConfigurationManager.getConfiguration().getStorageDirectories().get(0);

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TestDistributionGroupMedadata.class);
	
	
	/**
	 * Create the needed directory
	 */
	@BeforeClass
	public static void beforeClass() {
	    final String dirName = SSTableHelper.getDistributionGroupDir(STORAGE_DIRECTORY, DGROUP_NAME);

	    logger.info("Create: {}", dirName);
	    
	    final File dir = new File(dirName);
	    dir.mkdir();
	}
	
	/**
	 * Delete the needed directory
	 */
	@AfterClass
	public static void afterClass() {	    
	    final String dirName = SSTableHelper.getDistributionGroupDir(STORAGE_DIRECTORY, DGROUP_NAME);
	 
	    logger.info("Remove: {}", dirName);

	    final File dir = new File(dirName);
	    dir.delete();
	}
	
	/**
	 * Test reading and writing distribution group meta data
	 * @throws IOException 
	 */
	@Test(timeout=60000)
	public void testReadAndWriteDistributionGroupMetadata() throws IOException {
		final DistributionGroupMetadata distributionGroupMetadata1 = new DistributionGroupMetadata();
		final DistributionGroupMetadata distributionGroupMetadata2 = new DistributionGroupMetadata();
		distributionGroupMetadata1.setVersion(10);
		distributionGroupMetadata2.setVersion(20);	
		Assert.assertFalse(distributionGroupMetadata1.equals(distributionGroupMetadata2));

		DistributionGroupMetadataHelper.writeMedatadataForGroup(STORAGE_DIRECTORY, DGROUP_NAME, distributionGroupMetadata1);
		final DistributionGroupMetadata distributionGroupMetadata1read = 
				DistributionGroupMetadataHelper.getMedatadaForGroup(STORAGE_DIRECTORY, DGROUP_NAME);
		
		DistributionGroupMetadataHelper.writeMedatadataForGroup(STORAGE_DIRECTORY, DGROUP_NAME, distributionGroupMetadata2);
		final DistributionGroupMetadata distributionGroupMetadata2read = 
				DistributionGroupMetadataHelper.getMedatadaForGroup(STORAGE_DIRECTORY, DGROUP_NAME);
		
		Assert.assertTrue(distributionGroupMetadata1read.equals(distributionGroupMetadata1));
		Assert.assertTrue(distributionGroupMetadata2read.equals(distributionGroupMetadata2));
		
		Assert.assertEquals(distributionGroupMetadata2read.hashCode(), distributionGroupMetadata2.hashCode());
	}
	
	/**
	 * Test the to sting method
	 */
	@Test(timeout=60000)
	public void testToString() {
		final DistributionGroupMetadata distributionGroupMetadata2 = new DistributionGroupMetadata();
		Assert.assertTrue(distributionGroupMetadata2.toString().length() > 10);
	}

}
