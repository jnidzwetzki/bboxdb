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
package org.bboxdb.distribution.partitioner;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.zookeeper.NodeMutationHelper;
import org.bboxdb.distribution.zookeeper.ZookeeperNodeNames;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;

public abstract class AbstractGridSpacePartitioner extends AbstractSpacePartitioner {

	@Override
	public void createRootNode(final DistributionGroupConfiguration configuration) throws BBoxDBException {
				
		// [[0.0,5.0]:[0.0,5.0]];0.5;0.5
		final String spConfig = spacePartitionerContext.getSpacePartitionerConfig();
		
		if(spConfig.isEmpty()) {
			throw new BBoxDBException("Got empty space partitioner config");
		}
		
		final String[] splitConfig = spConfig.split(";");
		
		checkConfigParameter(configuration, splitConfig);
		
		try {
			final String distributionGroup = spacePartitionerContext.getDistributionGroupName();
			
			final String rootPath = 
					distributionGroupZookeeperAdapter.getDistributionGroupRootElementPath(distributionGroup);
			
			zookeeperClient.createDirectoryStructureRecursive(rootPath);
			
			final int nameprefix = distributionGroupZookeeperAdapter
					.getNextTableIdForDistributionGroup(distributionGroup);
						
			zookeeperClient.createPersistentNode(rootPath + "/" + ZookeeperNodeNames.NAME_NAMEPREFIX, 
					Integer.toString(nameprefix).getBytes());
			
			zookeeperClient.createPersistentNode(rootPath + "/" + ZookeeperNodeNames.NAME_SYSTEMS, 
					"".getBytes());
					
			final Hyperrectangle rootBox = new Hyperrectangle(splitConfig[0]);
			distributionRegionZookeeperAdapter.setBoundingBoxForPath(rootPath, rootBox);
			
			// Create grid
			createCells(splitConfig, configuration, rootPath, rootBox);
	
			zookeeperClient.createPersistentNode(rootPath + "/" + ZookeeperNodeNames.NAME_REGION_STATE, 
					DistributionRegionState.SPLIT.getStringValue().getBytes());		
			
			NodeMutationHelper.markNodeMutationAsComplete(zookeeperClient, rootPath);
		} catch (Exception e) {
			throw new BBoxDBException(e);
		}
	}

	/**
	 * Check the config prameter
	 * @param configuration
	 * @param splitConfig
	 * @throws BBoxDBException
	 */
	protected abstract void checkConfigParameter(final DistributionGroupConfiguration configuration, 
			final String[] splitConfig) throws BBoxDBException;
	
	/**
	 * Create the grid cells
	 * @param splitConfig
	 * @param configuration
	 * @param rootPath
	 * @param rootBox
	 * @throws Exception
	 */
	protected abstract void createCells(final String[] splitConfig, 
			final DistributionGroupConfiguration configuration, 
			final String rootPath, final Hyperrectangle rootBox) throws Exception;

}
