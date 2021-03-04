/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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

import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpacePartitionerFactory {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SpacePartitionerFactory.class);

	/**
	 * Return the space partitioner for the distribution group
	 * @param mapper 
	 * @param callback 
	 * @throws ZookeeperNotFoundException 
	 */
	public static SpacePartitioner getSpacePartitionerForDistributionGroup(
			final SpacePartitionerContext spacePartitionerContext) throws ZookeeperException {

		try {
			final String distributionGroup = spacePartitionerContext.getDistributionGroupName();
			
			final DistributionGroupConfiguration config = DistributionGroupConfigurationCache
					.getInstance().getDistributionGroupConfiguration(distributionGroup);

			final String spacePartitionerString = config.getSpacePartitioner();
			
			// Instance the classname
			final Class<?> classObject = Class.forName(spacePartitionerString);

			if(classObject == null) {
				throw new ClassNotFoundException("Unable to locate class: " + spacePartitionerString);
			}

			final Object factoryObject = classObject.newInstance();

			if(! (factoryObject instanceof SpacePartitioner)) {
				throw new ClassNotFoundException(spacePartitionerString + " is not a instance of SpacePartitioner");
			}

			final SpacePartitioner spacePartitioner = (SpacePartitioner) factoryObject;   
			
			spacePartitioner.init(spacePartitionerContext);

			return spacePartitioner;

		} catch (Exception e) {
			logger.warn("Unable to instance space partitioner for group: " 
					+ spacePartitionerContext.getDistributionGroupName(), e);
			throw new RuntimeException(e);
		} 
	}
}
